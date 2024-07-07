import duckdb
import numpy as np

db = duckdb.connect(':memory:')

# initialize two arrays
s1 = np.array([4,5,2.5,1.5,6.4,5.5,7.8,9.0,7.4,2.0,3.0])
s2 = np.array([3.5,3.2,4,6.1,3.2,4.8,7.1,6.0])
s1 = np.array([4,5.0,6,2.1])
s2 = np.array([3.5,3.2,4])

# Calculate a distance matrix between the two arrays
# Calculate the squared difference between each pair of points using Cartesian join
db.sql("""
WITH seq1 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s1   
    ),
    seq2 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s2   
    )
SELECT 
        (seq1.value - seq2.value)^2 as dist
       ,seq1.s_index as index1
       ,seq2.s_index as index2
    FROM seq1
    FULL OUTER JOIN
        seq2 ON 1=1
""").show()

# That gives us a table of the distances between each pair of points in the two sequences:
# ┌─────────────────────┬────────┬────────┐
# │        dist         │ index1 │ index2 │
# │       double        │ int64  │ int64  │
# ├─────────────────────┼────────┼────────┤
# │                0.25 │      0 │      0 │
# │                2.25 │      1 │      0 │
# │                 1.0 │      2 │      0 │
# │                 4.0 │      3 │      0 │
# │   8.410000000000002 │      4 │      0 │
# │                 4.0 │      5 │      0 │
# │               18.49 │      6 │      0 │
# │               30.25 │      7 │      0 │
# │  15.210000000000003 │      8 │      0 │
# │                2.25 │      9 │      0 │
# │                  ·  │      · │      · │
# │                  ·  │      · │      · │
# │                  ·  │      · │      · │
# │                 1.0 │      1 │      7 │
# │               12.25 │      2 │      7 │
# │               20.25 │      3 │      7 │
# │ 0.16000000000000028 │      4 │      7 │
# │                0.25 │      5 │      7 │
# │  3.2399999999999993 │      6 │      7 │
# │                 9.0 │      7 │      7 │
# │   1.960000000000001 │      8 │      7 │
# │                16.0 │      9 │      7 │
# │                 9.0 │     10 │      7 │
# ├─────────────────────┴────────┴────────┤
# │ 88 rows (20 shown)          3 columns │
# └───────────────────────────────────────┘

# Use a recursive CTE to calculate the total cost of the warping path to each point
# According to: cost_i_j = dist_i_j + min(
#                                           cost_i-1_j-1, (match)
#                                           cost_i-1_j,   (insertion)
#                                           cost_i_j-1,   (deletion)
#                                         )
db.sql("""
CREATE TABLE dist_matrix as
WITH 
    seq1 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s1   
    ),
    seq2 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s2   
    ),
    dist_matrix as (
        SELECT 
            (seq1.value - seq2.value)^2 as dist
            ,seq1.s_index as index1
            ,seq2.s_index as index2
        FROM seq1
        FULL OUTER JOIN
            seq2 ON 1=1
    )
    SELECT * FROM dist_matrix
""")
db.sql("""
WITH RECURSIVE
    cost_matrix as (
        -- start at 0,0
        SELECT 
            dist
            ,index1
            ,index2
            ,dist as cost
        FROM dist_matrix
        WHERE index1 = 0 and index2 = 0
        UNION ALL 
        -- recurse through 1 of 3 paths
        SELECT 
            d.dist
            ,d.index1
            ,d.index2
            ,d.dist + sub.cost as cost
        FROM dist_matrix d
        -- LEFT JOIN to a subquery that preselects the lowest cost of the three connected cells
        LEFT JOIN (
            -- match case
             SELECT * FROM cost_matrix c1 WHERE d.index1 = c1.index1 + 1 AND d.index2 = c1.index2 + 1
             UNION ALL 
            -- insertion case
             SELECT * FROM cost_matrix c2 WHERE d.index1 = c2.index1 + 1 AND d.index2 = c2.index2
             UNION ALL 
            -- deletion case
             SELECT * FROM cost_matrix c3 WHERE d.index1 = c3.index1 AND d.index2 = c3.index2 + 1
         ) sub on 1=1
       WHERE (d.index1 > 0 OR d.index2 > 0)
         AND 
       cost is not null
    )
SELECT  
    min(cost)
    ,index1
    ,index2
    FROM cost_matrix
    GROUP BY index1, index2
""").show()

db.sql("""SELECT * FROM dist_matrix""")

db.sql("""drop table dist_matrix""")

from dtaidistance.dtw import distance_fast, distance_matrix_fast, warping_path_fast, warping_paths_fast
from dtaidistance.dtw_visualisation import plot_warpingpaths

distance_fast(s1, s2)**2

distance_matrix_fast([s1,s2])
warping_path_fast(s1,s2)
warping_paths_fast(s1,s2)[1]

plot_warpingpaths(s1,s2,warping_paths_fast(s1,s2)[1])



# full query without intermediate table:
db.sql("""
WITH RECURSIVE
    seq1 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s1   
    ),
    seq2 as (
        SELECT * as value, row_number() OVER () - 1 as s_index
        FROM s2   
    ),
    dist_matrix as (
        SELECT 
            (seq1.value - seq2.value)^2 as dist
            ,seq1.s_index as index1
            ,seq2.s_index as index2
        FROM seq1
        FULL OUTER JOIN
            seq2 ON 1=1
    ),
    cost_matrix_all_steps as (
        -- start at 0,0
        SELECT 
            dist
            ,index1
            ,index2
            ,dist as cost
            ,NULL as step
        FROM dist_matrix
        WHERE index1 = 0 and index2 = 0
        UNION ALL 
        -- recurse through 1 of 3 paths
        SELECT 
            d.dist
            ,d.index1
            ,d.index2
            ,d.dist + sub.cost as cost
            , sub.step
        FROM dist_matrix d
        -- LEFT JOIN to a subquery that preselects the lowest cost of the three connected cells
        LEFT JOIN (
            -- match case
             SELECT * EXCLUDE(step), 0 as step FROM cost_matrix_all_steps c1 WHERE d.index1 = c1.index1 + 1 AND d.index2 = c1.index2 + 1
             UNION ALL 
            -- insertion case
             SELECT * EXCLUDE(step), 1 as step FROM cost_matrix_all_steps c2 WHERE d.index1 = c2.index1 + 1 AND d.index2 = c2.index2
             UNION ALL 
            -- deletion case
             SELECT * EXCLUDE(step), 2 as step FROM cost_matrix_all_steps c3 WHERE d.index1 = c3.index1 AND d.index2 = c3.index2 + 1
         ) sub on 1=1
       WHERE (d.index1 > 0 OR d.index2 > 0)
         AND 
       cost is not null
    ),
cost_matrix as (
    SELECT  
    min(cost)
    ,index1
    ,index2
    ,arg_min(step,cost) as step
    FROM cost_matrix_all_steps
    GROUP BY index1, index2
    ),
    warping_path as (
       -- recursive function to start at top-right corner and trace back to origin
       SELECT 
          * 
       ,CASE WHEN step = 0 or step = 1 THEN index1 - 1
                ELSE index1 END
        as next_index1 
       ,CASE WHEN step = 0 or step = 2 THEN index2 - 1
                ELSE index2 END
        as next_index2
       FROM cost_matrix
          WHERE index1 = (SELECT max(s_index) FROM seq1) AND index2 = (SELECT max(s_index) FROM seq2)
       
       UNION ALL
       
       SELECT 
          c.* 
       ,CASE WHEN c.step = 0 or c.step = 1 THEN c.index1 - 1
                ELSE c.index1 END
        as next_index1 
       ,CASE WHEN c.step = 0 or c.step = 2 THEN c.index2 - 1
                ELSE c.index2 END
        as next_index2
       FROM warping_path wp
       INNER JOIN cost_matrix c ON c.index1 = wp.next_index1 AND c.index2 = wp.next_index2
       WHERE c.index1 > 0 AND c.index2 > 0
       )
    SELECT * FROM warping_path
""").show()