# duckdb-dtw
Implementation of Dynamic Time Warping (DTW) in SQL.

## Motivation

I recently came across [this article](https://medium.com/trusted-data-science-haleon/fastdtw-in-action-optimizing-manufacturing-operations-c07f3cc5023c) discussing applicability of DTW to manufacturing operations. (Hat tip to [Evidently.ai's ML system design case studies database](https://www.evidentlyai.com/ml-system-design) for showcasing it). The applicability of the technique should be wide-ranging: identifying motifs or particular operations within time sequences, aligning phases of a sequense to a reference example, classifying sequences based on nearest neighbors comparisons, and many more.

However, I was surprised to find that there's no examples available of implementing DTW in pure SQL. Sure, you can use [one](https://pypi.org/project/dtaidistance/) [of](https://tslearn.readthedocs.io/en/stable/index.html) [the](https://pyts.readthedocs.io/en/stable/index.html) [existing](https://pypi.org/project/dtw-python/) [libraries](https://pypi.org/project/fastdtw/) that implements DTW as a UDF, or external call, but where is the fun in that? 

The algorithm, using dynamic programming, should be replicable with a combination of recursive CTEs and window functions. While these capabilities are available in many SQL engines, I'm going to focus development on DuckDB, since it tends to have high performance for analytical queries - and the eventual usage of a DTW algorithm would be as one step within an analytical pipeline.

## Approach

A good basic walkthrough of the DTW algorithm is provided in [this series of videos](https://www.youtube.com/playlist?list=PLmZlBIcArwhMJoGk5zpiRlkaHUqy5dLzL). I am interested in not only obtaining the distance, but the warping path as well, and the explanation provided there carries all the way through to a good implementation of tracking the path. 

Adapted from that source, but restated a bit to be more SQL-friendly, the algorithm is as follows:

* Initialize a distance matrix
    * Cartesian join of the two sequences should do the trick
* Calculate the cost matrix to traverse from `0,0` to `N,M`
  * Recursion over steps between cells from 'lower-left' to 'upper-right'
  * Record the traceback steps: `(0,1,2)` for `(match, insertion, deletion)`, respectively
* Record the path taken to trace back from `N,M` to `0,0`
  * Recurse over the recorded traceback steps
 
## Planning

- [x] Mock two test sequences
- [x] For the test sequences, compute reference distance matrix, distance, and warp path using the `dtaidistance` Python package
- [x] Calculate point-wise distance matrix
- [x] Calculate full distance matrix and validate with reference
- [ ] Calculate warp path and validate with reference
- [ ] Add features:
  - [ ] Window (e.g. Sakoe-Chiba band)
  - [ ] Other early-stopping approaches
  - [ ] Z-normalize pre-processing step
  - [ ] penalty
  - [ ] psi-relaxation
  - [ ] Different distance functions
  - [ ] Multi-dimensional series
- [ ] Adapt for comparing multiple sequences
  - [ ] One vs. many
  - [ ] Many vs. many
- [ ] Make DuckDB extension???
