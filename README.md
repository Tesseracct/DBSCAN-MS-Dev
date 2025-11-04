## Execution

This application runs as a standard Spark job, requiring the built assembly JAR. It supports either 7 (minimum required parameters) or 10 arguments. If the last three optional boolean flags are omitted, they default to `false`.

### Required Environment

Ensure your `spark-submit` command points to the compiled application JAR (e.g., `target/dbscanms-assembly.jar`).

### Argument Order and Types

The parameters must be provided in the following order:

| **#** | **Name**             | **Type**    | **Description**                                                                                                                                                                                              | **Required / Optional** |
|:-----:|:---------------------|:------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------:|
|   1   | `filepath`           | **String**  | Path to the input data file.                                                                                                                                                                                 |        Required         |
|   2   | `epsilon`            | **Float**   | DBSCAN neighborhood radius ($\epsilon$).                                                                                                                                                                     |        Required         |
|   3   | `minPts`             | **Int**     | Minimum number of points required to form a dense region ($MinPts$).                                                                                                                                         |        Required         |
|   4   | `numberOfPivots`     | **Int**     | Number of pivot points to use for the distributed partitioning scheme.                                                                                                                                       |        Required         |
|   5   | `numberOfPartitions` | **Int**     | The target number of Spark partitions.                                                                                                                                                                       |        Required         |
|   6   | `samplingDensity`    | **Float**   | Density of random sampling used to select pivots (0.0 < density $\leq$ 1.0).                                                                                                                                 |        Required         |
|   7   | `seed`               | **Int**     | Random seed for reproducible sampling and partitioning.                                                                                                                                                      |        Required         |
|   8   | `dataHasHeader`      | **Boolean** | If `true`, skips the first row of the input file. **(Default: `false`)**                                                                                                                                     |        Optional         |
|   9   | `dataHasRightLabel`  | **Boolean** | If `true`, treats the last column as a ground truth label for evaluation/output. **(Default: `false`)**                                                                                                      |        Optional         |
|  10   | `collectResult`      | **Boolean** | If `true`, collects results to the driver and prints them to console. **WARNING: Only use this for small test datasets.** If `false`, results are written to disk (default behavior). **(Default: `false`)** |        Optional         |

### Usage Example

**Full Command (10 Arguments):**
This example uses all optional arguments, setting `dataHasHeader` and `collectResults` to `true`.

```
spark-submit --class app.Main target/dbscanms-assembly.jar data/input.csv 0.5 5 10 8 0.01 42 true false true
```

**Minimal Command (7 Arguments):**
This example omits the optional arguments, defaulting them to `false`. The job will write results to disk.

```
spark-submit --class app.Main target/dbscanms-assembly.jar data/input.csv 0.5 5 10 8 0.01 42
```