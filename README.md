## Section 3


### 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

- It changed the speed of the `processedRowsPerSecond` speed which effected the total time for spark to process the
  data.

I modified the values in the SparkSession config and found that speed of processing was correlated with the
`processedRowsPerSecond` variable.


### 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

- `spark.streaming.kafka.maxRatePerPartition = 5` This increases the amount of data that each partition can obtain.
- `spark.default.parallelism = 3` This increases the number of cores used to carry out computation.

