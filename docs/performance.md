# Performance

Performance tests are often conducted in very controlled, steady state conditions. That's important when making comparisons. But it is also revealing to measure how performance changes over time when systems vary dynamically.

### AWS Databases

------------------------------------------------------------------------

For two PostgreSQL compatable AWS databases, the plot below measures how throughput changes as the number of workload threads increase for a simple OLTP [workload](../code/TptbmAws.java). This result is interesting because the serverless Aurora database, based on the AWS documentation, was expected to out perform the RDS version of PostgreSQL.

![](../images/aws-tps-postgres.png)

### Connection Pools

------------------------------------------------------------------------

This plot was created using data collected during a connection pool test for an application that caches data. As the workload begins and the cache fills, performance improves for all configurations. But the default (red) configuration performs best because it is not constrained by the limits of a connection pool.

![](../images/conn-pool.png)

This plot uses the same data set as above, but instead of a time series, the data is compared using box plots which clearly show how performance varies for each configuration. These box plots have long lower tails since workload performance improved over time.

![](../images/conn-pool-2.png)

### Transaction Response Performance

------------------------------------------------------------------------

This plot shows the average transaction response times for an application with 16 workload threads. In this case query transactions (blue) perform best followed by delete (red) and then insert/update (green) transaction types.

![](../images/vcn-response.png)

### Distributed Database Performance

------------------------------------------------------------------------

These box plots show how a workload executing on each node (instance) of a distributed system perform over a long duration load test. The nodes are ordered from left to right based on the median TPS measurement for each node.

![](../images/grid-tps.png)

------------------------------------------------------------------------
