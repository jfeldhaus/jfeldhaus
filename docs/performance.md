# Performance

Poor performance can ruin the value of otherwise high quality software. The studies below show how performance changes under various configurations and conditions, allowing developers to use their limited time wisely to optimize different operations.

## AWS Databases

For two PostgreSQL compatible AWS databases, the plot below depicts how transaction throughput changes as the number of threads increase for a simple OLTP workload. This [test](https://github.com/jfeldhaus/awsbench) was executed by deploying a distributed workload across docker container instances managed by the Elastic Container Service (ECS) in AWS. Performance tests like this one reveal how applications scale.

![](../images/aws-tps-postgres.png)

## Connection Pools

This plot was created using data collected during a connection pool test for an application that caches data. As the workload begins and the cache fills, performance improves for all configurations. But the default (red) configuration performs best because it is not constrained by the limits of a connection pool.

![](../images/conn-pool.png)

This plot uses the same data set as above, but instead of a time series, the data is compared using box plots which clearly show how performance varies for each configuration. These box plots have long lower tails since workload performance improved over time. Tests like this one help developers determine optimal configurations for their applications.

![](../images/conn-pool-2.png)

## Transaction Response Times

This plot shows the average transaction response times for an application with 16 workload threads. In this case query transactions perform best, as expected, followed by delete and then insert/update transaction types. This type of test helps developers determine where they should spend their time when optimizing transactions.

![](../images/vcn-response.png)

## Distributed Systems

These box plots show how a workload executing on each node of a 64 node distributed system perform over a long duration load test. The nodes are ordered from left to right based on the median TPS measurement for each node. Visualization techniques like this one help developers understand, at a high level, how the entire system performs, even with a large number of nodes.

![](../images/grid-tps.png)

------------------------------------------------------------------------
