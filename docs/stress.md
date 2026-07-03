# Stress Testing

## Observability

The interactive application below is used to monitor the status of complex simulations of telecommunications, trading and e-commerce workloads in near real time. It provides many different views of metrics and unstructured data generated over hours, days or weeks. This allows even the most obscure problems to be identified and traced back to their origins.

![](../images/test-monitoring.jpg)

## Anomaly Detection

This graph depicts the write progression of database transaction logs during a workload simulation for a database composed of 58 compute nodes. The simulation revealed an initially subtle, but ultimately fatal problem where nodes 43 and 44 failed to purge log files which resulted in disk space exhaustion.

![](../images/grid-log-holds.png)

------------------------------------------------------------------------
