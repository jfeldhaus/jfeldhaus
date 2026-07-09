# Stress Testing

The images below are from a database stress testing framework that automates the collection of a wide range of data across multiple nodes while managing the execution of complex distributed workloads. The goal is to detect anything that could impact the correct and efficient functioning of the system under extreme conditions.

## Anomaly Detection

This graph depicts the write progression of database transaction logs during a workload simulation for a database composed of 58 compute nodes. The simulation revealed an initially subtle, but ultimately fatal problem where nodes 43 and 44 failed to purge log files which resulted in disk space exhaustion.

![](../images/grid-log-holds.png)

## Events

This graph visualizes failure event data collected during a complex stress test of a distributed system. Capturing and visualizing failure patterns can help developers design systems that isolate the effects of failing nodes from the remaining healthy nodes.

![](../images/events.png)

## Resources

Data collection of system performance and resource usage is a basic requirement for effective stress tests. Resource leaks, like the ones below (shaded areas), can be ticking time bombs in production systems.

![](../images/pages.png)

## Observability

The interactive application below is used to monitor the status of complex simulations of telecommunications, trading and e-commerce workloads in near real time. It provides many different views of metrics and unstructured data generated over hours, days or weeks. This allows even the most obscure problems to be identified and traced back to their origins.

![](../images/test-monitoring.jpg)

------------------------------------------------------------------------
