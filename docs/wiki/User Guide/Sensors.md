# Cruise Control Metrics
Cruise Control uses [dropwizzard](http://www.dropwizard.io) metrics to report its own status. 

Cruise Control metrics are useful to monitor the state of Cruise Control itself. There are metrics for the following sensor types:

* Executor
* LoadMonitor
* UserTaskManager
* AnomalyDetector
* GoalOptimizer
* MetricFetcherManager
* KafkaCruiseControlServlet

### Executor Sensors

| DESCRIPTION                                                          | MBEAN NAME                                        						                                             |
|----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| The number of replica action in progress                             | kafka.cruisecontrol:name=Executor.replica-action-in-progress                                              |
| The number of leadership action in progress                          | kafka.cruisecontrol:name=Executor.leadership-action-in-progress                                           |
| The number of replica action pending                                 | kafka.cruisecontrol:name=Executor.replica-action-pending                                                  |
| The number of leadership action pending                              | kafka.cruisecontrol:name=Executor.leadership-action-pending                                               |
| The number of replica action aborting                                | kafka.cruisecontrol:name=Executor.replica-action-aborting                                                 |
| The number of leadership action aborting                             | kafka.cruisecontrol:name=Executor.leadership-action-aborting                                              |
| The number of replica action aborted                                 | kafka.cruisecontrol:name=Executor.replica-action-aborted                                                  |
| The number of leadership action aborted                              | kafka.cruisecontrol:name=Executor.leadership-action-aborted                                               |
| The number of replica action dead                                    | kafka.cruisecontrol:name=Executor.replica-action-dead                                                     |
| The number of leadership action dead                                 | kafka.cruisecontrol:name=Executor.leadership-action-dead                                                  |
| Has an ongoing execution in kafka_assigner mode                      | kafka.cruisecontrol:name=Executor.ongoing-execution-kafka_assigner                                        |
| Inter-broker total bytes to reassigned for the ongoing execution     | kafka.cruisecontrol:name=Executor.ongoing-execution-inter_broker_data_movement_total_bytes                |
| Inter-broker reassigned bytes for the ongoing execution              | kafka.cruisecontrol:name=Executor.ongoing-execution-inter_broker_data_movement_completed_bytes            |
| Inter-broker in-flight reassignement bytes for the ongoing execution | kafka.cruisecontrol:name=Executor.ongoing-execution-inter_broker_data_movement_moving_bytes               |
| Inter-broker remaining bytes to reassign for the ongoing execution   | kafka.cruisecontrol:name=Executor.ongoing-execution-inter_broker_data_movement_remaining_bytes            |
| Average leadership movement concurrency of all brokers               | kafka.cruisecontrol:name=ongoing-execution-inter_broker_data_movement_total_bytes                         |
| The number of (all) execution stopped                                | kafka.cruisecontrol:name=Executor.execution-stopped                                                       |
| The number of execution stopped by user                              | kafka.cruisecontrol:name=Executor.execution-stopped-by-user                                               |
| The number of execution started in kafka_assigner mode               | kafka.cruisecontrol:name=Executor.execution-started-kafka_assigner                                        |
| The number of execution started in non-kafka_assigner mode           | kafka.cruisecontrol:name=Executor.execution-started-non-kafka_assigner                                    |
| Per broker cap on inter-broker partition movements (deprecated)      | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-per-broker-cap                         |
| Per broker cap on intra-broker partition movements (deprecated)      | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-per-broker-cap                         |
| Global cap on leadership movements                                   | kafka.cruisecontrol:name=Executor.leadership-movements-global-cap                                         |
| The time taken to exectute proposals involving broker removal        | kafka.cruisecontrol:name=Executor.proposal_execution-time-involve-broker-removal                          |
| The time taken to exectute proposals involving broker demotion       | kafka.cruisecontrol:name=Executor.proposal-execution-time-involve-broker-demotion-only                    |
| The time taken to exectute proposals                                 | kafka.cruisecontrol:name=Executor.proposal-execution-time                                                 |
| Max inter-broker partition movement concurrency of all brokers       | kafka.cruisecontrol:name=inter-broker-partition-movements-max-concurrency                                 |
| Min inter-broker partition movement concurrency of all brokers       | kafka.cruisecontrol:name=inter-broker-partition-movements-min-concurrency                                 |
| Average inter-broker partition movement concurrency of all brokers   | kafka.cruisecontrol:name=inter-broker-partition-movements-avg-concurrency                                 |
| Max intra-broker partition movement concurrency of all brokers       | kafka.cruisecontrol:name=intra-broker-partition-movements-max-concurrency                                 |
| Min intra-broker partition movement concurrency of all brokers       | kafka.cruisecontrol:name=intra-broker-partition-movements-min-concurrency                                 |
| Average intra-broker partition movement concurrency of all brokers   | kafka.cruisecontrol:name=intra-broker-partition-movements-avg-concurrency                                 |
| Max leadership movement concurrency of all brokers                   | kafka.cruisecontrol:name=leadership-movements-max-concurrency                                             |
| Min leadership movement concurrency of all brokers                   | kafka.cruisecontrol:name=leadership-movements-min-concurrency                                             |
| Average leadership movement concurrency of all brokers               | kafka.cruisecontrol:name=leadership-movements-avg-concurrency                                             |



### LoadMonitor Sensors

| DESCRIPTION                                                                                                   | MBEAN NAME                                        						                              |
|---------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| The number of monitored windows                                                                               | kafka.cruisecontrol:name=LoadMonitor.total-monitored-windows                          |
| The number of partitions that is valid but require extrapolations                                             | kafka.cruisecontrol:name=LoadMonitor.num-partitions-with-extrapolations               |
| The number of topics                                                                                          | kafka.cruisecontrol:name=LoadMonitor.num-topics                                       |
| The metadata factor, which corresponds to (number of replicas) * (number of brokers with replicas) ^ exponent | kafka.cruisecontrol:name=LoadMonitor.metadata-factor                                  |
| The number of valid windows                                                                                   | kafka.cruisecontrol:name=LoadMonitor.valid-windows                                    |
| The monitored partition percentage                                                                            | kafka.cruisecontrol:name=LoadMonitor.monitored-partitions-percentage                  |
| Cluster model creation time in ms                                                                             | kafka.cruisecontrol:name=LoadMonitor.cluster-model-creation-timer                     |
| The cluster has partitions with ISR > replicas (0: No such partitions, 1: Has such partitions)                | kafka.cruisecontrol:name=LoadMonitor.has-partitions-with-isr-greater-than-replicas    |
| The number of dead brokers which host some replicas in the cluster                                            | kafka.cruisecontrol:name=LoadMonitor.dead-brokers-with-replicas                       |
| The number of brokers in the cluster that has at least one replica assigned                                   | kafka.cruisecontrol:name=LoadMonitor.brokers-with-replicas                            |
| The number of brokers in the cluster that has at least one offline replica                                    | kafka.cruisecontrol:name=LoadMonitor.brokers-with-offline-replicas                    |

### UserTaskManager Sensors

| DESCRIPTION                       | MBEAN NAME                                        				|
|-----------------------------------|-------------------------------------------------------------------|
| The number of active sessions     | kafka.cruisecontrol:name=UserTaskManager.num-active-sessions      |
| The number of active user tasks   | kafka.cruisecontrol:name=UserTaskManager.num-active-user-tasks    |

### AnomalyDetector Sensors

| DESCRIPTION                                                                                                       | MBEAN NAME                                        				                                                 |
|-------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| The number of self healing started                                                                                | kafka.cruisecontrol:name=AnomalyDetector.number-of-self-healing-started                                |
| The ongoing anomaly duration in ms                                                                                | kafka.cruisecontrol:name=AnomalyDetector.ongoing-anomaly-duration-ms                                   |
| Whether broker failure self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.broker_failure-self-healing-enabled                           |
| Whether goal violation self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.goal_violation-self-healing-enabled                           |
| Whether disk failure self-healing is enabled or not                                                               | kafka.cruisecontrol:name=AnomalyDetector.disk_failure-self-healing-enabled                             |
| Whether metric anomaly self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.metric_anomaly-self-healing-enabled                           |
| Whether topic anomaly self-healing is enabled or not                                                              | kafka.cruisecontrol:name=AnomalyDetector.topic_anomaly-self-healing-enabled                            |
| Whether goal violation detector identified goals that require human intervention (e.g. cluster expansion)         | kafka.cruisecontrol:name=AnomalyDetector.GOAL_VIOLATION-has-unfixable-goals                            |
| Balancedness score (100 = fully-balanced, 0 = fully-unbalanced, -1 = has dead-brokers / disks in cluster)         | kafka.cruisecontrol:name=AnomalyDetector.balancedness-score                                            |
| Whether the cluster is under-provisioned or not                                                                   | kafka.cruisecontrol:name=AnomalyDetector.under-provisioned                                             |
| Whether the cluster is over-provisioned or not                                                                    | kafka.cruisecontrol:name=AnomalyDetector.over-provisioned                                              |
| Whether the cluster is right-sized or not                                                                         | kafka.cruisecontrol:name=AnomalyDetector.right-sized                                                   |
| Mean time to start a self-healing fix                                                                             | kafka.cruisecontrol:name=AnomalyDetector.mean-time-to-start-fix-ms                                     |
| Broker failure rate                                                                                               | kafka.cruisecontrol:name=AnomalyDetector.broker-failure-rate                                           |
| Goal violation rate                                                                                               | kafka.cruisecontrol:name=AnomalyDetector.goal-violation-rate                                           |
| Metric anomaly rate                                                                                               | kafka.cruisecontrol:name=AnomalyDetector.metric-anomaly-rate                                           |
| Disk failure rate                                                                                                 | kafka.cruisecontrol:name=AnomalyDetector.disk-failure-rate                                             |
| Topic anomaly rate                                                                                                | kafka.cruisecontrol:name=AnomalyDetector.topic-anomaly-rate                                            |
| The number of brokers that are metric anomaly suspects, pending more evidence to conclude either way              | kafka.cruisecontrol:name=AnomalyDetector.num-suspect-metric-anomalies                                  |
| The number of brokers that have recently been identified with a metric anomaly                                    | kafka.cruisecontrol:name=AnomalyDetector.num-recent-metric-anomalies                                   |
| The number of brokers that continue to be identified with a metric anomaly for a prolonged period                 | kafka.cruisecontrol:name=AnomalyDetector.num-persistent-metric-anomalies                               |
| The cluster has partitions with RF > the number of eligible racks (0: No such partitions, 1: Has such partitions) | kafka.cruisecontrol:name=AnomalyDetector.has-partitions-with-replication-factor-greater-than-num-racks |
| The time taken by goal violation detection                                                                        | kafka.cruisecontrol:name=AnomalyDetector.goal-violation-detection-timer                                |
| The time taken to generate a fix for self-healing for broker failures                                             | kafka.cruisecontrol:name=AnomalyDetector.broker_failure-self-healing-fix-generation-timer              |
| The time taken to generate a fix for self-healing for maintenance events                                          | kafka.cruisecontrol:name=AnomalyDetector.maintenance_event-self-healing-fix-generation-timer           |
| The time taken to generate a fix for self-healing for disk failures                                               | kafka.cruisecontrol:name=AnomalyDetector.disk_failure-self-healing-fix-generation-timer                |
| The time taken to generate a fix for self-healing for metric anomalies                                            | kafka.cruisecontrol:name=AnomalyDetector.metric_anomaly-self-healing-fix-generation-timer              |
| The time taken to generate a fix for self-healing for goal violations                                             | kafka.cruisecontrol:name=AnomalyDetector.goal_violation-self-healing-fix-generation-timer              |
| The time taken to generate a fix for self-healing for topic anomalies                                             | kafka.cruisecontrol:name=AnomalyDetector.topic_anomaly-self-healing-fix-generation-timer               |
| The time from being detected to self-healing completed for broker failures                                        | kafka.cruisecontrol:name=AnomalyDetector.broker_failure-detect-to-fix-complete-timer                   |
| The time from being detected to self-healing completed for maintenance events                                     | kafka.cruisecontrol:name=AnomalyDetector.maintenance_event-detect-to-fix-complete-timer                |
| The time from being detected to self-healing completed for disk failures                                          | kafka.cruisecontrol:name=AnomalyDetector.disk_failure-detect-to-fix-complete-timer                     |
| The time from being detected to self-healing completed for metric anomalies                                       | kafka.cruisecontrol:name=AnomalyDetector.metric_anomaly-detect-to-fix-complete-timer                   |
| The time from being detected to self-healing completed for goal violations                                        | kafka.cruisecontrol:name=AnomalyDetector.goal_violation-detect-to-fix-complete-timer                   |
| The time from being detected to self-healing completed for topic anomalies                                        | kafka.cruisecontrol:name=AnomalyDetector.topic_anomaly-detect-to-fix-complete-timer                    |
| The rate at which automated rightsizing with actions are taken                                                    | kafka.cruisecontrol:name=AnomalyDetector.automated-rightsizing-rate                                    |

### GoalOptimizer Sensors

| DESCRIPTION                       | MBEAN NAME                                                        |
|-----------------------------------|-------------------------------------------------------------------|
| Proposal computation time in ms   | kafka.cruisecontrol:name=GoalOptimizer.proposal-computation-timer |


### MetricFetcherManager Sensors

| DESCRIPTION                                               | MBEAN NAME                                                                           |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------|
| The rate of partition metric sample fetch failures        | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-failure-rate |
| The time taken by each round of partition sample fetch    | kafka.cruisecontrol:name=MetricFetcherManager.partition-samples-fetcher-timer        |
| The rate of training sample fetch failures                | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-failure-rate  |
| The time taken by each training sample fetch              | kafka.cruisecontrol:name=MetricFetcherManager.training-samples-fetcher-timer         |

### KafkaCruiseControlServlet Sensors

| DESCRIPTION                                                   | MBEAN NAME                                                                                            |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Service time of a successful request in ms for each endpoint  | kafka.cruisecontrol:name=KafkaCruiseControlServlet.<endpoint-name>-successful-request-execution-timer |
| Request rate for each endpoint                                | kafka.cruisecontrol:name=KafkaCruiseControlServlet.<endpoint-name>-request-rate                       |
---
# Metrics Collection
You can easily expose Cruise Control metrics by [enabling](https://github.com/linkedin/cruise-control/blob/migrate_to_kafka_2_4/kafka-cruise-control-start.sh#L64) Java Management Extensions (JMX), and collect them by using your favourite JMX collection tool (e.g - [jmx_exporter](https://github.com/prometheus/jmx_exporter)).
