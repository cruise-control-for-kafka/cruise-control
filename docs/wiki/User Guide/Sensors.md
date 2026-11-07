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

| DESCRIPTION                                                                                          | MBEAN NAME                                                                                     |
|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------|
| The number of replica action in progress                                                             | kafka.cruisecontrol:name=Executor.replica-action-in-progress                                   |
| The number of leadership action in progress                                                          | kafka.cruisecontrol:name=Executor.leadership-action-in-progress                                |
| The number of intra-broker replica action in progress                                                | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-in-progress                      |
| The number of replica action pending                                                                 | kafka.cruisecontrol:name=Executor.replica-action-pending                                       |
| The number of leadership action pending                                                              | kafka.cruisecontrol:name=Executor.leadership-action-pending                                    |
| The number of intra-broker replica action pending                                                    | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-pending                          |
| The number of replica action aborting                                                                | kafka.cruisecontrol:name=Executor.replica-action-aborting                                      |
| The number of leadership action aborting                                                             | kafka.cruisecontrol:name=Executor.leadership-action-aborting                                   |
| The number of intra-broker replica action aborting                                                   | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-aborting                         |
| The number of replica action aborted                                                                 | kafka.cruisecontrol:name=Executor.replica-action-aborted                                       |
| The number of leadership action aborted                                                              | kafka.cruisecontrol:name=Executor.leadership-action-aborted                                    |
| The number of intra-broker replica action aborted                                                    | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-aborted                          |
| The number of replica action dead                                                                    | kafka.cruisecontrol:name=Executor.replica-action-dead                                          |
| The number of leadership action dead                                                                 | kafka.cruisecontrol:name=Executor.leadership-action-dead                                       |
| The number of intra-broker replica action dead                                                       | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-dead                             |
| The number of replica action completed                                                               | kafka.cruisecontrol:name=Executor.replica-action-completed                                     |
| The number of leadership action completed                                                            | kafka.cruisecontrol:name=Executor.leadership-action-completed                                  |
| The number of intra-broker replica action completed                                                  | kafka.cruisecontrol:name=Executor.intra-broker-replica-action-completed                        |
| Has an ongoing execution in kafka_assigner mode                                                      | kafka.cruisecontrol:name=Executor.ongoing-execution-kafka_assigner                             |
| Has an ongoing execution in non-kafka_assigner mode                                                  | kafka.cruisecontrol:name=Executor.ongoing-execution-non_kafka_assigner                         |
| The duration of the ongoing execution in ms (0 if there is no ongoing execution)                     | kafka.cruisecontrol:name=Executor.ongoing-execution-duration-ms                                |
| The inter-broker data to move in the ongoing execution in MB                                         | kafka.cruisecontrol:name=Executor.ongoing-execution-inter-broker-data-movement-total-MB        |
| The finished (completed, aborted or dead) inter-broker data movement of the ongoing execution in MB  | kafka.cruisecontrol:name=Executor.ongoing-execution-inter-broker-data-movement-finished-MB     |
| The completed inter-broker data movement of the ongoing execution in MB (excludes aborted and dead)  | kafka.cruisecontrol:name=Executor.ongoing-execution-inter-broker-data-movement-completed-MB    |
| The in-execution (in progress or aborting) inter-broker data movement of the ongoing execution in MB | kafka.cruisecontrol:name=Executor.ongoing-execution-inter-broker-data-movement-in-execution-MB |
| The remaining inter-broker data to move in the ongoing execution in MB (0 once a stop is requested)  | kafka.cruisecontrol:name=Executor.ongoing-execution-inter-broker-data-movement-remaining-MB    |
| The intra-broker data to move in the ongoing execution in MB                                         | kafka.cruisecontrol:name=Executor.ongoing-execution-intra-broker-data-movement-total-MB        |
| The finished (completed, aborted or dead) intra-broker data movement of the ongoing execution in MB  | kafka.cruisecontrol:name=Executor.ongoing-execution-intra-broker-data-movement-finished-MB     |
| The completed intra-broker data movement of the ongoing execution in MB (excludes aborted and dead)  | kafka.cruisecontrol:name=Executor.ongoing-execution-intra-broker-data-movement-completed-MB    |
| The in-execution (in progress or aborting) intra-broker data movement of the ongoing execution in MB | kafka.cruisecontrol:name=Executor.ongoing-execution-intra-broker-data-movement-in-execution-MB |
| The remaining intra-broker data to move in the ongoing execution in MB (0 once a stop is requested)  | kafka.cruisecontrol:name=Executor.ongoing-execution-intra-broker-data-movement-remaining-MB    |
| The number of (all) execution stopped                                                                | kafka.cruisecontrol:name=Executor.execution-stopped                                            |
| The number of execution stopped by user                                                              | kafka.cruisecontrol:name=Executor.execution-stopped-by-user                                    |
| The number of execution started in kafka_assigner mode                                               | kafka.cruisecontrol:name=Executor.execution-started-kafka_assigner                             |
| The number of execution started in non-kafka_assigner mode                                           | kafka.cruisecontrol:name=Executor.execution-started-non-kafka_assigner                         |
| Per broker cap on inter-broker partition movements (deprecated)                                      | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-per-broker-cap              |
| Per broker cap on intra-broker partition movements (deprecated)                                      | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-per-broker-cap              |
| Global cap on leadership movements                                                                   | kafka.cruisecontrol:name=Executor.leadership-movements-global-cap                              |
| The time taken to execute proposals involving broker removal                                         | kafka.cruisecontrol:name=Executor.proposal_execution-time-involve-broker-removal               |
| The time taken to execute proposals involving broker demotion                                        | kafka.cruisecontrol:name=Executor.proposal-execution-time-involve-broker-demotion-only         |
| The time taken to execute proposals                                                                  | kafka.cruisecontrol:name=Executor.proposal-execution-time                                      |
| The rate of completed inter-broker partition movements                                               | kafka.cruisecontrol:name=Executor.inter-broker-partition-movement-rate                         |
| The rate of completed intra-broker partition movements                                               | kafka.cruisecontrol:name=Executor.intra-broker-partition-movement-rate                         |
| The rate of completed leadership movements                                                           | kafka.cruisecontrol:name=Executor.leadership-movement-rate                                     |
| The rate of finished partition data movement in MB                                                   | kafka.cruisecontrol:name=Executor.partition-data-movement-rate-MB                              |
| The average number of finished partition movements per second in the latest replica movement phase   | kafka.cruisecontrol:name=Executor.partition-movement-count-per-second                          |
| The average finished partition data movement in MB per second in the latest replica movement phase   | kafka.cruisecontrol:name=Executor.partition-movement-MB-per-second                             |
| Max inter-broker partition movement concurrency of all brokers                                       | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-max-concurrency             |
| Min inter-broker partition movement concurrency of all brokers                                       | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-min-concurrency             |
| Average inter-broker partition movement concurrency of all brokers                                   | kafka.cruisecontrol:name=Executor.inter-broker-partition-movements-avg-concurrency             |
| Max intra-broker partition movement concurrency of all brokers                                       | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-max-concurrency             |
| Min intra-broker partition movement concurrency of all brokers                                       | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-min-concurrency             |
| Average intra-broker partition movement concurrency of all brokers                                   | kafka.cruisecontrol:name=Executor.intra-broker-partition-movements-avg-concurrency             |
| Max leadership movement concurrency of all brokers                                                   | kafka.cruisecontrol:name=Executor.leadership-movements-max-concurrency                         |
| Min leadership movement concurrency of all brokers                                                   | kafka.cruisecontrol:name=Executor.leadership-movements-min-concurrency                         |
| Average leadership movement concurrency of all brokers                                               | kafka.cruisecontrol:name=Executor.leadership-movements-avg-concurrency                         |

The `ongoing-execution-*-data-movement-*-MB` and `ongoing-execution-duration-ms` sensors describe the ongoing execution and are
reset to 0 once the execution finishes. They can be combined to monitor the progress of an execution: for example, the progress of
inter-broker replica movements is `completed-MB / total-MB`, and its remaining time can be estimated as
`(remaining-MB + in-execution-MB) / partition-movement-MB-per-second`. `finished-MB - completed-MB` is the data of the aborted and dead
tasks, e.g. the in-progress movements that were rolled back when the execution was stopped.


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
| The number of self healing failed to start                                                                        | kafka.cruisecontrol:name=AnomalyDetector.number-of-self-healing-failed-to-start                        |
| The ongoing anomaly duration in ms                                                                                | kafka.cruisecontrol:name=AnomalyDetector.ongoing-anomaly-duration-ms                                   |
| Whether broker failure self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.broker_failure-self-healing-enabled                           |
| Whether goal violation self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.goal_violation-self-healing-enabled                           |
| Whether disk failure self-healing is enabled or not                                                               | kafka.cruisecontrol:name=AnomalyDetector.disk_failure-self-healing-enabled                             |
| Whether metric anomaly self-healing is enabled or not                                                             | kafka.cruisecontrol:name=AnomalyDetector.metric_anomaly-self-healing-enabled                           |
| Whether topic anomaly self-healing is enabled or not                                                              | kafka.cruisecontrol:name=AnomalyDetector.topic_anomaly-self-healing-enabled                            |
| Whether maintenance event self-healing is enabled or not                                                          | kafka.cruisecontrol:name=AnomalyDetector.maintenance_event-self-healing-enabled                        |
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
| Maintenance event rate                                                                                            | kafka.cruisecontrol:name=AnomalyDetector.maintenance-event-rate                                        |
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
| The number of self-healing started for broker failures                                                            | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-broker_failure                |
| The number of self-healing started for maintenance events                                                         | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-maintenance_event             |
| The number of self-healing started for disk failures                                                              | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-disk_failure                  |
| The number of self-healing started for metric anomalies                                                           | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-metric_anomaly                |
| The number of self-healing started for goal violations                                                            | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-goal_violation                |
| The number of self-healing started for topic anomalies                                                            | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-started-for-topic_anomaly                 |
| The number of self-healing ended (successfully or with an error) for broker failures                              | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-broker_failure                  |
| The number of self-healing ended (successfully or with an error) for maintenance events                           | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-maintenance_event               |
| The number of self-healing ended (successfully or with an error) for disk failures                                | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-disk_failure                    |
| The number of self-healing ended (successfully or with an error) for metric anomalies                             | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-metric_anomaly                  |
| The number of self-healing ended (successfully or with an error) for goal violations                              | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-goal_violation                  |
| The number of self-healing ended (successfully or with an error) for topic anomalies                              | kafka.cruisecontrol:name=AnomalyDetector.num-of-self-healing-ended-for-topic_anomaly                   |
| The duration of the ongoing self-healing for broker failures in ms (0 if there is none)                           | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-broker_failure           |
| The duration of the ongoing self-healing for maintenance events in ms (0 if there is none)                        | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-maintenance_event        |
| The duration of the ongoing self-healing for disk failures in ms (0 if there is none)                             | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-disk_failure             |
| The duration of the ongoing self-healing for metric anomalies in ms (0 if there is none)                          | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-metric_anomaly           |
| The duration of the ongoing self-healing for goal violations in ms (0 if there is none)                           | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-goal_violation           |
| The duration of the ongoing self-healing for topic anomalies in ms (0 if there is none)                           | kafka.cruisecontrol:name=AnomalyDetector.ongoing-self-healing-duration-ms-for-topic_anomaly            |
| The rate at which automated rightsizing with actions are taken                                                    | kafka.cruisecontrol:name=AnomalyDetector.automated-rightsizing-rate                                    |

A self-healing of an anomaly type is in progress while `num-of-self-healing-started-for-<type>` is greater than `num-of-self-healing-ended-for-<type>`,
which is also when `ongoing-self-healing-duration-ms-for-<type>` is non-zero. The number of self-healing that completed successfully is the
count of `<type>-detect-to-fix-complete-timer`; the remaining ended self-healing completed with an error (e.g. a stopped or failed execution).

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
You can easily expose Cruise Control metrics by [enabling](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/kafka-cruise-control-start.sh#L83) Java Management Extensions (JMX), and collect them by using your favourite JMX collection tool (e.g - [jmx_exporter](https://github.com/prometheus/jmx_exporter)).
