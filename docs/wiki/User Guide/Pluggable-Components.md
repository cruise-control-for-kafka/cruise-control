## Metric Sampler
The metric sampler is one of the most important plugins in Cruise Control, it allows user to easily deploy Cruise Control to various environment and work with the existing metric system.

The default implementation of metric sampler is reading the broker metrics produced by `CruiseControlMetricsReporter` on the broker. This is assuming that users are running Kafka brokers by setting the `metric.reporters` configuration on the Kafka brokers to be `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`(see [quick start](https://github.com/cruise-control-for-kafka/cruise-control#quick-start) on how to do that).

## Metric Sampler Partition Assignor
When users have multiple metric sampler threads, the metric sampler partition assignor is responsible for assigning partitions to the metric samplers.
This is useful when users have some existing metric system. The default implementation assigns all the partitions of the same topic to the same metric sampler.

## Sample Store
The Sample Store is used to store the collected metric samples and training samples to external storage. One problem in metric sampling is that we are using some derived data from the raw metrics. And the way we derive the data relies on the metadata of the cluster at that moment. So when we look at the old metrics, if we do not know the metadata at the point the metric was collected the derived data would be inaccurate. Sample Store helps solve this problem by storing the derived data directly to an external storage for later loading.

The default implementation of Sample Store produces the samples back to the Kafka topic.

## Broker Capacity Config Resolver
The broker Capacity Config Resolver is the way for Cruise Control to get the broker capacity for each of the resources. The default implementation is file based properties. Users can also have a customized implementation to retrieve the capacity of the brokers from some hardware resource management system.

## Goals
The goals in Cruise Control are pluggable with different priorities.

* **Rack-awareness**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner.

 * **RackAwareDistributionGoal** - Contrary to `RackAwareGoal`, as long as replicas of each partition can achieve a perfectly
 even distribution across the racks, this goal lets placement of multiple replicas of a partition into a single rack.

* **ReplicaCapacityGoal**: Attempt to make all the brokers in a cluster to have less than a given number of replicas.

* **CapacityGoals**: Goals that ensures the broker resource utilization is below a given threshold for the corresponding resource. Capacity goals are: 
    * DiskCapacityGoal
    * NetworkInboundCapacityGoal
    * NetworkOutboundCapacityGoal
    * CpuCapacityGoal

* **ReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **PotentialNwOutGoal**: A goal that ensures the potential network output (when all the replicas becomes leaders) on each of the broker do not exceed the broker’s network outbound bandwidth capacity.

* **ResourceDistributionGoals**: Attempt to make the resource utilization variance among all the brokers are within a certain range. This goal does not do anything if the cluster is in a low utilization mode (when all the resource utilization of each broker is below a configured percentage.) This is not a single goal, but consists of the following separate goals for each of the resources. 
    * DiskUtilDistributionGoal
    * NetworkInboundUtilDistributionGoal
    * NetworkOutboundUtilDistributionGoal
    * CpuUtilDistributionGoal

* **TopicReplicaDistributionGoal**: Attempt to make the replicas of the same topic evenly distributed across the entire cluster.

* **LeaderReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of leader replicas.

* **LeaderBytesInDistributionGoal**: Attempt to make the leader bytes in rate on each host to be balanced.

* **PreferredLeaderElectionGoal**: Attempt to make the first replica in replica list leader replica of the partition for all topic partition.

* **KafkaAssignerGoals**: These goals are used to make Cruise Control behaves like [Kafka assigner tool](https://github.com/linkedin/kafka-tools/wiki/Kafka-Assigner). These goals will be picked up if `kafka_assigner` parameter is set to true in corresponding request(e.g. [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).
    * **KafkaAssignerDiskUsageDistributionGoal**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner. 
    * **KafkaAssignerEvenRackAwareGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **IntraBrokerDiskCapacityGoal**: Goals that ensures the disk resource utilization is below a given threshold. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance).

* **IntraBrokerDiskUsageDistributionGoal**: Attempt to make the utilization variance among all the disks within same broker are within a certain range. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance).

* **BrokerSetAwareGoal**: BrokerSet is defined as a subset of brokers in the cluster. This Goal will Attempt to make the replica movements constrained within the boundary of a BrokerSet.

## Anomaly Notifier
The anomaly notifier is a communication channel between Cruise Control and users.
It notifies users about the anomalies detected in the cluster as well as actions taken about the anomaly. Anomalies include:
* Broker failure
* Goal violation
* Metric Anomaly
* Disk failure
* Topic Anomaly

The actions users can take are:
* Fix the anomaly
* Wait some time and check if the anomaly still exists
* Ignore the anomaly

By default Cruise Control is configured to use `NoopNotifier` which ignores all the anomalies.

## Replica Movement Strategy
The strategy to determine the execution order for generated proposals. By default `BaseReplicaMovementStrategy` is used, which is totally random.
Sometimes this could result in prolonged execution time due to some long tail tasks in each execution batches. Other available strategies includes:
* **PrioritizeSmallReplicaMovementStrategy**: prioritize small sized replicas
* **PrioritizeLargeReplicaMovementStrategy**: prioritize large sized replicas
* **PostponeUrpReplicaMovementStrategy**: prioritize replicas for partition having no out-of-sync replica
* **PrioritizeMinIsrWithOfflineReplicasStrategy**: prioritize tasks with (At/Under)MinISR partitions with offline replicas

The strategies can be chained to use and can be dynamically set using `replica_movement_strategies` in corresponding request(e.g. [rebalance request](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).

## Execution State Store
The Execution State Store persists the state of the ongoing execution -- i.e. the parameters it was started with, its proposals, and the
latest state of each execution task. The state is updated continuously while the execution makes progress: when it starts, right before
a batch of tasks is submitted to Kafka (so that every task that Kafka may be executing is recorded as in progress), and whenever a task
finishes. The state is deleted once the execution finishes (i.e. completes, is stopped, or fails), and retained if Cruise Control is shut
down or crashes during the execution.

Upon startup, Cruise Control reconciles the persisted state (if any) with the current state of the cluster -- e.g. a task that was in
progress is checked for whether Kafka is still executing it, has completed it, or has not started it. Then, it either resumes the
remaining tasks of the interrupted execution (see `resume.interrupted.execution.on.startup`) -- monitoring the tasks that Kafka is still
executing rather than submitting them again -- or cleans it up: it waits for the in-flight tasks to finish and removes the replication
throttles left behind by the interrupted execution.

An interrupted execution is resumed only if the cluster is as the execution left it. It is cleaned up instead if it last made progress
longer ago than `resume.interrupted.execution.max.age.ms`, if it was resumed `resume.interrupted.execution.max.count` times already, or if
anything changed while Cruise Control was not running that the execution itself cannot explain: a partition of the execution is not
where the execution left it, a replica outside the execution was moved to another broker (or between the logdirs of a broker the
execution moves replicas between), partitions were created or deleted, a destination broker is not alive, a throttled rate of a broker
of the execution was changed, or a destination does not have enough usable space for the remaining movements. Resuming a plan made for
a different cluster could e.g. move replicas to a broker that has been filled up since.

The cleanup is reported as the `RECOVERING_INTERRUPTED_EXECUTION` executor state: new executions (and self-healing) wait for it, and
stopping it (e.g. `stop_proposal_execution`) cancels the in-flight reassignments of the interrupted execution right away -- as does
reaching `interrupted.execution.cleanup.timeout.ms`. A throttled rate is removed only if it still has the value that the execution set;
a rate changed by someone else in the meantime is kept. The state endpoint reports how the latest interrupted execution was recovered
until the next execution starts.

The default implementation of Execution State Store (see `FileExecutionStateStore`) keeps the state as a JSON document in a local file
(see `executor.state.file.path`). Custom implementations (e.g. backed by ZooKeeper or a Kafka topic) can be plugged in via
`executor.state.store.class`; `NoopExecutionStateStore` disables persisting the execution state.

## Maintenance Event Reader
When a system/tool in Kafka Ecosystem has knowledge about an upcoming planned maintenance, such as a switch change, an upcoming VM freeze, 
or reboot of VMs on cloud deployments, the desired preventative mitigation strategy (i.e. `maintenance event`) can be conveyed to 
Cruise Control for automated execution of it without human intervention.

Such maintenance events correspond to admin operations that the relevant system/tool submits to Cruise Control via a generic maintenance event store.
Then, Maintenance Event Reader retrieves the maintenance events from the relevant event store for automated execution.

The default implementation of Maintenance Event Reader (see `MaintenanceEventTopicReader`) consumes maintenance events from a Kafka topic.

## Provisioner
This component provides the means for adding or removing resources to / from the cluster.

Resources include:
* Brokers that can host replicas 
* Racks containing brokers that can host replicas
* Disks (i.e. relevant to JBOD deployments)
* Partitions of a topic (can only be added)

Constraints include:
* Topic name: If the resource is partition, the name of the topic must be specified.
* A typical broker id and its capacity (one cannot be specified without the other)
* Specific resource, such as {@link Resource#DISK}.
* Excluded racks -- i.e. racks for which brokers should not be added to or removed from
* Total resource capacity required to add or remove

## Broker Set Resolver
The broker set resolver is the way for Cruise Control to get the broker set information for all the brokers. The default implementation is file based properties. Users can also have a customized implementation to retrieve the broker set to broker mapping from some other source.

## Default Broker Set Assignment Policy
The default broker set assignment policy handles the case where a broker id is not provided with any mapping to a broker set. There is a default NoOpDefaultBrokerSetAssignmentPolicy that can be used if there is no default mapping and any mismatch will result in fast failure. Users can have a customized implementation to provide a default treatment for unmapped brokers.
