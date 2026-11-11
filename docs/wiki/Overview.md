## Architecture
The architecture of Cruise Control is illustrated below:
![Architecture](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/docs/images/architecture.png)

### REST API 
Cruise control provides a [REST API](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs) to allow users to interact with it. The REST API supports querying the load and optimization proposals of the Kafka cluster, as well as triggering admin operations. 

### Load Monitor 
The Load Monitor collects standard Kafka metrics from the cluster and derives per partition resource metrics that are not directly available (for e.g., it estimates CPU utilization on a per-partition basis). It then generates a cluster load model that accurately captures cluster resource utilization which includes disk utilization, CPU utilization, bytes-in rate and bytes-out rate at replica-granularity. It then feeds the cluster model into the detector and analyzer.

#### Load Monitor Workflow
![LoadMonitor](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/docs/images/loadMonitor.png)
#### Metric Fetcher Manager
The metric fetcher manager is responsible for coordinating all the sampling tasks. The metric fetcher manager creates a configurable number of metric fetcher threads to execute each of sampling task. It uses a metric sampler partition assignor to distribute the partitions to different metric fetcher threads.

#### Sampling Tasks
The Load Monitor has a metric fetcher manager to arrange different sampling tasks. There are three types of sampling tasks: 
* Metric Sampling Task
* Bootstrap Task
* Linear Model Training Task

Each sampling task is carried out by a configured number of Metric Fetcher threads. Each metric fetcher thread uses a pluggable Metric Sampler to fetch samples. 

#### Metric Sampler Partition Assignor
Each metric fetcher will be assigned a few number of partitions in the cluster to get the samples. The default partition assigner ensures:
* The number of partitions are even for all the metric fetchers
* The partitions of the same topic are assigned to the same metric fetcher

#### Metric Sample Aggregator
Metric Sample Aggregator is a module that organizes all the metric samples. The metric sample aggregator will put each metric sample into a load window according to the timestamp of the metric sample. A load window is the aggregated metric information during a given time window. For example, if the time window is 1 hour and user configured to keep 168 windows. Cruise Control will store 7 days of hourly load information.

When queried, the metric sampler aggregator will return all the aggregated load window back to the LoadMonitor. Users can define a minimum number of metric samples required in each load window to ensure the load window has enough metric samples. For example, if user specifies time window to be 1 hour and the sampling interval is 5 minutes, ideally each window should have 12 metric samples. User can define that a window with less than 10 metric samples is invalid and should not be included into the cluster load model.

#### Cluster Load Model
The cluster load model is the primary output of the Load Monitor. It is a software model reflecting the current replica assignment of the cluster with replica granularity load data for different resources. The cluster model provides interfaces to move partitions or replicas to simulate the impact on the cluster. Those interfaces are used by the Analyzer to generate for the optimization solution.

#### Sample Store
After the metric samples and training samples are accepted by the Metric Sample Aggregator and Cluster load Model, they will be passed to a pluggable Sample Store which can be saved to external storage for future loading. This is particularly useful when collecting the old metric samples is difficult or slow.

### Analyzer 
The Analyzer is the "brain" of Cruise Control. It uses a heuristic method to generate optimization proposals based on the user provided optimization goals and the cluster load model from the Load Monitor. 
Cruise control allows specifying hard goals and soft goals. A hard goal is one that must be satisfied (e.g. replica placement must be rack-aware). Soft goals on the other hand may be left unmet if doing so makes it possible to satisfy all the hard goals. The optimization would fail if the optimized results violate a hard goal. We have implemented the following hard and soft goals so far: 
* Replica placement must be rack-aware 
* Broker resource utilization must be within pre-defined thresholds 
* Network utilization must not be allowed to go beyond a pre-defined capacity even when all replicas on the broker become leaders 
* Attempt to achieve uniform resource utilization across all brokers 
* Attempt to achieve uniform bytes-in rate of leader partitions across brokers 
* Attempt to evenly distribute partitions of a specific topic across all brokers 
* Attempt to evenly distribute replicas (globally) across all brokers
* Attempt to evenly distribute leader replicas (globally) across all brokers
* Disk utilization must be within pre-defined thresholds for each disk within a broker
* Attempt to evenly distribute disk utilization across disks of each broker

At a high level, the goal optimization logic is as follows: 
```
For each goal g in the goal list ordered by priority { 
 For each broker b { 
   while b does not meet g’s requirement { 
     For each replica r on b sorted by the resource utilization density { 
       Move r (or the leadership of r) to another eligible broker b’ so b’ still satisfies g and all the satisfied goals 
       Finish the optimization for b once g is satisfied. 
     } 
     Fail the optimization if g is a hard goal and is not satisfied for b 
   } 
 } 
 Add g to the satisfied goals 
}
```
Note that all the goals are pluggable.  Users can [write their own goals](https://github.com/cruise-control-for-kafka/cruise-control/wiki/Write-your-own-goals) and add them to Cruise Control.

### Anomaly Detector 
The anomaly detector identifies four types of anomalies: 
* Broker failures – i.e. a non-empty broker crash or leaves a cluster. This results in offline replicas/under replicated partitions. When this happens, Cruise Control will send a notification out and if self-healing for this anomaly type is enabled, Cruise Control will trigger an operation to move all the offline replicas to other healthy brokers in the cluster. Since this can happen during normal cluster bounces as well, the anomaly detector provides for a configurable grace period before it triggers the notifier and fix the cluster. 
* Goal violations - i.e. an optimization goal is violated. When this happens, Cruise Control will send a notification out and if self-healing for this anomaly type is enabled, Cruise Control will proactively attempt to address the goal violation by automatically analyzing the load, and executing optimization proposals. 
* Intra-broker goal violations - i.e. an intra-broker goal is violated, which indicates an imbalance across the disks of brokers in a JBOD deployment. These violations are checked only if the capacity of at least one broker specifies multiple disks. When this happens, Cruise Control will send a notification out and if self-healing for this anomaly type is enabled, Cruise Control will balance the disks of brokers by moving replicas between the disks of the same broker.
* Disk failure - i.e. one of the non-empty disk dies, note this is only related with Kafka broker running on JBOD disk. When this happens, Cruise Control will send a notification out and if self-healing for this anomaly type is enabled, Cruise Control will trigger an operation to move all the offline replicas to other healthy brokers in the cluster. 
* Metric anomaly - i.e. one of the metrics Cruise Control collected observes anomaly in value (e.g. a sudden rise in log flush time metrics). When this happens, Cruise Control will send a notification out. Currently there is no standardized self-healing operation defined for metric anomaly since different metric anomalies expect different remediations. User can define their own anomaly and remediation operation by implementing their own [MetricAnomaly](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control-core/src/main/java/com/linkedin/cruisecontrol/detector/metricanomaly/MetricAnomaly.java) and [MetricAnomalyFinder](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control-core/src/main/java/com/linkedin/cruisecontrol/detector/metricanomaly/MetricAnomalyFinder.java).
* Topic anomaly - i.e. one or more topics in cluster violates user-defined properties (e.g. some partitions are too large in disk). When this happens, Cruise Control will send a notification out. Currently there is no standardized self-healing operation defined for topic anomaly since different topic anomalies expect different remediations. User can define their own anomaly and remediation operation by implementing their own [TopicAnomaly](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/detector/TopicAnomaly.java) and [TopicAnomalyFinder](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/detector/TopicAnomalyFinder.java).


### Executor
The executor is responsible for carrying out the optimization proposals from the analyzer. The executor is designed in a way that it is safely interruptible when executing the proposals. The executor ensures that the execution is resource-aware and does not overwhelm any broker.

## Pluggable modules
Cruise Control has a few [pluggable components](https://github.com/cruise-control-for-kafka/cruise-control/wiki/Pluggable-Components) to meet various requirements from the users.

## Configurations
Please find all the configurations of Cruise Control in this [wiki page](https://github.com/cruise-control-for-kafka/cruise-control/wiki/Configurations)
