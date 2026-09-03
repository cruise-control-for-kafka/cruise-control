Cruise Control for Apache Kafka
===================

[![CI](https://github.com/linkedin/cruise-control/actions/workflows/ci.yaml/badge.svg)](https://github.com/linkedin/cruise-control/actions/workflows/ci.yaml)

### Introduction ###
  Cruise Control is a product that helps run Apache Kafka clusters at large scale. Due to the popularity of 
  Apache Kafka, many companies have increasingly large Kafka clusters with hundreds of brokers. At LinkedIn, we have 10K+ Kafka brokers, 
  which means broker deaths are an almost daily occurrence and balancing the workload of Kafka also becomes a big overhead. 
  
  Kafka Cruise Control is designed to address this operational scalability issue.
  
### Features ###
  Kafka Cruise Control provides the following features out of the box:
  
  * Resource utilization tracking for brokers, topics, and partitions.
  
  * Query the current Kafka cluster state to see the online and offline partitions, in-sync and out-of-sync replicas,
  replicas under `min.insync.replicas`, online and offline logDirs, and distribution of replicas in the cluster.
  
  * Multi-goal rebalance proposal generation for:
    * Rack-awareness
    * Resource capacity violation checks (CPU, DISK, Network I/O)
    * Per-broker replica count violation check
    * Resource utilization balance (CPU, DISK, Network I/O)
    * Leader traffic distribution
    * Replica distribution for topics
    * Global replica distribution
    * Global leader replica distribution
    * Custom goals that you wrote and plugged in
  
  * Anomaly detection, alerting, and self-healing for the Kafka cluster, including:
    * Goal violation
    * Broker failure detection
    * Metric anomaly detection
    * Disk failure detection (not available in `kafka_0_11_and_1_0` branch)
    * Slow broker detection (not available in `kafka_0_11_and_1_0` branch)
    * Intra-Broker goal violation
  
  * Admin operations, including:
    * Add brokers
    * Remove brokers
    * Demote brokers
    * Rebalance the cluster
    * Fix offline replicas (not available in `kafka_0_11_and_1_0` branch)
    * Perform preferred leader election (PLE)
    * Fix offline replicas
    * Adjust replication factor

### Environment Requirements ###
* The `main` (previously `migrate_to_kafka_2_5`) branch of Cruise Control is compatible with Apache Kafka `2.5+` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.*`),
  `2.6` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.11+`), `2.7` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.36+`),
  `2.8` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.66+`), `3.0` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.85+`),
  `3.1` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.85+`), `3.8` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.142+`),
  `3.9` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.143+`), and `4.0` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.5.144+`)
* The `migrate_to_kafka_2_4` branch of Cruise Control is compatible with Apache Kafka `2.4` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.4.*`).
* The `kafka_2_0_to_2_3` branch (deprecated) of Cruise Control is compatible with Apache Kafka `2.0`, `2.1`, `2.2`, and `2.3` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `2.0.*`).
* The `kafka_0_11_and_1_0` branch (deprecated) of Cruise Control is compatible with Apache Kafka `0.11.0.0`, `1.0`, and `1.1` (i.e. [Releases](https://github.com/linkedin/cruise-control/releases) with `0.1.*`).
* `message.format.version` `0.10.0` and above is needed.
* The `kafka_2_0_to_2_3` and `kafka_0_11_and_1_0` branches compile with `Scala 2.11`.
* The branch `migrate_to_kafka_2_4` compiles with `Scala 2.12`.
* The branch `migrate_to_kafka_2_5` compile with `Scala 2.13`.
* This project requires Java 17.

#### Known Compatibility Issues ####
* Support for Apache Kafka `2.0`, `2.1`, `2.2`, and `2.3` requires [KAFKA-8875](https://issues.apache.org/jira/browse/KAFKA-8875) hotfix.

### Quick Start ###
0. Get Cruise Control
    1. (Option-1): via `git clone`
        * `git clone https://github.com/linkedin/cruise-control.git && cd cruise-control/`
    2. (Option-2): via browsing the available releases:
        * Browse `https://github.com/linkedin/cruise-control/releases` to pick a release -- e.g. `0.1.10`
        * Get and extract the release: `wget https://github.com/linkedin/cruise-control/archive/0.1.10.tar.gz 
        && tar zxvf 0.1.10.tar.gz && cd cruise-control-0.1.10/`
        * Initialize the local repo: `git init && git add . && git commit -m "Init local repo."
        && git tag -a 0.1.10 -m "Init local version."`
1. This step is required if `CruiseControlMetricsReporter` is used for metrics collection (i.e. the default for Cruise
Control). The metrics reporter periodically samples the Kafka raw metrics on the broker and sends them to a Kafka topic.
    * `./gradlew jar` (Note: This project requires Java 17)
    * Copy `./cruise-control-metrics-reporter/build/libs/cruise-control-metrics-reporter-A.B.C.jar` (Where `A.B.C` is
    the version of the Cruise Control) to your Kafka server dependency jar folder. For Apache Kafka, the folder would
    be `core/build/dependant-libs-SCALA_VERSION/` (for a Kafka source checkout) or `libs/` (for a Kafka release download).
    * Modify Kafka server configuration to set `metric.reporters` to
    `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`. For Apache Kafka, server 
    properties are located at `./config/server.properties`.
    * If `SSL` is enabled, ensure that the relevant client configurations are properly set for all brokers in 
    `./config/server.properties`. Note that `CruiseControlMetricsReporter` takes all configurations for vanilla 
    `KafkaProducer` with a prefix of `cruise.control.metrics.reporter.` -- e.g. 
    `cruise.control.metrics.reporter.ssl.truststore.password`.    
    * If the default broker cleanup policy is `compact`, make sure that the topic to which Cruise Control metrics
    reporter should send messages is created with the `delete` cleanup policy -- the default metrics reporter topic
    is `__CruiseControlMetrics`.
2. Start Kafka server ([See tutorial](https://kafka.apache.org/quickstart)), and if you're using a ZooKeeper-based Kafka cluster also start a ZooKeeper server.
3. Modify `config/cruisecontrol.properties` of Cruise Control:
    * (Required) fill in `bootstrap.servers` to the Kafka cluster to be monitored.
    * (Required) update `capacity.config.file` to the path of your capacity file.  
      * Capacity file is a JSON file that provides the capacity of the brokers
      * You can start Cruise Control server with the default file (`config/capacityJBOD.json`), but it may not reflect the actual capacity of the brokers 
      * See [BrokerCapacityConfigurationFileResolver configurations](https://github.com/linkedin/cruise-control/wiki/Configurations#brokercapacityconfigurationfileresolver-configurations) for more information and examples
    * (Optional) set `metric.sampler.class` to your implementation (the default sampler class is `CruiseControlMetricsReporterSampler`) 
    * (Optional) set `sample.store.class` to your implementation if you have one (the default `SampleStore` is `KafkaSampleStore`)
4. Run the following command 
    ```
    ./gradlew jar copyDependantLibs
    ./kafka-cruise-control-start.sh [-jars PATH_TO_YOUR_JAR_1,PATH_TO_YOUR_JAR_2] config/cruisecontrol.properties [port]
    ```
    JAR files correspond to your applications and `port` enables customizing the Cruise Control port number (default: `9090`).
    * (Note) To emit Cruise Control JMX metrics on a particular port (e.g. `56666`), `export JMX_PORT=56666` before
    running `kafka-cruise-control-start.sh`
5. (Verify your setup) Visit `http://localhost:9090/kafkacruisecontrol/state` (or 
`http://localhost:\[port\]/kafkacruisecontrol/state` if you specified the port when starting Cruise Control).

**Note**: 
* Cruise Control will need some time to read the raw Kafka metrics from the cluster.
* The metrics of a newly up broker may take a few minutes to get stable. Cruise Control will drop the inconsistent 
metrics (e.g when topic bytes-in is higher than broker bytes-in), so first few windows may not have enough valid partitions.

### REST API ###
Cruise Control provides a [REST API](https://github.com/linkedin/cruise-control/wiki/REST-APIs) for users 
to interact with. See the wiki page for more details.

### How Does It Work ###
Cruise Control relies on the recent load information of replicas to optimize the cluster.

Cruise Control periodically collects resource utilization samples at both broker- and partition-level to 
infer the traffic pattern of each partition. Based on the traffic characteristics and distribution of all the partitions, 
it derives the load impact of each partition over the brokers. Cruise Control then builds a workload
model to simulate the workload of the Kafka cluster. The goal optimizer explores different ways to generate 
cluster workload optimization proposals based on the user-specified list of goals.

Cruise Control also monitors the liveness of all the brokers in the cluster.
To avoid the loss of redundancy, Cruise Control automatically moves replicas from failed brokers to alive ones.

For more details about how Cruise Control achieves that, see 
[these slides](https://www.slideshare.net/JiangjieQin/introduction-to-kafka-cruise-control-68180931).

### Configurations for Cruise Control ###
To read more about the configurations. Check the 
[configurations wiki page](https://github.com/linkedin/cruise-control/wiki/Configurations).

### Artifactory ###
Published at [Jfrog Artifactory](https://linkedin.jfrog.io/linkedin/webapp/#/artifacts/browse/tree/General/cruise-control). See [available releases](https://github.com/linkedin/cruise-control/releases). 

### Pluggable Components ###
More about pluggable components can be found in the 
[pluggable components wiki page](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components).

#### Metric Sampler #### 
The metric sampler enables users to deploy Cruise Control to various environments and work with the existing metric systems.

Cruise Control provides a metrics reporter that can be configured in your Apache Kafka server. Metrics reporter generates
performance metrics to a Kafka metrics topic that can be consumed by Cruise Control.

#### Sample Store ####
The Sample Store enables storage of collected metric samples and training samples in an external storage. 

Metric sampling uses derived data from the raw metrics, and the accuracy of the derived data depends on the metadata of the cluster at that point.
Hence, when we look at the old metrics, if we do not know the metadata at the point the metric was collected, the derived data would not be accurate.
Sample Store helps solving this problem by storing the derived data directly to an external storage for later loading.

The default Sample Store implementation produces metric samples back to Kafka.

#### Goals ####
The goals in Cruise Control are pluggable with different priorities. The default goals in order of decreasing priority are:
 * **RackAwareGoal** - Ensures that all replicas of each partition are assigned in a rack aware manner -- i.e. no more than one replica of 
 each partition resides in the same rack.
 * **RackAwareDistributionGoal** - A relaxed version of `RackAwareGoal`. Contrary to `RackAwareGoal`, as long as replicas of each partition
 can achieve a perfectly even distribution across the racks, this goal lets placement of multiple replicas of a partition into a single rack.
 * **MinTopicLeadersPerBrokerGoal** - Ensures that each alive broker has at least a certain number of leader replica of each topic in a configured set of topics
 * **ReplicaCapacityGoal** - Ensures that the maximum number of replicas per broker is under the specified maximum limit.
 * **DiskCapacityGoal** - Ensures that Disk space usage of each broker is below a given threshold.
 * **NetworkInboundCapacityGoal** - Ensures that inbound network utilization of each broker is below a given threshold.
 * **NetworkOutboundCapacityGoal** - Ensures that outbound network utilization of each broker is below a given threshold.
 * **CpuCapacityGoal** - Ensures that CPU utilization of each broker is below a given threshold.
 * **ReplicaDistributionGoal** - Attempts to make all the brokers in a cluster have a similar number of replicas.
 * **PotentialNwOutGoal** - Ensures that the potential network output (when all the replicas in the broker become leaders) on each of the broker do 
 not exceed the broker’s network outbound bandwidth capacity.
 * **DiskUsageDistributionGoal** - Attempts to keep the Disk space usage variance among brokers within a certain range relative to the average Disk utilization.
 * **NetworkInboundUsageDistributionGoal** - Attempts to keep the inbound network utilization variance among brokers within a certain range relative to the average inbound network utilization.
 * **NetworkOutboundUsageDistributionGoal** - Attempts to keep the outbound network utilization variance among brokers within a certain range relative to the average outbound network utilization.
 * **CpuUsageDistributionGoal** - Attempts to keep the CPU usage variance among brokers within a certain range relative to the average CPU utilization.
 * **LeaderReplicaDistributionGoal** - Attempts to make all the brokers in a cluster have a similar number of leader replicas.
 * **LeaderBytesInDistributionGoal** - Attempts to equalize the leader bytes in rate on each host.
 * **TopicReplicaDistributionGoal** - Attempts to maintain an even distribution of any topic's partitions across the entire cluster.
 * **PreferredLeaderElectionGoal** - Simply move the leaders to the first replica of each partition.
 * **KafkaAssignerDiskUsageDistributionGoal** - (Kafka-assigner mode) Attempts to distribute disk usage evenly among brokers based on swap.
 * **IntraBrokerDiskCapacityGoal** - (Rebalance-disk mode, not available in `kafka_0_11_and_1_0` branch) Ensures that Disk space usage of each disk is below a given threshold.
 * **IntraBrokerDiskUsageDistributionGoal** - (Rebalance-disk mode, not available in `kafka_0_11_and_1_0` branch) Attempts to keep the Disk space usage variance among disks within a certain range relative to the average broker Disk utilization.

#### Anomaly Notifier ####
The anomaly notifier allows users to be notified when an anomaly is detected. Anomalies include:
 * Broker failure
 * Goal violation
 * Metric anomaly
 * Disk failure (not available in `kafka_0_11_and_1_0` branch)
 * Slow brokers (not available in `kafka_0_11_and_1_0` branch)
 * Topic replication factor anomaly (not available in `kafka_0_11_and_1_0` branch)
 * Topic partition size anomaly (not available in `kafka_0_11_and_1_0` branch)
 * Maintenance Events (not available in `kafka_0_11_and_1_0` branch)
 
In addition to anomaly notifications, users can enable actions to be taken in response to an anomaly by turning self-healing
on for the relevant anomaly detectors. Multiple anomaly detectors work in harmony using distinct mitigation mechanisms.
Their actions broadly fall into the following categories:
 * **fix** - fix the problem right away (e.g. start a rebalance, fix offline replicas)
 * **check** - check the situation again after a configurable delay (e.g. adopt a grace period before fixing broker failures)
 * **ignore** - ignore the anomaly (e.g. self-healing is disabled)
