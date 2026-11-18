Cruise Control supports for changing topic replication factor.

# Motivation
In Kafka cluster, partitions of topics can be replicated across a configurable number of brokers. This is mainly for better resilience to unexpected failures(hardware failure, network issue, software crash etc.) and it is controlled by topic's replication factor config. A common admin operation to run Kafka cluster in production is to increase/decrease some topics' replication factor to make trade-off between fault-tolerance and resource utilization/latency(especially case of produce with `ack=all`).

Cruise Control, designed to be the exclusive management system for Kafka cluster, should have native support for this common admin operation. 

# Implementation overview
The question of how to change replication factor for some topics boils down to two core questions.
1. To increase replication factor of certain topic, which broker to assign the newly created replica to
2. To decrease replication factor, which one of current replicas to delete

With the infrastructure built in Cruise Control, we can come up with a pretty "smart" answer. Based on the cluster workload model Cruise Control generates and the list of provided optimization goals, Cruise Control can reuse same heuristic algorithm used for rebalance operation to determine the location of the newly created replicas.

At high-level, the decision is made in 2 steps.
1. Tentatively add new replicas to brokers in cluster in a rack-aware, round-robin way
2. Further optimize new replica's location with provided goal list

To decrease the replication factor, the leader replica is always retained, and the replicas to delete are selected in the following order:
1. Offline replicas (i.e. replicas on dead brokers or broken disks)
2. Out-of-sync replicas
3. The remaining followers, following the reverse order of their position in the replica list of the partition

Hence, decreasing the replication factor works even if there are dead brokers or broken disks in the cluster, as long as all offline replicas are removed by the replication factor change (i.e. each partition with offline replicas has enough online replicas to satisfy the target replication factor). Otherwise, the request is rejected, and the offline replicas can be fixed first (e.g. using `fix_offline_replicas` endpoint). Removing out-of-sync replicas first avoids waiting for lagging replicas to catch up, e.g. in a cluster under heavy load. Moreover, replication throttle is not applied to replica movements that only remove replicas, since they do not replicate any data.

# Instruction
To access this new utility, a new POST endpoint,`topic_configuration` is added to Cruise Control. 
See request detail and supported parameters at [REST API wiki page](https://github.com/cruise-control-for-kafka/cruise-control/wiki/REST-APIs#change-kafka-topic-configuration).

Note that the parameters for this endpoint is very similar to the ones of `rebalance` endpoint, the two unique parameters are `topic` and `replication_factor`. `replication_factor` is used to set the target replication factor, and  `topic` parameter is used to set topics to apply the change. What set for `topic` parameter will be treated as a regular expression, so user can do tricks like `topic=.*` to change replication factor for all topics in the cluster.

In the response of this endpoint, a full list topics get changed will be returned. So user can check whether the regular expression set in `topic` parameter works as expected.

One example request 
> curl -X POST  -c cookie "localhost:2540/kafkacruisecontrol/topic_configuration?topic=__KafkaCruiseControlPartitionMetricSamples&replication_factor=4"

And get response
> Optimization has 64 inter-broker replica(1216 MB) moves, 0 intra-broker replica(0 MB) moves and 83 leadership moves with a cluster model of 168 recent windows and 100.000% of the partitions covered.
>
> ...
>
> Cluster load after updating replication factor of topics [__KafkaCruiseControlPartitionMetricSamples] to 4
>
> ...

If the regular expression becomes too long to fit in the request header, it could also be specified in request body in JSON format, so for above example, the request can be modified to

> curl -d "@data.json" -H "Content-Type: application/json"-X POST  -c cookie "localhost:2540/kafkacruisecontrol/


And in data.json, specify
>{
>
>    replication_factor: {
>
>        topic_by_replication_factor : {
>
>            4 : __KafkaCruiseControlPartitionMetricSamples
>
>    }
>
>}