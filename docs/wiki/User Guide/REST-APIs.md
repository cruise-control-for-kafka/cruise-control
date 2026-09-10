## Contents

- [Asynchronous Endpoints](#asynchronous-endpoints)
    - [UUIDs](#uuids)
    - [Cookies](#cookies)
- [GET Requests](#get-requests)
    * [Query the state of Cruise Control](#query-the-state-of-cruise-control)
    * [Query the current cluster load](#query-the-current-cluster-load)
    * [Query partition resource utilization](#query-partition-resource-utilization)
    * [Query partition and replica state](#query-partition-and-replica-state)
    * [Get optimization proposals](#get-optimization-proposals)
    * [Query the user request result](#query-the-user-request-result)
- [POST Requests](#post-requests)
    * [Trigger a workload balance](#trigger-a-workload-balance)
    * [Add a list of new brokers to Kafka Cluster](#add-a-list-of-new-brokers-to-kafka-cluster)
    * [Decommission a list of brokers from the Kafka cluster](#decommission-a-list-of-brokers-from-the-kafka-cluster)
    * [Fix offline replicas in Kafka cluster](#fix-offline-replicas-in-kafka-cluster)
    * [Demote a list of brokers from the Kafka cluster](#demote-a-list-of-brokers-from-the-kafka-cluster)
    * [Remove disks from brokers in the Kafka cluster](#remove-disks-from-brokers-in-the-kafka-cluster)
    * [Stop the current proposal execution task](#stop-the-current-proposal-execution-task)
    * [Pause metrics load sampling](#pause-metrics-load-sampling)
    * [Resume metrics load sampling](#resume-metrics-load-sampling)
    * [Change Kafka topic configuration](#change-kafka-topic-configuration)
    * [Change Cruise Control configuration](#change-cruise-control-configuration)
    * [2-step Verification](#2-step-verification)
    * [Rightsize the cluster with the Provisioner](#rightsize-the-cluster-with-the-provisioner)

## Asynchronous Endpoints

**Please ensure proper use of UUIDs or cookies to interact with async endpoints.**

Selected Cruise Control (CC) endpoints accept async calls to avoid blocking more than a configured period of time 
(via `webserver.request.maxBlockTimeMs` configuration).
If the server-side processing of such requests takes more than this configured time, along with a with `SC_ACCEPTED` code, 
CC returns a progress response.
This response contains both (1) a `sessionId`, and (2) a UUID corresponding to the client cookie and the request, respectively.
The completed response of an in-progress request can be retrieved within a predefined time.
Using cookies, this response can be retrieved before the timeout is configured via `webserver.session.maxExpiryTimeMs`.
Using the UUID, the timeout is configured via relevant completed user task retention time configuration 
(see `completed.kafka.monitor.user.task.retention.time.ms`, `completed.cruise.control.monitor.user.task.retention.time.ms`,
`completed.kafka.admin.user.task.retention.time.ms`, `completed.cruise.control.admin.user.task.retention.time.ms`,
and `completed.user.task.retention.time.ms` ).

### UUIDs

Here is an example how to use **UUID** with requests using `cURL`:

1. Create a new request

 `curl -vv -X POST "http://CRUISE_CONTROL_HOST:9090/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

2. Retrieve the `User-Task-ID` from response header, e.g. `User-Task-ID: 5ce7c299-53b3-48b6-b72e-6623e25bd9a8`

3. Specifying the `User-Task-ID` in request that has not completed

 `curl -vv -X POST -H "User-Task-ID: 5ce7c299-53b3-48b6-b72e-6623e25bd9a8" "http://CRUISE_CONTROL_HOST:9090/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

### Cookies

Here is an example of how to use **cookies** with requests using `cURL`:

1. Create a cookie associated with a new request

 `curl -X POST -c /tmp/mycookie-jar.txt "http://CRUISE_CONTROL_HOST:9090/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

2. Use an existing cookie from the created file for a request that has not completed

 `curl -X POST -b /tmp/mycookie-jar.txt "http://CRUISE_CONTROL_HOST:9090/kafkacruisecontrol/remove_broker?brokerid=1234&dryrun=false"`

Note that a `User-Task-ID` or a `sessionId` and is applicable for an entire `URL`, including its parameters. Hence, the same endpoint with different parameters would create and use a different `User-Task-Id`.

## GET Requests

The GET requests in Kafka Cruise Control REST API are for read only operations, i.e. the operations that do not have any external impacts. The GET requests include the following operations:

* [Query the state of Cruise Control](#query-the-state-of-cruise-control)
* [Query the current cluster load](#query-the-current-cluster-load)
* [Query partition resource utilization](#query-partition-resource-utilization)
* [Query partition and replica state](#query-partition-and-replica-state)
* [Get optimization proposals](#get-optimization-proposals)
* [Query the user request result](#query-the-user-request-result)
* [Query user permissions](#query-user-permissions)

### Query the state of Cruise Control

User can query the state of Kafka Cruise Control at any time by issuing a HTTP GET request.

    GET /kafkacruisecontrol/state

Supported parameters are:

| PARAMETER     | TYPE      | DESCRIPTION                                                                                                                                   | DEFAULT               | OPTIONAL  |
|---------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| substates     | list      | substates for which to retrieve state from cruise-control, available substates are `analyzer`, `monitor`, `executor` and `anomaly_detector`   | all substates         | yes       | 
| json          | boolean   | return in JSON format or not                                                                                                                  | false                 | yes       | 
| verbose       | boolean   | return detailed state information                                                                                                             | false                 | yes       | 
| super_verbose | boolean   | return more detailed state information                                                                                                        | false                 | yes       |
| doAs          | string    | propagated user by the trusted proxy service                                                                                                  | null                  | yes       | 
| reason        | string    | reason for the request                                                                                                                        | "No reason provided"  | yes       | 

The returned state contains the following information:
* **Monitor State**:
  * State: `NOT_STARTED` / `RUNNING` / `SAMPLING` / `PAUSED` / `BOOTSTRAPPING` / `TRAINING` / `LOADING`,
  * Bootstrapping progress (If state is `BOOTSTRAPPING`)
  * Number of valid monitored windows / number of total monitored windows
  * Number of valid partitions out of the total number of partitions
  * Percentage of the partitions that are valid
  * Time and reason of recently sampling task pause/resume
* **Executor State**:
  * State: `NO_TASK_IN_PROGRESS` /
	   `STARTING_EXECUTION` /
	   `INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS` /
	   `INTRA_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS` /
	   `LEADER_MOVEMENT_TASK_IN_PROGRESS` /
	   `STOPPING_EXECUTION` /
	   `INITIALIZING_PROPOSAL_EXECUTION` /
	   `GENERATING_PROPOSALS_FOR_EXECUTION`
  * Inter-broker replica movement progress (if state is `INTER_BROKER_REPLICA_MOVEMENT_IN_PROGRESS`)
  * Intra-broker replica movement progress (if state is `INTRA_BROKER_REPLICA_MOVEMENT_IN_PROGRESS`)
  * Leadership movement progress (if state is `LEADERSHIP_MOVEMENT`)
  * movement concurrency
  * UUID triggers the execution
* **Analyzer State**:
  * isProposalReady: Is there a proposal cached
  * ReadyGoals: A list of goals that are ready for running
* **Anomaly Detector State**:
  * selfHealingEnabled: Anomaly type for which self healing is enabled
  * selfHealingDisabled: Anomaly type for which self healing is disabled
  * recentGoalViolations: Recently detected goal violations
  * recentBrokerFailures: Recently detected broker failures
  * recentDiskFailures: Recently detected disk failures
  * recentMetricAnomalies: Recently detected metric anomalies

If `verbose` is set to `true`, the details about monitored windows and goals will be displayed.
If `super_verbose` is set to `true`, the details about extrapolation made on metric samples will be displayed.
If `substates` is not set, the full state will be displayed; if it is set to the specific substate(s), only state(s) of interest will be displayed and response will be returned faster.

### Query the current cluster load
Once Cruise Control Load Monitor shows it is in the `RUNNING` state, Users can use the following HTTP GET to get the cluster load:

    GET /kafkacruisecontrol/load

Supported parameters are:

| PARAMETER                 | TYPE      | DESCRIPTION                                                                       | DEFAULT                       | OPTIONAL  |
|---------------------------|-----------|-----------------------------------------------------------------------------------|-------------------------------|-----------|
| start                     | long      | start time of the cluster load                                                    | time of earliest valid window | yes       |
| end                       | long      | end time of the cluster load                                                      | current system time           | yes       |
| time                      | long      | end time of the cluster load                                                      | current system time           | yes       | 
| allow_capacity_estimation | boolean   | whether to allow broker capacity to be estimated from other broker in the cluster | true                          | yes       | 
| populate_disk_info        | boolean   | whether show the load of each disk of broker                                      | false                         | yes       | 
| capacity_only             | boolean   | whether show only the cluster capacity or the utilization, as well.               | false                         | yes       |
| json                      | boolean   | return in JSON format or not                                                      | false                         | yes       | 
| verbose                   | boolean   | return detailed state information                                                 | false                         | yes       |
| doAs                      | string    | propagated user by the trusted proxy service                                      | null                          | yes       | 
| reason                    | string    | reason for the request                                                            | "No reason provided"          | yes       | 

If the number of workload snapshots for the given timestamp is not sufficient to generate a good load model, an exception will be returned.

Timestamp for `start`/`end`/`time` is in milliseconds since the epoch; what `System.currentTimeMillis()` returns.  The time zone is the time zone of the Cruise Control server.

If `allow_capacity_estimation` is set to `true`, for brokers missing capacity information Cruise Control will make estimations based on other brokers in the cluster; otherwise an `IllegalStateException` will be thrown and shown in response.

The response contains both load-per-broker and load-per-host information. This is specifically useful when multiple brokers are hosted by the same machine.

NOTE: The load shown is only for the load from the valid partitions. i.e the partitions with enough metric samples. So please always check the `LoadMonitor`'s state(via `State` endpoint) to decide whether the workload is representative enough.

### Query partition resource utilization
The following GET request gives the partition load sorted by the utilization of a given resource:

    GET /kafkacruisecontrol/partition_load

Supported parameters are:

| PARAMETER                 | TYPE          | DESCRIPTION                                                                                                       | DEFAULT                       | OPTIONAL  |
|---------------------------|---------------|-------------------------------------------------------------------------------------------------------------------|-------------------------------|-----------|
| resource                  | string        | resource type to sort partition load, available resources are `DISK`/`CPU`/`NW_IN`/`NW_OUT`                       | `DISK`                        | yes       |
| start                     | long          | the timestamp in millisecond of the earliest metric sample use to generate load                                   | time of earliest valid window | yes       |
| end                       | long          | the timestamp in millisecond of the latest metric sample use to generate load                                     | current system time           | yes       |
| entries                   | integer       | number of partition load entries to report in response                                                            | `MAX_INT`                     | yes       |
| json                      | boolean       | return in JSON format or not                                                                                      | false                         | yes       | 
| allow_capacity_estimation | boolean       | whether to allow capacity estimation when cruise-control is unable to obtain all per-broker capacity information  | true                          | yes       |
| max_load                  | boolean       | whether report the max load for partition in windows                                                              | false                         | yes       |
| avg_load                  | boolean       | whether report the average load for partition in windows                                                          | false                         | yes       |
| topic                     | regex         | regular expression to filter partition load to report based on partition's topic                                  | null                          | yes       |
| partition                 | integer/range | partition number(e.g. 10) range(e.g. 1-10) to filter partition load to report                                     | null                          | yes       |
| min_valid_partition_ratio | double        | minimal valid partition ratio requirement for cluster model                                                       | null                          | yes       |
| brokerid                  | int           | broker id to to filter partition load to report                                                                   | null                          | yes       |
| doAs                      | string        | propagated user by the trusted proxy service                                                                      | null                          | yes       |
| reason                    | string        | reason for the request                                                                                            | "No reason provided"          | yes       | 

The returned result would be a partition list sorted by the utilization of the specified resource in the time range specified by `start` and `end`. The resource can be `CPU`, `NW_IN`, `NW_OUT` and `DISK`. By default, the `start` is the earliest monitored time, the `end` is current wall clock time, `resource` is `DISK`, and `entries` is the all partitions in the cluster.

By specifying `topic`,`partition` and/or `brokerid` parameter, client can filter returned partition entries.

The `min_valid_partition_ratio` specifies minimal monitored valid partition percentage needed to calculate the partition load. If this parameter is not set in request, the config value `min.valid.partition.ratio` will be used.

The `max_load` parameter specifies whether report the maximal historical value or not. The `avg_load` parameter specifies whether report the average historical value or not. If both are not specified or specified as `false`, for `DISK` resource, latest value will be reported; for `NW_IN`/`NW_OUT`/`CPU` resource, average value will be reported.

### Query partition and replica state
The following GET request gives partition healthiness on the cluster:

    GET /kafkacruisecontrol/kafka_cluster_state

Supported parameters are:

| PARAMETER | TYPE      | DESCRIPTION                                                                       | DEFAULT              | OPTIONAL  |
|-----------|-----------|-----------------------------------------------------------------------------------|----------------------|-----------|
| topic     | regex     | regular expression to filter partition state to report based on partition's topic | null                 | yes       | 
| json      | boolean   | return in JSON format or not                                                      | false                | yes       | 
| verbose   | boolean   | return detailed state information                                                 | false                | yes       |
| doAs      | string    | propagated user by the trusted proxy service                                      | null                 | yes       | 
| reason    | string    | reason for the request                                                            | "No reason provided" | yes       | 

The returned result contains the following information
* For each broker
  * Distribution of leader/follower/out-of-sync/offline replica information
  * Online/offline disks

* For each partition
  * Distribution of leader/follower/in-sync/out-of-sync/offline replica information

### Get optimization proposals
The following GET request returns the optimization proposals generated based on the workload model of the given timestamp. The workload summary before and after the optimization will also be returned.

    GET /kafkacruisecontrol/proposals

Supported parameters are:

| PARAMETER                         | TYPE      | DESCRIPTION                                                                           | DEFAULT              | OPTIONAL  |
|-----------------------------------|-----------|---------------------------------------------------------------------------------------|----------------------|-----------|
| ignore_proposal_cache             | boolean   | whether to ignore the cached proposal or not                                          | false                | yes       | 
| data_from                         | string    | whether to calculate proposal from available valid partitions or valid windows        | `VALID_WINDOWS`      | yes       |
| goals                             | list      | list of goals used to generate proposal                                               | default goals        | yes       |
| kafka_assigner                    | boolean   | whether to use Kafka assigner mode to generate proposals                              | false                | yes       |
| allow_capacity_estimation         | boolean   | whether to allow broker capacity to be estimated                                      | true                 | yes       |
| excluded_topics                   | regex     | regular expression to specify topics excluded from replica and leadership movement    | null                 | yes       |
| use_ready_default_goals           | boolean   | whether to use only ready goals to generate proposals                                 | false                | yes       |
| exclude_recently_demoted_brokers  | boolean   | whether to allow leader replicas to be moved to recently demoted brokers              | false                | yes       |
| exclude_recently_removed_brokers  | boolean   | whether allow replicas to be moved to recently removed broker                         | false                | yes       |
| destination_broker_ids            | boolean   | specify brokers to move replicas to                                                   | available brokers    | yes       |
| rebalance_disk                    | boolean   | whether to balance load between disks within brokers (requires JBOD Kafka deployment) | false                | yes       |
| json                              | boolean   | return in JSON format or not                                                          | false                | yes       | 
| verbose                           | boolean   | return detailed state information                                                     | false                | yes       |
| doAs                              | string    | propagated user by the trusted proxy service                                          | null                 | yes       | 
| fast_mode                         | boolean   | true to compute proposals in fast mode, false otherwise                               | true                 | yes       |
| reason                            | string    | reason for the request                                                                | "No reason provided" | yes       | 

Proposal can be generated based on **valid_window** or **valid_partitions**.

**valid_windows:** rebalance the cluster based on the information in the available valid snapshot windows. A valid snapshot window is a windows whose valid monitored partitions coverage meets the requirements of all the goals. (This is the default behavior)

**valid_partitions:** rebalance the cluster based on all the available valid partitions. All the snapshot windows will be included in this case.

Users can only specify either `valid_windows` or `valid_partitions`, but not both.

Kafka cruise control tries to precompute the optimization proposal in the background and caches the best proposal to serve when user queries. If users want to have a fresh proposal without reading it from the proposal cache, set the `ignore_proposal_cache` flag to true. The precomputing always uses available valid partitions to generate the proposals.

By default the proposal will be returned from the cache where all the pre-defined goals are used. Detailed information about the reliability of the proposals will also be returned. If users want to run with a different set of goals, they can specify the `goals` parameter with the goal names (simple class name).

If `verbose` is turned on, Cruise Control will return all the generated proposals. Otherwise a summary of the proposals will be returned.

If `kafka_assigner` is turned on, the proposals will be generated in Kafka Assigner mode,i.e. Cruise Control behaves like [kafka assigner tool](https://github.com/linkedin/kafka-tools/wiki/Kafka-Assigner). This mode performs optimizations using the goals specific to Kafka Assigner -- i.e. goals with name `KafkaAssigner*`.

Users can specify `excluded_topics` to prevent certain topics' replicas from moving in the generated proposals.

If `use_ready_default_goals` is turned on, Cruise Control will use whatever ready goals(based on available metric data) to calculate the proposals.

### Query the user request result
The following get request allows user to get a full list of all the active/completed(and not recycled) tasks inside Cruise Control, with their initial request detail(request time/IP address/request URL and parameter) and UUID information. User can then use the returned UUID and URL to fetch the original final result of the specific request.

    GET /kafkacruisecontrol/user_tasks

Supported parameters are:

| PARAMETER             | TYPE      | DESCRIPTION                                                                           | DEFAULT              | OPTIONAL  |
|-----------------------|-----------|---------------------------------------------------------------------------------------|----------------------|-----------|
| user_task_ids         | list      | comma separated UUIDs to filter the task results Cruise Control report                | all tasks            | yes       | 
| client_ids            | list      | comma separated IP addresses to filter the task results Cruise Control report         | all users            | yes       | 
| entries               | integer   | number of partition load entries to report in response                                | `MAX_INT`            | yes       |
| endpoints             | list      | comma separated endpoints to filter the task results Cruise Control report            | all endpoints        | yes       | 
| types                 | string    | comma separated HTTP request types to filter the task results Cruise Control report   | all request types    | yes       | 
| json                  | boolean   | return in JSON format or not                                                          | false                | yes       | 
| fetch_completed_task  | boolean   | whether return the original request's final response                                  | false                | yes       |
| doAs                  | string    | propagated user by the trusted proxy service                                          | null                 | yes       |
| reason                | string    | reason for the request                                                                | "No reason provided" | yes       | 

User can use `user_task_ids`/`client_ids`/`endpoints`/`types` make Cruise Control only return requests they are interested. By default all the requests get returned.

If `fetch_completed_task` is set to `true`, the original response of each request will be returned. In the case where a task completed with errors the response will be `CompletedWithError`.

### Query user permissions

The following get request will return the currently authenticated user's permissions as a list of the assigned user roles. The role list can include any of the following: `"ADMIN"`, `"USER"` and `"VIEWER"`. 

    GET /kafkacruisecontrol/permissions

Supported parameters are:

| PARAMETER           | TYPE    | DESCRIPTION                                      | DEFAULT              | OPTIONAL |
|---------------------|---------|--------------------------------------------------|----------------------|----------|
| doAs                | string  | propagated user by the trusted proxy service     | null                 | yes      |
| json                | boolean | return in JSON format or not                     | false                | yes      | 
| get_response_schema | boolean | return JSON schema in the response header or not | false                | yes      |
| reason              | string  | reason for the request                           | "No reason provided" | yes      |

In case there is no security enabled in the Kafka Cluster, the request will return with `400` HTTP error code and the following message: `"Unable to retrieve privilege information for an unsecure connection". 

## POST Requests
The post requests of Kafka Cruise Control REST API are operations that will have impact on the Kafka cluster. The post operations include:
* [Trigger a workload balance](#trigger-a-workload-balance)
* [Add a list of new brokers to Kafka Cluster](#add-a-list-of-new-brokers-to-kafka-cluster)
* [Decommission a list of brokers from the Kafka cluster](#decommission-a-list-of-brokers-from-the-kafka-cluster)
* [Fix offline replicas in Kafka cluster](#fix-offline-replicas-in-kafka-cluster)
* [Demote a list of brokers from the Kafka cluster](#demote-a-list-of-brokers-from-the-kafka-cluster)
* [Remove disks from brokers in the Kafka cluster](#remove-disks-from-brokers-in-the-kafka-cluster)
* [Stop the current proposal execution task](#stop-the-current-proposal-execution-task)
* [Pause metrics load sampling](#pause-metrics-load-sampling)
* [Resume metrics load sampling](#resume-metrics-load-sampling)
* [Change Kafka topic configuration](#change-kafka-topic-configuration)
* [Change Cruise Control configuration](#change-cruise-control-configuration)

**Most of the POST actions has a dry-run mode, which only generate the proposals and estimated result but not really execute the proposals.** To avoid accidentally triggering of data movement, by default all the POST actions are in dry run mode. **To let Kafka Cruise Control actually move data, users need to explicitly set `dryrun=false`.**

### Trigger a workload balance
The following POST request will let Kafka Cruise Control rebalance a Kafka cluster

    POST /kafkacruisecontrol/rebalance

Supported parameters are:

| PARAMETER                                     | TYPE      | DESCRIPTION                                                                                                                           | DEFAULT               | OPTIONAL  |
|-----------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| dryrun                                        | boolean   | whether dry-run the request or not                                                                                                    | true                  | yes       | 
| data_from                                     | string    | whether generate proposal from available valid partitions or valid windows                                                            | `VALID_WINDOW`        | yes       | 
| goals                                         | list      | list of goals used to generate proposal                                                                                               | default goals         | yes       | 
| kafka_assigner                                | boolean   | whether to use Kafka assigner mode to generate proposals                                                                              | false                 | yes       | 
| allow_capacity_estimation                     | boolean   | whether to allow broker capacity to be estimated                                                                                      | true                  | yes       | 
| concurrent_partition_movements_per_broker     | integer   | upper bound of ongoing replica movements going into/out of each broker                                                                | null                  | yes       | 
| max_partition_movements_in_cluster            | integer   | upper bound of maximum replica movements going into/out of brokers across cluster                                                     | `default_config`                  | yes       | 
| concurrent_intra_broker_partition_movements   | integer   | upper bound of ongoing replica movements between disks within each broker                                                             | null                  | yes       | 
| concurrent_leader_movements                   | integer   | upper bound of ongoing leadership movements                                                                                           | null                  | yes       | 
| skip_hard_goal_check                          | boolean   | Whether allow hard goals be skipped in proposal generation                                                                            | false                 | yes       | 
| excluded_topics                               | regex     | regular expression to specify topics excluded from replica and leadership movement                                                    | null                  | yes       | 
| use_ready_default_goals                       | boolean   | whether only use ready goals to generate proposal                                                                                     | false                 | yes       | 
| exclude_recently_demoted_brokers              | boolean   | whether to allow leader replicas to be moved to recently demoted brokers                                                              | false                 | yes       | 
| exclude_recently_removed_brokers              | boolean   | whether allow replicas to be moved to recently removed broker                                                                         | false                 | yes       | 
| replica_movement_strategies                   | string    | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use    | null                  | yes       | 
| ignore_proposal_cache                         | boolean   | whether to ignore the cached proposal or not                                                                                          | false                 | yes       | 
| replication_throttle                          | long      | upper bound on the bandwidth used to move replicas (in bytes per second)                                                              | null                  | yes       | 
| destination_broker_ids                        | list      | specify brokers to move replicas to                                                                                                   | available brokers     | yes       | 
| rebalance_disk                                | boolean   | whether to balance load between disks within brokers (requires JBOD Kafka deployment)                                                 | false                 | yes       | 
| json                                          | boolean   | return in JSON format or not                                                                                                          | false                 | yes       | 
| verbose                                       | boolean   | return detailed state information                                                                                                     | false                 | yes       | 
| reason                                        | string    | reason for the request                                                                                                                | "No reason provided"  | yes       | 
| doAs                                          | string    | propagated user by the trusted proxy service                                                                                          | null                  | yes       | 
| fast_mode                                     | boolean   | true to compute proposals in fast mode, false otherwise                                                                               | true                  | yes       |

Similar to the [GET interface for getting proposals](https://github.com/linkedin/cruise-control/wiki/REST-APIs/_edit#get-optimization-proposals), the rebalance can also be based on available valid windows or available valid partitions.

User can specify goals to use for rebalance via `goals` parameter. When `goals` is provided, the cached proposals will be ignored.

When rebalancing a cluster, all the brokers in the cluster(except recently removed/demoted brokers if `exclude_recently_removed_brokers`/`exclude_recently_demoted_brokers` set to `true`) are eligible to give / receive replicas. All the brokers will be throttled during the partition movement. The throttling can be set in two ways.

* By throttling number of replica concurrently moving into a broker. It is gated by the request parameter `concurrent_partition_movements_per_broker` or config value `num.concurrent.partition.movements.per.broker` (if request parameter is not set), `concurrent_intra_partition_movements` or config value `num.concurrent.intra.broker.partition.movements`. Similarly, the number of concurrent partition leadership changes is gated by request parameter `concurrent_leader_movements` or config value `num.concurrent.leader.movements`.
* By throttling the bandwidth used to move replicas into a broker. It is gated by the request parameter `replication_throttle` or config value `default.replication.throttle` (if request parameter is not set).

### Add a list of new brokers to Kafka Cluster
The following POST request adds the given brokers to the Kafka cluster

    POST /kafkacruisecontrol/add_broker?brokerid=[id1,id2...]

Supported parameters are:

| PARAMETER                                 | TYPE      | DESCRIPTION                                                                                                                           | DEFAULT               | OPTIONAL  |
|-------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| brokerid                                  | list      | list of ids of new broker added to the cluster                                                                                        | N/A                   | **no**    | 
| dryrun                                    | boolean   | whether dry-run the request or not                                                                                                    | true                  | yes       | 
| data_from                                 | string    | whether generate proposal from available valid partitions or valid windows                                                            | `VALID_WINDOWS`       | yes       | 
| goals                                     | list      | list of goals used to generate proposal                                                                                               | default goals         | yes       | 
| kafka_assigner                            | boolean   | whether to use Kafka assigner mode to generate proposals                                                                              | false                 | yes       | 
| allow_capacity_estimation                 | boolean   | whether to allow broker capacity to be estimated from other broker in the cluster                                                     | true                  | yes       | 
| concurrent_partition_movements_per_broker | integer   | upper bound of ongoing replica movements going into/out of each broker                                                                | null                  | yes       | 
| max_partition_movements_in_cluster        | integer   | upper bound of maximum replica movements going into/out of brokers across cluster                                                     | `default_config`                  | yes       | 
| concurrent_leader_movements               | integer   | upper bound of ongoing leadership movements                                                                                           | null                  | yes       | 
| skip_hard_goal_check                      | boolean   | whether allow hard goals be skipped in proposal generation                                                                            | false                 | yes       | 
| excluded_topics                           | regex     | regular expression to specify topics excluded from replica and leadership movement                                                    | null                  | yes       | 
| use_ready_default_goals                   | boolean   | whether only use ready goals to generate proposal                                                                                     | false                 | yes       | 
| exclude_recently_demoted_brokers          | boolean   | whether to allow leader replicas to be moved to recently demoted brokers                                                              | false                 | yes       | 
| exclude_recently_removed_brokers          | boolean   | whether allow replicas to be moved to recently removed broker                                                                         | false                 | yes       | 
| replica_movement_strategies               | string    | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use    | null                  | yes       | 
| replication_throttle                      | long      | Upper bound on the bandwidth used to move replicas (in bytes per second)                                                              | null                  | yes       | 
| throttle_added_broker                     | boolean   | whether throttle replica movement to new broker or not                                                                                | true                 | yes       | 
| json                                      | boolean   | return in JSON format or not                                                                                                          | false                 | yes       | 
| verbose                                   | boolean   | return detailed state information                                                                                                     | false                 | yes       | 
| reason                                    | string    | reason for the request                                                                                                                | "No reason provided"  | yes       | 
| doAs                                      | string    | propagated user by the trusted proxy service                                                                                          | null                  | yes       | 
| fast_mode                                 | boolean   | true to compute proposals in fast mode, false otherwise                                                                               | true                  | yes       |


When adding new brokers to a Kafka cluster, Cruise Control makes sure that the **replicas will only be moved from the existing brokers to the provided new broker**, but not moved among existing brokers. 

Users can choose whether to throttle replica movement to the newly added broker via `throttle_added_broker`, in either case, the replica movement out of current broker are throttled, and the throttling can be set in the same way as [`rebalance` request](#trigger-a-workload-balance).

### Decommission a list of brokers from the Kafka cluster
The following POST request removes a list of brokers from the Kafka cluster:

    POST /kafkacruisecontrol/remove_broker?brokerid=[id1,id2...]

Supported parameters are:

| PARAMETER                                 | TYPE      | DESCRIPTION                                                                                                                           | DEFAULT               | OPTIONAL  |
|-------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| brokerid                                  | list      | list of ids of broker to be removed from the cluster                                                                                  | N/A                   | **no**    | 
| dryrun                                    | boolean   | whether dry-run the request or not                                                                                                    | true                  | yes       | 
| data_from                                 | string    | specify either generate proposal from available valid partitions or valid windows                                                     | `VALID_WINDOW`        | yes       | 
| goals                                     | list      | list of goals used to generate proposal                                                                                               | default goals         | yes       | 
| kafka_assigner                            | boolean   | whether to use Kafka assigner mode to generate proposals                                                                              | false                 | yes       | 
| allow_capacity_estimation                 | boolean   | whether to allow broker capacity to be estimated from other brokers in the cluster                                                    | true                  | yes       | 
| concurrent_partition_movements_per_broker | integer   | upper bound of ongoing replica movements going into/out of each broker                                                                | null                  | yes       | 
| max_partition_movements_in_cluster        | integer   | upper bound of maximum replica movements going into/out of brokers across cluster                                                     | `default_config`                  | yes       | 
| concurrent_leader_movements               | integer   | upper bound of ongoing leadership movements                                                                                           | null                  | yes       | 
| skip_hard_goal_check                      | boolean   | whether allow hard goals be skipped in proposal generation                                                                            | false                 | yes       | 
| excluded_topics                           | regex     | regular expression to specify topics excluded from replica and leadership movement                                                    | null                  | yes       | 
| use_ready_default_goals                   | boolean   | whether only use ready goals to generate proposal                                                                                     | false                 | yes       | 
| exclude_recently_demoted_brokers          | boolean   | whether to allow leader replicas to be moved to recently demoted brokers                                                              | false                 | yes       | 
| exclude_recently_removed_brokers          | boolean   | whether allow replicas to be moved to recently removed broker                                                                         | false                 | yes       | 
| replica_movement_strategies               | string    | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use    | null                  | yes       | 
| replication_throttle                      | long      | upper bound on the bandwidth used to move replicas (in bytes per second)                                                              | false                 | yes       | 
| throttle_removed_broker                   | boolean   | whether throttle replica movement out of the removed broker or not                                                                    | true                 | yes       | 
| destination_broker_ids                    | list      | specify brokers to move replicas to                                                                                                   | available brokers     | yes       | 
| json                                      | boolean   | return in JSON format or not                                                                                                          | false                 | yes       | 
| verbose                                   | boolean   | return detailed state information                                                                                                     | false                 | yes       | 
| reason                                    | string    | reason for the request                                                                                                                | "No reason provided"  | yes       | 
| doAs                                      | string    | propagated user by the trusted proxy service                                                                                          | null                  | yes       | 
| fast_mode                                 | boolean   | true to compute proposals in fast mode, false otherwise                                                                               | true                  | yes       |

Similar to adding brokers to a cluster, removing brokers from a cluster will **only move partitions from the brokers to be removed to the other existing brokers**. There won't be partition movements among remaining brokers. And user can specify the destination broker for these replica movement via `destination_broker_ids` parameter.

Like adding brokers, users can choose whether to throttle the removed broker during the partition movement. If the removed brokers are throttled, the number of partitions concurrently moving out of a broker, number of concurrent partition leadership changes and bandwidth used for replica movement are gated in the same way as add_broker above.

**Note if the topics specified in `excluded_topics` has replicas on the removed broker, the replicas will still get moved off the broker.**

### Fix offline replicas in Kafka cluster
The following POST request moves all the offline replicas from dead disks/brokers. **This endpoint is not available in `kafka_0_11_and_1_0` branch.** 

    POST /kafkacruisecontrol/fix_offline_replicas

Supported parameters are:

| PARAMETER                                 | TYPE      | DESCRIPTION                                                                                                                           | DEFAULT               | OPTIONAL  |
|-------------------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| dryrun                                    | boolean   | whether dry-run the request or not                                                                                                    | true                  | yes       |
| data_from                                 | string    | specify either generate proposal from available valid partitions or valid windows                                                     | `VALID_WINDOW`        | yes       | 
| goals                                     | list      | list of goals used to generate proposal                                                                                               | default goals         | yes       | 
| kafka_assigner                            | boolean   | whether to use Kafka assigner mode to generate proposals                                                                              | false                 | yes       | 
| allow_capacity_estimation                 | boolean   | whether to allow broker capacity to be estimated from other brokers in the cluster                                                    | true                  | yes       |
| concurrent_partition_movements_per_broker | integer   | upper bound of ongoing replica movements going into/out of each broker                                                                | null                  | yes       | 
| max_partition_movements_in_cluster        | integer   | upper bound of maximum replica movements going into/out of brokers across cluster                                                     | `default_config`                  | yes       | 
| concurrent_leader_movements               | integer   | upper bound of ongoing leadership movements                                                                                           | null                  | yes       | 
| skip_hard_goal_check                      | boolean   | Whether allow hard goals be skipped in proposal generation                                                                            | false                 | yes       | 
| excluded_topics                           | regex     | regular expression to specify topics excluded from replica and leadership movement                                                    | null                  | yes       | 
| use_ready_default_goals                   | boolean   | whether only use ready goals to generate proposal                                                                                     | false                 | yes       | 
| exclude_recently_demoted_brokers          | boolean   | whether to allow leader replicas to be moved to recently demoted brokers                                                              | false                 | yes       | 
| exclude_recently_removed_brokers          | boolean   | whether allow replicas to be moved to recently removed broker                                                                         | false                 | yes       | 
| replica_movement_strategies               | string    | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use    | null                  | yes       | 
| replication_throttle                      | long      | upper bound on the bandwidth used to move replicas (in bytes per second)                                                              | null                  | yes       | 
| json                                      | boolean   | return in JSON format or not                                                                                                          | false                 | yes       | 
| verbose                                   | boolean   | return detailed state information                                                                                                     | false                 | yes       | 
| reason                                    | string    | reason for the request                                                                                                                | "No reason provided"  | yes       | 
| doAs                                      | string    | propagated user by the trusted proxy service                                                                                          | null                  | yes       | 
| fast_mode                                 | boolean   | true to compute proposals in fast mode, false otherwise                                                                               | true                  | yes       |

Likewise, users can throttle partition movement, the throttling can be set in the same way as [`rebalance` request](#trigger-a-workload-balance).

**Note if the topics specified in `excluded_topics` has offline replicas, the replicas will still get moved to healthy brokers.**

### Demote a list of brokers from the Kafka cluster
The following POST request moves all the leader replicas away from a list of brokers.

    POST /kafkacruisecontrol/demote_broker?brokerid=[id1, id2...]

User can also request to move all the leader replicas away from the a list of disks via 

     POST /kafkacruisecontrol/demote_broker?brokerid_and_logdirs=[id1-logdir1, id2-logdir2...]

Supported parameters are:

| PARAMETER                         | TYPE      | DESCRIPTION                                                                                                                           | DEFAULT               | OPTIONAL                                  |
|-----------------------------------|-----------|---------------------------------------------------------------------------------------------------------------------------------------|-----------------------|-------------------------------------------|
| brokerid                          | list      | list of ids of broker to be demoted in the cluster                                                                                    | N/A                   | **no if brokerid_and_logdirs is not set** |
| brokerid_and_logdirs              | list      | list of broker id and logdir pair to be demoted in the cluster                                                                        | N/A                   |   **no if brokerids is not set**          |
| dryrun                            | boolean   | whether dry-run the request or not                                                                                                    | true                  | yes                                       |
| allow_capacity_estimation         | boolean   | whether to allow broker capacity to be estimated                                                                                      | true                  | yes                                       |
| concurrent_leader_movements       | integer   | upper bound of ongoing leadership movements                                                                                           | null                  | yes                                       |
| skip_urp_demotion                 | boolean   | whether skip demoting leader replicas for under replicated partitions                                                                 | true                  | yes                                       |
| exclude_follower_demotion         | boolean   | whether skip demoting follower replicas on the broker to be demoted                                                                   | true                  | yes                                       |
| exclude_recently_demoted_brokers  | boolean   | whether to allow leader replicas to be moved to recently demoted brokers                                                              | false                 | yes                                       |
| replica_movement_strategies       | string    | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use    | null                  | yes                                       |
| replication_throttle              | long      | upper bound on the bandwidth used to move replicas (in bytes per second)                                                              | null                  | yes                                       |
| json                              | boolean   | return in JSON format or not                                                                                                          | false                 | yes                                       |
| verbose                           | boolean   | return detailed state information                                                                                                     | false                 | yes                                       |
| reason                            | string    | reason for the request                                                                                                                | "No reason provided"  | yes                                       |
| doAs                              | string    | propagated user by the trusted proxy service                                                                                          | null                  | yes                                       |

Demoting a broker/disk is consist of tow steps.
  * Make all the replicas on given broker/disk the least preferred replicas for leadership election
    within their corresponding partitions
  * Trigger a preferred leader election on the partitions to migrate the leader replicas off the broker/disk

Set `skip_urp_demotion` to false will cancel outstanding operations if partitions stay under replicated; Set `exclude_follower_demotion` will skip operations on the partitions which only have follower replicas on the brokers/disks to be demoted. The purpose of the former is to prevent the URP recovery process from blocking the demotion execution, the latter ensures that the demotion operation is limited to leaders.

### Remove disks from brokers in the Kafka cluster
The following POST request moves all replicas from the specified disks to the remaining disks on the same broker. This enables decommissioning individual disks in a JBOD Kafka deployment without removing the entire broker.

    POST /kafkacruisecontrol/remove_disks?brokerid_and_logdirs=[id1-logdir1,id1-logdir2...]

For example, to remove two disks from broker `101`:

    POST /kafkacruisecontrol/remove_disks?brokerid_and_logdirs=101-/tmp/kafka-logs-1,101-/tmp/kafka-logs-2&dryrun=false

Supported parameters are:

| PARAMETER                 | TYPE      | DESCRIPTION                                                                                       | DEFAULT               | OPTIONAL  |
|---------------------------|-----------|---------------------------------------------------------------------------------------------------|-----------------------|-----------|
| brokerid_and_logdirs      | list      | list of broker id and logdir pairs for disks to remove from the cluster                           | N/A                   | **no**    |
| dryrun                    | boolean   | whether dry-run the request or not                                                                | true                  | yes       |
| stop_ongoing_execution    | boolean   | whether to stop the ongoing execution (if any) and start executing the given request              | false                 | yes       |
| reason                    | string    | reason for the request; required for non-dry-run requests when `request.reason.required` is enabled               | "No reason provided" when not required | conditional |
| json                      | boolean   | return in JSON format or not                                                                      | false                 | yes       |

Note that `brokerid_and_logdirs` takes comma as delimiter between two broker id and logdir pairs — i.e. we assume a valid logdir name contains no comma. Each pair uses a hyphen (`-`) as the delimiter between broker id and logdir name (e.g. `101-/tmp/kafka-logs-1`).

Unlike most other optimization endpoints, `remove_disks` does not accept a `goals` parameter. Proposal generation always uses `IntraBrokerDiskCapacityGoal` with disk-emptying enabled, regardless of the `intra.broker.goals` or `default.goals` configuration. This goal generates intra-broker replica movement proposals to move all replicas off the specified disks while keeping each remaining disk under the configured capacity threshold.

Before generating proposals, Cruise Control validates that:
* Each specified logdir exists on the corresponding broker.
* At least one logdir remains on each affected broker.
* The remaining disk capacity on each affected broker is sufficient to absorb the load from the disks being removed (based on the configured disk capacity threshold).

Replica movements are limited to disks within the same broker — there is no inter-broker replica movement. `stop_ongoing_execution` and `dryrun` cannot both be set to `true`.

### Stop the current proposal execution task
The following POST request will let Kafka Cruise Control stop an ongoing `rebalance`, `add_broker`,  `remove_broker`, `fix_offline_replica`, `topic_configuration`, `demote_broker` or `remove_disks` operation:

    POST /kafkacruisecontrol/stop_proposal_execution

Supported parameters are:

| PARAMETER           | TYPE      | DESCRIPTION                                                                                                   | DEFAULT               | OPTIONAL  |
|---------------------|-----------|---------------------------------------------------------------------------------------------------------------|-----------------------|-----------|
| force_stop          | boolean   | (not supported in Kafka 2.4 or above) stop an ongoing execution forcefully by deleting Kafka internal zNodes  | false                 | yes       |
| stop_external_agent | boolean   | (required Kafka 2.4 or above) stop an ongoing execution even if it is started by an external agent            | true                  | yes       |
| review_id           | integer   | review id for 2-step verification                                                                             | N/A                   | yes       |
| json                | boolean   | return in JSON format or not                                                                                  | false                 | yes       |
| get_response_schema | boolean   | return JSON schema in response header or not                                                                  | false                 | yes       |
| doAs                | string    | propagated user by the trusted proxy service                                                                  | null                  | yes       |
| reason              | string    | reason for the request                                                                                        | "No reason provided"  | yes       | 

Note that **Cruise Control does not wait for the ongoing batch to finish when it stops execution**, i.e. the in-progress batch may still be running after Cruise Control stops the execution.

The `stop_external_agent` parameter is a no-op for CC versions before `2.4.73` and `2.5.65`.

### Pause metrics load sampling
The following POST request will let Kafka Cruise Control pause an ongoing metrics sampling process:

    POST /kafkacruisecontrol/pause_sampling

Supported parameters are:

| PARAMETER | TYPE      | DESCRIPTION                                   | DEFAULT               | OPTIONAL  |
|-----------|-----------|-----------------------------------------------|-----------------------|-----------|
| reason    | string    | reason to pause sampling                      | "No reason provided"  | yes       | 
| json      | boolean   | return in JSON format or not                  | false                 | yes       | 
| doAs      | string    | propagated user by the trusted proxy service  | null                  | yes       | 

The reason to pause sampling will be recorded and shows up in `state` endpoint(under `LoadMonitor` sub state).

### Resume metrics load sampling
The following POST request will let Kafka Cruise Control resume a paused metrics sampling process:

    POST /kafkacruisecontrol/resume_sampling

| PARAMETER | TYPE      | DESCRIPTION                                   | DEFAULT               | OPTIONAL  |
|-----------|-----------|-----------------------------------------------|-----------------------|-----------|
| reason    | string    | reason to resume sampling                     | "No reason provided"  | yes       | 
| json      | boolean   | return in JSON format or not                  | false                 | yes       | 
| doAs      | string    | propagated user by the trusted proxy service  | null                  | yes       | 

The reason to resume sampling will be recorded and shows up in `state` endpoint(under `LoadMonitor` sub state).

### Change Kafka topic configuration
Currently Cruise Control only supports changing topic's replication factor via `topic_configuration` endpoint. Ultimately we want make Cruise Control the central place to change any topic configurations(partition count, retention time etc.).

The following POST request can change topic's replication factor.

    POST /kafkacruisecontrol/topic_configuration?topic=[topic_regex]&replication_factor=[target_replication_factor]

Supported parameters are:

| PARAMETER                                  | TYPE     | DESCRIPTION                                                                                                                          | DEFAULT                | OPTIONAL  |
|--------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------|------------------------|-----------|
| dryrun                                     | boolean  | whether dry-run the request or not                                                                                                   | true                   | yes       | 
| topic                                      | regex    | regular expression to specify subject topics                                                                                         | N/A                    | **no**    | 
| replication_factor                         | Integer  | target replication factor                                                                                                            | N/A                    | **no**    | 
| data_from                                  | string   | whether generate proposal from available valid partitions or valid windows                                                           | `VALID_WINDOW`         | yes       | 
| goals                                      | list     | list of goals used to generate proposal                                                                                              | default goals          | yes       | 
| allow_capacity_estimation                  | boolean  | whether to allow broker capacity to be estimated                                                                                     | true                   | yes       | 
| concurrent_partition_movements_per_broker  | integer  | upper bound of ongoing replica movements going into/out of each broker                                                               | null                   | yes       | 
| max_partition_movements_in_cluster         | integer  | upper bound of maximum replica movements going into/out of brokers across cluster                                                    | `default_config`       | yes       | 
| concurrent_leader_movements                | integer  | upper bound of ongoing leadership movements                                                                                          | null                   | yes       | 
| skip_hard_goal_check                       | boolean  | Whether allow hard goals be skipped in proposal generation                                                                           | false                  | yes       | 
| skip_rack_awareness_check                  | boolean  | Whether to skip the rack awareness sanity check or not                                                                               | false                  | yes       |
| exclude_recently_demoted_brokers           | Boolean  | Whether to allow leader replicas to be moved to recently demoted brokers                                                             | false                  | yes       | 
| exclude_recently_removed_brokers           | Boolean  | Whether allow replicas to be moved to recently removed broker                                                                        | false                  | yes       | 
| replica_movement_strategies                | string   | [replica movement strategy](https://github.com/linkedin/cruise-control/wiki/Pluggable-Components#replica-movement-strategy) to use   | null                   | yes       | 
| replication_throttle                       | long     | upper bound on the bandwidth used to move replicas (in bytes per second)                                                             | null                   | yes       | 
| json                                       | boolean  | return in JSON format or not                                                                                                         | false                  | yes       | 
| verbose                                    | boolean  | return detailed state information                                                                                                    | false                  | yes       | 
| reason                                     | string   | reason for the request                                                                                                               | "No reason provided"   | yes       | 
| doAs                                       | string   | propagated user by the trusted proxy service                                                                                         | null                   | yes       | 
| fast_mode                                  | boolean  | true to compute proposals in fast mode, false otherwise                                                                              | true                   | yes       |

Changing topic's replication factor will not move any existing replicas. `goals` are used to determine which replica to be deleted(to decrease topic's replication factor) and which broker to assign new replica (to increase topic's replication factor).

Note sometimes the topic regex can be too long to put at POST request head, in this case user can specify topic regex and target replication factor pairs in POST request body. For details, check [Change-topic-replication-factor-through-Cruise-Control wiki page](https://github.com/linkedin/cruise-control/wiki/Change-topic-replication-factor-through-Cruise-Control).

### Change Cruise Control configuration
Some Cruise Control configs can be changed dynamically via `admin` endpoint, which includes
* Dynamically change the partition and leadership concurrency and the interval between checking and updating (if needed) the progress of an ongoing execution.
* Enable/disable self-healing for the specified anomaly types.
* Drop selected recently removed/demoted brokers.
* Enable/disable the specified concurrency adjusters.
* Enable/disable the (At/Under)MinISR-based concurrency adjustment.

Supported parameters are:

| PARAMETER                                     | TYPE      | DESCRIPTION                                                               | DEFAULT               | OPTIONAL  |
|-----------------------------------------------|-----------|---------------------------------------------------------------------------|-----------------------|-----------|
| disable_self_healing_for                      | list      | list of anomaly types to disable self-healing                             | N/A                   | yes       |
| enable_self_healing_for                       | list      | list of anomaly types to enable self-healing                              | N/A                   | yes       |
| concurrent_partition_movements_per_broker     | integer   | upper bound of ongoing replica movements into/out of a broker             | N/A                   | yes       |
| max_partition_movements_in_cluster            | integer   | upper bound of max replica movements into/out of brokers across cluster   | N/A                   | yes       |                                                            
| concurrent_intra_broker_partition_movements   | integer   | upper bound of ongoing replica movements between disks within a broker    | N/A                   | yes       |
| concurrent_leader_movements                   | integer   | upper bound of ongoing leadership movements                               | N/A                   | yes       |
| drop_recently_removed_brokers                 | list      | list of id of recently removed brokers to be dropped                      | N/A                   | yes       |
| drop_recently_demoted_brokers                 | list      | list of id of recently demoted brokers to be dropped                      | N/A                   | yes       |
| doAs                                          | string    | propagated user by the trusted proxy service                              | null                  | yes       | 
| json                                          | boolean   | return in JSON format or not                                              | false                 | yes       |
| review_id                                     | integer   | review id for 2-step verification                                         | N/A                   | yes       |
| execution_progress_check_interval_ms          | long      | execution progress check interval in milliseconds                         | N/A                   | yes       |
| get_response_schema                           | boolean   | return JSON schema in response header or not                              | false                 | yes       |
| disable_concurrency_adjuster_for              | list      | disable concurrency adjuster for given concurrency types                  | N/A                   | yes       |
| enable_concurrency_adjuster_for               | list      | enable concurrency adjuster for given concurrency types                   | N/A                   | yes       |
| min_isr_based_concurrency_adjustment          | boolean   | enable (true) or disable (false) MinISR-based concurrency adjustment      | N/A                   | yes       |
| reason                                        | string    | reason for the request                                                    | "No reason provided"  | yes       | 

To Enable/disable self-healing, send POST request like:

     POST /kafkacruisecontrol/admin?disable_self_healing_for=[anomaly_type]

To increase/decrease execution concurrency, send POST request like:

     POST /kafkacruisecontrol/admin?concurrent_partition_movements_per_broker=[integer]

To drop recently removed/demoted brokers, send POST request like:

     POSTs /kafkacruisecontrol/admin?drop_recently_removed_brokers=[broker_ids]

### 2-step Verification

2-step verification aims to help users verify the command they (or their peers) intend to run by letting them review requests explicitly to approve or discard them, and enable execution of only the approved requests. Read [2 step verification for POST requests](https://github.com/linkedin/cruise-control/wiki/2-step-verification-for-POST-requests) for more detail.

### Rightsize the cluster with the Provisioner
The following POST request can create a request to the provisioner to rightsize the broker or partition of a cluster.

    POST /kafkacruisecontrol/rightsize

Supported parameters are:

| PARAMETER                           | TYPE      | DESCRIPTION                                                                       | DEFAULT               | OPTIONAL  |
|-------------------------------------|-----------|-----------------------------------------------------------------------------------|-----------------------|-----------|
| num_brokers_to_add                  | integer   | difference in broker count to rightsize towards                                   | -1                    | yes       |
| partition_count                     | integer   | target number of partitions to rightsize towards                                  | -1                    | yes       |
| topic                               | regex     | regular expression to specify subject topics                                      | null                  | yes       | 
| doAs                                | string    | propagated user by the trusted proxy service                                      | null                  | yes       |
| get_response_schema                 | boolean   | return JSON schema in response header or not                                      | false                 | yes       |
| json                                | boolean   | return in JSON format or not                                                      | false                 | yes       |
| reason                              | string    | reason for the request                                                            | "No reason provided"  | yes       | 
