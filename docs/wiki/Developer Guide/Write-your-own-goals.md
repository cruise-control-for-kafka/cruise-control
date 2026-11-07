Cruise Control has provided a few goals out of the box for users to choose from. In addition to that, Cruise Control allows users to write their own goals to optimize a Kafka cluster in whichever way they want. We encourage users to implement more Cruise Control goals and share it with the community.

## Goal Interface
A Cruise Control goal is an implementation of the [`Goal interface`](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/analyzer/goals/Goal.java)

We have provided an [abstract goal](https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/analyzer/goals/AbstractGoal.java) class with some defined steps of optimization.

## Tips for implementing a goal
* **Cluster Healthiness** - It is important to always check if a cluster has dead brokers or not. Usually an unhealthy cluster needs to be handled differently because something has to be done to fix it. And sometimes you may not want to do anything other than fixing the cluster.
* **Previously Optimized Goals** - During the implementation, always make sure all the operations you plan to do meets the requirement of the previously optimized goals (i.e. the goals with higher priority) by invoking the `isProposalAcceptable(BalancingProposal, ClusterModel)` method of optimized goals.
* **Excluded Topics** - remember to check excluded topics list so that those topics are not touched.
* **Intra-Broker Goals** - a goal that only moves replicas between the disks of a broker (like the goals in `intra.broker.goals`) should override `isIntraBrokerGoal()` to return `true`: it cannot change the load of a broker, so the stats shown for its result are disk-level only.

## Still have questions?
Please ask in our [Slack channel](https://join.slack.com/t/cruise-control-corp/shared_invite/zt-491ndlgpw-M1BOo97QMcYGDoXOvfI1jA).
