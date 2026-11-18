# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# To be able to instantiate Endpoint objects
import cruisecontrolclient.client.Endpoint as Endpoint

# To be able to make more precise type hints
from typing import Dict, Tuple, Type


class ExecutionContext:
    # Define the in-order available endpoints for programmatically building the argparse CLI
    available_endpoints: Tuple[Type[Endpoint.AbstractEndpoint]] = (
        Endpoint.AddBrokerEndpoint,
        Endpoint.AdminEndpoint,
        Endpoint.BootstrapEndpoint,
        Endpoint.DemoteBrokerEndpoint,
        Endpoint.FixOfflineReplicasEndpoint,
        Endpoint.KafkaClusterStateEndpoint,
        Endpoint.LoadEndpoint,
        Endpoint.PartitionLoadEndpoint,
        Endpoint.PauseSamplingEndpoint,
        Endpoint.Permissions,
        Endpoint.ProposalsEndpoint,
        Endpoint.RebalanceEndpoint,
        Endpoint.RemoveBrokerEndpoint,
        Endpoint.ResumeSamplingEndpoint,
        Endpoint.RightsizeEndpoint,
        Endpoint.ReviewBoardEndpoint,
        Endpoint.ReviewEndpoint,
        Endpoint.StateEndpoint,
        Endpoint.StopProposalExecutionEndpoint,
        Endpoint.TrainEndpoint,
        Endpoint.TopicConfigurationEndpoint,
        Endpoint.UserTasksEndpoint
    )

    # A mapping from the names of the subparsers to the Endpoint object they should instantiate.
    #
    # Like:
    # {"add_broker": AddBrokerEndpoint,
    #  "add_brokers": AddBrokerEndpoint,
    #  "admin": AdminEndpoint,
    #  "bootstrap": BootstrapEndpoint,
    #  "demote_broker": DemoteBrokerEndpoint,
    #  "demote_brokers": DemoteBrokerEndpoint,
    #  "fix_offline_replicas": FixOfflineReplicasEndpoint,
    #  "kafka_cluster_state": KafkaClusterStateEndpoint,
    #  "load": LoadEndpoint,
    #  "partition_load": PartitionLoadEndpoint,
    #  "pause_sampling": PauseSamplingEndpoint,
    #  "permissions": PermissionsEndpoint,
    #  "proposals": ProposalsEndpoint,
    #  "rebalance": RebalanceEndpoint,
    #  "remove_broker": RemoveBrokerEndpoint,
    #  "remove_brokers": RemoveBrokerEndpoint,
    #  "resume_sampling": ResumeSamplingEndpoint,
    #  "rightsize": RightsizeEndpoint,
    #  "state": StateEndpoint,
    #  "stop_proposal_execution": StopProposalExecutionEndpoint,
    #  "stop": StopProposalExecutionEndpoint,
    #  "train": TrainEndpoint,
    #  "user_task": UserTasksEndpoint,
    #  "user_tasks": UserTasksEndpoint}
    dest_to_Endpoint: Dict[str, Type[Endpoint.AbstractEndpoint]] = {}
    available_parameter_set = set()
    for endpoint in available_endpoints:
        dest_to_Endpoint[endpoint.name] = endpoint
        if 'aliases' in endpoint.argparse_properties['kwargs']:
            endpoint_aliases = endpoint.argparse_properties['kwargs']['aliases']
            for alias in endpoint_aliases:
                dest_to_Endpoint[alias] = endpoint
        for parameter in endpoint.available_Parameters:
            available_parameter_set.add(parameter)

    # A mapping from the names of the argparse parameters to their cruise-control parameter name
    #
    # Like:
    # {'allow_capacity_estimation': 'allow_capacity_estimation',
    #  'brokers': 'brokerid',
    #  'clearmetrics': 'clearmetrics',
    #  'client_id': 'client_ids',
    #  'concurrency': 'concurrent_partition_movements_per_broker',
    #  'data_from': 'data_from',
    #  'disable_self_healing_for': 'disable_self_healing_for',
    #  'dry_run': 'dryRun',
    #  'enable_self_healing_for': 'enable_self_healing_for',
    #  'end_timestamp': 'end',
    #  'endpoint': 'endpoints',
    #  'exclude_follower_demotion': 'exclude_follower_demotion',
    #  'exclude_recently_demoted_brokers': 'exclude_recently_demoted_brokers',
    #  'exclude_recently_removed_brokers': 'exclude_recently_removed_brokers',
    #  'excluded_topics': 'excluded_topics',
    #  'goals': 'goals',
    #  'included_topics': 'included_topics',
    #  'ignore_proposal_cache': 'ignore_proposal_cache',
    #  'leader_concurrency': 'concurrent_leader_movements',
    #  'max_load': 'max_load',
    #  'min_valid_partition_ratio': 'min_valid_partition_ratio',
    #  'number_of_entries_to_show': 'entries',
    #  'partition': 'partition',
    #  'reason': 'reason',
    #  'resource': 'resource',
    #  'skip_hard_goal_check': 'skip_hard_goal_check',
    #  'skip_urp_demotion': 'skip_urp_demotion',
    #  'start_timestamp': 'start',
    #  'strategies': 'replica_movement_strategies',
    #  'substate': 'substates',
    #  'super_verbose': 'super_verbose',
    #  'text': 'json',
    #  'throttle_removed_broker': 'throttle_removed_broker',
    #  'timestamp': 'time',
    #  'topic': 'topic',
    #  'types': 'types',
    #  'use_ready_default_goals': 'use_ready_default_goals',
    #  'user_task_ids': 'user_task_ids',
    #  'verbose': 'verbose'}
    flag_to_parameter_name: Dict[str, str] = {}
    for parameter in available_parameter_set:
        argparse_parameter_name = None
        # argparse names this parameter's flag after the first string in 'args'
        for possible_argparse_name in parameter.argparse_properties['args']:
            # argparse chooses flag names only from the --flags
            if not possible_argparse_name.startswith('-'):
                argparse_parameter_name = possible_argparse_name.replace('-', '_')
            elif not possible_argparse_name.startswith('--'):
                continue
            else:
                argparse_parameter_name = possible_argparse_name.lstrip('-').replace('-', '_')
                break
        if argparse_parameter_name and argparse_parameter_name in flag_to_parameter_name:
            raise ValueError(f"Colliding parameter flags: {argparse_parameter_name}")
        else:
            flag_to_parameter_name[argparse_parameter_name] = parameter.name

    def __init__(self):
        # Define the argparse flags that cannot be used for endpoint parameters
        #
        # This helps the multi-stage argument parsing not to conflict with itself
        # during the different stages.
        #
        # To this will probably be added things like:
        #   'add_parameter'
        #   'endpoint_subparser'
        #   'socket_address'
        #   'remove_parameter'
        #
        # If you've added flags to the argument_parser, they should be added to
        # this set.
        self.non_parameter_flags = set()
