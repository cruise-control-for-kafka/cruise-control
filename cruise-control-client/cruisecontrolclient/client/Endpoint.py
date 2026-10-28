# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

# For tighter subclass inheritance
from abc import ABCMeta

# To allow us to make use of the Parameter convenience class
import cruisecontrolclient.client.CCParameter as CCParameter

# To allow us to make more-precise type hints
from typing import Callable, ClassVar, Dict, List, Tuple, Union


class AbstractEndpoint(metaclass=ABCMeta):
    """
    An abstract representation of a cruise-control endpoint.

    Note that this class also provides methods for returning a correctly-
    concatenated endpoint with parameters.
    """

    # A string, like 'load' or 'state'
    # Meant to match those enumerated Strings at:
    # https://github.com/cruise-control-for-kafka/cruise-control/blob/main/cruise-control/src/main/java/com/linkedin/kafka/cruisecontrol/servlet/CruiseControlEndPoint.java#L16
    name: ClassVar[str]

    # A human-readable string that describes this endpoint
    description: ClassVar[str]

    # A string, like 'GET' or 'POST'
    http_method: ClassVar[str]

    # Whether this endpoint can mutate the kafka cluster via a proposal execution.
    #
    # Primarily, this flag is intended to signal to users of this client
    # when they may need to perform de-conflict checks on the targeted kafka cluster.
    can_execute_proposal: ClassVar[bool]

    # An ordered collection of the known Parameter classes that can be instantiated for this Endpoint.
    available_Parameters: ClassVar[Tuple[CCParameter.AbstractParameter]]

    # Define a convenience data structure to help in programmatically building CLIs
    argparse_properties: ClassVar[Dict[str, Union[Tuple[str], Dict[str, str]]]] = \
        {
            'args': (),
            'kwargs': {}
        }

    def __init__(self):
        # A mapping of 'parameter' strings
        self.parameter_name_to_available_Parameters: Dict[
            str, Callable[[Union[str, int, bool]], CCParameter.AbstractParameter]] = \
            {ap.name: ap for ap in self.available_Parameters}

        # Stores the instantiated Parameters for this Endpoint.
        #
        # As parameters are added via add_param, if their 'parameter'
        self.parameter_name_to_instantiated_Parameters: Dict[str, CCParameter.AbstractParameter] = {}

        # Stores the URL parameters for which there is no Parameter class defined.
        #
        # This is intended to future-proof against cruise-control adding new
        # parameters before this client has a chance to implement them.
        self.parameter_name_to_value: Dict[str, str] = {}

    def add_param(self, parameter_name: str, value: Union[str, int, bool]) -> None:
        """
        Adds this parameter to this Endpoint, overriding any previous value-definition
        for this parameter.

        If the supplied 'parameter' matches one of the known Parameters for this
        Endpoint, that Parameter will be instantiated and its value validated.
        If the supplied value is not valid, Parameter will raise a ValueError.

        If the supplied 'parameter' does not match one of the known Parameters
        for this Endpoint, that 'parameter': 'value' mapping will be added to
        self.parameter_name_to_value without validation.

        :param parameter_name:
        :param value:
        :return:
        """
        if parameter_name in self.parameter_name_to_available_Parameters:
            Parameter_to_instantiate = self.parameter_name_to_available_Parameters[parameter_name]
            self.parameter_name_to_instantiated_Parameters[parameter_name] = Parameter_to_instantiate(value)
        else:
            self.parameter_name_to_value[parameter_name] = value

    def get_value(self, parameter_name: str) -> Union[str, None]:
        """
        Returns value if this parameter exists in this endpoint.

        Returns None otherwise

        :param parameter_name:
        :return:
        """
        if parameter_name in self.parameter_name_to_instantiated_Parameters:
            return self.parameter_name_to_instantiated_Parameters[parameter_name].value
        elif parameter_name in self.parameter_name_to_value:
            return self.parameter_name_to_value[parameter_name]
        else:
            return None

    def has_param(self, parameter_name: str) -> bool:
        """
        Returns True if this endpoint already has this parameter, False otherwise.

        :param parameter_name:
        :return:
        """
        return parameter_name in self.parameter_name_to_instantiated_Parameters or parameter_name in self.parameter_name_to_value

    def remove_param(self, parameter_name: str) -> None:
        """
        Remove this parameter from this Endpoint.

        If the supplied 'parameter' matches one of the known Parameters for this
        Endpoint, that Parameter will be removed from self.parameter_name_to_instantiated_Parameters.

        If the supplied 'parameter' does not match one of the known Parameters
        for this Endpoint, that 'parameter': 'value' mapping will be removed from
        self.parameter_name_to_value, if present.

        :param parameter_name:
        :return:
        """
        if parameter_name in self.parameter_name_to_instantiated_Parameters:
            self.parameter_name_to_instantiated_Parameters.pop(parameter_name)
        elif parameter_name in self.parameter_name_to_value:
            self.parameter_name_to_value.pop(parameter_name)

    def get_composed_params(self) -> Dict[str, Union[bool, int, str]]:
        """
        Returns a requests-compatible dictionary of this Endpoint's current parameters.

        :return: A dict like:
            {'json': True,
             'allow_capacity_estimation': False}
        """
        # All parameter:value mappings that have been specified for this endpoint.
        #
        # First, add the parameter=value pairs from the Parameters we could instantiate.
        # Next, add (or override) with the parameter=value pairs that were explicitly defined.
        combined_parameter_to_value = {}

        # Update our mapping with parameter: value pairs from Parameter objects
        if self.parameter_name_to_instantiated_Parameters:
            combined_parameter_to_value.update(
                {name: ip.value for name, ip in self.parameter_name_to_instantiated_Parameters.items()})
        # Update our mapping with parameter: value pairs that lack Parameter objects
        if self.parameter_name_to_value:
            combined_parameter_to_value.update(self.parameter_name_to_value)

        return combined_parameter_to_value


class AddBrokerEndpoint(AbstractEndpoint):
    name = "add_broker"
    description = "Move partitions to the specified brokers, according to the specified goals"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.BrokerIdParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.ExcludedTopicsParameter,
        CCParameter.GoalsParameter,
        CCParameter.IncludedTopicsParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.SkipHardGoalCheckParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.ThrottleRemovedBrokerParameter,
        CCParameter.UseReadyDefaultGoalsParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=['add_brokers', 'add-broker', 'add-brokers'], help=description)
    }

    def __init__(self, broker_ids: Union[str, List[str]]):
        AbstractEndpoint.__init__(self)
        self.add_param("brokerid", broker_ids)


class AdminEndpoint(AbstractEndpoint):
    name = "admin"
    description = "Used to change runtime configurations on the cruise-control server itself"
    http_method = "POST"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DisableSelfHealingForParameter,
        CCParameter.DropRecentlyDemotedBrokersParameter,
        CCParameter.DropRecentlyRemovedBrokersParameter,
        CCParameter.EnableSelfHealingForParameter,
        CCParameter.JSONParameter,
        CCParameter.ReviewIDParameter,
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class BootstrapEndpoint(AbstractEndpoint):
    name = "bootstrap"
    description = "Bootstrap the load monitor"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.ClearMetricsParameter,
        CCParameter.EndParameter,
        CCParameter.JSONParameter,
        CCParameter.StartParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class DemoteBrokerEndpoint(AbstractEndpoint):
    name = "demote_broker"
    description = "Remove leadership and preferred leadership from the specified brokers"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.BrokerIdParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeFollowerDemotionParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.SkipURPDemotionParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=['demote_brokers', 'demote-broker', 'demote-brokers'], help=description)
    }

    def __init__(self, broker_ids: Union[str, List[str]]):
        AbstractEndpoint.__init__(self)
        self.add_param("brokerid", broker_ids)


class FixOfflineReplicasEndpoint(AbstractEndpoint):
    # Warning, this Endpoint is only supported in kafka 1.1 and above
    name = "fix_offline_replicas"
    description = "Fixes the offline replicas in the cluster (kafka 1.1+ only)"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.ExcludedTopicsParameter,
        CCParameter.GoalsParameter,
        CCParameter.IncludedTopicsParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.SkipHardGoalCheckParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.UseReadyDefaultGoalsParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class KafkaClusterStateEndpoint(AbstractEndpoint):
    name = "kafka_cluster_state"
    description = "Get under-replicated and offline partitions (and under MinISR partitions in kafka 2.0+)"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.TopicParameter,
        CCParameter.JSONParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class LoadEndpoint(AbstractEndpoint):
    name = "load"
    description = "Get the load on each kafka broker"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.JSONParameter,
        CCParameter.TimeParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }

    def __init__(self):
        AbstractEndpoint.__init__(self)


class PartitionLoadEndpoint(AbstractEndpoint):
    name = "partition_load"
    description = "Get the resource load for each partition"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.EndParameter,
        CCParameter.EntriesParameter,
        CCParameter.JSONParameter,
        CCParameter.MaxLoadParameter,
        CCParameter.MinValidPartitionRatioParameter,
        CCParameter.PartitionParameter,
        CCParameter.ResourceParameter,
        CCParameter.StartParameter,
        CCParameter.TopicParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class PauseSamplingEndpoint(AbstractEndpoint):
    name = "pause_sampling"
    description = "Pause metrics load sampling"
    http_method = "POST"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReviewIDParameter,
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }

class PermissionsEndpoint(AbstractEndpoint):
    name = "permissions"
    description = "Get the permissions of a user specified as a query parameter"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }

class ProposalsEndpoint(AbstractEndpoint):
    name = "proposals"
    description = "Get current proposals"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.DataFromParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.ExcludedTopicsParameter,
        CCParameter.GoalsParameter,
        CCParameter.IgnoreProposalCacheParameter,
        CCParameter.IncludedTopicsParameter,
        CCParameter.JSONParameter,
        CCParameter.UseReadyDefaultGoalsParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class RebalanceEndpoint(AbstractEndpoint):
    name = "rebalance"
    description = "Rebalance the partition distribution in the kafka cluster, according to the specified goals"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DestinationBrokerIdsParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.ExcludedTopicsParameter,
        CCParameter.GoalsParameter,
        CCParameter.IgnoreProposalCacheParameter,
        CCParameter.IncludedTopicsParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.SkipHardGoalCheckParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.UseReadyDefaultGoalsParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }

    def __init__(self):
        AbstractEndpoint.__init__(self)


class RemoveBrokerEndpoint(AbstractEndpoint):
    name = "remove_broker"
    description = "Remove all partitions from the specified brokers, according to the specified goals"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.BrokerIdParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DestinationBrokerIdsParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.ExcludedTopicsParameter,
        CCParameter.GoalsParameter,
        CCParameter.IncludedTopicsParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.SkipHardGoalCheckParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.ThrottleRemovedBrokerParameter,
        CCParameter.UseReadyDefaultGoalsParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=['remove_brokers', 'remove-broker', 'remove-brokers'], help=description)
    }

    def __init__(self, broker_ids: Union[str, List[str]]):
        AbstractEndpoint.__init__(self)
        self.add_param("brokerid", broker_ids)


class ResumeSamplingEndpoint(AbstractEndpoint):
    name = "resume_sampling"
    description = "Resume metrics load sampling"
    http_method = "POST"
    can_execute_proposal = False
    available_Parameters = {
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReviewIDParameter,
    }
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class ReviewEndpoint(AbstractEndpoint):
    name = "review"
    description = "Create, approve, or discard reviews"
    http_method = "POST"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.ApproveParameter,
        CCParameter.DiscardParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class ReviewBoardEndpoint(AbstractEndpoint):
    name = "review_board"
    description = "View already-created reviews"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.JSONParameter,
        CCParameter.ReviewIDsParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class RightsizeEndpoint(AbstractEndpoint):
    name = "rightsize"
    description = "Rightsize the broker or partition count"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.JSONParameter,
        CCParameter.TopicParameter,
        CCParameter.PartitionCountParameter,
        CCParameter.NumBrokersToAddParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class StateEndpoint(AbstractEndpoint):
    name = "state"
    description = "Get the state of cruise control"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.JSONParameter,
        CCParameter.SubstatesParameter,
        CCParameter.SuperVerboseParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }

    def __init__(self):
        AbstractEndpoint.__init__(self)
        # Note that we most often want the executor substate, so set that
        # as a default parameter
        self.add_param("substates", "executor")


class StopProposalExecutionEndpoint(AbstractEndpoint):
    name = "stop_proposal_execution"
    description = "Stop the currently-executing proposal"
    http_method = "POST"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.ForceStopParameter,
        CCParameter.JSONParameter,
        CCParameter.ReviewIDParameter,
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-'), 'stop'], help=description)
    }


class TopicConfigurationEndpoint(AbstractEndpoint):
    name = "topic_configuration"
    description = "Update the configuration of the specified topics"
    http_method = "POST"
    can_execute_proposal = True
    available_Parameters = (
        CCParameter.AllowCapacityEstimationParameter,
        CCParameter.ConcurrentLeaderMovementsParameter,
        CCParameter.ConcurrentPartitionMovementsPerBrokerParameter,
        CCParameter.DryRunParameter,
        CCParameter.ExcludeRecentlyDemotedBrokersParameter,
        CCParameter.ExcludeRecentlyRemovedBrokersParameter,
        CCParameter.GoalsParameter,
        CCParameter.JSONParameter,
        CCParameter.ReasonParameter,
        CCParameter.ReplicaMovementStrategiesParameter,
        CCParameter.ReplicationFactorParameter,
        CCParameter.ReviewIDParameter,
        CCParameter.SkipHardGoalCheckParameter,
        CCParameter.SkipRackAwarenessCheckParameter,
        CCParameter.StopOngoingExecutionParameter,
        CCParameter.TopicParameter,
        CCParameter.VerboseParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=[name.replace('_', '-')], help=description)
    }


class TrainEndpoint(AbstractEndpoint):
    name = "train"
    description = "Train the linear regression model"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.EndParameter,
        CCParameter.JSONParameter,
        CCParameter.StartParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(help=description)
    }


class UserTasksEndpoint(AbstractEndpoint):
    name = "user_tasks"
    description = "Get the recent user tasks from cruise control"
    http_method = "GET"
    can_execute_proposal = False
    available_Parameters = (
        CCParameter.ClientIdsParameter,
        CCParameter.EndpointsParameter,
        CCParameter.EntriesParameter,
        CCParameter.JSONParameter,
        CCParameter.TypesParameter,
        CCParameter.UserTaskIdsParameter
    )
    argparse_properties = {
        'args': (name,),
        'kwargs': dict(aliases=['user_task', 'user-tasks', 'user-task'], help=description)
    }
