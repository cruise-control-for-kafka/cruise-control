from typing import Set, Union, Collection  # noqa

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractSetOfChoicesParameter(AbstractParameter):
    lowercase_set_of_choices: Set[str] = set()

    def __init__(self, value: Union[str, Collection[str]]):
        if isinstance(value, Collection) and type(value) != str:
            value = ','.join(value)
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if type(self.value) != str and type(self.value) != Collection:
            raise ValueError(f"{self.value} must be either a string or a Collection")


class DisableSelfHealingForParameter(AbstractSetOfChoicesParameter):
    """disable_self_healing_for=[Set-of-{@link AnomalyType}]"""
    lowercase_set_of_choices = {'broker_failure', 'disk_failure', 'goal_violation', 'intra_broker_goal_violation',
                                'maintenance_event', 'metric_anomaly', 'topic_anomaly'}
    name = 'disable_self_healing_for'
    description = "The anomaly detectors for which to disable self-healing"
    argparse_properties = {
        'args': ('--disable-self-healing-for', '--disable-self-healing'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='ANOMALY_DETECTOR', choices=lowercase_set_of_choices, nargs='+',
                       type=str.lower)

    }


class EnableSelfHealingForParameter(AbstractSetOfChoicesParameter):
    """enable_self_healing_for=[Set-of-{@link AnomalyType}]"""
    lowercase_set_of_choices = {'broker_failure', 'disk_failure', 'goal_violation', 'intra_broker_goal_violation',
                                'maintenance_event', 'metric_anomaly', 'topic_anomaly'}
    name = 'enable_self_healing_for'
    description = "The anomaly detectors for which to enable self-healing"
    argparse_properties = {
        'args': ('--enable-self-healing-for', '--enable-self-healing'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='ANOMALY_DETECTOR', choices=lowercase_set_of_choices, nargs='+',
                       type=str.lower)
    }


class ResourceParameter(AbstractSetOfChoicesParameter):
    """resource=[RESOURCE]"""
    lowercase_set_of_choices = {"cpu", "nw_in", "nw_out", "disk"}
    name = 'resource'
    description = "The host and broker-level resource by which to sort the cruise-control response"
    argparse_properties = {
        'args': ('--resource',),
        'kwargs': dict(help=description, metavar='RESOURCE', choices=lowercase_set_of_choices, type=str.lower)
    }


class SubstatesParameter(AbstractSetOfChoicesParameter):
    """substates=[SUBSTATES]"""
    lowercase_set_of_choices = {'executor', 'analyzer', 'monitor', 'anomaly_detector'}
    name = 'substates'
    description = "The substates for which to retrieve state from cruise-control"
    argparse_properties = {
        'args': ('--substate', '--substates'),
        # nargs='+' allows for multiple of the set to be specified
        'kwargs': dict(help=description, metavar='SUBSTATE', choices=lowercase_set_of_choices, nargs='+',
                       type=str.lower)
    }
