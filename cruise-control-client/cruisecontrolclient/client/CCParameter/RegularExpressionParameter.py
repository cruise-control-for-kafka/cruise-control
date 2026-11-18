import re
from typing import Union, Pattern

from cruisecontrolclient.client.CCParameter.Parameter import AbstractParameter


class AbstractRegularExpressionParameter(AbstractParameter):
    def __init__(self, value: Union[str, Pattern]):
        AbstractParameter.__init__(self, value)

    def validate_value(self):
        if isinstance(self.value, Pattern):
            # If we've been passed a re.Pattern, obtain the string that comprised
            # this re.Pattern, and store that string as our value.
            #
            # This allows us to store a value that is the closest to what
            # cruise-control expects.
            self.value = self.value.pattern
        elif type(self.value) == str:
            # A string is not necessarily a regular expression; verify whether
            # it compiles into one.
            try:
                re.compile(self.value)
            except re.error as e:
                raise ValueError(f"{self.value} is not a valid regular expression", e)
        else:
            raise ValueError(f"{self.value} must be either a string or a re.Pattern")


class ExcludedTopicsParameter(AbstractRegularExpressionParameter):
    """excluded_topics=[pattern]"""

    name = "excluded_topics"
    description = "A regular expression matching which topics to exclude from this endpoint's action"
    argparse_properties = {
        "args": ("--excluded-topics", "--exclude-topics", "--exclude-topic"),
        "kwargs": dict(help=description, metavar="REGEX"),
    }


class IncludedTopicsParameter(AbstractRegularExpressionParameter):
    """included_topics=[pattern]"""

    name = "included_topics"
    description = ("A regular expression matching the only topics to include in this endpoint's action; "
                   "mutually exclusive with excluded_topics")
    argparse_properties = {
        "args": ("--included-topics", "--include-topics", "--include-topic"),
        "kwargs": dict(help=description, metavar="REGEX"),
    }


class TopicParameter(AbstractRegularExpressionParameter):
    """topic=[topic]"""

    name = "topic"
    description = "A regular expression matching the desired topics"
    argparse_properties = {
        "args": ("--topic",),
        "kwargs": dict(help=description, metavar="REGEX"),
    }
