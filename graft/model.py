from datetime import datetime
from dataclasses import dataclass, field, fields

_integer = ((lambda x: x >= 0), "Expected zero or a positive integer. Got {0} instead.")
_zero_or_positive = dict(validators=(_integer,))


def _validate_dataclass(instance):
    for fld in fields(instance):
        value = getattr(instance, fld.name)
        _validate_type(fld.name, value, fld.type)
        for validator, message in fld.metadata.get('validators', tuple()):
            if not validator(value):
                raise ValueError(f"On '{fld.name}'. {message.format(value)}")


def _validate_type(name, value, expected):
    if not isinstance(value, expected):
        msg = (f"Expected '{name}' to be of type: '{expected}'. "
               f"Got: '{type(value).__name__}' instead.")
        raise TypeError(msg)


def validate_types(objects):
    for name, (value, expected) in objects.items():
        _validate_type(name, value, expected)


@dataclass(frozen=True)
class Index:
    """Log indices are composed of an `index` and `term` integers"""
    index: int = field(metadata=_zero_or_positive)
    term: int = field(metadata=_zero_or_positive)

    __post_init__ = _validate_dataclass


@dataclass(frozen=True)
class Entry:
    """Entries are composed of a `term` (int) and an `item`, which can be anything"""
    term: int = field(metadata=_zero_or_positive)
    item: object

    __post_init__ = _validate_dataclass


@dataclass(frozen=True)
class _BaseMessage:
    """Messages are frozen and include at least creation `time`, `sender` and `term`"""
    time: datetime = field(default_factory=datetime.now, init=False)  # for debug mainly
    sender: int  # peer that has sent the message
    term: int = field(metadata=_zero_or_positive)

    __post_init__ = _validate_dataclass


@dataclass(frozen=True)
class AppendEntriesRequest(_BaseMessage):
    after: Index  # log index after which the entries should be added
    entries: tuple  # log entries to store (empty for heartbeat)
    leader_commit: int = field(metadata=_zero_or_positive)


@dataclass(frozen=True)
class AppendEntriesReply(_BaseMessage):
    success: bool  # true if follower successfully added requested entries
    match_index: int = field(metadata=_zero_or_positive)  # sender's known matched index


@dataclass(frozen=True)
class VoteRequest(_BaseMessage):
    last_log_index: Index  # index of candidate’s (sender) last log entry


@dataclass(frozen=True)
class VoteReply(_BaseMessage):
    granted: bool  # true means candidate received vote
