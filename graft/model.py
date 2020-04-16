from datetime import datetime
from dataclasses import dataclass, field


def _validate_from_annotations(instance):
    objects = {getattr(instance, attr): cls for attr, cls in instance.__annotations__.items()}
    validate_types(objects)


def validate_types(objects):
    for obj, objtype in objects.items():
        if not isinstance(obj, objtype):
            msg = (f"Expected '{obj}' to be of type: '{objtype}'. "
                   f"Got: '{type(obj).__name__}' instead.")
            raise TypeError(msg)


# log internals
@dataclass(frozen=True)
class Index:
    """Log indices are composed of an `index` and `term` integers"""
    index: int
    term: int

    def __post_init__(self):
        _validate_from_annotations(self)
        if (index := self.index) < 0:
            msg = f"'index' should be a value greater than 0. Got {index} instead."
            raise ValueError(msg)


@dataclass(frozen=True)
class Entry:
    """Entries are composed of a `term` (int) and an `item`, which can be anything"""
    term: int
    item: object

    __post_init__ = _validate_from_annotations


# messages
@dataclass(frozen=True)
class _BaseMessage:
    """Messages are frozen and include at least creation `time`, `sender` and `term`"""
    time: datetime = field(default_factory=datetime.now, init=False)  # for debug mainly
    sender: int  # peer that has sent the message
    term: int

    __post_init__ = _validate_from_annotations


@dataclass(frozen=True)
class AppendEntriesRequest(_BaseMessage):
    after_log_index: int  # index of log entry immediately preceding new ones
    after_log_index_term: int  # index' term entry after which entries will be appended
    entries: tuple  # log entries to store (empty for heartbeat)
    leader_commit: int  # leader’s commit index


@dataclass(frozen=True)
class AppendEntriesReply(_BaseMessage):
    success: bool  # true if follower contained entry matching index and index_term
    match_index: int  # track what index was requested to be appended on


@dataclass(frozen=True)
class VoteRequest(_BaseMessage):
    last_log_index: int  # index of candidate’s (sender) last log entry
    last_log_term: int  # term of candidate’s (sender) last log entry


@dataclass(frozen=True)
class VoteReply(_BaseMessage):
    granted: bool  # true means candidate received vote
