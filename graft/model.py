from dataclasses import dataclass


def _validate_from_annotations(instance):
    objects = {getattr(instance, attr): cls for attr, cls in instance.__annotations__.items()}
    validate_types(objects)


def validate_types(objects):
    for obj, objtype in objects.items():
        if not isinstance(obj, objtype):
            msg = (f"Expected '{obj}' to be of type: '{objtype}'. "
                   f"Got: '{type(obj).__name__}' instead.")
            raise TypeError(msg)


# logger internals
@dataclass(frozen=True)
class Index:
    """Index of a raft logger.

    Index should always be zero or a positive integer.
    """
    index: int
    term: int

    def __post_init__(self):
        _validate_from_annotations(self)
        if (index := self.index) < 0:
            msg = f"'index' should be a value greater than 0. Got {index} instead."
            raise ValueError(msg)


@dataclass(frozen=True)
class Entry:
    """Entry of a raft logger"""
    term: int
    item: object

    __post_init__ = _validate_from_annotations


# messages
@dataclass(frozen=True)
class _BaseMessage:
    sender: int  # peer that has sent the message
    term: int

    __post_init__ = _validate_from_annotations


@dataclass(frozen=True)
class AppendEntriesRequest(_BaseMessage):
    index: int
    entries: tuple
    commit_index: int
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesReply(_BaseMessage):
    success: bool
    match_index: int


@dataclass(frozen=True)
class ElectionRequest(_BaseMessage):
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class ElectionReply(_BaseMessage):
    favour: bool
