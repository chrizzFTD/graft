import immutables
from functools import lru_cache

from . import model


def new() -> immutables.Map:
    """Create a new, empty log."""
    return immutables.Map()


@lru_cache(maxsize=1)  # idempotent: calling with same arguments has same result
def append(log: immutables.Map, after: model.Index, *entries: model.Entry) -> immutables.Map:
    """Append the given entries to `log` *after* the given :class:`graft.model.Index`.

    :raises AppendError: If the operation is unsuccessful, which can happen if `after`
        index does not exist in the given `log`. Unless index.key is 0 (the origin).
        For example:
            >>> index = model.Index(key=2, term=4)
            >>> assert log[2].term == index.term  # index and term match existing entry
            >>> index = model.Index(key=0, term=4)  # will work since index it's origin
    """
    for name, object, expected in (
            ("log", log, immutables.Map), ("after", after, model.Index),
            *((f'entry {i}', e, model.Entry) for i, e in enumerate(entries))):
        if not isinstance(object, expected):
            msg = (f"Expected '{name}' to be of type: '{expected}'. "
                   f"Got: '{type(object).__name__}' instead.")
            raise TypeError(msg)
    key = after.key
    # No gap is there between requested `after` index and size of current logger.
    try:
        after_own_entry = log[key]
    except KeyError:
        # Special case: Appending logger entries at index 0 always works
        if key != 0:
            msg = f"Requested index '{key=}' does not exist on the current log"
            raise AppendError(msg)
    else:
        # after index term should always match own term
        if (actual := after_own_entry.term) != (requested := after.term):
            msg = f"Index {key=} exists but terms are not equal. {requested=}, {actual=}"
            raise AppendError(msg)

    max_key = max(log, default=0)
    to_delete = set(range(key + 1, max_key + 1))
    if missing:= to_delete.difference(log):
        msg = f"Missing keys from logger: {missing=}. Existing: {sorted(log)}"
        raise AppendError(msg)

    new_entries = immutables.Map({key + i: e for i, e in enumerate(entries, start=1)})
    if set(new_entries) == set(log) or (not log) or (1 in new_entries):
        # if the keys of `new_entries` is exactly the same as the keys on `logger`,
        # return that already. Do the same if original logger is empty.
        # finally, if the index that we need to replace is 1, use new_entries instead
        return new_entries

    with log.mutate() as mm:
        for i in to_delete:
            del mm[i]
        mm.update(new_entries)
        log = mm.finish()
    return log


class AppendError(ValueError):
    """Raised by :func:`append` when an invalid :class:`graft.model.Index` is passed"""
