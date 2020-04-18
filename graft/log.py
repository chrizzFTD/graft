import immutables
from graft import model
from functools import lru_cache


def new():
    return immutables.Map()


@lru_cache(maxsize=1)  # idempotent: calling with same arguments has same result
def append(log: immutables.Map, after: model.Index, *entries: model.Entry) -> immutables.Map:
    """Append the given logger entries to the current logger *after* the given index.

    :raises AppendError: If the operation is unsuccessful. Which can happen if the
        requested `after` index does not exist in the given `log` (including it's term).
        Unless index.index is 0. For example:
            >>> index = model.Index(2, term=4)
            >>> assert log[2].term == index.term  # index and term match existing entry
            >>> index = model.Index(0, term=4)  # will work since index it's origin
    """
    model.validate_types({
        "log": (log, immutables.Map), "after": (after, model.Index),
        **{f'entry {i}': (entry, model.Entry) for i, entry in enumerate(entries)},
    })
    after_i = after.index
    # empty list of entries is ok
    new_entries = immutables.Map(
        {after_i + i: entry for i, entry in enumerate(entries, start=1)}
    )

    # No gap is there between requested `after` index and size of current logger.
    try:
        after_own_entry = log[after_i]
    except KeyError:
        # Special case: Appending logger entries at index 0 always works
        if after_i != 0:
            msg = f"Requested index '{after_i}' does not exist on the current log"
            raise AppendError(msg)
    else:
        # after index term should always match own term
        if (actual := after_own_entry.term) != (requested := after.term):
            msg = f"Index {after_i} exists but terms are not equal. {requested=}, {actual=}"
            raise AppendError(msg)

    max_index = max(log, default=0)
    to_delete = set(range(after_i + 1, max_index + 1))
    if missing:= to_delete.difference(log):
        msg = f"Missing keys from logger: {missing=}. Existing: {sorted(log)}"
        raise AppendError(msg)

    elif set(new_entries) == set(log) or (not log) or (1 in new_entries):
        # if the keys of `new_entries` is exactly the same as the keys on `logger`,
        # return that already. Do the same if original logger is empty.
        # finally, if the index that we need to replace is 1, use new_entries instead
        return new_entries

    with log.mutate() as mm:
        for i in to_delete:
            # If you get KeyError, go here: https://github.com/MagicStack/immutables/issues/24
            del mm[i]
        mm.update(new_entries)
        log = mm.finish()
    return log


class AppendError(ValueError):
    """Raised by `append` when operation fails"""
