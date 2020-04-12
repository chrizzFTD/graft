import immutables
from graft import model
from functools import lru_cache


class AppendError(ValueError):
    pass


def new():
    return immutables.Map()


@lru_cache(maxsize=1)  # 4 - cache only last operation
def append(log: immutables.Map, after: model.Index, *entries: model.Entry) -> immutables.Map:
    """Append the given logger entries to the current logger *after* the given index.

    :raises AppendError: If the operation is unsuccessful. This can be one of the following:

    Operation will be successful as long as:

        1. No gap is there between requested `after` index and size of current logger.
          - For example, if there are currently 5 entries in the logger, and `after` = Index(9,...).
        2. There is a logger-continuity condition where every append operation must also take the term number of the previous entry. For example, if appending at index 9, the prev_term value must match the value of logger[8].term. If there is a mismatch, the operation fails (return False).

        3. Special case: Appending logger entries at index 0 always works. That's the start of the logger and there are no prior entries.

        4. append() is "idempotent." Basically, that means that append() can be called repeatedly with the same arguments and the end result is always the same. For example, if you called append() twice in a row to add the same entry at index 10, it just puts the entry at index 10 and does not result in any data duplication or corruption.

        5. Calling append() with an empty list of entries is allowed. In this case, it should report True or False to indicate if it would have been legal to add new entries at the specified position.

        6. If there are already existing entries at the specified logger position and all of the conditions for successfully adding a logger entry are met (see point 2), the existing entries and everything that follows are deleted. The new entries are then added in their place.
    """
    to_validate = {log: immutables.Map, after: model.Index}
    to_validate.update({e: model.Entry for e in entries})
    model.validate_types(to_validate)
    after_i = after.index
    new_entries = immutables.Map(
        {after_i + i: entry for i, entry in enumerate(entries, start=1)}
    )

    # 1
    try:
        after_own_entry = log[after_i]
    except KeyError:
        # 3
        if after_i != 0:
            msg = f"Requested index '{after_i}' does not exist on the current logger"
            raise AppendError(msg)
    else:
        # 2
        if (actual := after_own_entry.term) != (requested := after.term):
            msg = f"Index {after_i} exists but terms are not equal. {requested=}, {actual=}"
            raise AppendError(msg)

    # 5 empty list of entries is ok. mutate
    max_index = max(log, default=0)
    to_delete = set(range(after_i + 1, max_index + 1))
    if missing:= to_delete.difference(log):
        msg = f"Missing keys from logger: {missing=}. Existing: {sorted(log)}"
        raise AppendError(msg)

    elif set(new_entries) == set(log) or (not log) or (1 in new_entries):
        # if the keys of `new_entries` is exactly the same as the keys on `logger`,
        # return that already. Do the same if original logger is empty
        # finally, if the index that we need to replace is all the way back to 1, use new_entries instead
        return new_entries

    with log.mutate() as mm:
        for i in to_delete:
            # If you get KeyError, go here: https://github.com/MagicStack/immutables/issues/24
            del mm[i]
        mm.update(new_entries)
        log = mm.finish()
    return log
