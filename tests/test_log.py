import unittest
import immutables
from graft import log, model


class TestLog(unittest.TestCase):
    def setUp(self) -> None:
        populated = log.new()
        entries = (model.Entry(i, i) for i in range(1, 10))
        self.log_with_9 = log.append(populated, model.Index(0, 0), *entries)

    def test_invalid_arguments(self):
        with self.assertRaises(TypeError):
            log.append(1, model.Index(1,2))

        with self.assertRaises(TypeError):
            log.append(log.new(), 2)

        with self.assertRaises(TypeError):
            log.append(log.new(), model.Index(0, 0), "hello")

    def test_log_with_gap(self):
        broken = immutables.Map({
            1: model.Entry(1, "hello"),
            3: model.Entry(1, 1),
        })
        result = log.append(broken, model.Index(1, 1), model.Entry(1, "world"))
        self.assertEqual(result, immutables.Map({
            1: model.Entry(1, "hello"),
            2: model.Entry(1, "world"),
        }))

    def test_missing_after_index(self):
        """(a) False. Missing entry at index 10. (b) False. Many missing entries."""
        with self.assertRaises(log.AppendError):
            log.append(log.new(), model.Index(10, 1))
        with self.assertRaises(log.AppendError):
            log.append(log.new(), model.Index(5, 5), model.Entry(5, 'should_fail'))

    def test_slice_update(self):
        """(c) True. Entry already in position 11 is replaced. (d) True. Entries at position 11,12 are replaced."""
        log_to_update = self.log_with_9
        new_entries_no = 2
        new_entries = [model.Entry(i, 'multi') for i in range(new_entries_no)]
        after_index_key = 5
        update1 = log.append(log_to_update, model.Index(5, after_index_key), *new_entries)
        self.assertTrue(update1)
        self.assertEqual(len(update1), after_index_key+new_entries_no)
        self.assertEqual(update1[6], model.Entry(0, 'multi'))
        self.assertIs(update1[7], new_entries[1])
        update2 = log.append(update1, model.Index(0, 6), model.Entry(5, 'single_replace'))
        self.assertTrue(update2)
        self.assertEqual(update2[7], model.Entry(5, 'single_replace'))

    def test_previous_term_mismatch(self):
        """(f) False. Previous term mismatch."""
        log_to_update = self.log_with_9
        with self.assertRaises(log.AppendError):
            log.append(log_to_update, model.Index(2,9))
        with self.assertRaises(log.AppendError):
            log.append(log_to_update, model.Index(2, 9), model.Entry(5, 'should fail'))

    def test_idempotent(self):
        """Check that the last operation is idempotent"""
        empty = log.new()
        good_term = 2
        bad_term = 7
        index_zero = model.Index(good_term, 0)
        single_entry = model.Entry(term=good_term, item=True)
        update1 = log.append(empty, index_zero)
        update2 = log.append(empty, index_zero)
        self.assertIs(update1, update2)

        update3 = log.append(update1, index_zero, single_entry, single_entry)
        update4 = log.append(update2, index_zero, single_entry, single_entry)

        self.assertIs(update3, update4)

        # make a couple of calls to ensure older than 1 calls are not cached
        update5 = log.append(update4, model.Index(good_term, 2), single_entry)
        update6 = log.append(update5, model.Index(good_term, 3), single_entry)
        expected = immutables.Map({
            1: single_entry,
            2: single_entry,
            3: single_entry,
            4: single_entry,
        })

        with self.assertRaises(log.AppendError):
            log.append(update6, model.Index(bad_term, 1))
        with self.assertRaises(log.AppendError):
            log.append(update6, model.Index(bad_term, 1))

        self.assertEqual(expected, update6)

        update7 = log.append(update6, model.Index(good_term, 1), model.Entry(1, "not_cached"))
        expected = immutables.Map({
            1: model.Entry(term=good_term, item=True),
            2: model.Entry(1, "not_cached"),
        })
        self.assertEqual(expected, update7)
        update8 = log.append(update7, index_zero)
        self.assertEqual(log.new(), update8)
