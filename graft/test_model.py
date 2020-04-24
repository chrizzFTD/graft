import unittest
import dataclasses
from graft import model


class TestIndex(unittest.TestCase):

    def test_index(self):
        index = model.Index(1, 2)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            index.key = 3

        with self.assertRaises(TypeError):
            model.Index('hello', 0)

        with self.assertRaises(TypeError):
            model.Index(0, 'hello')

        with self.assertRaises(ValueError):
            model.Index(-1, 0)

        with self.assertRaises(ValueError):
            model.Index(0, -1)


class TestEntry(unittest.TestCase):

    def test_index(self):
        entry = model.Entry(1, 2)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            entry.term = 3

        with self.assertRaises(dataclasses.FrozenInstanceError):
            entry.item = 4

        with self.assertRaises(TypeError):
            model.Entry('hello', 0)

        with self.assertRaises(ValueError):
            model.Entry(-1, 0)


class TestMessages(unittest.TestCase):

    def test_base_message(self):
        msg = model._BaseMessage(sender=1, term=2)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            msg.sender = 2

        with self.assertRaises(dataclasses.FrozenInstanceError):
            msg.term = 3

        with self.assertRaises(TypeError):
            model._BaseMessage('hello', 0)

        with self.assertRaises(TypeError):
            model._BaseMessage(0, 'hello')

        with self.assertRaises(ValueError):
            model._BaseMessage(0, -1)
