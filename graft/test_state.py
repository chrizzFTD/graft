import logging
import unittest
from dataclasses import FrozenInstanceError

from graft import state, model

logger = logging.getLogger(__name__)


class _BadController(state.BaseController):
    pass


class _GoodController(state.BaseController):
    peer_id = 1
    peers = {1, 2}

    def send(self, target, message):
        pass


class CollectorController(state.BaseController):
    def __init__(self, *args, **kwargs):
        super(CollectorController, self).__init__(*args, **kwargs)
        self.messages = dict()

    @property
    def peers(self):
        return set(range(1, 6)) - {self.peer_id}

    def send(self, target_id: int, message: model._BaseMessage):
        super().send(target_id, message)
        logger.info(f"Sending {message=} to {target_id=}")
        self.messages[target_id] = message


class TestControllerCreation(unittest.TestCase):

    def test_good(self):
        """Verify ability to instantiate"""
        self.assertIsNotNone(_GoodController(1))

    def test_bad(self):
        """Verify can't instantiate invalid subclasses"""
        with self.assertRaises(TypeError):
            _BadController()
        with self.assertRaises(TypeError):
            _BadController(1)


class TestController(unittest.TestCase):
    def setUp(self) -> None:
        self.ctrl1 = CollectorController(2)

    def test_assignment(self):
        """Verify we can not change peer id used to initialise instance"""
        with self.assertRaises(FrozenInstanceError):
            self.ctrl1.peer_id = self.ctrl1.peer_id + 1

    def test_send(self):
        """Verify we succeed when sending a message to a different peer and fail otherwise"""
        self.ctrl1.send(3, 'hello')
        with self.assertRaises(state.TargetIsSenderError):
            self.ctrl1.send(self.ctrl1.peer_id, "should fail")


class TestState(unittest.TestCase):
    def setUp(self) -> None:
        self.ctrl1 = CollectorController(2)
        self.state1 = state.State()

    def test_follower_start_timeout(self):
        fol = self.state1
        initial_tem = fol.term
        ctrl = self.ctrl1
        # default role is follower
        self.assertEqual(fol.role, state.Roles.FOLLOWER)
        fol.timeout(ctrl)
        self.assertEqual(fol.role, state.Roles.CANDIDATE)
        self.assertEqual(fol.term, initial_tem+1)
        self.assertEqual(len(ctrl.messages), len(ctrl.peers))
        for msg in ctrl.messages.values():
            self.assertIsInstance(msg, model.VoteRequest)
        self.assertEqual(len(set(ctrl.messages.values())), 1)  # message should be the same
        self.assertEqual(msg.last_log_index, model.Index(0, 0))
        self.assertEqual(msg.sender, ctrl.peer_id)

    def test_leader_timeout(self):
        """Verify leader does not change state on timeout"""
        lead = self.state1
        lead.role = state.Roles.LEADER
        lead.timeout(self.ctrl1)
        self.assertEqual(lead.role, state.Roles.LEADER)

    def test_candidate_append_request(self):
        """While waiting for votes, a candidate may receive an AppendEntries RPC from
        another server claiming to be leader.

        If the leader’s term (included in its RPC) is at least as large as the
        candidate’s current term, then the candidate recognizes the leader as legitimate
        and returns to followerstate.

        :return:
        """


if __name__ == '__main__':
    unittest.main()
