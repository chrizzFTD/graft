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

    def test_follower_cant_append(self):
        """Verify a follower can not append entries to a log"""
        follower = self.state1
        follower.append(self.ctrl1, "hello")
        self.assertEqual(follower.role, state.Roles.FOLLOWER)
        self.assertEqual(0, len(self.ctrl1.messages))
        follower._leader_append_entries_on_follower(2, self.ctrl1)
        self.assertEqual(0, len(self.ctrl1.messages))

    def test_become_leader(self):
        """Verify becoming leader does sets required state"""
        lead = self.state1
        self.assertEqual(lead.role, state.Roles.FOLLOWER)
        lead.votes_received_from.update(self.ctrl1.peers)
        lead.become_leader(self.ctrl1)
        # votes received should be cleared
        self.assertFalse(lead.votes_received_from)
        # becoming leader should have sent a message
        for msg in self.ctrl1.messages.values():
            self.assertIsInstance(msg, model.AppendEntriesRequest)
        self.assertEqual(len(self.ctrl1.peers), len(self.ctrl1.messages))
        self.assertEqual(lead.role, state.Roles.LEADER)

    def test_candidate_append_request(self):
        """While waiting for votes, a candidate may receive an AppendEntries RPC from
        another server claiming to be leader.

        If the leader’s term (included in its RPC) is at least as large as the
        candidate’s current term, then the candidate recognizes the leader as legitimate
        and returns to follower state.

        :return:
        """
        candidate = self.state1
        candidate.term = 2
        self.assertEqual(candidate.role, state.Roles.FOLLOWER)
        candidate.role = state.Roles.CANDIDATE
        request = model.AppendEntriesRequest(
            term=candidate.term-1,
            sender=self.ctrl1.peer_id+1,  # so follower can redirect clients
            after=model.Index(term=candidate.term+1, key=0),
            entries=tuple(),
            leader_commit=1,
        )
        candidate.on_append_entries_request(self.ctrl1, request)
        # request with lower term should not change state
        self.assertEqual(candidate.role, state.Roles.CANDIDATE)
        # request with lower term should change state
        request = model.AppendEntriesRequest(
            term=candidate.term + 1,
            sender=self.ctrl1.peer_id + 1,  # so follower can redirect clients
            after=model.Index(term=candidate.term + 1, key=0),
            entries=tuple(),
            leader_commit=1,
        )
        candidate.on_append_entries_request(self.ctrl1, request)
        self.assertEqual(candidate.role, state.Roles.FOLLOWER)


if __name__ == '__main__':
    unittest.main()
