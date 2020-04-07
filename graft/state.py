"""
Figure 4: Server states. Followers only respond to requests
from other servers. If a follower receives no communication,
it becomes a candidate and initiates an election. A candidate
that receives votes from a majority of the full cluster becomes
the new leader. Leaders typically operate until they fail.
"""
import abc
import math
import logging
import immutables
from enum import Enum, auto
from collections import Counter
from dataclasses import dataclass, field

from graft import log, model

logger = logging.getLogger(__name__)


class Roles(Enum):
    LEADER = auto()
    CANDIDATE = auto()
    FOLLOWER = auto()


class TargetIsSenderError(ValueError):
    pass


@dataclass(frozen=True)
class BaseController(abc.ABC):
    peer_id: int

    @property
    @abc.abstractmethod
    def peers(self) -> set:
        pass

    @abc.abstractmethod
    def send(self, target_peer: int, message: object):
        if target_peer == self.peer_id:
            msg = f"Can't send {message=} to ourselves: {self.peer_id}, {target_peer=}"
            raise TargetIsSenderError(msg)


@dataclass
class State:
    role: Roles = Roles.FOLLOWER
    term: int = 0  # increased after every election finishes
    log: immutables.Map = field(default_factory=immutables.Map)

    commit_index: int = 0  # logger commit index

    # Can only vote once in a given term
    vote_given_to: int = 0
    votes_received_from: set = field(default_factory=set)

    heard_leader = False

    # Keep track of what followers are in failed mode
    _retrying_followers = set()

    def become_leader(self, control: BaseController):
        self.role = Roles.LEADER
        logger.debug(f"Becoming leader from: {control.peer_id}")
        self.match_index = {}
        self._consensus = Counter()
        self.next_index = dict()
        self._retrying_followers.clear()
        self.heartbeat(control)

    def become_follower(self):
        self.vote_given_to = 0
        self.role = Roles.FOLLOWER
        self.heard_leader = False
        logger.debug("Became follower")

    def heartbeat(self, control):
        if self.role != Roles.LEADER:
            return
        # Sending an empty entry is the heartbeat
        logger.debug(f"Sending hearbeat from: {control.peer_id}")
        size = len(self.log)
        term = self.log[size].term if self.log else 1
        self._leader_append_entries(control, model.Index(size, term))

    def _debug_log(self):
        if logger.level <= logging.DEBUG and (size:= len(self.log)):
            logger.debug(f"Log {size=}")
            for i in sorted(filter(lambda x: x>0, {1, size-1, size})):
                logger.debug(f"Index {i}: {self.log[i]}")

    def append(self, control, item):
        if self.role != Roles.LEADER:
            return
        entry = model.Entry(self.term, item)
        index = model.Index(len(self.log), self.log[len(self.log)].term if self.log else self.term)
        self._leader_append_entries(control, index, entry)

    def _leader_append_entries(self, control, index:model.Index, *entries):
        self.log = log.append(self.log, index, *entries)
        self._consensus.update((index.index,))
        self._debug_log()
        message = model.AppendEntriesRequest(
            sender=control.peer_id,
            index=index.index,
            entries=entries,
            commit_index=self.commit_index,
            term=self.term,
            leader_commit=self.commit_index,
        )
        followers = control.peers - self._retrying_followers
        logger.debug(f"To {followers=}")
        for follower in followers:
            control.send(follower, message)

    def _append(self, after_index, after_term, entries):
        index = model.Index(after_index, after_term)
        try:
            self.log = log.append(self.log, index, *entries)
        except log.AppendError as exc:
            logger.debug(exc)
            return False
        if logger.level <= logging.DEBUG and (size:= len(self.log)):
            logger.debug(f"Log {size=}")
            for i in sorted(filter(lambda x: x>0, {1, index.index-1, index.index})):
                logger.debug(f"Index {i}: {self.log[i]}")
        return True

    def handle_timeout(self, control: BaseController):
        if self.role == Roles.LEADER:
            return
        if not self.heard_leader:
            logger.warning(f"Didnt hear from leader. Calling an election")
            self.role = Roles.CANDIDATE
            self.term += 1
            candidate = control.peer_id
            self.vote_given_to = candidate  # Vote for myself
            self.votes_received_from = {candidate}  # Set of votes received
            last_index = len(self.log)
            msg = model.ElectionRequest(
                sender=candidate,
                term=self.term,
                last_log_index=last_index,
                last_log_term=self.log[last_index].term if self.log else 0,
            )
            logger.critical(f"Requesting vote {msg=}")
            for peer in control.peers:
                control.send(peer, msg)
        self.heard_leader = False

    @property
    def _last_log_term(self):
        return self.log[len(self.log)].term if self.log else 0

    def _msg_gt_term(self, message) -> bool:
        if message.term > self.term:
            # a stronger leader is out there somewhere, use its term and become follower
            self.term = message.term
            self.become_follower()
            return True

    def _msg_ge_term(self, message) -> bool:
        result = self._msg_gt_term(message) or message.term == self.term
        return result

    def _majority(self, control):
        if (peersize := len(control.peers)) % 2 == 0:
            return abs(peersize / 2 + 1)
        else:
            return math.ceil(peersize / 2)

    def on_election_request(self, control: BaseController, msg: model.ElectionRequest):
        """May only vote for one candidate in a given term (subsequent requests denied)

        - Wont vote if candidate's logger is not as up-to-date as oneself
        - Grant vote if last entry in candidate's logger has a greater term than myself.
        """
        msgterm_ge_recvd = self._msg_ge_term(msg)

        # Make sure we've only voted once (or already voted for the sender)
        logger.debug(f"{self.vote_given_to=}")
        logger.debug(f"{msg.sender=}")
        can_vote_for_sender = not self.vote_given_to or self.vote_given_to == msg.sender
        logger.debug(f"{can_vote_for_sender=}")
        logger.debug(f"{self.votes_received_from=}")
        if self.log or msg.last_log_index:
            logger.debug(f"{msg.last_log_term=}")
            logger.debug(f"{self._last_log_term=}")
            logterm_ge_recvd = msg.last_log_term >= self._last_log_term
            log_ge_recvd = msg.last_log_index >= len(self.log)
            logger.debug(f"{self._last_log_term=}")
            # logger term from message greater or equal to ours and it's more up to date
            updated_log_recvd = logterm_ge_recvd and log_ge_recvd
            logger.debug(f"{updated_log_recvd=}")
        else:
            # start case: if neither our own logger exists or the message last logger, say yes
            updated_log_recvd = True

        favour = (
            msgterm_ge_recvd  # message term has greater or equal term than us
            and can_vote_for_sender
            and updated_log_recvd
        )

        if favour:
            self.vote_given_to = msg.sender

        response = model.ElectionReply(
            sender=control.peer_id,
            term=self.term,
            favour=favour,
        )
        logger.debug(f"Vote requested from {msg.sender}, {response=}")
        for peer in control.peers:
            control.send(peer, response)

    def on_election_reply(self, control: BaseController, msg: model.ElectionReply):
        if self._msg_ge_term(msg) and self.role == Roles.CANDIDATE and msg.favour:
            self.votes_received_from.add(msg.sender)
            if len(self.votes_received_from) >= self._majority(control):
                self.become_leader(control)
                self.votes_received_from.clear()

    def on_append_entries_request(self, control: BaseController, msg: model.AppendEntriesRequest):
        # On failure. We tell the leader the index where we tried
        # to append.  The leader can use this to try earlier entries.
        # If replying to a leader from an earlier term, they'll get a message
        # with our term number embedded in it (which should instantly flip
        # them to follower state).  See Point 1, Figure 2 (AppendEntries RPC).
        match_index = msg.index
        success = False
        if self._msg_ge_term(msg):
            self.heard_leader = True
            if self.role == Roles.CANDIDATE:
                # since we heard from the leader, switch to follower
                self.become_follower()
            success = self._append(msg.index, msg.term, msg.entries)
            if success and msg.leader_commit > self.commit_index:
                match_index = len(self.log)
                self.commit_index = min(msg.leader_commit, len(self.log))

        response = model.AppendEntriesReply(
            sender=control.peer_id,
            term=self.term,
            success=success,
            match_index=match_index,
        )
        control.send(msg.sender, response)

    def on_append_entries_reply(self, control:BaseController, msg: model.AppendEntriesReply):
        if not self._msg_ge_term(msg):
            pass

        if self.role != Roles.LEADER:
            return  # Discard the message.  We're not the leader anymore. Oh well.
        # logger.warning(f"on_append_entries_reply {self.role=}")
        if msg.success:
            self.next_index[msg.sender] = msg.match_index + 1
            self._consensus.update((msg.match_index,))
            # logger.warning(f"on_append_entries_reply {self._consensus=}")
            highest_commit = self.last_commit(control)
            # logger.warning(f"{highest_commit=}")
            if highest_commit > self.commit_index and self.log[highest_commit].term == self.term:
                self.commit_index = highest_commit
        else:
            # backtrack until we have a match
            newindex = msg.match_index - 1
            self.next_index[msg.sender] = newindex
            # new_prev_term = self.logger[newindex].term if newindex else 1
            new_entries = tuple(self.log[i] for i in range(newindex+1, len(self.log)+1))
            newmsg = model.AppendEntriesRequest(
                sender=control.peer_id,
                index=newindex,
                term=self.term,
                entries=new_entries,
                commit_index=self.commit_index,
                leader_commit=self.commit_index,
            )
            control.send(msg.sender, newmsg)

    def last_commit(self, control):
        return max((index for index, amount in self._consensus.most_common() if amount >= self._majority(control)), default=0)
