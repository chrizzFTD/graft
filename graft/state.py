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
from enum import Enum, auto
from dataclasses import dataclass

from graft import log, model

logger = logging.getLogger(__name__)


class Roles(Enum):
    # Followers are passive, they only respond to leaders and candidates requests.
    # If contacted by a client, they redirect it to the leader.
    FOLLOWER = auto()
    # When no leader is heard, followers become candidates & submit an election request.
    CANDIDATE = auto()
    # Handles all client requests & propagates the requests to replicate on followers.
    LEADER = auto()


class TargetIsSenderError(ValueError):
    """Raised by `BaseController.send` when sender is receiver"""


@dataclass(frozen=True)
class BaseController(abc.ABC):
    """A controller needs to be frozen and provide a `send` method."""
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


class State:
    def __init__(self):
        # persistent
        self.term = 0  # increased monotonically after every election finishes
        # volatile
        self.commit_index = 0  # index of highest log entry known to be committed, increased monotonically
        self.votes_received_from = set()
        self.log = log.new()
        self.become_follower()

    def become_follower(self):
        """Respond to RPCs from candidates and leaders"""
        self.voted_for = 0  # reset vote anytime we become followers
        self.role = Roles.FOLLOWER
        self.heard_leader = False
        self.votes_received_from.clear()
        logger.debug("Became follower")

    def become_leader(self, control: BaseController):
        self.role = Roles.LEADER
        logger.debug(f"Becoming leader from: {control.peer_id}")
        # Reinitialized after election
        # for each server, index of the next log entry to send to that server
        # (initialized to leader last log index + 1)
        self.next_index = {p: len(self.log) + 1 for p in control.peers}
        # for each server, index of highest log entry known to be replicated on server
        # (initialized to 0, increases monotonically)
        self.match_index = {p: 0 for p in control.peers}
        self.votes_received_from.clear()
        self.heartbeat(control)

    def heartbeat(self, control):
        """Upon election: send initial empty AppendEntries RPCs
            (heartbeat) to each server; repeat during idle periods to
            prevent election timeouts (§5.2)

        :param control:
        :return:
        """
        self._leader_append_entries(control)

    def append(self, control, item):
        """If command received from client: append entry to local log,
        respond after entry applied to state machine (§5.3)

        :param control:
        :param item:
        :return:
        """
        entry = model.Entry(self.term, item)
        self._leader_append_entries(control, entry)

    def _leader_append_entries(self, control, *entries):
        if self.role != Roles.LEADER:
            return
        after_index = len(self.log)
        after_term = self.log[after_index].term if after_index else self.term
        self.log = log.append(self.log, model.Index(after_index, after_term), *entries)
        for follower in control.peers:
            self._leader_append_entries_on_follower(follower, control)

    def _leader_append_entries_on_follower(self, peer_id: int, control: BaseController):
        if self.role != Roles.LEADER:
            return
        size = len(self.log)
        next_follower_index = self.next_index[peer_id]
        # If last log index ≥ nextIndex for a follower: send
        # AppendEntries RPC with log entries starting at nextIndex
        if size and size >= next_follower_index:
            start_range = next_follower_index or 1
            new_entries = (self.log[i] for i in range(start_range, size+1))
        else:
            new_entries = tuple()
        after_i = max(next_follower_index - 1, 0) if size else 0
        after_t = self.log[after_i].term if after_i else self.term
        message = model.AppendEntriesRequest(
            term=self.term,  # leader’s term
            sender=control.peer_id,  # so follower can redirect clients
            after=model.Index(after_i, after_t),
            entries=tuple(new_entries),
            leader_commit=self.commit_index,
        )
        control.send(peer_id, message)

    def _append(self, after, entries):
        try:
            self.log = log.append(self.log, after, *entries)
        except log.AppendError as exc:
            logger.debug(exc)
            return False
        return True

    def timeout(self, control: BaseController):
        """If a follower receives no communication, it becomes a candidate and initiates an election.

        On conversion to candidate, start election:
            • Increment currentTerm
            • Vote for self
            • Reset election timer
            • Send RequestVote RPCs to all other servers
            • If votes received from majority of servers: become leader
            • If AppendEntries RPC received from new leader: convert to
            follower
            • If election timeout elapses: start new election
        """
        logger.debug(f"Handling timeout")
        if self.role == Roles.LEADER:
            return
        if not self.heard_leader:
            logger.warning(f"Didnt hear from leader. Calling an election")
            self.role = Roles.CANDIDATE
            self.term += 1  # Increment currentTerm
            candidate = control.peer_id
            self.voted_for = candidate  # Vote for self
            # Reset election timer
            self.votes_received_from = {candidate}  # Set of votes received
            # index of highest log entry applied to state machine
            last_applied = len(self.log)
            msg = model.VoteRequest(
                sender=candidate,
                term=self.term,
                last_log_index=last_applied,
                last_log_term=self.log[last_applied].term if self.log else 0,
            )
            logger.debug(f"Requesting vote {msg}")
            # Send RequestVote RPCs to all other servers
            for peer in control.peers:
                control.send(peer, msg)
        self.heard_leader = False

    @property
    def _last_log_term(self):
        return self.log[len(self.log)].term if self.log else 0

    def _majority(self, control):
        if (peersize := len(control.peers)) % 2 == 0:
            return abs(peersize / 2 + 1)
        else:
            return math.ceil(peersize / 2)

    def _handle_base_message(self, message):
        """Rules for all Servers

         - If commitIndex > lastApplied: increment lastApplied, apply
        log[lastApplied] to state machine (§5.3)
        - If RPC request or response contains term T > currentTerm:
        set currentTerm = T, convert to follower (§5.1)
        """
        if message.term > self.term:
            self.term = message.term
            logger.debug("Becoming follower from _handle_base_message")
            self.become_follower()

    def on_election_request(self, control: BaseController, msg: model.VoteRequest):
        """May only vote for one candidate in a given term (subsequent requests denied)

        - Wont vote if candidate's logger is not as up-to-date as oneself
        - Grant vote if last entry in candidate's logger has a greater term than myself.
        """
        self._handle_base_message(msg)
        # Make sure we've only voted once (or already voted for the sender)
        can_vote_for_sender = not self.voted_for or self.voted_for == msg.sender

        updated_log_recvd = True
        if self.log:  # see if log from sender is equal or more up to date than ours
            logterm_ge_recvd = msg.last_log_term >= self._last_log_term
            log_ge_recvd = msg.last_log_index >= len(self.log)
            updated_log_recvd = logterm_ge_recvd and log_ge_recvd

        granted = can_vote_for_sender and updated_log_recvd and self.term <= msg.term

        if granted:
            self.voted_for = msg.sender

        response = model.VoteReply(
            sender=control.peer_id,
            term=self.term,
            granted=granted,
        )
        for peer in control.peers:
            control.send(peer, response)

    def on_election_reply(self, control: BaseController, msg: model.VoteReply):
        """A candidate that receives votes from a majority of the full cluster becomes the new leader

        1. Reply false if term < currentTerm (§5.1)
        2. If votedFor is null or candidateId, and candidate’s log is at
        least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)


            • If votes received from majority of servers: become leader
            • If AppendEntries RPC received from new leader: convert to
            follower
        """
        self._handle_base_message(msg)
        if self.role == Roles.CANDIDATE and msg.granted:
            self.votes_received_from.add(msg.sender)
            if len(self.votes_received_from) >= self._majority(control):
                self.become_leader(control)
                self.votes_received_from.clear()

    def on_append_entries_request(self, control: BaseController, msg: model.AppendEntriesRequest):
        """
        1. Reply false if term < currentTerm (§5.1)
        2. Reply false if log doesn’t contain an entry at prevLogIndex
        whose term matches prevLogTerm (§5.3)
        3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (§5.3)
        4. Append any new entries not already in the log
        5. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of last new entry)
        """
        self.heard_leader = True
        self._handle_base_message(msg)

        success = False
        if self.role == Roles.CANDIDATE and self.term == msg.term:
            # If the leader’s term (included in its RPC) is at least as large as the
            # candidate’s current term, then the candidate recognizes the leader
            # as legitimate and returns to follower state.
            self.become_follower()

        # 1. Reply false if term < currentTerm (§5.1)
        # If the term in the RPC is smaller than the candidate’s current term,
        # then the candidate rejects the RPC and continues in candidate state.
        if self.term <= msg.term:
            # 2. Reply false if log doesn’t contain an entry at after_log_index whose term
            # matches after_log_index_term (§5.3)
            if not (after_i:= msg.after.index):
                success = True
            elif after_i and after_i in self.log:
                if self.log[after_i].term == msg.after.term:
                    success = True
        if success:  # we're good to attempt adding to our own log.
            # 3. If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it (§5.3)
            success = self._append(msg.after, msg.entries)

        response = model.AppendEntriesReply(
            sender=control.peer_id,
            term=self.term,
            success=success,
            match_index=min(msg.leader_commit, len(self.log)),
        )
        control.send(msg.sender, response)

    def on_append_entries_reply(self, control: BaseController, msg: model.AppendEntriesReply):
        self._handle_base_message(msg)
        if self.role != Roles.LEADER:
            return
        # If successful: update nextIndex and matchIndex for follower
        if msg.success:
            self.next_index[msg.sender] = msg.match_index + 1
        else:
            # backtrack until we have a match
            logger.critical(f"Append entries failed for {msg.sender} at index {self.next_index[msg.sender]}: {msg}")
            max_next_index = max(self.next_index[msg.sender] - 1, 0)
            self.next_index[msg.sender] = min(max_next_index, msg.match_index)
            logger.critical(f"Back tracking and retrying now at {self.next_index[msg.sender]}")
            self._leader_append_entries_on_follower(msg.sender, control)
