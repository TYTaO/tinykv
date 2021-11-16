// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	votes := make(map[uint64]bool)
	for _, p := range c.peers {
		votes[p] = false
	}
	r := &Raft{
		id:               c.ID,
		State:            StateFollower,
		votes:            votes,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
			})
		}
	}
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.becomeCandidate()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Debugf("[%v] become Follower\n", r.id)
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Debugf("[%v] become Candidate\n", r.id)
	resetVotes(r)
	r.State = StateCandidate
	r.Term += 1
	r.electionElapsed = 0

	for id, _ := range r.votes {
		if id == r.id {
			// vote for itself
			r.votes[id] = true
			checkVoteNumsHasGot(r)
			if checkVoteNumsHasGot(r) {
				r.becomeLeader()
			}
			continue
		}
		log.Debugf("[%v] send voteReq to [%v]\n", r.id, id)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		})
	}

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Debugf("[%v] become Leader\n", r.id)
	r.State = StateLeader
	// send heartbeat
	for id, _ := range r.votes {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      id,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled  响应
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if !IsLocalMsg(m.MsgType) && m.Term < r.Term {
		return nil
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.Term = m.Term
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  false,
				})
				r.Vote = m.From
			}
			log.Debugf("[%v] receive voteReq from [%v] and vote: %v\n", r.id, m.From, !m.Reject)
		case pb.MessageType_MsgHeartbeat:
			r.Term = m.Term
			r.electionElapsed = 0
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			log.Debugf("[%v] receive voteResponse from [%v] vote: %v\n", r.id, m.From, !m.Reject)
			if m.Term == r.Term {
				r.votes[m.From] = !m.Reject
			}
			checkVoteNumsHasGot(r)
			if checkVoteNumsHasGot(r) {
				r.becomeLeader()
			}
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.Term = m.Term
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  false,
				})
				r.Vote = m.From
				r.becomeFollower(m.Term, m.From)
			} else { // ==
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})
			}
			log.Debugf("[%v] receive voteReq from [%v] and vote: %v\n", r.id, m.From, !m.Reject)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for k, _ := range r.votes {
				if k == r.id {
					continue
				}
				r.sendHeartbeat(k)
			}
		case pb.MessageType_MsgHeartbeatResponse:
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.Term = m.Term
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  false,
				})
				r.Vote = m.From
				r.becomeFollower(m.Term, m.From)
			} else { // ==
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					To:      m.From,
					From:    r.id,
					Term:    r.Term,
					Reject:  true,
				})
			}
			log.Debugf("[%v] receive voteReq from [%v] and vote: %v\n", r.id, m.From, !m.Reject)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		return
	}
	if m.Term > r.Term {
		r.Term = m.Term
	}
	r.heartbeatElapsed = 0
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func checkVoteNumsHasGot(r *Raft) bool {
	i := 0
	for _, v := range r.votes {
		if v {
			i++
		}
	}
	return i > len(r.votes)/2
}

func resetVotes(r *Raft) {
	for k, _ := range r.votes {
		r.votes[k] = false
	}
}
