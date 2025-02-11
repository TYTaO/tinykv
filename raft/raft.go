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

	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				panic(err)
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				panic(err)
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
	}

	//if r.State == StateLeader {
	//	r.heartbeatElapsed++
	//}
	r.electionElapsed++
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	log.Debugf("[%v] become Follower\n", r.id)
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	log.Debugf("[%v] become Candidate\n", r.id)
	r.State = StateCandidate
	r.Term += 1
	r.electionElapsed = 0

	for id, _ := range r.votes {
		if id == r.id {
			// vote for itself
			r.votes[id] = true
			continue
		}
		log.Debugf("[%v] send voteReq to [%v]\n", r.id, id)
		r.votes[id] = false
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
	r.heartbeatElapsed = r.heartbeatTimeout
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled  响应
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//if m.Term < r.Term {
	//	return nil
	//}
	switch r.State {
	case StateFollower:
		err := r.stepStateFollower(m)
		return err
	case StateCandidate:
		err := r.stepStateCandidate(m)
		return err
	case StateLeader:
		err := r.stepStateLeader(m)
		return err
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// Reject Response
	rejectMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}

	// Outdated message.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, rejectMsg)
		return
	}
	r.becomeFollower(m.Term, m.From)

	/*
			// Doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
			preLogTerm, err := r.RaftLog.Term(m.Index)
			if err != nil || preLogTerm != m.LogTerm {
				r.msgs = append(r.msgs, rejectMsg)
				return
			}

			// Same index but different terms.
			lastIndex := r.RaftLog.LastIndex()
			current_index := 0
			for index, entry := range m.Entries {
				if entry.Index > lastIndex {
					current_index = index
					break
				}

				term, err := r.RaftLog.Term(entry.Index)
				if err != nil {
					return
				}
				// delete logs
				if term != entry.Term {
					r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
					r.RaftLog.entries = r.RaftLog.entries[:entry.Index-r.RaftLog.first]
					break
				}
				current_index = index
			}

			// Append logs.
			if len(m.Entries) > 0 && m.Entries[current_index].Index > r.RaftLog.LastIndex() {
				for _, entry := range m.Entries[current_index:] {
					r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
						EntryType: entry.EntryType,
						Term:      entry.Term,
						Index:     entry.Index,
						Data:      entry.Data,
					})
				}
			}

		 	fmt.Println(m.Commit, r.RaftLog.committed)

			// leaderCommit > commitIndex
			if m.Commit > r.RaftLog.committed {
				lastIndex = m.Index
				if len(m.Entries) > 0 {
					lastIndex = m.Entries[len(m.Entries)-1].Index
				}
				r.RaftLog.committed = min(m.Commit, lastIndex)
			}
	*/

	// Accept
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	// Outdated request.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgHeartbeatResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})
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
