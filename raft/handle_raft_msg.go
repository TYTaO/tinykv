package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) broadcastVotes() {
	for follower, _ := range r.Prs {
		if follower == r.id {
			// vote for itself
			r.votes[r.id] = true
			checkVoteNumsHasGot(r)
			if checkVoteNumsHasGot(r) {
				r.becomeLeader()
				// send heartbeat
				if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
					log.Error(err)
				}
			}
			continue
		}
		log.Debugf("[%v] send voteReq to [%v]\n", r.id, follower)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      follower,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.reset(m.Term)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})
}

// handleRequestVote handle requests votes for election.
func (r *Raft) handleRequestVote(m pb.Message) {
	oldTerm := r.Term
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		msg.Reject = false
		msg.Term = r.Term
	} else if m.Term == r.Term && (r.Vote == None || r.Vote == m.From) { // just for pass test self req
		msg.Reject = false
		r.Vote = m.From
	} else { // ==
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
	log.Debugf("[%v] term:%v receive voteReq from [%v] term:%v and vote: %v\n", r.id, oldTerm, m.From, m.Term, !m.Reject)
}

// handleRequestVoteResponse handle response to 'MessageType_MsgHeartbeat'.
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Debugf("[%v] receive voteResponse from [%v] vote: %v\n", r.id, m.From, !m.Reject)
	if m.Term == r.Term {
		r.votes[m.From] = !m.Reject
	}
	checkVoteNumsHasGot(r)
	if checkVoteNumsHasGot(r) {
		r.becomeLeader()
		// send heartbeat
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Error(err)
		}
	}
}

// reset一系列操作
func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
		r.Vote = None
	}
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetVotes()
	r.randomElectionTimeout = r.getRandomElectionTimeout()
}

func (r *Raft) getRandomElectionTimeout() int {
	return r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func checkVoteNumsHasGot(r *Raft) bool {
	i := 0
	for _, v := range r.votes {
		if v {
			i++
		}
	}
	return i > len(r.Prs)/2
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	for follower, _ := range r.Prs {
		if follower == r.id {
			continue
		}
		r.sendHeartbeat(follower)
	}
}
