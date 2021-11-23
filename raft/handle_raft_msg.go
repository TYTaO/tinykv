package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) broadAppendEntries() {
	for follower, _ := range r.Prs {
		if follower == r.id {
			continue
		}
		r.sendAppend(follower)
	}
}

func (r *Raft) broadcastVotes() {
	for follower, _ := range r.Prs {
		if follower == r.id {
			// vote for itself
			r.votes[r.id] = true
			hasFinished, isSuccess := checkVoteNumsHasGot(r)
			if hasFinished && isSuccess {
				r.becomeLeader()
				// send heartbeat
				if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
					log.Error(err)
				}
			}
			if hasFinished && !isSuccess {
				r.becomeFollower(r.Term, None)
			}
			continue
		}
		log.Debugf("[%v] send voteReq to [%v]\n", r.id, follower)
		logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
		if err != nil {
			// todo handle this err
			logTerm = 0
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      follower,
			From:    r.id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: logTerm,
		})
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	log.Debugf("[%v] <- [%v] receive appendEntries", r.id, m.From)
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.reset(m.Term)
	}
	if ok := r.RaftLog.tryAppend(m); ok {
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.LastIndex(), Reject: false})
	} else {
		r.msgs = append(r.msgs, pb.Message{To: m.From, From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index - 1, Reject: true})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	logTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		// todo handle this err
		logTerm = 0
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: logTerm,
	})
}

// handleRequestVote handle requests votes for election.
func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None) // for update the term to the more new
	}
	oldTerm := r.Term
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term > r.Term && !r.RaftLog.moreUpToDate(m) {
		r.Vote = m.From
		msg.Reject = false
		msg.Term = r.Term
	} else if m.Term == r.Term && (r.Vote == None || r.Vote == m.From) && !r.RaftLog.moreUpToDate(m) { // just for pass test self req
		msg.Reject = false
		r.Vote = m.From
	} else { // ==
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
	log.Debugf("[%v] term:%v receive voteReq from [%v] term:%v and vote: %v\n", r.id, oldTerm, m.From, m.Term, !msg.Reject)
}

// handleRequestVoteResponse handle response to 'MessageType_MsgHeartbeat'.
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Debugf("[%v] receive voteResponse from [%v] vote: %v\n", r.id, m.From, !m.Reject)
	if m.Term == r.Term {
		r.votes[m.From] = !m.Reject
	}
	hasFinished, isSuccess := checkVoteNumsHasGot(r)
	if hasFinished && isSuccess {
		r.becomeLeader()
		// send heartbeat
		if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
			log.Error(err)
		}
	}
	if hasFinished && !isSuccess {
		r.becomeFollower(r.Term, None)
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

func checkVoteNumsHasGot(r *Raft) (hasFinished bool, isSuccess bool) {
	succ := 0
	fail := 0
	for _, v := range r.votes {
		if v {
			succ++
		} else {
			fail++
		}
	}
	if succ > len(r.Prs)/2 {
		return true, true
	}
	if fail > len(r.Prs)/2 {
		return true, false
	}
	return false, false
}

func (r *Raft) resetVotes() {
	r.votes = make(map[uint64]bool)
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, e := range m.Entries {
		e.Index = uint64(len(r.RaftLog.entries) + 1)
		e.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
		r.Prs[r.id].Match = e.Index
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1
		if 1 > len(r.Prs)/2 {
			r.RaftLog.committed = e.Index
		}
	}
	log.Debugf("[%v] handle propose append entries: %v", r.id, r.RaftLog.entries)
	r.broadAppendEntries()
}

func (r *Raft) handleBeat(m pb.Message) {
	for follower, _ := range r.Prs {
		if follower == r.id {
			continue
		}
		r.sendHeartbeat(follower)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		log.Error(err)
		return
	}
	if r.Term != logTerm {
		log.Debugf("[%v] <- [%v] receive old term AppendResponse", r.id, m.From)
		return
	}
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
		// map
		num, ok := r.RaftLog.recordAppendRespNum[int(m.Index)]
		if ok {
			r.RaftLog.recordAppendRespNum[int(m.Index)] = num + 1
		} else {
			r.RaftLog.recordAppendRespNum[int(m.Index)] = 1
		}
		// commit    +1: self
		if r.RaftLog.recordAppendRespNum[int(m.Index)]+1 > len(r.Prs)/2 && m.Index > r.RaftLog.committed {
			r.RaftLog.committed = m.Index
			// todo storage
			//storage := r.RaftLog.storage.(*MemoryStorage)
			//index, _ := storage.LastIndex()
			//storage.Append(r.RaftLog.entries[index:])
			//r.RaftLog.stabled = r.RaftLog.committed
			// to tell follower the updated commit
			r.broadAppendEntries()
		}
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.RaftLog.moreUpToDate(m) {
		r.broadAppendEntries()
	}
}
