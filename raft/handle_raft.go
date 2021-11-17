package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// stepStateFollower step of stateFollower.
func (r *Raft) stepStateFollower(m pb.Message) error {
	// Code Here
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		r.handleStartNewElection()

	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:

	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:

	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:

	// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:

	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

// stepStateCandidate step of stateCandidate.
func (r *Raft) stepStateCandidate(m pb.Message) error {
	// Code Here
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:
		r.handleStartNewElection()

	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:

	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:

	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:

	// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:

	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

// stepStateLeader step of stateLeader.
func (r *Raft) stepStateLeader(m pb.Message) error {
	// Code Here
	switch m.MsgType {
	// 'MessageType_MsgHup' is a local message used for election. If an election timeout happened,
	// the node should pass 'MessageType_MsgHup' to its Step method and start a new election.
	case pb.MessageType_MsgHup:

	// 'MessageType_MsgBeat' is a local message that signals the leader to send a heartbeat
	// of the 'MessageType_MsgHeartbeat' type to its followers.
	case pb.MessageType_MsgBeat:
		r.handleSendHeatBeat()

	// 'MessageType_MsgPropose' is a local message that proposes to append data to the leader's log entries.
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)

	// 'MessageType_MsgAppend' contains log entries to replicate.
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)

	// 'MessageType_MsgAppendResponse' is response to log replication request('MessageType_MsgAppend').
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse()

	// 'MessageType_MsgRequestVote' requests votes for election.
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	// 'MessageType_MsgRequestVoteResponse' contains responses from voting request.
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)

	// 'MessageType_MsgSnapshot' requests to install a snapshot message.
	case pb.MessageType_MsgSnapshot:

	// 'MessageType_MsgHeartbeat' sends heartbeat from leader to its followers.
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	// 'MessageType_MsgHeartbeatResponse' is a response to 'MessageType_MsgHeartbeat'.
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeat(m)

	// 'MessageType_MsgTransferLeader' requests the leader to transfer its leadership.
	case pb.MessageType_MsgTransferLeader:
		r.handleHeartbeatResponse(m)
	// 'MessageType_MsgTimeoutNow' send from the leader to the leadership transfer target, to let
	// the transfer target timeout immediately and start a new election.
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

// handleStartNewElection handle Start a new election.
func (r *Raft) handleStartNewElection() {
	// Code Here.
	r.becomeCandidate()

	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// send request vote to other peers.
	for peer := range r.Prs {
		if peer != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      peer,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}

}

// handleRequestVote handle requests votes for election.
func (r *Raft) handleRequestVote(m pb.Message) {
	// Code Here.
	// send vote response to from-peer.
	msgRefuse := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}

	// Outdated request.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, msgRefuse)
		return
	}

	// Current node is outdated
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// Send vote.
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	})

}

// handleRequestVoteResponse handle response to 'MessageType_MsgHeartbeat'.
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Code Here.

	// Outdated request response.
	if m.Term < r.Term {
		return
	}

	// Current node is outdated
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

	// Update votes.
	r.votes[m.From] = !m.Reject
	accept, refuse := 0, 0
	for peer, _ := range r.votes {
		if r.votes[peer] {
			accept++
		} else {
			refuse++
		}

		// Most accept or refuse.
		if accept > len(r.votes)/2 {
			r.becomeLeader()
			return
		}
	}
}

// handleSendHeatBeat handle the leader to send a heartbeat.
func (r *Raft) handleSendHeatBeat() {
	// Code Here.

	// Send heartbeat to other nodes.
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

// handlePropose handle proposes to append data to the leader's log entries.
func (r *Raft) handlePropose(m pb.Message) {
	// Code Here.

	// Current node is outdated
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

}

// handlePropose handle response to log replication request('MessageType_MsgAppend').
func (r *Raft) handleAppendResponse() {
	// Code Here.
}

// handleHeartbeatResponse handle response to 'MessageType_MsgHeartbeat'.
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Code Here.

	// Outdated leader
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}

}
