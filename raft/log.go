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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// account to progress match
	recordAppendRespNum map[int]int
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//entries := storage.(*MemoryStorage).ents
	entries := make([]pb.Entry, 0)
	// append storage log to raftlog todo
	ms := storage.(*MemoryStorage)
	for _, e := range ms.ents[ms.firstIndex():] {
		entries = append(entries, e)
	}
	return &RaftLog{
		storage:             storage,
		entries:             entries,
		recordAppendRespNum: map[int]int{},
		stabled:             uint64(ms.lastIndex()),
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	entries := l.entries[l.stabled:]
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied:l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if int(i) > len(l.entries) {
		return 0, errors.New("index out of i")
	}
	return l.entries[i-1].Term, nil
}

func (l *RaftLog) tryAppend(m pb.Message) bool {
	isConflict := false
	if l.matchTerm(m.Index, m.LogTerm) {
		// check conflict
		for _, e := range m.Entries {
			if !l.checkNoConflict(e) {
				isConflict = true
			}
		}
		if isConflict {
			// delete the existing conflict entry and all that follow it
			l.entries = l.entries[:m.Index]
			// update(del) entry in storage todo ？
			l.stabled = m.Index
			ms := l.storage.(*MemoryStorage)
			ms.Append(l.entries)
			// append new log
			for _, e := range m.Entries {
				l.entries = append(l.entries, *e)
			}
		}
		lastIndexOfNewEntry := m.Index
		if len(m.Entries) != 0 {
			lastIndexOfNewEntry = m.Entries[len(m.Entries)-1].Index
		}
		l.committed = min(m.Commit, lastIndexOfNewEntry)
		return true
	} else {
		return false
	}
}

// index and term ===  :前面的日志都相同
func (l *RaftLog) matchTerm(index uint64, logTerm uint64) bool {
	// no pre log
	if index == 0 {
		return true
	}
	term, err := l.Term(index)
	if err != nil {
		return false
	}
	return term == logTerm
}

func (l *RaftLog) checkNoConflict(e *pb.Entry) bool {
	term, err := l.Term(e.Index)
	if err != nil {
		return false
	}
	return term == e.Term
}

// more up to date
func (l *RaftLog) moreUpToDate(m pb.Message) bool {
	// check term
	term, err := l.Term(l.LastIndex())
	if err != nil {
		return false
	}
	if term > m.LogTerm {
		return true
	} else if term < m.LogTerm {
		return false
	}
	// == check index
	return l.LastIndex() > m.Index
}
