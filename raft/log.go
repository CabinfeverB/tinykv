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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	stabled, _ := storage.LastIndex()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	log := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   hardState.Commit,
		stabled:   stabled,
		entries:   entries,
	}
	return log
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
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.stabled - l.entries[0].Index
	return l.entries[offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	firstIndex := l.entries[0].Index
	return l.entries[l.applied-firstIndex+1 : l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	l.stabled, _ = l.storage.LastIndex()
	if i <= l.stabled {
		return l.storage.Term(i)
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.entries[0].Index].Term, nil
}

func (l *RaftLog) SliceEntries(begin uint64) []*pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	ret := make([]*pb.Entry, 0)
	firstIndex := l.entries[0].Index
	pos := int(begin - firstIndex)
	for i := pos; i < len(l.entries); i++ {
		ret = append(ret, &l.entries[i])
	}
	return ret
}

func (l *RaftLog) cleanup(index uint64) {
	if index > l.LastIndex() {
		return
	}
	firstIndex := l.entries[0].Index
	l.entries = l.entries[0 : index-firstIndex]
	l.stabled = min(l.stabled, l.LastIndex())
}

func (l *RaftLog) AppendEntries(entries []*pb.Entry, lastIndex, lastTerm uint64) bool {
	// if lastIndex <= l.committed {
	// 	if lastIndex+uint64(len(entries)) <= l.committed {
	// 		return true
	// 	}
	// 	lastIndex = l.committed
	// 	lastTerm, _ = l.Term(l.committed)
	// 	entries = entries[l.committed-lastIndex:]
	// }
	// fmt.Println("input ", lastIndex, lastTerm, entries)
	// fmt.Printf("%+v\n", l)
	if lastIndex > l.LastIndex() {
		return false
	}
	term, err := l.Term(lastIndex)
	if err != nil || term != lastTerm {
		// bacause of TestFollowerCheckMessageType_MsgAppend2AB round 3
		// l.cleanup(lastIndex)
		return false
	}
	for {
		if len(entries) > 0 && lastIndex+1 <= l.LastIndex() {
			term, _ = l.Term(lastIndex + 1)
			if entries[0].Term == term {
				lastIndex++
				entries = entries[1:]
			} else {
				l.cleanup(lastIndex + 1)
				break
			}
		} else {
			break
		}
	}

	for _, entry := range entries {
		l.entries = append(l.entries, *entry)
	}
	return true
}

func (l *RaftLog) UpdateCommit(leaderCommit uint64) {
	l.committed = min(leaderCommit, l.LastIndex())
}

func CreateNoopEntry(index, term uint64) pb.Entry {
	return pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Index:     index,
		Term:      term,
	}
}
