package dblog

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
)

type OnSchedule func(response *pb.DumpInfoResponse) error
type CancelFunc func()

type Scheduler interface {
	Schedule(uri string, dumps []*pb.DumpInfoResponse) error
	Register(uri string, client net.Addr, fn OnSchedule) CancelFunc
	Ack(uri string, client net.Addr, requeue string)
}

type MemoryScheduler struct {
	ScheduleInterval time.Duration

	pending   map[string]*pending
	clients   map[string]map[net.Addr]*track
	pendingMu sync.Mutex
	clientsMu sync.Mutex
}

func (s *MemoryScheduler) Schedule(uri string, dumps []*pb.DumpInfoResponse) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, ok := s.pending[uri]; ok {
		return errors.New("already scheduled")
	}
	s.pending[uri] = &pending{dumps: dumps}
	go s.schedule(uri)
	return nil
}

func (s *MemoryScheduler) schedule(uri string) {
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, uri)
		s.pendingMu.Unlock()
	}()

	for {
		var candidate *track
		var dump *pb.DumpInfoResponse
		time.Sleep(s.ScheduleInterval)

		busy := 0
		remain := 0

		s.clientsMu.Lock()
		if clients, ok := s.clients[uri]; ok {
			for _, track := range clients {
				if track.dump == nil {
					candidate = track
				} else {
					busy++
				}
			}
		}
		s.clientsMu.Unlock()

		s.pendingMu.Lock()
		if pending, ok := s.pending[uri]; ok {
			remain = pending.Remaining()
			if candidate != nil {
				dump = pending.Pop()
			}
		}
		s.pendingMu.Unlock()

		if candidate != nil && dump != nil {
			s.clientsMu.Lock()
			candidate.dump = dump
			s.clientsMu.Unlock()
			if err := candidate.schedule(dump); err != nil {
				candidate.cancel()
			}
		}

		if busy == 0 && remain == 0 {
			return
		}
	}
}

func (s *MemoryScheduler) Register(uri string, client net.Addr, fn OnSchedule) CancelFunc {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	clients, ok := s.clients[uri]
	if !ok {
		clients = make(map[net.Addr]*track)
		s.clients[uri] = clients
	}
	track := &track{schedule: fn, cancel: func() {
		s.Ack(uri, client, "canceled")
		s.clientsMu.Lock()
		delete(clients, client)
		s.clientsMu.Unlock()
	}}
	clients[client] = track
	return track.cancel
}

func (s *MemoryScheduler) Ack(uri string, client net.Addr, requeue string) {
	var dump *pb.DumpInfoResponse

	s.clientsMu.Lock()
	if clients, ok := s.clients[uri]; ok {
		if track, ok := clients[client]; ok {
			dump = track.dump
			track.dump = nil
		}
	}
	s.clientsMu.Unlock()

	if dump != nil && requeue != "" {
		s.pendingMu.Lock()
		if pending, ok := s.pending[uri]; ok {
			pending.Push(dump)
		}
		s.pendingMu.Unlock()
	}
}

type track struct {
	dump     *pb.DumpInfoResponse
	schedule OnSchedule
	cancel   CancelFunc
}

type pending struct {
	dumps  []*pb.DumpInfoResponse
	offset int
}

func (p *pending) Remaining() int {
	return len(p.dumps) - p.offset
}

func (p *pending) Pop() *pb.DumpInfoResponse {
	if len(p.dumps) == p.offset {
		return nil
	}
	ret := p.dumps[p.offset]
	p.offset++
	return ret
}

func (p *pending) Push(dump *pb.DumpInfoResponse) {
	if p.offset == 0 {
		return
	}
	p.offset--
	p.dumps[p.offset] = dump
}
