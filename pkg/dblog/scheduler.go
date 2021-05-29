package dblog

import (
	"errors"
	"sync"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
)

type OnSchedule func(response *pb.DumpInfoResponse) error
type AfterSchedule func()
type CancelFunc func()

var ErrAlreadyScheduled = errors.New("already scheduled")
var ErrAlreadyRegistered = errors.New("already registered")

type Scheduler interface {
	Schedule(uri string, dumps []*pb.DumpInfoResponse, fn AfterSchedule) error
	Register(uri string, client string, fn OnSchedule) (CancelFunc, error)
	Ack(uri string, client string, requeue string)
	SetCoolDown(uri string, dur time.Duration)
}

func NewMemoryScheduler(interval time.Duration) *MemoryScheduler {
	return &MemoryScheduler{
		interval: interval,
		pending:  make(map[string]*pending),
		clients:  make(map[string]map[string]*track),
	}
}

type MemoryScheduler struct {
	interval  time.Duration
	pending   map[string]*pending
	clients   map[string]map[string]*track
	pendingMu sync.Mutex
	clientsMu sync.Mutex
}

func (s *MemoryScheduler) Schedule(uri string, dumps []*pb.DumpInfoResponse, fn AfterSchedule) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()

	if _, ok := s.pending[uri]; ok {
		return ErrAlreadyScheduled
	}
	s.pending[uri] = &pending{dumps: dumps}

	go s.schedule(uri, fn)
	return nil
}

func (s *MemoryScheduler) schedule(uri string, fn AfterSchedule) {
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, uri)
		s.pendingMu.Unlock()
		fn()
	}()

	for {
		time.Sleep(s.interval)

		loops := 0
		s.clientsMu.Lock()
		loops = len(s.clients[uri])
		s.clientsMu.Unlock()
		loops++

		for i := 0; i < loops; i++ {
			if s.scheduleOne(uri) {
				return
			}
		}
	}
}

func (s *MemoryScheduler) scheduleOne(uri string) bool {
	var candidate *track
	var dump *pb.DumpInfoResponse

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
		if candidate != nil && (pending.coolDown == 0 || time.Now().Sub(candidate.ackTs) > pending.coolDown) {
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

	return busy == 0 && remain == 0
}

func (s *MemoryScheduler) Register(uri string, client string, fn OnSchedule) (CancelFunc, error) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	clients, ok := s.clients[uri]
	if !ok {
		clients = make(map[string]*track)
		s.clients[uri] = clients
	}
	if _, ok = clients[client]; ok {
		return nil, ErrAlreadyRegistered
	}
	track := &track{schedule: fn, cancel: func() {
		s.Ack(uri, client, "canceled")
		s.clientsMu.Lock()
		delete(clients, client)
		s.clientsMu.Unlock()
	}}
	clients[client] = track

	return track.cancel, nil
}

func (s *MemoryScheduler) Ack(uri string, client string, requeue string) {
	var dump *pb.DumpInfoResponse

	s.clientsMu.Lock()
	if clients, ok := s.clients[uri]; ok {
		if track, ok := clients[client]; ok {
			dump = track.dump
			track.dump = nil
			track.ackTs = time.Now()
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

func (s *MemoryScheduler) SetCoolDown(uri string, dur time.Duration) {
	s.pendingMu.Lock()
	if pending, ok := s.pending[uri]; ok {
		pending.coolDown = dur
	}
	s.pendingMu.Unlock()
}

type track struct {
	dump     *pb.DumpInfoResponse
	ackTs    time.Time
	schedule OnSchedule
	cancel   CancelFunc
}

type pending struct {
	dumps    []*pb.DumpInfoResponse
	coolDown time.Duration
	offset   int
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
