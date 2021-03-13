package dblog

import (
	"errors"
	"sync"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
	"github.com/sirupsen/logrus"
)

type OnSchedule func(response *pb.DumpInfoResponse) error
type CancelFunc func()

var ErrAlreadyScheduled = errors.New("already scheduled")
var ErrAlreadyRegistered = errors.New("already registered")

type Scheduler interface {
	Schedule(uri string, dumps []*pb.DumpInfoResponse) error
	Register(uri string, client string, fn OnSchedule) (CancelFunc, error)
	Ack(uri string, client string, requeue string)
}

func NewMemoryScheduler(interval time.Duration) *MemoryScheduler {
	return &MemoryScheduler{
		interval: interval,
		pending:  make(map[string]*pending),
		clients:  make(map[string]map[string]*track),
		log:      logrus.WithFields(logrus.Fields{"From": "MemoryScheduler"}),
	}
}

type MemoryScheduler struct {
	interval  time.Duration
	pending   map[string]*pending
	clients   map[string]map[string]*track
	pendingMu sync.Mutex
	clientsMu sync.Mutex
	log       *logrus.Entry
}

func (s *MemoryScheduler) Schedule(uri string, dumps []*pb.DumpInfoResponse) error {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if _, ok := s.pending[uri]; ok {
		return ErrAlreadyScheduled
	}
	s.pending[uri] = &pending{dumps: dumps}
	s.log.WithFields(logrus.Fields{
		"URI":     uri,
		"NumDump": len(dumps),
	}).Infof("start scheduling dumps of %s", uri)
	go s.schedule(uri)
	return nil
}

func (s *MemoryScheduler) schedule(uri string) {
	defer func() {
		s.pendingMu.Lock()
		delete(s.pending, uri)
		s.pendingMu.Unlock()
		s.log.WithFields(logrus.Fields{
			"URI": uri,
		}).Infof("finish scheduling dumps of %s", uri)
	}()

	for {
		time.Sleep(s.interval)

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
		s.log.WithFields(logrus.Fields{
			"URI":    uri,
			"Client": client,
		}).Infof("unregistered client %s from uri %s", client, uri)
	}}
	clients[client] = track

	s.log.WithFields(logrus.Fields{
		"URI":    uri,
		"Client": client,
	}).Infof("registered client %s to uri %s", client, uri)

	return track.cancel, nil
}

func (s *MemoryScheduler) Ack(uri string, client string, requeue string) {
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
