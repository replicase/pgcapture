package dblog

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/rueian/pgcapture/pkg/pb"
)

func TestMemoryScheduler_Schedule(t *testing.T) {
	URI1 := "URI1"
	URI2 := "URI2"

	groups := map[string]*group{
		URI1: {dumps: makeDumps(URI1, 100), clients: make([]*client, 2), done: make(chan struct{})},
		URI2: {dumps: makeDumps(URI2, 100), clients: make([]*client, 2), done: make(chan struct{})},
	}

	s := NewMemoryScheduler(time.Millisecond)

	// start schedule each group without error
	for uri, group := range groups {
		group := group
		if err := s.Schedule(uri, group.dumps, func() {
			close(group.done)
		}); err != nil {
			t.Fatal(err)
		}
	}

	for uri, group := range groups {
		if err := s.Schedule(uri, group.dumps, nil); err != ErrAlreadyScheduled {
			t.Fatal("scheduled uri should be reject until finished")
		}
	}

	// callback of each group should not have race
	for uri, group := range groups {
		uri := uri
		group := group
		for i := range group.clients {
			i := i
			group.clients[i] = &client{}
			group.clients[i].cancel, _ = s.Register(uri, strconv.Itoa(i), func(dump *pb.DumpInfoResponse) error {
				if dump != group.dumps[group.counter] {
					t.Fatalf("dump should be delivered in order if no error")
				}
				group.counter++
				group.clients[i].counter++
				go func() {
					s.Ack(uri, strconv.Itoa(i), "")
				}()
				return nil
			})
		}
	}

	for uri, group := range groups {
		for i := range group.clients {
			if _, err := s.Register(uri, strconv.Itoa(i), nil); err != ErrAlreadyRegistered {
				t.Fatal("client can't be registered twice until unregistered")
			}
		}
	}

	for _, group := range groups {
		<-group.done
		for _, client := range group.clients {
			if client.counter == 0 {
				t.Fatalf("all clients should be scheduled")
			}
			client.cancel()
		}
	}

	done := make(chan struct{})
	// start a new schedule with the same uri
	dumps := []*pb.DumpInfoResponse{{Table: URI1, PageBegin: 777}}
	if err := s.Schedule(URI1, dumps, func() {
		done <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}

	// error client should be unregistered later
	scheduled := make(chan struct{})
	if _, err := s.Register(URI1, "1", func(dump *pb.DumpInfoResponse) error {
		scheduled <- struct{}{}
		return errors.New("any error")
	}); err != nil {
		t.Fatal(err)
	}
	// wait for unregister error client
	<-scheduled
	// register again, and re-consume the fail dump
	var err error
	var cancel CancelFunc
	for {
		cancel, err = s.Register(URI1, "1", func(dump *pb.DumpInfoResponse) error {
			if dump != dumps[0] {
				t.Fatalf("dump not match")
			}
			go func() {
				s.Ack(URI1, "1", "")
			}()
			scheduled <- struct{}{}
			return nil
		})
		if err == nil {
			break
		}
		if err == ErrAlreadyRegistered {
			continue
		}
		t.Fatal(err)
	}
	<-scheduled
	<-done
	cancel()

	// start a new schedule with the same uri
	// and set cool down duration
	// stop schedule in the middle
	dumps = []*pb.DumpInfoResponse{
		{Table: URI1, PageBegin: 777},
		{Table: URI1, PageBegin: 999},
		{Table: URI1, PageBegin: 777},
		{Table: URI1, PageBegin: 999},
	}
	if err := s.Schedule(URI1, dumps, func() {
		done <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}
	coolDown := time.Millisecond * 10
	s.SetCoolDown(URI1, coolDown)

	var received []time.Time
	if _, err := s.Register(URI1, "1", func(dump *pb.DumpInfoResponse) error {
		received = append(received, time.Now())
		if len(received) == 2 {
			s.StopSchedule(URI1)
		}
		go func() {
			s.Ack(URI1, "1", "")
		}()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-done
	if received[1].Sub(received[0]) < coolDown {
		t.Fatalf("received gap should not be smaller then cool down interval")
	}
	if len(received) != 2 {
		t.Fatalf("scheduler is not stopped as requested")
	}
}

func makeDumps(table string, n int) (dumps []*pb.DumpInfoResponse) {
	for i := 0; i < n; i++ {
		dumps = append(dumps, &pb.DumpInfoResponse{Table: table, PageBegin: uint32(i)})
	}
	return
}

type group struct {
	dumps   []*pb.DumpInfoResponse
	counter int
	clients []*client
	done    chan struct{}
}

type client struct {
	counter int
	cancel  CancelFunc
}
