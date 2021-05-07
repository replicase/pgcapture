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
		if err := s.Schedule(uri, group.dumps); err != nil {
			t.Fatal(err)
		}
	}

	for uri, group := range groups {
		if err := s.Schedule(uri, group.dumps); err != ErrAlreadyScheduled {
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
				if group.counter != len(group.dumps) {
					go func() {
						s.Ack(uri, strconv.Itoa(i), "")
					}()
				} else {
					go func() {
						s.Ack(uri, strconv.Itoa(i), "")
						close(group.done)
					}()
				}
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
				t.Fatalf("full client should be scheduled")
			}
			client.cancel()
		}
	}

	// wait schedule finished
	time.Sleep(time.Millisecond * 100)
	// start a new schedule with the same uri
	dumps := []*pb.DumpInfoResponse{{Table: URI1, PageBegin: 777}}
	if err := s.Schedule(URI1, dumps); err != nil {
		t.Fatal(err)
	}
	// error client should be unregistered later
	if _, err := s.Register(URI1, "1", func(dump *pb.DumpInfoResponse) error {
		return errors.New("any error")
	}); err != nil {
		t.Fatal(err)
	}
	// wait for unregister error client
	time.Sleep(time.Millisecond * 100)
	// register again, and re-consume the fail dump
	done := make(chan struct{})
	if _, err := s.Register(URI1, "1", func(dump *pb.DumpInfoResponse) error {
		if dump != dumps[0] {
			t.Fatalf("dump not match")
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-done
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
