package source

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/pb"
	"log"
	"time"
)

type PulsarSource struct {
	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	client       pulsar.Client
	reader       pulsar.Reader

	commitTime time.Time
}

func (p *PulsarSource) Setup() (err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	return err
}

func (p *PulsarSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	p.reader, err = p.client.CreateReader(pulsar.ReaderOptions{
		Topic:                   p.PulsarTopic,
		StartMessageID:          pulsar.EarliestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}
	if !cp.Time.IsZero() {
		if err = p.reader.SeekByTime(cp.Time); err != nil {
			return nil, err
		}
	}
	changes = make(chan Change, 100)
	go func(cp Checkpoint) {
		defer close(changes)
		for p.reader.HasNext() {
			msg, err := p.reader.Next(context.Background())
			if err != nil {
				log.Fatalf("from pulsar failed: %v", err)
				return
			}
			lsn, err := pglogrepl.ParseLSN(msg.Properties()["lsn"])
			if err != nil {
				log.Fatalf("from pulsar parse lsn failed: %v", err)
				return
			}
			if cp.LSN > uint64(lsn) {
				continue
			}
			m := &pb.Message{}
			if err = proto.Unmarshal(msg.Payload(), m); err != nil {
				log.Fatalf("from pulsar parse proto failed: %v", err)
				return
			}
			changes <- Change{LSN: uint64(lsn), Message: m}
		}
	}(cp)
	return changes, nil
}

func (p *PulsarSource) Commit(cp Checkpoint) {
	return
}

func (p *PulsarSource) Stop() {
	p.reader.Close()
}
