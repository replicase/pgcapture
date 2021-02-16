package source

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/golang/protobuf/proto"
	"github.com/jackc/pglogrepl"
	"github.com/rueian/pgcapture/pkg/pb"
	"os"
	"time"
)

type PulsarSource struct {
	BaseSource

	PulsarOption pulsar.ClientOptions
	PulsarTopic  string
	client       pulsar.Client
	reader       pulsar.Reader

	consistent bool
}

func (p *PulsarSource) Setup() (err error) {
	p.client, err = pulsar.NewClient(p.PulsarOption)
	return err
}

func (p *PulsarSource) Capture(cp Checkpoint) (changes chan Change, err error) {
	var sid pulsar.MessageID
	if cp.Time.IsZero() {
		sid = pulsar.LatestMessageID()
	} else {
		sid = pulsar.EarliestMessageID()
	}

	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	p.reader, err = p.client.CreateReader(pulsar.ReaderOptions{
		Name:                    host,
		Topic:                   p.PulsarTopic,
		StartMessageID:          sid,
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}

	if !cp.Time.IsZero() {
		if err = p.reader.SeekByTime(cp.Time.Add(-1 * time.Second)); err != nil {
			return nil, err
		}
	}

	return p.BaseSource.capture(func(ctx context.Context) (change Change, err error) {
		var msg pulsar.Message
		var lsn pglogrepl.LSN

		msg, err = p.reader.Next(ctx)
		if err != nil {
			return
		}
		lsn, err = pglogrepl.ParseLSN(msg.Properties()["lsn"])
		if err != nil {
			return
		}

		if !p.consistent && cp.LSN != 0 {
			p.consistent = cp.LSN == uint64(lsn)
			return
		}

		m := &pb.Message{}
		if err = proto.Unmarshal(msg.Payload(), m); err != nil {
			return
		}

		if !p.consistent && cp.LSN == 0 {
			if p.consistent = m.GetBegin() != nil; !p.consistent {
				return
			}
		}

		change = Change{Checkpoint: Checkpoint{LSN: uint64(lsn)}, Message: m}
		return
	}, func() {
		p.reader.Close()
		p.client.Close()
	})
}

func (p *PulsarSource) Commit(cp Checkpoint) {
	return
}
