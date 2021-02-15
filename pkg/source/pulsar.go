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
		StartMessageID:          pulsar.LatestMessageID(),
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}
	if cp.MID != nil {
		mid, err := pulsar.DeserializeMessageID(cp.MID)
		if err != nil {
			return nil, err
		}
		if err = p.reader.Seek(mid); err != nil {
			return nil, err
		}
	} else if !cp.Time.IsZero() {
		if err = p.reader.SeekByTime(cp.Time.Add(time.Hour * -1)); err != nil {
			return nil, err
		}
	}
	changes = make(chan Change, 100)
	go func(cp Checkpoint) {
		defer close(changes)
		var consistent bool
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
			if cp.LSN != 0 && !consistent {
				consistent = cp.LSN == uint64(lsn)
				continue
			}

			m := &pb.Message{}
			if err = proto.Unmarshal(msg.Payload(), m); err != nil {
				log.Fatalf("from pulsar parse proto failed: %v", err)
				return
			}
			changes <- Change{
				Checkpoint: Checkpoint{
					LSN:  uint64(lsn),
					MID:  msg.ID().Serialize(),
					Time: msg.EventTime(),
				},
				Message: m,
			}
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
