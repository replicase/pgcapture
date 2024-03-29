package cursor

import "github.com/apache/pulsar-client-go/pulsar"

type Tracker interface {
	Last() (cp Checkpoint, err error)
	Start()
	Commit(cp Checkpoint, mid pulsar.MessageID) error
	Close()
}
