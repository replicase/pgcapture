package test

import (
	"github.com/rueian/pgcapture/pkg/sink"
	"github.com/rueian/pgcapture/pkg/source"
)

func SourceToSink(src source.Source, sk sink.Sink, shutdown chan struct{}, cb func()) (err error) {
	lastCheckPoint, err := sk.Setup()
	if err != nil {
		panic(err)
	}

	changes, err := src.Capture(lastCheckPoint)
	if err != nil {
		panic(err)
	}

	go func() {
		checkpoints := sk.Apply(changes)
		for cp := range checkpoints {
			src.Commit(cp)
		}
	}()

	go cb()

	<-shutdown
	sk.Stop()
	src.Stop()
	if err := sk.Error(); err != nil {
		return err
	}
	if err := src.Error(); err != nil {
		return err
	}
	return nil
}
