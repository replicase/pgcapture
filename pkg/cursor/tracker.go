package cursor

type Tracker interface {
	Last() (cp Checkpoint, err error)
	Commit(cp Checkpoint) error
}
