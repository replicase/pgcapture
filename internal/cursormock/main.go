package cursormock

//go:generate mockgen -destination=mock.go -package=$GOPACKAGE -source=../../pkg/cursor/tracker.go Tracker
