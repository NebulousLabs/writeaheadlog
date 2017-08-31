package wal

// These interfaces define the wal's dependencies. Using the smallest
// interface possible makes it easier to mock these dependencies in testing.
type (
	dependencies interface {
		disrupt(string) bool
	}
)

type prodDependencies struct{}

func (prodDependencies) disrupt(string) bool { return false }
