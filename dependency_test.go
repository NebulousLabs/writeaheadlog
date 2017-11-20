package writeaheadlog

//dependencyUncleanShutdown prevnts the wal from marking the logfile as clean
//upon shutdown
type dependencyUncleanShutdown struct {
	prodDependencies
}

func (dependencyUncleanShutdown) disrupt(s string) bool {
	if s == "UncleanShutdown" {
		return true
	}
	return false
}

// dependencyRecoveryFail causes the RecoveryComplete function to fail after
// the metadata was changed to wipe
type dependencyRecoveryFail struct {
	prodDependencies
}

func (dependencyRecoveryFail) disrupt(s string) bool {
	if s == "RecoveryFail" {
		return true
	}
	if s == "UncleanShutdown" {
		return true
	}
	return false
}
