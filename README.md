# A general purpose, high performance write-ahead-log

A write-ahead-log (WAL) ensures durability and atomicity for updating data on
disk if used correctly.  Instructions are marshalled and passed to the WAL
before they are applied to disk. The WAL makes sure that the instructions are
synced to the WAL file before the user applies them. That way the wal can
notify the user about unfinished updates due to a sudden power outage. It is up
to the caller to guarantee that the instructions are idempotent and consistent.

## Usage

First the wal needs to be opened by calling `New(string path)`. This will
create a new WAL at the provided path or load an existing one. The latter will
recover the WAL and return all the updates of transactions that were not
completed.

```
// Open the WAL.
updates, wal, err := New(walPath)
if err != nil {
    return err
}

if len(updates) != 0 {
	// Apparently the system crashed. Handle the unfinished updates
	// accordingly.
    applyUpdates(updates)

	// After the recovery is complete we need to signal the WAL that we are
	// done. All calls to 'NewTransaction' will return an error until
	// RecoveryComplete() has been called.
    if err := wal.RecoveryComplete(); err != nil {
        return err
    }
}
```

The wal can then be used to create a `Transaction` like this using a set of
updates:

```
// Create the WAL transaction.
tx, err := ca.wal.NewTransaction(updates)
if err != nil {
    return err
}
```

An `Update` consists of a Name, Version and Instructions. One transaction can
hold multiple updates.  After the `Transaction` is created the caller might
want to do some kind of setup. Once completed the next step is to signal the
WAL that the setup is complete.

```
// Signal completed setup and then wait for the commitment to finish. This
// will cause the WAL to call fsync on it's underlying file.
errChan := tx.SignalSetupComplete()
err = <-errChan
if err != nil {
    return err
}
```

This will write the updates to disk and commit them. The caller needs to wait
on the returned channel to ensure a successfull commit. After the commit it is
safe for the caller to apply the updates to disk and once finished, signal the
wal that it can now mark the transaction as applied and recycle the
transaction.

```
// Signal that the updates were applied. This also causes the WAL to fsync and
// allows it to recycle used pages. The caller might run this in a goroutine
// but if the system crashes again before the call finishes the caller might
// receive the already applied instructions when recovering the WAL.
err = tx.SignalUpdatesApplied()
if err != nil {
    return err
}
```
