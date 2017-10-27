# A general purpose, high performance write-ahead-log

## Usage

First the wal needs to be opened by calling `New(string path)`. This will create a new WAL at the provided path or load
an existing one. The latter will recover the WAL and return all the updates of transactions that were not completed.

```
// Open the WAL.                                                                                 
updates, wal, err := New(walPath)                                                            
if err != nil {                                                                                  
    return nil, err                                                                              
} 
```

The wal can then be used to create a `Transaction` like this using a set of updates:

```
// Create the WAL transaction.                                                                   
tx, err := ca.wal.NewTransaction(updates)                                                        
if err != nil {                                                                                  
    return err                                                                                   
}   
```

An `Update` consists of a Name, Version and Instructions. One transaction can hold multiple updates.
After the `Transaction` is created the caller might want to do some kind of setup. Once completed the next step is to signal the WAL that the setup is complete.

```
// Signal completed setup and then wait for the commitment to finish.                            
errChan := tx.SignalSetupComplete()                                                              
err = <-errChan                                                                                  
if err != nil {                                                                                  
    return err                                                                                   
} 
```

This will write the updates to disk and commit them. The caller needs to wait on the returned channel to ensure a successfull commit. After the commit it is safe for the caller to apply the updates to disk and once finished, signal the wal that it can now mark the transaction as applied and recycle the transaction.

```
err = tx.SignalUpdatesApplied()                                                                  
if err != nil {                                                                                  
    return err                                                                                   
}
´´´
