## LSMT package
The lsmt package provides a single level embedded log structured merge tree (LSMT) written completely in GO for efficient data storage and retrieval.

It features a memory-based AVL tree (memtable) that temporarily holds key-value pairs before flushing them to sorted string tables (SSTables) on disk.

Compaction is triggered when the number of SSTables reaches a certain threshold, and the SSTables are merged to reduce the number of files and improve read performance.

## Benchmarking
11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz UBuntu with WDC WDS500G2B0A-00SM50(HDD) we put 1 MILLION keys in `523ms` 523 milliseconds

That is roughly *1,912,577 operations per second.*

<div>
    <h1 align="center"><img width="328" src="drawing.png"></h1>
</div>

### Features
- `Memtable` - The use of an in-memory AVL tree (memtable) allows for fast insertions and lookups. By accumulating writes in memory, the implementation reduces the number of disk I/O operations.
- `Batch Writes to SSTables` -  Instead of writing each key-value pair immediately to disk, the system flushes the memtable to an SSTable when it reaches a predefined size (memtableFlushSize). This batching improves write performance.
- `Compaction Strategy` - The compaction process merges multiple SSTables into fewer ones, reducing the number of files and thus the amount of disk I/O needed for reads. The implementation also ensures that a minimum number of SSTables is retained to optimize read performance.
- `Efficient Key Lookups` - The use of binary search on SSTables improves the efficiency of key lookups. Each SSTable is stored sorted, allowing fast search operations.
- `Range Queries` -  The implementation supports various range queries (e.g., Range, GreaterThan, LessThan), which can be optimized for both the memtable and SSTables.
- `Concurrent Access` -  The use of read-write mutexes allows concurrent reads while ensuring safe writes to the memtable, which can improve performance in multi-threaded environments.
- `Tombstones for Deletions` - Instead of physically removing key-value pairs from SSTables, tombstones are written to represent deletions. This avoids the overhead of immediate compaction and allows the system to manage deletions in a more efficient way.
- `File Management` - The implementation supports splitting large SSTables into smaller ones, which can help maintain read performance by keeping SSTables manageable in size.
- `Lazy Loading of SSTables` - SSTables are only loaded into memory when necessary, minimizing memory usage and startup time.
- `WAL for Durability` - The implementation uses a write-ahead log (WAL) to ensure durability. The WAL records all write operations before they are applied to the memtable, providing a way to recover the system in case of a crash.
- `Transaction Support` - The implementation supports transactions, allowing multiple write operations to be grouped together and applied atomically to the memtable.


### Usage
Importing
```
github.com/guycipher/lsmt
```

```go
// Create a new LSM-tree in the specified directory
directory := "data"

// You can specify the directory, file permissions, max memtable size (amount of keyv's), and compaction interval (amount of ssTables before compaction), amount of minimum sstables after compaction
l, err := lsmt.New(directory, os.FileMode(0777), 10, 5, 2)
if err != nil {
    fmt.Println("Error creating LSM-tree:", err)
    return
}

defer os.RemoveAll(directory) // Clean up after use

// Successfully created the LSM-tree
fmt.Println("LSM-tree created successfully!")
```

### Put
You can insert a value into a key using the ``Put`` method.
If you try to insert a key that already exists, the value will be updated.
```go
// Assume lst is already created
// Insert key-value pairs into the LSM-tree
if err := l.Put([]byte("key1"), []byte("value1")); err != nil {
    fmt.Println("Error inserting key1:", err)
}
if err := l.Put([]byte("key2"), []byte("value2")); err != nil {
    fmt.Println("Error inserting key2:", err)
}

fmt.Println("Key-value pairs inserted successfully!")
```

### Get
To get a value you can you the ``Get`` method.  The get method will return all the keys values.
```go
// Assume lst is already created and populated
value, err := l.Get([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### NGet
To get all keys not equal to the key you can use the ``NGet`` method.
```go
// Assume lst is already created and populated
keys, values, err:= l.NGet([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved values not equal to key1:", string(value))
}
```

### Delete
Delete key2
```go
// Assume lst is already created
if err := l.Delete([]byte("key2")); err != nil {
    fmt.Println("Error deleting key2:", err)
} else {
    fmt.Println("key2 marked for deletion.")
}
```

### Range
Get all keys between key56 and key100
```go
// Assume lst is already created and populated
keys, values, err := l.Range([]byte("key56"), []byte("key100"))
if err != nil {
    log.Fatal(err)
}
for i, key := range keys {
    fmt.Printf("Key: %s, Value: %s\n", string(key), string(values[i]))
}
```

### NRange
Get all keys not between key1 and key3
```go
// Assume lst is already created and populated
keys, values, err := l.NRange([]byte("key1"), []byte("key3"))
if err != nil {
    log.Fatal(err)
}
for i, key := range keys {
    fmt.Printf("Key: %s, Value: %s\n", string(key), string(values[i]))
}
```

### GreaterThan
Get all keys greater than key1
```go
// Assume lst is already created and populated
keys, values, err := l.GreaterThan([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### GreaterThanEqual
Get all keys greater than or equal to key1
```go
// Assume lst is already created and populated
keys, values, err := l.GreaterThanEqual([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### LessThan
Get all keys less than key1
```go
// Assume lst is already created and populated
keys, values, err := l.LessThan([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### LessThanEqual
Get all keys less than or equal to key1
```go
// Assume lst is already created and populated
keys, values, err := l.LessThanEqual([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### Compaction
```go
// Assume lst is already created and populated
if err := l.Compact(); err != nil {
    fmt.Println("Error compacting LSM-tree:", err)
} else {
    fmt.Println("LSM-tree compacted successfully!")
}
```

### Transactions
```go
// Start a new transaction
tx := l.BeginTransaction()

// Add a put operation to the transaction
tx.AddPut([]byte("key1"), []byte("value1"))

// Add a delete operation to the transaction
tx.AddDelete([]byte("key2"))

// Commit the transaction
if err := l.CommitTransaction(tx); err != nil {
fmt.Println("Error committing transaction:", err)
}

```

#### Rollback
```go
// Abort the transaction
l.RollbackTransaction(tx)
```

### WAL Recovery
```go
// Assume lst is already created
ops, err := l.GetWAL().Recover()
if err != nil {
    fmt.Println("Error recovering WAL:", err)
} else {
    err := l.RunRecoveredOperations(ops)
    if err != nil {
        fmt.Println("Error running recovered operations:", err)
    }

    fmt.Println("Recovered operations:", ops)
}
```

### Close
Flushes the memtable to disk and closes all opened sstables
```go
// Assume lst is already created and populated
if err := l.Close(); err != nil {
    fmt.Println("Error closing LSM-tree:", err)
} else {
    fmt.Println("LSM-tree closed successfully!")
}
```
