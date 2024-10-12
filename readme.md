## LSMT package
The lsmt package provides a single level embedded log structured merge tree (LSMT) written completely in GO for efficient data storage and retrieval.

It features a memory-based AVL tree (memtable) that temporarily holds key-value pairs before flushing them to sorted string tables (SSTables) on disk.

The package supports basic operations like insertion (Put), retrieval (Get), and deletion (Delete), as well as compaction to optimize storage by merging and removing tombstoned entries.
Designed with concurrency in mind, it utilizes read-write locks for safe concurrent access.

### Usage
```go
// Create a new LSM-tree in the specified directory
directory := "data"

// You can specify the directory, file permissions, max memtable size (amount of keyv's), and compaction interval (amount of ssTables before compaction), amount of minimum sstables after compaction
lst, err := lmst.New(directory, os.ModePerm(0777), 10, 5, 2)
if err != nil {
    fmt.Println("Error creating LSM-tree:", err)
    return
}

defer os.RemoveAll(directory) // Clean up after use

// Successfully created the LSM-tree
fmt.Println("LSM-tree created successfully!")
```

### Put
```go
// Assume lst is already created
// Insert key-value pairs into the LSM-tree
if err := lst.Put([]byte("key1"), []byte("value1")); err != nil {
    fmt.Println("Error inserting key1:", err)
}
if err := lst.Put([]byte("key2"), []byte("value2")); err != nil {
    fmt.Println("Error inserting key2:", err)
}

fmt.Println("Key-value pairs inserted successfully!")
```

### Get
```go
// Assume lst is already created and populated
value, err := lst.Get([]byte("key1"))
if err != nil {
    fmt.Println("Error retrieving key1:", err)
} else {
    fmt.Println("Retrieved value for key1:", string(value))
}
```

### Delete
```go
// Assume lst is already created
if err := lst.Delete([]byte("key2")); err != nil {
    fmt.Println("Error deleting key2:", err)
} else {
    fmt.Println("key2 marked for deletion.")
}
```

### Compaction
```go
// Assume lst is already created and populated
if err := lst.Compact(); err != nil {
    fmt.Println("Error compacting LSM-tree:", err)
} else {
    fmt.Println("LSM-tree compacted successfully!")
}
```