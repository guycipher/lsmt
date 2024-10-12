// Package lsmt provides a single-level embedded log-structured merge-tree (LSM-tree)
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package lsmt

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"lsmt/avl"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

const SSTABLE_EXTENSION = ".sst"
const TOMBSTONE_VALUE = "$tombstone"

// LSMT is the main struct for the log-structured merge-tree.
type LSMT struct {
	memtable           *avl.AVLTree // The memtable is an in-memory AVL tree.
	memtableSize       atomic.Int64
	memtableLock       *sync.RWMutex // Lock for the memtable.
	sstables           []*SSTable    // The list of current SSTables.
	sstablesLock       *sync.RWMutex // Lock for the list of SSTables.
	directory          string        // The directory where the SSTables are stored.
	memtableFlushSize  int           // The size at which the memtable should be flushed to disk.
	compactionInterval int           // The interval at which the LSM-tree should be compacted. (in number of SSTables)
	minimumSSTables    int           // The minimum number of SSTables to keep.  On compaction, we will always keep this number of SSTables instead of one large SSTable.
}

// SSTable is a struct representing a sorted string table.
type SSTable struct {
	file   *os.File      // The opened SSTable file.
	minKey []byte        // The minimum key in the SSTable.
	maxKey []byte        // The maximum key in the SSTable.
	lock   *sync.RWMutex // Lock for the SSTable.
}

// New creates a new LSM-tree or opens an existing one.
func New(directory string, directoryPerm os.FileMode, memtableFlushSize, compactionInterval int, minimumSSTables int) (*LSMT, error) {
	if directory == "" {
		return nil, errors.New("directory cannot be empty")
	}

	// Check if the directory exists
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		// Create the directory if it doesn't exist
		err = os.Mkdir(directory, directoryPerm)
		if err != nil {
			return nil, err
		}

		return &LSMT{
			memtable:           avl.NewAVLTree(),
			memtableLock:       &sync.RWMutex{},
			sstables:           make([]*SSTable, 0),
			sstablesLock:       &sync.RWMutex{},
			directory:          directory,
			memtableFlushSize:  memtableFlushSize,
			compactionInterval: compactionInterval,
			minimumSSTables:    minimumSSTables,
		}, nil
	} else {

		// We create the directory and populate it with the SSTables
		files, err := os.ReadDir(directory)
		if err != nil {
			return nil, err
		}

		sstables := make([]*SSTable, 0)

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			if !strings.HasSuffix(file.Name(), SSTABLE_EXTENSION) {
				continue
			}

			// Open the SSTable file
			sstableFile, err := os.OpenFile(directory+string(os.PathSeparator)+file.Name(), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}

			// Create a new SSTable
			sstable := &SSTable{
				file:   sstableFile,
				minKey: nil,
				maxKey: nil,
				lock:   &sync.RWMutex{},
			}

			// Add the SSTable to the list of SSTables
			sstables = append(sstables, sstable)

			return &LSMT{
				memtable:           avl.NewAVLTree(),
				memtableLock:       &sync.RWMutex{},
				sstables:           sstables,
				sstablesLock:       &sync.RWMutex{},
				directory:          directory,
				memtableFlushSize:  memtableFlushSize,
				compactionInterval: compactionInterval,
				minimumSSTables:    minimumSSTables,
			}, nil
		}

	}

	return nil, errors.New("directory is not a directory")

}

// Put inserts a key-value pair into the LSM-tree.
func (l *LSMT) Put(key, value []byte) error {
	// We will first put the key-value pair in the memtable.
	// If the memtable size exceeds the flush size, we will flush the memtable to disk.

	// Lock memtable for writing.
	l.memtableLock.Lock()
	defer l.memtableLock.Unlock()

	// Put the key-value pair in the memtable.
	l.memtable.Insert(key, value)

	// If the memtable size exceeds the flush size, flush the memtable to disk.
	if l.memtableSize.Load() > int64(l.memtableFlushSize) {
		log.Println("Flushing memtable...")
		if err := l.flushMemtable(); err != nil {
			return err
		}
	} else {
		l.memtableSize.Swap(l.memtableSize.Load() + 1)
	}

	return nil
}

// flushMemtable flushes the memtable to disk, creating a new SSTable.
func (l *LSMT) flushMemtable() error {
	// We will create a new SSTable from the memtable and add it to the list of SSTables.
	// We will then clear the memtable.

	// Create a new SSTable from the memtable.
	sstable, err := l.newSSTable(l.directory, l.memtable)
	if err != nil {
		return err
	}

	// Lock sstables
	l.sstablesLock.Lock()
	defer l.sstablesLock.Unlock()

	// Add the SSTable to the list of SSTables.
	l.sstables = append(l.sstables, sstable)

	// Clear the memtable.
	l.memtable = avl.NewAVLTree()
	l.memtableSize.Swap(0)

	// Check the amount of sstables and if we need to compact
	if len(l.sstables) > l.compactionInterval {
		log.Println("Compacting LSM-tree...")
		if err := l.Compact(); err != nil {
			return err
		}

	}

	return nil
}

// KeyValue is a struct representing a key-value pair.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// newSSTable creates a new SSTable file from the memtable.
func (l *LSMT) newSSTable(directory string, memtable *avl.AVLTree) (*SSTable, error) {

	// Create a sorted map from the memtable which will be used to create the SSTable.

	sstableSlice := make([]*KeyValue, 0)

	memtable.InOrderTraversal(func(node *avl.Node) {
		sstableSlice = append(sstableSlice, &KeyValue{Key: node.Key, Value: node.Value})
	})

	if len(sstableSlice) == 0 {
		return nil, nil
	}

	// Based on amount of sstables we name the file
	fileName := fmt.Sprintf("%s%s%d%s", directory, string(os.PathSeparator), len(l.sstables), SSTABLE_EXTENSION)

	// Create a new SSTable file.
	ssltableFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	enc := gob.NewEncoder(ssltableFile)

	enc.Encode(sstableSlice)

	return &SSTable{
		file:   ssltableFile,
		minKey: sstableSlice[0].Key,
		maxKey: sstableSlice[len(sstableSlice)-1].Key,
	}, nil
}

// getSSTableKVs reads the key-value pairs from the SSTable file.
func getSSTableKVs(file *os.File) ([]*KeyValue, error) {
	// Seek to start of file
	_, err := file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	// Decode the SSTable file.
	dec := gob.NewDecoder(file)

	var kvs []*KeyValue
	err = dec.Decode(&kvs)
	if err != nil {
		return nil, err
	}

	return kvs, nil
}

// Get retrieves the value for a given key from the LSM-tree.
func (l *LSMT) Get(key []byte) ([]byte, error) {
	// We will first check the memtable for the key.
	// If the key is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the key.
	if node := l.memtable.Search(key); node != nil {
		l.memtableLock.RUnlock()

		if bytes.Compare(node.Value, []byte(TOMBSTONE_VALUE)) == 0 {
			return nil, errors.New("key not found")
		}

		return node.Value, nil
	}

	l.memtableLock.RUnlock()

	// Search the SSTables for the key.
	for i := len(l.sstables) - 1; i >= 0; i-- {
		// Lock the SSTable for reading.
		l.sstablesLock.RLock()
		defer l.sstablesLock.RUnlock()

		sstable := l.sstables[i]

		// If the key is not within the range of this SSTable, skip it.
		if bytes.Compare(key, sstable.minKey) < 0 || bytes.Compare(key, sstable.maxKey) > 0 {
			continue
		}

		// Read the key-value pairs from the SSTable file.
		kvs, err := getSSTableKVs(sstable.file)
		if err != nil {
			return nil, err
		}

		// Perform a binary search on the SSTable.
		index := binarySearch(kvs, key)
		if index != -1 {
			return kvs[index].Value, nil
		}
	}

	return nil, errors.New("key not found")
}

// Delete removes a key from the LSM-tree.
func (l *LSMT) Delete(key []byte) error {
	// We will write a tombstone value to the memtable for the key.

	// Lock memtable for writing.
	l.memtableLock.Lock()
	defer l.memtableLock.Unlock()

	// Write a tombstone value to the memtable for the key.
	l.memtable.Insert(key, []byte(TOMBSTONE_VALUE))

	return nil
}

// binarySearch performs a binary search on the key-value pairs to find the key.
func binarySearch(kvs []*KeyValue, key []byte) int {
	low, high := 0, len(kvs)-1

	for low <= high {
		mid := low + (high-low)/2

		if bytes.Compare(kvs[mid].Key, key) == 0 {
			return mid
		} else if bytes.Compare(kvs[mid].Key, key) < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return -1
}

// Compact compacts the LSM-tree by merging all SSTables into a single SSTable.
func (l *LSMT) Compact() error {
	// Create a new empty memtable.
	newMemtable := avl.NewAVLTree()

	// Iterate over all existing SSTables.
	for _, sstable := range l.sstables {
		// Read all key-value pairs from the SSTable.
		kvs, err := getSSTableKVs(sstable.file)
		if err != nil {
			return err
		}

		// For each key-value pair, check if the value is a tombstone.
		for _, kv := range kvs {
			if bytes.Compare(kv.Value, []byte(TOMBSTONE_VALUE)) == 0 { // If the value is a tombstone, skip this key-value pair
				continue
			}

			// If the value is not a tombstone, add it to the new memtable.
			newMemtable.Insert(kv.Key, kv.Value)

		}

		sstable.file.Close() // Close the SSTable file.

	}

	// We remove all the sstables in the directory lmst directory..
	files, err := os.ReadDir(l.directory)
	if err != nil {
		return err
	}

	for _, file := range files {

		if file.IsDir() {
			continue
		}

		if !strings.HasSuffix(file.Name(), SSTABLE_EXTENSION) {
			continue
		}

		err = os.Remove(fmt.Sprintf("%s%s%s", l.directory, string(os.PathSeparator), file.Name()))
		if err != nil {
			return err
		}
	}

	// Clear the sstables
	l.sstables = make([]*SSTable, 0)

	// Flush the new memtable to disk, creating a new SSTable.
	newSSTable, err := l.newSSTable(l.directory, newMemtable)
	if err != nil {
		return err
	}

	// Replace the list of old SSTables with the new SSTable.
	l.sstables = []*SSTable{newSSTable}

	// We will now split the sstable into smaller sstables
	l.sstables, err = l.SplitSSTable(newSSTable, l.minimumSSTables)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the LSM-tree gracefully closing all opened SSTable files.
func (l *LSMT) Close() error {
	// Close all SSTable files.
	for _, sstable := range l.sstables {
		if err := sstable.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// SplitSSTable splits a compacted SSTable into n smaller SSTables.
func (l *LSMT) SplitSSTable(sstable *SSTable, n int) ([]*SSTable, error) {
	// Read all key-value pairs from the SSTable.
	kvs, err := getSSTableKVs(sstable.file)
	if err != nil {
		return nil, err
	}

	// Close the SSTable file.
	sstable.file.Close()

	// delete the sstable file
	err = os.Remove(sstable.file.Name())
	if err != nil {
		return nil, err
	}

	memTables := make([]*avl.AVLTree, n)

	for i := 0; i < n; i++ {
		memTables[i] = avl.NewAVLTree()
	}

	memtSeq := 0

	for _, kv := range kvs {
		memTables[memtSeq].Insert(kv.Key, kv.Value)
		memtSeq++
		if memtSeq == n {
			memtSeq = 0
		}
	}

	sstables := make([]*SSTable, n)

	for i := 0; i < n; i++ {
		sst, err := l.newSSTable(l.directory, memTables[i])
		if err != nil {
			return nil, err
		}

		if sst == nil {
			continue
		}

		sstables[i] = sst
	}

	return sstables, nil
}
