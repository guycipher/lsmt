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
	"github.com/guycipher/lsmt/avl"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const SSTABLE_EXTENSION = ".sst"
const TOMBSTONE_VALUE = "$tombstone"
const WAL_EXTENSION = ".wal"

// LSMT is the main struct for the log-structured merge-tree.
type LSMT struct {
	memtable           *avl.AVLTree   // The memtable is an in-memory AVL tree.
	memtableSize       atomic.Int64   // The size of the memtable.
	memtableLock       *sync.RWMutex  // Lock for the memtable.
	sstables           []*SSTable     // The list of current SSTables.
	sstablesLock       *sync.RWMutex  // Lock for the list of SSTables.
	directory          string         // The directory where the SSTables are stored.
	memtableFlushSize  int            // The size at which the memtable should be flushed to disk.
	compactionInterval int            // The interval at which the LSM-tree should be compacted. (in number of SSTables)
	minimumSSTables    int            // The minimum number of SSTables to keep.  On compaction, we will always keep this number of SSTables instead of one large SSTable.
	activeTransactions []*Transaction // List of active transactions
	wal                *Wal
	isFlushing         atomic.Int32
}

// Wal is a struct representing a write-ahead log.
type Wal struct {
	pager *Pager        // The pager for the write-ahead log.
	lock  *sync.RWMutex // Lock for the write-ahead log.
}

// SSTable is a struct representing a sorted string table.
type SSTable struct {
	pager  *Pager        // The pager for the SSTable.
	minKey []byte        // The minimum key in the SSTable.
	maxKey []byte        // The maximum key in the SSTable.
	lock   *sync.RWMutex // Lock for the SSTable.
}

// OperationType is an enum representing the type of operation.
type OperationType int

const (
	OpPut OperationType = iota
	OpDelete
)

// Operation is a struct representing an operation in a transaction.
type Operation struct {
	Type  OperationType
	Key   []byte // The key of the operation.
	Value []byte // Only used for OpPut
}

// Transaction is a struct representing a transaction.
type Transaction struct {
	Operations []Operation // List of operations in the transaction.
	Aborted    bool        // Whether the transaction has been aborted.
}

// SSTableIterator is an iterator for SSTable.
type SSTableIterator struct {
	pager       *Pager
	currentPage int64
	maxPages    int64
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

		// Create the write-ahead log
		walPager, err := OpenPager(directory+string(os.PathSeparator)+WAL_EXTENSION, os.O_CREATE|os.O_RDWR, 0644)
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
			wal:                &Wal{lock: &sync.RWMutex{}, pager: walPager},
		}, nil
	} else {

		// Open the write-ahead log
		walPager, err := OpenPager(directory+string(os.PathSeparator)+WAL_EXTENSION, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

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
			sstablePager, err := OpenPager(fmt.Sprintf("%s%s%s", directory, string(os.PathSeparator), file.Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}

			// Create a new SSTable
			sstable := &SSTable{
				minKey: nil,
				maxKey: nil,
				lock:   &sync.RWMutex{},
				pager:  sstablePager,
			}

			// Add the SSTable to the list of SSTables
			sstables = append(sstables, sstable)
		}

		return &LSMT{
			memtable:           avl.NewAVLTree(),
			memtableLock:       &sync.RWMutex{},
			sstables:           sstables,
			sstablesLock:       &sync.RWMutex{},
			directory:          directory,
			memtableFlushSize:  memtableFlushSize,
			compactionInterval: compactionInterval,
			minimumSSTables:    minimumSSTables,
			wal:                &Wal{lock: &sync.RWMutex{}, pager: walPager},
		}, nil

	}

}

// encodeOperation encodes an operation.
func encodeOperation(op Operation) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(op)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil

}

// decodeOperation decodes an operation.
func decodeOperation(data []byte) (Operation, error) {
	var op Operation
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&op)
	if err != nil {
		return op, err
	}
	return op, nil
}

// WriteOperation writes an operation to the write-ahead log.
func (wal *Wal) WriteOperation(op Operation) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	encoded, err := encodeOperation(op)
	if err != nil {
		return err
	}

	_, err = wal.pager.Write(encoded)
	if err != nil {
		return err
	}

	return nil
}

// Recover reads the write-ahead log and recovers the operations.
func (wal *Wal) Recover() ([]Operation, error) {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	var operations []Operation

	pageCount := wal.pager.Count()
	for i := int64(0); i < pageCount; i++ {
		data, err := wal.pager.GetPage(i)
		if err != nil {
			return nil, err
		}

		op, err := decodeOperation(data)
		if err != nil {
			return nil, err
		}

		operations = append(operations, op)
	}

	return operations, nil
}

// RunRecoveredOperations runs the recovered operations from the write-ahead log.
func (l *LSMT) RunRecoveredOperations(operations []Operation) error {
	for _, op := range operations {
		switch op.Type {
		case OpPut:
			err := l.Put(op.Key, op.Value)
			if err != nil {
				return err
			}
		case OpDelete:
			err := l.Delete(op.Key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Ok returns whether the iterator is valid.
func (it *SSTableIterator) Ok() bool {
	return it.currentPage < it.maxPages
}

// Next returns the next key-value pair from the SSTable.
func (it *SSTableIterator) Next() (*KeyValue, error) {
	// Read the next key-value pair from the SSTable.
	data, err := it.pager.GetPage(it.currentPage)
	if err != nil {
		return nil, err
	}

	kv, err := decodeKv(data)
	if err != nil {
		return nil, err
	}

	it.currentPage++

	return kv, nil

}

// Put inserts a key-value pair into the LSM-tree.
func (l *LSMT) Put(key, value []byte) error {
	// We will first put the key-value pair in the memtable.
	// If the memtable size exceeds the flush size, we will flush the memtable to disk.

	// Lock memtable for writing.
	l.memtableLock.Lock()
	defer l.memtableLock.Unlock()

	// Append the operation to the write-ahead log.
	err := l.wal.WriteOperation(Operation{
		Type:  OpPut,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	// Check if value is tombstone
	if bytes.Compare(value, []byte(TOMBSTONE_VALUE)) == 0 {
		return errors.New("value cannot be a tombstone")
	}

	// Put the key-value pair in the memtable.
	l.memtable.Insert(key, value)

	// If the memtable size exceeds the flush size, flush the memtable to disk.
	if l.memtableSize.Load() > int64(l.memtableFlushSize) {
		if err := l.flushMemtable(); err != nil {
			return err
		}
	} else {
		l.memtableSize.Store(l.memtableSize.Load() + 1)

	}

	return nil
}

// getSSTableIterator returns an iterator for the SSTable.
func getSSTableIterator(pager *Pager) (*SSTableIterator, error) {
	return &SSTableIterator{
		pager:    pager,
		maxPages: pager.PagesCount(),
	}, nil
}

// flushMemtable flushes the memtable to disk, creating a new SSTable.
func (l *LSMT) flushMemtable() error {
	// We will create a new SSTable from the memtable and add it to the list of SSTables.
	// We will then clear the memtable.

	if l.isFlushing.Load() == 1 {
		for l.isFlushing.Load() == 1 {
			time.Sleep(10 * time.Nanosecond)
		}
	}

	l.isFlushing.Store(1)

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
		if err := l.Compact(); err != nil {
			return err
		}

	}

	l.isFlushing.Store(0)

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
	ssltablePager, err := OpenPager(fileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	for _, kv := range sstableSlice {
		encoded, err := encodeKv(kv)
		if err != nil {
			return nil, err
		}

		_, err = ssltablePager.Write(encoded)
		if err != nil {
			return nil, err
		}
	}

	return &SSTable{
		minKey: sstableSlice[0].Key,
		maxKey: sstableSlice[len(sstableSlice)-1].Key,
		lock:   &sync.RWMutex{},
		pager:  ssltablePager,
	}, nil
}

// encodeKv
func encodeKv(kv *KeyValue) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(kv)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeKv
func decodeKv(data []byte) (*KeyValue, error) {
	var kv KeyValue
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&kv)
	if err != nil {
		return nil, err
	}
	return &kv, nil

}

// Get retrieves the value for a given key from the LSM-tree.
func (l *LSMT) Get(key []byte) ([]byte, error) {
	// We will first check the memtable for the key.
	// If the key is not found in the memtable, we will search the SSTables.

	// Check if we are flushing
	for l.isFlushing.Load() == 1 {
		time.Sleep(10 * time.Nanosecond)
	}

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

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// If the key is not within the range of this SSTable, skip it.
		if bytes.Compare(key, sstable.minKey) < 0 || bytes.Compare(key, sstable.maxKey) > 0 {
			sstable.lock.RUnlock()
			continue
		}

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, err
			}
			if bytes.Compare(kv.Key, key) == 0 {
				sstable.lock.RUnlock()
				return kv.Value, nil
			}
		}

		sstable.lock.RUnlock()
	}

	return nil, errors.New("key not found")
}

// Delete removes a key from the LSM-tree.
func (l *LSMT) Delete(key []byte) error {
	// Check if we are flushing
	for l.isFlushing.Load() == 1 {
		time.Sleep(10 * time.Nanosecond)
	}

	// Append the operation to the write-ahead log.
	err := l.wal.WriteOperation(Operation{
		Type: OpPut,
		Key:  key,
	})

	if err != nil {
		return err
	}

	// We will write a tombstone value to the memtable for the key.

	// Lock memtable for writing.
	l.memtableLock.Lock()
	defer l.memtableLock.Unlock()

	// Write a tombstone value to the memtable for the key.
	l.memtable.Insert(key, []byte(TOMBSTONE_VALUE))

	return nil
}

// Compact compacts the LSM-tree by merging all SSTables into a single SSTable.
func (l *LSMT) Compact() error {
	// Create a new empty memtable.
	newMemtable := avl.NewAVLTree()

	// Iterate over all existing SSTables.
	for _, sstable := range l.sstables {
		// Read all key-value pairs from the SSTable.

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			return err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			if bytes.Compare(kv.Value, []byte(TOMBSTONE_VALUE)) == 0 { // If the value is a tombstone, skip this key-value pair
				continue
			}

			// If the value is not a tombstone, add it to the new memtable.
			newMemtable.Insert(kv.Key, kv.Value)
		}

		sstable.pager.Close() // Close the SSTable pager.

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

		err = os.Remove(fmt.Sprintf("%s%s%s", l.directory, string(os.PathSeparator), file.Name()+".del"))
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
	// Check size of memtable
	if l.memtableSize.Load() > 0 {

		// Flush the memtable to disk.
		if err := l.flushMemtable(); err != nil {
			return err
		}
	}

	// Close the write-ahead log.
	if err := l.wal.pager.Close(); err != nil {
		return err
	}

	if len(l.sstables) > 0 {
		// Close all SSTable pagers.
		for _, sstable := range l.sstables {
			if sstable.pager != nil {
				if err := sstable.pager.Close(); err != nil {
					return err
				}
			}

		}
	}

	return nil
}

// SplitSSTable splits a compacted SSTable into n smaller SSTables.
func (l *LSMT) SplitSSTable(sstable *SSTable, n int) ([]*SSTable, error) {

	memTables := make([]*avl.AVLTree, n)

	for i := 0; i < n; i++ {
		memTables[i] = avl.NewAVLTree()
	}

	memtSeq := 0

	if sstable == nil {
		return nil, nil

	}

	// Get an iterator for the SSTable file.
	it, err := getSSTableIterator(sstable.pager)
	if err != nil {
		return nil, err
	}

	// Iterate over the SSTable.
	for it.Ok() {
		kv, err := it.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// If the value is a tombstone, skip this key-value pair.
		if bytes.Compare(kv.Value, []byte(TOMBSTONE_VALUE)) == 0 {
			continue
		}

		// If the value is not a tombstone, add it to the memtable.
		if memtSeq < len(memTables) {
			memTables[memtSeq].Insert(kv.Key, kv.Value)

			// If we have reached the size of the memtable, flush it to disk.
			if memTables[memtSeq].GetSize() >= l.memtableFlushSize {
				memtSeq++
			}
		}

	}

	// Close the SSTable pager.
	sstable.pager.Close()

	// delete the sstable file
	err = os.Remove(sstable.pager.file.Name())
	if err != nil {
		return nil, err
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

// Range retrieves all key-value pairs within a given range from the LSM-tree.
func (l *LSMT) Range(start, end []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, start) >= 0 && bytes.Compare(node.Key, end) <= 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// If the range is not within the range of this SSTable, skip it.
		if bytes.Compare(start, sstable.minKey) < 0 || bytes.Compare(end, sstable.maxKey) > 0 {
			continue
		}

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, start) >= 0 && bytes.Compare(kv.Key, end) <= 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// NRange retrieves all key-value pairs not within a given range from the LSM-tree.
func (l *LSMT) NRange(start, end []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, start) < 0 || bytes.Compare(node.Key, end) > 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// If the range is not within the range of this SSTable, skip it.
		if bytes.Compare(start, sstable.minKey) < 0 || bytes.Compare(end, sstable.maxKey) > 0 {
			continue
		}

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, start) < 0 || bytes.Compare(kv.Key, end) > 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil

}

// GreaterThan retrieves all key-value pairs greater than the key from the LSM-tree.
func (l *LSMT) GreaterThan(key []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, key) > 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, key) > 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// GreaterThanEqual retrieves all key-value pairs greater than or equal to the key from the LSM-tree.
func (l *LSMT) GreaterThanEqual(key []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, key) >= 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, key) >= 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}
		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// LessThan retrieves all key-value pairs less than the key from the LSM-tree.
func (l *LSMT) LessThan(key []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, key) < 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, key) < 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// LessThanEqual retrieves all key-value pairs less than or equal to the key from the LSM-tree.
func (l *LSMT) LessThanEqual(key []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, key) <= 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, key) <= 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}

		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// NGet retrieves all key-value pairs not equal to the key from the LSM-tree.
func (l *LSMT) NGet(key []byte) ([][]byte, [][]byte, error) {
	// We will first check the memtable for the range.
	// If the range is not found in the memtable, we will search the SSTables.

	// Lock memtable for reading.
	l.memtableLock.RLock()

	// Check the memtable for the range.
	var keys [][]byte
	var values [][]byte

	l.memtable.InOrderTraversal(func(node *avl.Node) {
		if bytes.Compare(node.Key, key) != 0 {
			keys = append(keys, node.Key)
			values = append(values, node.Value)
		}
	})

	l.memtableLock.RUnlock()

	// Search the SSTables for the range.
	for i := len(l.sstables) - 1; i >= 0; i-- {

		sstable := l.sstables[i]

		sstable.lock.RLock()

		// Get an iterator for the SSTable file.
		it, err := getSSTableIterator(sstable.pager)
		if err != nil {
			sstable.lock.RUnlock()
			return nil, nil, err
		}

		// Iterate over the SSTable.
		for it.Ok() {
			kv, err := it.Next()
			if err == io.EOF {
				break
			} else if err != nil {
				sstable.lock.RUnlock()
				return nil, nil, err
			}

			if bytes.Compare(kv.Key, key) != 0 {
				keys = append(keys, kv.Key)
				values = append(values, kv.Value)
			}
		}

		sstable.lock.RUnlock()
	}

	return keys, values, nil
}

// BeginTransaction starts a new transaction.
func (l *LSMT) BeginTransaction() *Transaction {
	tx := &Transaction{Operations: make([]Operation, 0), Aborted: false}
	l.activeTransactions = append(l.activeTransactions, tx)
	return tx
}

// AddPut adds a put operation to a transaction.
func (tx *Transaction) AddPut(key, value []byte) {
	tx.Operations = append(tx.Operations, Operation{Type: OpPut, Key: key, Value: value})
}

// AddDelete adds a delete operation to a transaction.
func (tx *Transaction) AddDelete(key []byte) {
	tx.Operations = append(tx.Operations, Operation{Type: OpDelete, Key: key})
}

// CommitTransaction commits a transaction.
func (l *LSMT) CommitTransaction(tx *Transaction) error {
	if tx.Aborted {
		return errors.New("transaction has been aborted")
	}

	for _, op := range tx.Operations {
		switch op.Type {
		case OpPut:
			if err := l.Put(op.Key, op.Value); err != nil {
				return err
			}
		case OpDelete:
			if err := l.Delete(op.Key); err != nil {
				return err
			}
		}
	}

	// Remove the transaction from the active list.
	for i, t := range l.activeTransactions {
		if t == tx {
			l.activeTransactions = append(l.activeTransactions[:i], l.activeTransactions[i+1:]...)
			break
		}
	}

	return nil
}

// RollbackTransaction aborts a transaction.
func (l *LSMT) RollbackTransaction(tx *Transaction) {
	tx.Aborted = true
	for i, t := range l.activeTransactions {
		if t == tx {
			// Remove the transaction from the active list.
			l.activeTransactions = append(l.activeTransactions[:i], l.activeTransactions[i+1:]...)
			break
		}
	}
}

// GetWal returns the write-ahead log.
func (l *LSMT) GetWal() *Wal {
	return l.wal
}
