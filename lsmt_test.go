// Package lsmt tests
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
	"fmt"
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 128, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	// Check if the directory exists
	if _, err := os.Stat("my_lsm_tree"); os.IsNotExist(err) {
		t.Fatal(err)
	}

}

func TestLMST_Put(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 128, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	// Insert 256 key-value pairs
	for i := 0; i < 256; i++ {
		err = lsmt.Put([]byte(string(fmt.Sprintf("%d", i))), []byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// There should be 2 sstables
	// 0.sst and 1.sst
	if len(lsmt.sstables) != 2 {
		t.Fatalf("expected 2 sstables, got %d", len(lsmt.sstables))
	}

	expectFiles := []string{"0.sst", "1.sst"}

	// Read the directory
	files, err := os.ReadDir("my_lsm_tree")
	if err != nil {
		t.Fatal(err)
	}

	for i, file := range files {
		if file.Name() != expectFiles[i] {
			t.Fatalf("expected %s, got %s", expectFiles[i], file.Name())
		}
	}

}

func TestLMST_Compact(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 128, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	// Insert 384 key-value pairs
	for i := 0; i < 384; i++ {
		err = lsmt.Put([]byte(string(fmt.Sprintf("%d", i))), []byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// this will create 3 sstables, the system will know at that point as sstables is over compactionInverval of 2 at that point will compact
	// 0.sst, 1.sst, 2.sst
	// to 0.sst

	if len(lsmt.sstables) != 1 {
		t.Fatalf("expected 1 sstables, got %d", len(lsmt.sstables))
	}

	// Check for 0.sst
	if _, err := os.Stat("my_lsm_tree/0.sst"); os.IsNotExist(err) {
		t.Fatal(err)
	}

}

func TestLMST_Delete(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 128, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	// Insert 256 key-value pairs
	for i := 0; i < 256; i++ {
		err = lsmt.Put([]byte(string(fmt.Sprintf("%d", i))), []byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Delete 128 key-value pairs
	for i := 0; i < 128; i++ {
		err = lsmt.Delete([]byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// There should be 2 sstables
	// 0.sst and 1.sst
	if len(lsmt.sstables) != 2 {
		t.Fatalf("expected 2 sstables, got %d", len(lsmt.sstables))
	}

	expectFiles := []string{"0.sst", "1.sst"}

	// Read the directory
	files, err := os.ReadDir("my_lsm_tree")
	if err != nil {
		t.Fatal(err)
	}

	for i, file := range files {
		if file.Name() != expectFiles[i] {
			t.Fatalf("expected %s, got %s", expectFiles[i], file.Name())
		}
	}

	// Check if the key is deleted
	_, err = lsmt.Get([]byte("1"))
	if err == nil {
		t.Fatal("expected key to be deleted")
	}
}

func TestLMST_Get(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 15_000, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	// Insert 100,000 key-value pairs
	for i := 0; i < 100_000; i++ {
		err = lsmt.Put([]byte(string(fmt.Sprintf("%d", i))), []byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get the key
	value, err := lsmt.Get([]byte("99822"))
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "99822" {
		t.Fatalf("expected 99822, got %s", string(value))
	}
}

func TestLSMT_NGet(t *testing.T) {
	defer os.RemoveAll("my_lsm_tree")
	lsmt, err := New("my_lsm_tree", 0755, 128, 2, 1)
	if err != nil {
		t.Fatal(err)
	}

	if lsmt == nil {
		t.Fatal("expected non-nil lmst")
	}

	for i := 0; i < 10; i++ {
		err = lsmt.Put([]byte(string(fmt.Sprintf("%d", i))), []byte(string(fmt.Sprintf("%d", i))))
		if err != nil {
			t.Fatal(err)
		}
	}

	keys, _, err := lsmt.NGet([]byte("4"))
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 9 {
		t.Fatalf("expected 9 keys, got %d", len(keys))
	}

	expectKeys := [][]byte{
		[]byte("0"),
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
		[]byte("5"),
		[]byte("6"),
		[]byte("7"),
		[]byte("8"),
		[]byte("9"),
	}

	for _, key := range keys {
		for j, expectKey := range expectKeys {
			if string(key) == string(expectKey) {
				// remove the key from the expectKeys
				expectKeys = append(expectKeys[:j], expectKeys[j+1:]...)
				break
			}
			if j == len(expectKeys)-1 {
				t.Fatalf("expected key to be %s, got %s", string(expectKey), string(key))
			}
		}
	}

}
