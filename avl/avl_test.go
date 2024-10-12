// Package avl tests
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
package avl

import (
	"bytes"
	"testing"
)

func TestAVLTree_InsertAndSearch(t *testing.T) {
	tree := NewAVLTree()

	// Insert keys and values
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key2"), []byte("value2"))
	tree.Insert([]byte("key3"), []byte("value3"))

	// Search for inserted keys
	tests := []struct {
		key      []byte
		expected []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	}

	for _, test := range tests {
		node := tree.Search(test.key)
		if node == nil || !bytes.Equal(node.Value, test.expected) {
			t.Errorf("Expected %s for key %s, got %s", test.expected, test.key, node.Value)
		}
	}
}

func TestAVLTree_InsertDuplicateKey(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key1"), []byte("value2")) // Update the value for key1

	node := tree.Search([]byte("key1"))
	if node == nil || !bytes.Equal(node.Value, []byte("value2")) {
		t.Errorf("Expected value2 for key1, got %s", node.Value)
	}
}

func TestAVLTree_Delete(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key2"), []byte("value2"))
	tree.Insert([]byte("key3"), []byte("value3"))

	tree.Delete([]byte("key2")) // Delete key2

	// Check if key2 is deleted
	node := tree.Search([]byte("key2"))
	if node != nil {
		t.Errorf("Expected key2 to be deleted, but it still exists with value %s", node.Value)
	}

	// Ensure other keys are still present
	if tree.Search([]byte("key1")) == nil {
		t.Errorf("Expected key1 to exist")
	}
	if tree.Search([]byte("key3")) == nil {
		t.Errorf("Expected key3 to exist")
	}
}

func TestAVLTree_InOrderKeys(t *testing.T) {
	tree := NewAVLTree()
	keys := [][]byte{[]byte("key3"), []byte("key1"), []byte("key2")}
	values := [][]byte{[]byte("value3"), []byte("value1"), []byte("value2")}

	// Build the tree from keys and values
	tree = BuildAVLFromKeys(keys, values)

	expectedKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	inOrderKeys := tree.InOrderKeys()

	for i := range expectedKeys {
		if !bytes.Equal(inOrderKeys[i], expectedKeys[i]) {
			t.Errorf("Expected key %s at index %d, got %s", expectedKeys[i], i, inOrderKeys[i])
		}

	}
}

func TestAVLTree_GetInOrderKeys(t *testing.T) {
	tree := NewAVLTree()
	tree.Insert([]byte("key1"), []byte("value1"))
	tree.Insert([]byte("key3"), []byte("value3"))
	tree.Insert([]byte("key2"), []byte("value2"))

	keys, values := tree.GetInOrderKeys()

	expectedKeys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	expectedValues := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}

	for i := range expectedKeys {
		if !bytes.Equal(keys[i], expectedKeys[i]) {
			t.Errorf("Expected key %s at index %d, got %s", expectedKeys[i], i, keys[i])
		}
		if !bytes.Equal(values[i], expectedValues[i]) {
			t.Errorf("Expected value %s at index %d, got %s", expectedValues[i], i, values[i])
		}
	}
}
