// Package avl implements an AVL tree
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
	"fmt"
)

// AVLTree is a self-balancing binary search tree.
type AVLTree struct {
	Root *Node // Teh root of the AVL tree.
}

// Node represents a node in the AVL tree.
type Node struct {
	Key    []byte // The key of the node.
	Value  []byte // The value of the node.
	Left   *Node  // The left child of the node.
	Right  *Node  // The right child of the node.
	Height int    // The height of the node.
}

// NewAVLTree creates a new AVL tree.
func NewAVLTree() *AVLTree {
	return &AVLTree{}
}

// height returns the height of the node.
func height(node *Node) int {
	if node == nil {
		return 0
	}
	return node.Height
}

// max returns the maximum of two integers.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// rightRotate rotates the node to the right.
func rightRotate(y *Node) *Node {
	x := y.Left
	T2 := x.Right

	x.Right = y
	y.Left = T2

	y.Height = max(height(y.Left), height(y.Right)) + 1
	x.Height = max(height(x.Left), height(x.Right)) + 1

	return x
}

// leftRotate rotates the node to the left.
func leftRotate(x *Node) *Node {
	y := x.Right
	T2 := y.Left

	y.Left = x
	x.Right = T2

	x.Height = max(height(x.Left), height(x.Right)) + 1
	y.Height = max(height(y.Left), height(y.Right)) + 1

	return y
}

// getBalance returns the balance factor of the node.
func getBalance(N *Node) int {
	if N == nil {
		return 0
	}
	return height(N.Left) - height(N.Right)
}

// Insert inserts a node with the given key and value into the AVL tree.
func (t *AVLTree) Insert(key, val []byte) {
	t.Root = t.insert(t.Root, key, val) // Update the root after insertion
}

// insert inserts a node with the given key and value into the AVL tree.
func (t *AVLTree) insert(node *Node, key, val []byte) *Node {
	if node == nil {
		return &Node{Key: key, Height: 1, Value: val}
	}

	// Compare keys (assuming K is unique)
	if bytes.Compare(key, node.Key) < 0 {
		node.Left = t.insert(node.Left, key, val)
	} else if bytes.Compare(key, node.Key) > 0 {
		node.Right = t.insert(node.Right, key, val)
	} else {
		// Duplicate key, replace the value
		node.Value = val
		return node
	}

	node.Height = 1 + max(height(node.Left), height(node.Right))

	balance := getBalance(node)

	if balance > 1 && bytes.Compare(key, node.Left.Key) < 0 {
		return rightRotate(node)
	}
	if balance < -1 && bytes.Compare(key, node.Right.Key) > 0 {
		return leftRotate(node)
	}
	if balance > 1 && bytes.Compare(key, node.Left.Key) > 0 {
		node.Left = leftRotate(node.Left)
		return rightRotate(node)
	}
	if balance < -1 && bytes.Compare(key, node.Right.Key) < 0 {
		node.Right = rightRotate(node.Right)
		return leftRotate(node)
	}

	return node
}

// Search searches for a node with the given key in the AVL tree.
func (t *AVLTree) Search(key []byte) *Node {
	return t.search(t.Root, key)
}

// search searches for a node with the given key in the AVL tree.
func (t *AVLTree) search(node *Node, key []byte) *Node {
	if node == nil || bytes.Compare(node.Key, key) == 0 {
		return node
	}
	if bytes.Compare(key, node.Key) < 0 {
		return t.search(node.Left, key)
	}
	return t.search(node.Right, key)
}

// InOrderTraversal traverses the AVL tree in-order.
func (t *AVLTree) InOrderTraversal(f func(*Node)) {
	t.inOrderTraversal(t.Root, f)
}

// inOrderTraversal traverses the AVL tree in-order.
func (t *AVLTree) inOrderTraversal(node *Node, f func(*Node)) {
	if node != nil {
		t.inOrderTraversal(node.Left, f)
		f(node)
		t.inOrderTraversal(node.Right, f)
	}
}

// Print prints the AVL tree in-order. (for debugging)
func (t *AVLTree) Print(node *Node) {
	if node != nil {
		t.Print(node.Left)
		fmt.Printf("%s: %v ", string(node.Key), node.Value)
		t.Print(node.Right)
	}
}

// GetInOrderKeys returns the keys of the AVL tree in-order.
func (t *AVLTree) GetInOrderKeys() ([][]byte, [][]byte) {
	var keys [][]byte
	var values [][]byte
	t.inOrderTraversal(t.Root, func(node *Node) {
		keys = append(keys, node.Key)
		values = append(values, node.Value)
	})
	return keys, values
}

// BuildAVLFromKeys builds an AVL tree from a list of keys.
func BuildAVLFromKeys(keys [][]byte, values [][]byte) *AVLTree {
	t := NewAVLTree()
	for i, key := range keys {
		t.Insert(key, values[i])
	}
	return t
}

// Delete deletes the node with the given key from the AVL tree.
func (t *AVLTree) Delete(key []byte) {
	t.Root = t.delete(t.Root, key)
}

// delete deletes the node with the given key from the AVL tree.
func (t *AVLTree) delete(node *Node, key []byte) *Node {
	if node == nil {
		return node
	}

	// Compare keys
	if bytes.Compare(key, node.Key) < 0 {
		node.Left = t.delete(node.Left, key)
	} else if bytes.Compare(key, node.Key) > 0 {
		node.Right = t.delete(node.Right, key)
	} else {
		// Node with only one child or no child
		if node.Left == nil {
			return node.Right
		} else if node.Right == nil {
			return node.Left
		}

		// Node with two children, get the in-order predecessor (maximum in the left subtree)
		maxNode := t.getMaxNode(node.Left)
		node.Key = maxNode.Key
		node.Left = t.delete(node.Left, maxNode.Key)
	}

	// Update height of the current node
	node.Height = 1 + max(height(node.Left), height(node.Right))

	// Rebalance the tree
	balance := getBalance(node)

	// Left Left Case
	if balance > 1 && getBalance(node.Left) >= 0 {
		return rightRotate(node)
	}
	// Left Right Case
	if balance > 1 && getBalance(node.Left) < 0 {
		node.Left = leftRotate(node.Left)
		return rightRotate(node)
	}
	// Right Right Case
	if balance < -1 && getBalance(node.Right) <= 0 {
		return leftRotate(node)
	}
	// Right Left Case
	if balance < -1 && getBalance(node.Right) > 0 {
		node.Right = rightRotate(node.Right)
		return leftRotate(node)
	}

	return node
}

// getMaxNode returns the node with the maximum key in the subtree rooted at node.
func (t *AVLTree) getMaxNode(node *Node) *Node {
	if node == nil || node.Right == nil {
		return node
	}
	return t.getMaxNode(node.Right)
}

// GetSize returns the number of nodes in the AVL tree.
func (t *AVLTree) GetSize() int {
	return t.getSize(t.Root)
}

// getSize returns the number of nodes in the subtree rooted at node.
func (t *AVLTree) getSize(node *Node) int {
	if node == nil {
		return 0
	}
	return 1 + t.getSize(node.Left) + t.getSize(node.Right)
}

// InOrderKeys returns the keys of the AVL tree in-order.
func (t *AVLTree) InOrderKeys() [][]byte {
	var keys [][]byte
	t.InOrderTraversal(func(node *Node) {
		keys = append(keys, node.Key)
	})
	return keys
}
