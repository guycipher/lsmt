// Package lsmt benchmarker
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

func BenchmarkPut(b *testing.B) {
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2, 1)
	defer os.RemoveAll("my_lsm_tree")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsmt.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func BenchmarkGet(b *testing.B) {
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2, 1)
	defer os.RemoveAll("my_lsm_tree")

	for i := 0; i < 10000; i++ {
		lsmt.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsmt.Get([]byte(fmt.Sprintf("%d", i%10000)))
	}
}

func BenchmarkDelete(b *testing.B) {
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2, 1)
	defer os.RemoveAll("my_lsm_tree")

	for i := 0; i < 10000; i++ {
		lsmt.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsmt.Delete([]byte(fmt.Sprintf("%d", i%10000)))
	}
}
