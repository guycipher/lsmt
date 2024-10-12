package lsmt

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2)
	defer os.RemoveAll("my_lsm_tree")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsmt.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func BenchmarkGet(b *testing.B) {
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2)
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
	lsmt, _ := New("my_lsm_tree", 0755, 100_000, 2)
	defer os.RemoveAll("my_lsm_tree")

	for i := 0; i < 10000; i++ {
		lsmt.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lsmt.Delete([]byte(fmt.Sprintf("%d", i%10000)))
	}
}
