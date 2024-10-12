package lmst

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	lmst, _ := New("my_lsm_tree", 0755, 100_000, 2)
	defer os.RemoveAll("my_lsm_tree")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lmst.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}
}

func BenchmarkGet(b *testing.B) {
	lmst, _ := New("my_lsm_tree", 0755, 100_000, 2)
	defer os.RemoveAll("my_lsm_tree")

	for i := 0; i < 10000; i++ {
		lmst.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lmst.Get([]byte(fmt.Sprintf("%d", i%10000)))
	}
}

func BenchmarkDelete(b *testing.B) {
	lmst, _ := New("my_lsm_tree", 0755, 100_000, 2)
	defer os.RemoveAll("my_lsm_tree")

	for i := 0; i < 10000; i++ {
		lmst.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		lmst.Delete([]byte(fmt.Sprintf("%d", i%10000)))
	}
}
