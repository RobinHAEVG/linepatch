package linepatch

import (
	"context"
	"os"
	"testing"
)

func BenchmarkLinePatch(b *testing.B) {
	file := ".benchmark_temp.txt"
	_ = os.WriteFile(file, []byte("initial line\n"), 0644)
	b.Cleanup(func() {
		_ = os.Remove(file)
	})
	linepatch := LinePatch{
		StartLine:   1,
		DeleteCount: 1,
		Insert:      []string{"benchmark line"},
	}
	ps := PatchSet{
		Patches: []LinePatch{linepatch},
	}
	b.ResetTimer()
	for b.Loop() {
		_ = ApplyFile(context.TODO(), file, ps)
	}
}
