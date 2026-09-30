package linepatch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sample.txt")
	if err := os.WriteFile(p, []byte(content), 0o640); err != nil {
		t.Fatal(err)
	}
	return p
}

func readAll(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestReadFileLinesAndHash(t *testing.T) {
	p := writeTemp(t, "one\r\ntwo\r\nthree")
	got, err := ReadFileLines(context.Background(), p, LineRange{StartLine: 2, Count: 2})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got.Lines, "|") != "two|three" || got.TotalLines != 3 || got.FileHash == "" {
		t.Fatalf("unexpected result: %#v", got)
	}
	beyond, err := ReadFileLines(context.Background(), p, LineRange{StartLine: 20, Count: 3})
	if err != nil || len(beyond.Lines) != 0 || beyond.TotalLines != 3 {
		t.Fatalf("unexpected beyond-EOF result: %#v, %v", beyond, err)
	}
}

func TestApplyBatchPreservesCRLFAndFinalNewline(t *testing.T) {
	p := writeTemp(t, "a\r\nb\r\nc")
	read, err := ReadFileLines(context.Background(), p, LineRange{StartLine: 1, Count: 3})
	if err != nil {
		t.Fatal(err)
	}
	set := PatchSet{
		ExpectedFileHash: read.FileHash,
		Patches: []LinePatch{
			{StartLine: 3, DeleteCount: 1, Insert: []string{"z"}},
			{StartLine: 2, Insert: []string{"x"}},
		},
	}
	if err := ApplyFile(context.Background(), p, set); err != nil {
		t.Fatal(err)
	}
	if got, want := readAll(t, p), "a\r\nx\r\nb\r\nz"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAppendAddsSeparatorAndPreservesFinalNewlinePolicy(t *testing.T) {
	p := writeTemp(t, "a")
	if err := ApplyFile(context.Background(), p, PatchSet{Patches: []LinePatch{{StartLine: 2, Insert: []string{"b"}}}}); err != nil {
		t.Fatal(err)
	}
	if got, want := readAll(t, p), "a\nb"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	p = writeTemp(t, "a\n")
	if err := ApplyFile(context.Background(), p, PatchSet{Patches: []LinePatch{{StartLine: 2, Insert: []string{"b"}}}}); err != nil {
		t.Fatal(err)
	}
	if got, want := readAll(t, p), "a\nb\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestRejectsStaleHashAndOverlappingPatches(t *testing.T) {
	p := writeTemp(t, "a\nb\nc\n")
	err := ApplyFile(context.Background(), p, PatchSet{ExpectedFileHash: "stale", Patches: []LinePatch{{StartLine: 1, DeleteCount: 1}}})
	if !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("expected concurrent modification error, got %v", err)
	}
	err = ApplyFile(context.Background(), p, PatchSet{Patches: []LinePatch{{StartLine: 1, DeleteCount: 2}, {StartLine: 2, DeleteCount: 1}}})
	if !errors.Is(err, ErrPatchConflict) {
		t.Fatalf("expected conflict error, got %v", err)
	}
	if got, want := readAll(t, p), "a\nb\nc\n"; got != want {
		t.Fatalf("file changed after rejected patch: %q", got)
	}
}

func TestLongLine(t *testing.T) {
	long := strings.Repeat("x", 200_000)
	p := writeTemp(t, long+"\nend")
	got, err := ReadFileLines(context.Background(), p, LineRange{StartLine: 1, Count: 1})
	if err != nil || len(got.Lines) != 1 || got.Lines[0] != long {
		t.Fatalf("long line read failed: err=%v, len=%d", err, len(got.Lines))
	}
}
