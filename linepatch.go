// Package linepatch provides streaming, line-oriented reads and atomic batch
// edits for regular files.
package linepatch

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	// ErrInvalidRange indicates an invalid line range or a patch outside the file.
	ErrInvalidRange = errors.New("linepatch: invalid line range")
	// ErrPatchConflict indicates overlapping or ambiguously ordered patches.
	ErrPatchConflict = errors.New("linepatch: conflicting patches")
	// ErrConcurrentModification indicates that the file changed during the operation
	// or did not match PatchSet.ExpectedFileHash.
	ErrConcurrentModification = errors.New("linepatch: file changed since it was read")
	// ErrSymlink is returned rather than replacing a symlink itself by accident.
	ErrSymlink = errors.New("linepatch: symbolic links are not supported")
	// ErrNotRegularFile indicates that the target is not a regular file.
	ErrNotRegularFile = errors.New("linepatch: target is not a regular file")
)

// LineRange selects Count lines beginning at StartLine. Line numbers are 1-based.
// A Count of zero selects no lines. A read that extends beyond EOF returns the
// available lines; ReadResult.TotalLines and len(ReadResult.Lines) reveal the result.
type LineRange struct {
	StartLine int64
	Count     int64
}

// ReadResult contains the requested lines and metadata about the complete file.
// Lines do not include line terminators. FileHash is the lowercase hex SHA-256
// of the exact file bytes, including its line endings.
type ReadResult struct {
	StartLine  int64
	Lines      []string
	TotalLines int64
	FileHash   string
}

// LinePatch replaces DeleteCount lines beginning at StartLine with Insert lines.
// StartLine is 1-based and all positions refer to the original file, not to the
// result of earlier patches. Insert strings are individual lines without CR or LF.
// DeleteCount == 0 represents an insertion before StartLine. StartLine may be
// TotalLines+1 to append at EOF.
type LinePatch struct {
	StartLine   int64
	DeleteCount int64
	Insert      []string
}

// PatchSet applies all Patches to one original file version. If ExpectedFileHash
// is non-empty, ApplyFile rejects the operation unless the exact file bytes have
// that SHA-256 hash. Obtain it from ReadFileLines or FileHash.
type PatchSet struct {
	ExpectedFileHash string
	Patches          []LinePatch
}

type fileInfo struct {
	totalLines int64
	fileHash   string
	newline    []byte
	finalNL    bool
}

// ReadFileLines reads a line range without retaining the complete file in memory.
// It scans the file to compute FileHash and TotalLines, while retaining only the
// requested lines. Lines are returned without LF or CRLF terminators.
func ReadFileLines(ctx context.Context, path string, r LineRange) (ReadResult, error) {
	if ctx == nil {
		return ReadResult{}, errors.New("linepatch: nil context")
	}
	if r.StartLine < 1 || r.Count < 0 || r.Count > int64(^uint64(0)>>1)-r.StartLine {
		return ReadResult{}, fmt.Errorf("%w: start=%d count=%d", ErrInvalidRange, r.StartLine, r.Count)
	}

	f, _, err := openRegular(path)
	if err != nil {
		return ReadResult{}, err
	}
	defer f.Close()

	var lines []string
	info, err := scanFile(ctx, f, func(lineNo int64, raw []byte) {
		if r.Count == 0 || lineNo < r.StartLine || lineNo >= r.StartLine+r.Count {
			return
		}
		content := raw
		if len(content) > 0 && content[len(content)-1] == '\n' {
			content = content[:len(content)-1]
			if len(content) > 0 && content[len(content)-1] == '\r' {
				content = content[:len(content)-1]
			}
		}
		lines = append(lines, string(content))
	})
	if err != nil {
		return ReadResult{}, err
	}
	return ReadResult{
		StartLine:  r.StartLine,
		Lines:      lines,
		TotalLines: info.totalLines,
		FileHash:   info.fileHash,
	}, nil
}

// FileHash returns the lowercase hex SHA-256 of a regular file's exact bytes.
func FileHash(ctx context.Context, path string) (string, error) {
	if ctx == nil {
		return "", errors.New("linepatch: nil context")
	}
	f, _, err := openRegular(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := scanFile(ctx, f, nil)
	if err != nil {
		return "", err
	}
	return info.fileHash, nil
}

// ApplyFile validates and applies a batch of line patches. It streams the source
// into a temporary file and only replaces the target after every check succeeds.
// Existing line endings are kept for unchanged lines; inserted lines use the
// dominant line ending found in the original file (LF for an empty/no-EOL file).
// The presence or absence of a final line terminator is preserved for non-empty
// results. The target must be a regular file, not a symlink.
func ApplyFile(ctx context.Context, path string, set PatchSet) error {
	if ctx == nil {
		return errors.New("linepatch: nil context")
	}
	f, initialInfo, err := openRegular(path)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := scanFile(ctx, f, nil)
	if err != nil {
		return err
	}
	if set.ExpectedFileHash != "" && !strings.EqualFold(set.ExpectedFileHash, info.fileHash) {
		return fmt.Errorf("%w: expected %s, found %s", ErrConcurrentModification, set.ExpectedFileHash, info.fileHash)
	}

	patches, err := validatePatches(set.Patches, info.totalLines)
	if err != nil {
		return err
	}
	if len(patches) == 0 {
		return nil
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("linepatch: rewind %q: %w", path, err)
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	tmp, err := os.CreateTemp(dir, "."+base+".linepatch-*")
	if err != nil {
		return fmt.Errorf("linepatch: create temporary file beside %q: %w", path, err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(initialInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("linepatch: preserve file mode: %w", err)
	}

	secondHash := sha256.New()
	reader := bufio.NewReaderSize(io.TeeReader(f, secondHash), 64*1024)
	writer := bufio.NewWriterSize(tmp, 64*1024)
	out := &outputTracker{w: writer}

	current, hasCurrent, err := readRecord(reader)
	if err != nil {
		return fmt.Errorf("linepatch: read source: %w", err)
	}
	lineNo := int64(1)
	patchIndex := 0

	for hasCurrent || patchIndex < len(patches) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if patchIndex < len(patches) && patches[patchIndex].StartLine == lineNo {
			p := patches[patchIndex]
			reachesEOF := p.StartLine+p.DeleteCount == info.totalLines+1
			if reachesEOF && len(p.Insert) > 0 && out.wrote && !out.endsInLF {
				if _, err := out.Write(info.newline); err != nil {
					return fmt.Errorf("linepatch: separate appended lines: %w", err)
				}
			}
			for _, line := range p.Insert {
				if _, err := out.Write([]byte(line)); err != nil {
					return fmt.Errorf("linepatch: write inserted line: %w", err)
				}
				if _, err := out.Write(info.newline); err != nil {
					return fmt.Errorf("linepatch: write line ending: %w", err)
				}
			}
			for n := int64(0); n < p.DeleteCount; n++ {
				if !hasCurrent {
					return fmt.Errorf("%w: patch extends beyond EOF", ErrInvalidRange)
				}
				lineNo++
				current, hasCurrent, err = readRecord(reader)
				if err != nil {
					return fmt.Errorf("linepatch: read source: %w", err)
				}
			}
			patchIndex++
			continue
		}

		if !hasCurrent {
			if patchIndex < len(patches) {
				return fmt.Errorf("%w: patch starts after EOF", ErrInvalidRange)
			}
			break
		}
		if patchIndex < len(patches) && patches[patchIndex].StartLine < lineNo {
			return fmt.Errorf("%w: patch starts at line %d after its position was passed", ErrInvalidRange, patches[patchIndex].StartLine)
		}
		if _, err := out.Write(current); err != nil {
			return fmt.Errorf("linepatch: copy source line: %w", err)
		}
		lineNo++
		current, hasCurrent, err = readRecord(reader)
		if err != nil {
			return fmt.Errorf("linepatch: read source: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("linepatch: flush temporary file: %w", err)
	}
	expectedHash, err := hex.DecodeString(info.fileHash)
	if err != nil {
		return fmt.Errorf("linepatch: decode internal file hash: %w", err)
	}
	if !bytes.Equal(secondHash.Sum(nil), expectedHash) {
		return ErrConcurrentModification
	}
	if err := preserveFinalNewline(tmp, info.finalNL, info.newline); err != nil {
		return fmt.Errorf("linepatch: preserve final newline: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("linepatch: sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("linepatch: close temporary file: %w", err)
	}

	currentInfo, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("linepatch: recheck target: %w", err)
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(initialInfo, currentInfo) {
		return ErrConcurrentModification
	}
	if err = os.Remove(path); err != nil {
		return fmt.Errorf("linepatch: remove original file %q: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("linepatch: move file %q: %w", path, err)
	}
	committed = true
	return nil
}

func validatePatches(input []LinePatch, totalLines int64) ([]LinePatch, error) {
	patches := append([]LinePatch(nil), input...)
	for _, p := range patches {
		if p.StartLine < 1 || p.DeleteCount < 0 || p.DeleteCount > int64(^uint64(0)>>1)-p.StartLine {
			return nil, fmt.Errorf("%w: start=%d delete=%d", ErrInvalidRange, p.StartLine, p.DeleteCount)
		}
		for _, line := range p.Insert {
			if strings.ContainsAny(line, "\r\n") {
				return nil, fmt.Errorf("linepatch: inserted values must be single lines without CR or LF")
			}
		}
	}
	sort.SliceStable(patches, func(i, j int) bool {
		return patches[i].StartLine < patches[j].StartLine
	})

	var previousStart, previousEnd int64
	for i, p := range patches {
		if p.StartLine > totalLines+1 || p.DeleteCount > totalLines-(p.StartLine-1) {
			return nil, fmt.Errorf("%w: start=%d delete=%d file-lines=%d", ErrInvalidRange, p.StartLine, p.DeleteCount, totalLines)
		}
		end := p.StartLine + p.DeleteCount
		if i > 0 && (p.StartLine == previousStart || p.StartLine < previousEnd) {
			return nil, fmt.Errorf("%w: patches at/near line %d overlap", ErrPatchConflict, p.StartLine)
		}
		previousStart, previousEnd = p.StartLine, end
	}
	return patches, nil
}

func openRegular(path string) (*os.File, os.FileInfo, error) {
	li, err := os.Lstat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("linepatch: stat %q: %w", path, err)
	}
	if li.Mode()&os.ModeSymlink != 0 {
		return nil, nil, fmt.Errorf("%w: %q", ErrSymlink, path)
	}
	if !li.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("%w: %q", ErrNotRegularFile, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("linepatch: open %q: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("linepatch: stat opened file %q: %w", path, err)
	}
	if !fi.Mode().IsRegular() || !os.SameFile(li, fi) {
		_ = f.Close()
		return nil, nil, fmt.Errorf("%w: %q changed while opening", ErrConcurrentModification, path)
	}
	return f, fi, nil
}

// scanFile reads one record at a time. It hashes exact bytes and, when visit is
// non-nil, invokes visit with a 1-based line number and its raw bytes.
func scanFile(ctx context.Context, f *os.File, visit func(int64, []byte)) (fileInfo, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fileInfo{}, fmt.Errorf("linepatch: seek: %w", err)
	}
	h := sha256.New()
	r := bufio.NewReaderSize(f, 64*1024)
	var result fileInfo
	var lfCount, crlfCount int64
	var firstNewline []byte
	for {
		if err := ctx.Err(); err != nil {
			return fileInfo{}, err
		}
		raw, err := r.ReadBytes('\n')
		if len(raw) > 0 {
			_, _ = h.Write(raw)
			result.totalLines++
			result.finalNL = raw[len(raw)-1] == '\n'
			if result.finalNL {
				ending := []byte{'\n'}
				if len(raw) >= 2 && raw[len(raw)-2] == '\r' {
					ending = []byte{'\r', '\n'}
					crlfCount++
				} else {
					lfCount++
				}
				if firstNewline == nil {
					firstNewline = ending
				}
			}
			if visit != nil {
				visit(result.totalLines, raw)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return fileInfo{}, fmt.Errorf("linepatch: read: %w", err)
		}
	}
	if crlfCount > lfCount {
		result.newline = []byte{'\r', '\n'}
	} else if lfCount > crlfCount {
		result.newline = []byte{'\n'}
	} else if firstNewline != nil {
		result.newline = firstNewline
	} else {
		result.newline = []byte{'\n'}
	}
	result.fileHash = hex.EncodeToString(h.Sum(nil))
	return result, nil
}

func readRecord(r *bufio.Reader) ([]byte, bool, error) {
	raw, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	return raw, true, nil
}

type outputTracker struct {
	w        io.Writer
	wrote    bool
	endsInLF bool
}

func (o *outputTracker) Write(p []byte) (int, error) {
	n, err := o.w.Write(p)
	if n > 0 {
		o.wrote = true
		o.endsInLF = p[n-1] == '\n'
	}
	return n, err
}

func preserveFinalNewline(f *os.File, want bool, newline []byte) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	if size == 0 {
		return nil
	}
	var last [1]byte
	if _, err := f.ReadAt(last[:], size-1); err != nil {
		return err
	}
	endsWithLF := last[0] == '\n'
	if want {
		if endsWithLF {
			return nil
		}
		_, err := f.WriteAt(newline, size)
		return err
	}
	if !endsWithLF {
		return nil
	}
	trim := int64(1)
	if size >= 2 {
		var prev [1]byte
		if _, err := f.ReadAt(prev[:], size-2); err != nil {
			return err
		}
		if prev[0] == '\r' {
			trim = 2
		}
	}
	return f.Truncate(size - trim)
}
