# linepatch

Streaming line-range reads and atomic batch edits for regular files.

## API

- `ReadFileLines(ctx, path, LineRange{StartLine, Count})` reads a 1-based line range and returns the exact file SHA-256 and total line count.
- `ApplyFile(ctx, path, PatchSet{ExpectedFileHash, Patches})` applies a batch against one original file version.
- `FileHash(ctx, path)` computes the exact-byte SHA-256 without retaining the file in memory.

A patch replaces `DeleteCount` source lines starting at `StartLine` with `Insert` lines. All patch coordinates refer to the original file. Insertion before line 10 is `StartLine: 10, DeleteCount: 0`; appending is at `TotalLines+1`. A replacement of lines 181 through 193 inclusive uses `StartLine: 181, DeleteCount: 13`.

Patches are sorted internally. Overlapping or same-start patches are rejected. Inserted strings represent individual lines and must not contain CR or LF. Unchanged source line endings are preserved; inserted lines use the dominant source line ending. The final line-terminator policy is preserved for non-empty results. Writes use a temporary file in the target directory and rename only after validation and writing succeed. Symlinks and non-regular files are rejected.

`ReadFileLines` scans the full file to produce its hash and line count, but retains only the requested lines in memory. `ApplyFile` also streams the file; memory use is proportional to the largest physical line and the patch set, not the whole file. A supplied expected hash detects stale reads. Filesystem-level locking is not provided, so callers requiring strict multi-process serialization should add an appropriate lock.

## Test

Run `go test ./...` from this directory. Before publishing or importing the package from another module, change the module path in `go.mod` to your repository path.
