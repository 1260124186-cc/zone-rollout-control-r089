package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func replaceFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	encoded = append(encoded, '\n')

	temp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".pending-*")
	if err != nil {
		return fmt.Errorf("create pending snapshot: %w", err)
	}
	tempPath := temp.Name()
	remove := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if err := temp.Chmod(0o600); err != nil {
		remove()
		return fmt.Errorf("set pending snapshot mode: %w", err)
	}
	if _, err := temp.Write(encoded); err != nil {
		remove()
		return fmt.Errorf("write pending snapshot: %w", err)
	}
	if err := temp.Sync(); err != nil {
		remove()
		return fmt.Errorf("sync pending snapshot: %w", err)
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("close pending snapshot: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("replace snapshot: %w", err)
	}
	if directory, err := os.Open(filepath.Dir(path)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

// pendingSnapshotCandidates lists leftover intermediate snapshot files that an
// interrupted replacement may have produced, newest first.
func pendingSnapshotCandidates(path string) []string {
	directory := filepath.Dir(path)
	prefix := "." + filepath.Base(path) + ".pending-"
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	modified := make(map[string]time.Time, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		names = append(names, entry.Name())
		modified[entry.Name()] = info.ModTime()
	}
	sort.Slice(names, func(i, j int) bool {
		left := modified[names[i]]
		right := modified[names[j]]
		if !left.Equal(right) {
			return left.After(right)
		}
		return names[i] > names[j]
	})
	paths := make([]string, 0, len(names))
	for _, name := range names {
		paths = append(paths, filepath.Join(directory, name))
	}
	return paths
}

// adoptPendingSnapshot treats a leftover intermediate snapshot as the source of
// truth when the primary snapshot file is missing, so a deployment that
// restarted during a replacement keeps reading its most recent bytes.
func adoptPendingSnapshot(path string) ([]byte, bool, error) {
	for _, candidate := range pendingSnapshotCandidates(path) {
		payload, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		return payload, true, nil
	}
	return nil, false, nil
}
