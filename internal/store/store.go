package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
)

type Store struct {
	mu    sync.RWMutex
	path  string
	state domain.State
}

// Open loads the snapshot at path. A missing file is the only case that
// initializes a blank state, written atomically. An existing file that cannot
// be decoded completely is reported as an error and left byte-for-byte
// untouched, so the process fails before any write can reach it and the file
// stays available for manual recovery. Leftover intermediate files from
// interrupted replacements are never treated as valid state.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("snapshot path is required")
	}
	state := blankState()
	payload, err := os.ReadFile(path)
	switch {
	case err == nil:
		decoded, decodeErr := decodeSnapshot(payload)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode snapshot %s (file left untouched for manual recovery): %w", path, decodeErr)
		}
		state = decoded
	case errors.Is(err, os.ErrNotExist):
		if err := replaceFile(path, state); err != nil {
			return nil, fmt.Errorf("initialize snapshot: %w", err)
		}
	default:
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	return &Store{path: path, state: state}, nil
}

// decodeSnapshot strictly decodes a snapshot payload. The document must be a
// single JSON object with a schema version the service recognizes; older
// structures decode deterministically through normalize, while anything
// unrecognized fails instead of being partially salvaged.
func decodeSnapshot(payload []byte) (domain.State, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return domain.State{}, errors.New("snapshot is not a JSON object")
	}
	var state domain.State
	if err := json.Unmarshal(trimmed, &state); err != nil {
		return domain.State{}, err
	}
	if state.SchemaVersion > schemaVersion {
		return domain.State{}, fmt.Errorf("schema version %d is newer than supported version %d", state.SchemaVersion, schemaVersion)
	}
	return normalize(state), nil
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Revision
}

func (s *Store) Read(ctx context.Context, fn func(*domain.State) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(&s.state)
}

func (s *Store) Snapshot(ctx context.Context) (domain.State, error) {
	var snapshot domain.State
	err := s.Read(ctx, func(state *domain.State) error {
		copied, err := copyState(state)
		if err != nil {
			return err
		}
		snapshot = copied
		return nil
	})
	return snapshot, err
}

func (s *Store) Write(ctx context.Context, fn func(*domain.State) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	next, err := copyState(&s.state)
	if err != nil {
		return domain.NewError(domain.CodeStorageFailure, "could not copy state", map[string]any{
			"reason": err.Error(),
		})
	}
	if err := fn(&next); err != nil {
		return err
	}
	next.SchemaVersion = schemaVersion
	next.Revision = s.state.Revision + 1
	if err := replaceFile(s.path, next); err != nil {
		return domain.NewError(domain.CodeStorageFailure, "could not persist state", map[string]any{
			"reason": err.Error(),
		})
	}
	s.state = next
	return nil
}

func copyState(source *domain.State) (domain.State, error) {
	payload, err := json.Marshal(source)
	if err != nil {
		return domain.State{}, err
	}
	var copied domain.State
	if err := json.Unmarshal(payload, &copied); err != nil {
		return domain.State{}, err
	}
	return normalize(copied), nil
}

func AddAudit(
	state *domain.State,
	ids platform.IDGenerator,
	entityType string,
	entityID string,
	action string,
	at time.Time,
	attributes map[string]any,
) {
	state.AuditEvents = append(state.AuditEvents, domain.AuditEvent{
		ID:         ids.New("aud"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		At:         at.UTC(),
		Attributes: attributes,
	})
}
