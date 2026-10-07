package store

import (
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

// Open loads the snapshot at path. A new, empty state is initialized only when
// the snapshot file does not exist. When the file exists but cannot be parsed
// completely and validated, Open returns an error without creating, removing,
// renaming or overwriting any file, so the damaged snapshot is preserved for
// manual recovery. Leftover temporary files from an interrupted replacement are
// never treated as state.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("snapshot path is required")
	}
	payload, err := os.ReadFile(path)
	switch {
	case err == nil:
		state, decodeErr := decodeSnapshot(payload)
		if decodeErr != nil {
			return nil, fmt.Errorf("load snapshot %s: %w", path, decodeErr)
		}
		return &Store{path: path, state: state}, nil
	case errors.Is(err, os.ErrNotExist):
		// Only a genuinely missing snapshot may start from a blank state.
		// Persist it through the same atomic path used by transactions so the
		// directory never holds an informal half-written initial file.
		state := blankState()
		if err := replaceFile(path, state); err != nil {
			return nil, fmt.Errorf("initialize snapshot: %w", err)
		}
		return &Store{path: path, state: state}, nil
	default:
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
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
