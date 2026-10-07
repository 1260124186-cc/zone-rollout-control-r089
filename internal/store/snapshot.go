package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"solo-0014-zone-rollout-control/internal/domain"
)

// snapshotMembers are the top-level JSON members written by every current
// schema snapshot. domain.State fields carry no omitempty, so a complete
// snapshot contains exactly these keys.
var snapshotMembers = []string{
	"schema_version",
	"revision",
	"plans",
	"revisions",
	"nodes",
	"leases",
	"lease_groups",
	"probes",
	"assessments",
	"lease_request_keys",
	"assessment_request_keys",
	"audit_events",
}

// decodeSnapshot parses a complete snapshot. Current-schema snapshots are
// decoded strictly: any truncation, missing member, trailing byte, duplicate
// key, unknown key or broken cross-reference is rejected. Snapshots written by
// older recognizable schema versions are accepted and normalized, but their
// cross-references must still be intact.
func decodeSnapshot(payload []byte) (domain.State, error) {
	members, err := readSnapshotMembers(payload)
	if err != nil {
		return domain.State{}, err
	}

	version := 0
	if raw, ok := members["schema_version"]; ok {
		if err := json.Unmarshal(raw, &version); err != nil {
			return domain.State{}, fmt.Errorf("schema_version: %w", err)
		}
	}
	switch {
	case version == schemaVersion:
		if err := requireMembers(members, snapshotMembers); err != nil {
			return domain.State{}, err
		}
		var state domain.State
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&state); err != nil {
			return domain.State{}, fmt.Errorf("parse snapshot: %w", err)
		}
		if err := requireNonNullMembers(members); err != nil {
			return domain.State{}, err
		}
		state = normalize(state)
		if err := validateIntegrity(state); err != nil {
			return domain.State{}, err
		}
		return state, nil
	case version < schemaVersion:
		// Older recognizable structures still get a deterministic reading:
		// missing members normalize to empty collections, unknown members are
		// ignored, but referenced entities must still exist.
		var state domain.State
		if err := json.Unmarshal(payload, &state); err != nil {
			return domain.State{}, fmt.Errorf("parse legacy snapshot: %w", err)
		}
		state = normalize(state)
		if err := validateIntegrity(state); err != nil {
			return domain.State{}, err
		}
		return state, nil
	default:
		return domain.State{}, fmt.Errorf("unsupported snapshot schema_version %d (service supports up to %d)", version, schemaVersion)
	}
}

// readSnapshotMembers performs the structural checks that json.Unmarshal would
// silently accept on damaged input. It ensures the document is one JSON object
// with no trailing data, no duplicate member names at any level, and captures
// each top-level member's raw bytes.
func readSnapshotMembers(payload []byte) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, fmt.Errorf("snapshot is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	opening, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}
	delimiter, ok := opening.(json.Delim)
	if !ok || delimiter != '{' {
		return nil, fmt.Errorf("snapshot must be a JSON object")
	}

	members := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("parse snapshot: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil, fmt.Errorf("parse snapshot: object key is not a string")
		}
		if _, exists := members[key]; exists {
			return nil, fmt.Errorf("duplicate snapshot member %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("parse snapshot member %q: %w", key, err)
		}
		if err := rejectDuplicateKeys(raw, key); err != nil {
			return nil, err
		}
		members[key] = raw
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}
	if end, ok := closing.(json.Delim); !ok || end != '}' {
		return nil, fmt.Errorf("parse snapshot: object is not closed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("snapshot contains trailing data")
		}
		return nil, fmt.Errorf("parse snapshot: %w", err)
	}
	return members, nil
}

// rejectDuplicateKeys walks a raw JSON value and rejects any object that names
// the same key more than once. Duplicate keys are a common signature of two
// documents being concatenated during a partial write.
func rejectDuplicateKeys(raw json.RawMessage, path string) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("parse snapshot member %q: %w", path, err)
	}
	delim, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("parse snapshot member %q: %w", path, err)
			}
			key := keyToken.(string)
			childPath := path + "." + key
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate member %q", childPath)
			}
			seen[key] = struct{}{}
			var child json.RawMessage
			if err := decoder.Decode(&child); err != nil {
				return fmt.Errorf("parse snapshot member %q: %w", childPath, err)
			}
			if err := rejectDuplicateKeys(child, childPath); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return fmt.Errorf("parse snapshot member %q: %w", path, err)
		}
	case '[':
		index := 0
		for decoder.More() {
			var child json.RawMessage
			if err := decoder.Decode(&child); err != nil {
				return fmt.Errorf("parse snapshot member %s[%d]: %w", path, index, err)
			}
			if err := rejectDuplicateKeys(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
			index++
		}
		if _, err := decoder.Token(); err != nil {
			return fmt.Errorf("parse snapshot member %q: %w", path, err)
		}
	}
	return nil
}

func requireMembers(members map[string]json.RawMessage, expected []string) error {
	for _, key := range expected {
		if _, ok := members[key]; !ok {
			return fmt.Errorf("snapshot is missing required member %q", key)
		}
	}
	return nil
}

// requireNonNullMembers rejects explicit nulls in current-schema members.
// normalize() would otherwise turn a damaged "plans": null into an empty map.
func requireNonNullMembers(members map[string]json.RawMessage) error {
	for _, key := range snapshotMembers {
		raw, ok := members[key]
		if !ok {
			continue
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("snapshot member %q is null", key)
		}
	}
	return nil
}

// validateIntegrity verifies that every cross-reference in the snapshot points
// at an existing entity and that every back-reference is matched. The services
// only ever produce states that satisfy these invariants, so a violation means
// the bytes are damaged rather than merely old.
func validateIntegrity(state domain.State) error {
	for id, plan := range state.Plans {
		if plan.ID != id {
			return fmt.Errorf("plan %q is stored under mismatched id %q", plan.ID, id)
		}
		if plan.LatestRevisionID != "" {
			revision, ok := state.Revisions[plan.LatestRevisionID]
			if !ok {
				return fmt.Errorf("plan %q references missing revision %q", id, plan.LatestRevisionID)
			}
			if revision.PlanID != id {
				return fmt.Errorf("revision %q belongs to plan %q, not %q", revision.ID, revision.PlanID, id)
			}
		}
		if plan.LatestAssessmentID != "" {
			assessment, ok := state.Assessments[plan.LatestAssessmentID]
			if !ok {
				return fmt.Errorf("plan %q references missing assessment %q", id, plan.LatestAssessmentID)
			}
			if assessment.PlanID != id {
				return fmt.Errorf("assessment %q belongs to plan %q, not %q", assessment.ID, assessment.PlanID, id)
			}
		}
	}

	for id, revision := range state.Revisions {
		if revision.ID != id {
			return fmt.Errorf("revision %q is stored under mismatched id %q", revision.ID, id)
		}
		if _, ok := state.Plans[revision.PlanID]; !ok {
			return fmt.Errorf("revision %q references missing plan %q", id, revision.PlanID)
		}
		if revision.EntryCount != len(revision.Entries) {
			return fmt.Errorf("revision %q declares entry_count %d but contains %d entries",
				id, revision.EntryCount, len(revision.Entries))
		}
	}

	for id, node := range state.Nodes {
		if node.ID != id {
			return fmt.Errorf("node %q is stored under mismatched id %q", node.ID, id)
		}
	}

	for id, lease := range state.Leases {
		if lease.ID != id {
			return fmt.Errorf("lease %q is stored under mismatched id %q", lease.ID, id)
		}
		if _, ok := state.Plans[lease.PlanID]; !ok {
			return fmt.Errorf("lease %q references missing plan %q", id, lease.PlanID)
		}
		if _, ok := state.Nodes[lease.NodeID]; !ok {
			return fmt.Errorf("lease %q references missing node %q", id, lease.NodeID)
		}
		group, ok := state.LeaseGroups[lease.AllocationID]
		if !ok {
			return fmt.Errorf("lease %q references missing lease group %q", id, lease.AllocationID)
		}
		if !containsID(group.LeaseIDs, id) {
			return fmt.Errorf("lease group %q does not contain lease %q", group.ID, id)
		}
		if lease.ProbeID != "" {
			probe, ok := state.Probes[lease.ProbeID]
			if !ok {
				return fmt.Errorf("lease %q references missing probe %q", id, lease.ProbeID)
			}
			if probe.LeaseID != id {
				return fmt.Errorf("probe %q belongs to lease %q, not %q", probe.ID, probe.LeaseID, id)
			}
		}
	}

	claimedByNode := make(map[string]string)
	for id, group := range state.LeaseGroups {
		if group.ID != id {
			return fmt.Errorf("lease group %q is stored under mismatched id %q", group.ID, id)
		}
		if _, ok := state.Plans[group.PlanID]; !ok {
			return fmt.Errorf("lease group %q references missing plan %q", id, group.PlanID)
		}
		if len(group.LeaseIDs) == 0 {
			return fmt.Errorf("lease group %q contains no leases", id)
		}
		seenLeases := make(map[string]struct{}, len(group.LeaseIDs))
		for _, leaseID := range group.LeaseIDs {
			lease, ok := state.Leases[leaseID]
			if !ok {
				return fmt.Errorf("lease group %q references missing lease %q", id, leaseID)
			}
			if lease.AllocationID != id {
				return fmt.Errorf("lease %q belongs to group %q, not %q", leaseID, lease.AllocationID, id)
			}
			if _, duplicate := seenLeases[leaseID]; duplicate {
				return fmt.Errorf("lease group %q lists lease %q more than once", id, leaseID)
			}
			seenLeases[leaseID] = struct{}{}
			if lease.State == domain.LeaseClaimed {
				if other, exists := claimedByNode[lease.NodeID]; exists {
					return fmt.Errorf("node %q has two claimed leases %q and %q", lease.NodeID, other, leaseID)
				}
				claimedByNode[lease.NodeID] = leaseID
			}
		}
	}

	for id, probe := range state.Probes {
		if probe.ID != id {
			return fmt.Errorf("probe %q is stored under mismatched id %q", probe.ID, id)
		}
		lease, ok := state.Leases[probe.LeaseID]
		if !ok {
			return fmt.Errorf("probe %q references missing lease %q", id, probe.LeaseID)
		}
		if lease.ProbeID != id {
			return fmt.Errorf("lease %q does not point back at probe %q", lease.ID, id)
		}
		if _, ok := state.Plans[probe.PlanID]; !ok {
			return fmt.Errorf("probe %q references missing plan %q", id, probe.PlanID)
		}
		if _, ok := state.Nodes[probe.NodeID]; !ok {
			return fmt.Errorf("probe %q references missing node %q", id, probe.NodeID)
		}
	}

	for id, assessment := range state.Assessments {
		if assessment.ID != id {
			return fmt.Errorf("assessment %q is stored under mismatched id %q", assessment.ID, id)
		}
		if _, ok := state.Plans[assessment.PlanID]; !ok {
			return fmt.Errorf("assessment %q references missing plan %q", id, assessment.PlanID)
		}
		revision, ok := state.Revisions[assessment.RevisionID]
		if !ok {
			return fmt.Errorf("assessment %q references missing revision %q", id, assessment.RevisionID)
		}
		if revision.PlanID != assessment.PlanID {
			return fmt.Errorf("assessment %q revision %q belongs to a different plan", id, assessment.RevisionID)
		}
	}

	for requestIndex, groupID := range state.LeaseRequestKeys {
		group, ok := state.LeaseGroups[groupID]
		if !ok {
			return fmt.Errorf("lease request index %q references missing lease group %q", requestIndex, groupID)
		}
		if expected := group.PlanID + "\x00" + group.RequestKey; requestIndex != expected {
			return fmt.Errorf("lease request index %q does not match group %q", requestIndex, groupID)
		}
	}

	for requestIndex, assessmentID := range state.AssessmentRequestKeys {
		assessment, ok := state.Assessments[assessmentID]
		if !ok {
			return fmt.Errorf("assessment request index %q references missing assessment %q", requestIndex, assessmentID)
		}
		if expected := assessment.PlanID + "\x00" + assessment.RequestKey; requestIndex != expected {
			return fmt.Errorf("assessment request index %q does not match assessment %q", requestIndex, assessmentID)
		}
	}
	return nil
}

func containsID(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
