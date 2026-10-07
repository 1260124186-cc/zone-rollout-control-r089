package store

import (
	"bytes"
	"encoding/json"

	"solo-0014-zone-rollout-control/internal/domain"
)

const schemaVersion = 1

func blankState() domain.State {
	return normalize(domain.State{})
}

func normalize(state domain.State) domain.State {
	if state.SchemaVersion == 0 {
		state.SchemaVersion = schemaVersion
	}
	if state.Plans == nil {
		state.Plans = make(map[string]domain.RolloutPlan)
	}
	if state.Revisions == nil {
		state.Revisions = make(map[string]domain.ZoneRevision)
	}
	if state.Nodes == nil {
		state.Nodes = make(map[string]domain.ResolverNode)
	}
	if state.Leases == nil {
		state.Leases = make(map[string]domain.NodeLease)
	}
	if state.LeaseGroups == nil {
		state.LeaseGroups = make(map[string]domain.LeaseGroup)
	}
	if state.Probes == nil {
		state.Probes = make(map[string]domain.Probe)
	}
	if state.Assessments == nil {
		state.Assessments = make(map[string]domain.RolloutAssessment)
	}
	if state.LeaseRequestKeys == nil {
		state.LeaseRequestKeys = make(map[string]string)
	}
	if state.AssessmentRequestKeys == nil {
		state.AssessmentRequestKeys = make(map[string]string)
	}
	if state.AuditEvents == nil {
		state.AuditEvents = []domain.AuditEvent{}
	}
	return state
}

// recoverState performs a best-effort decode of a snapshot payload that failed
// strict decoding. It keeps every complete top-level member it can read and
// ignores the remainder, so a partially written snapshot still yields a usable
// state instead of blocking startup.
func recoverState(payload []byte) (domain.State, bool) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil {
		return domain.State{}, false
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return domain.State{}, false
	}
	raw := make(map[string]json.RawMessage)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			break
		}
		key, ok := keyToken.(string)
		if !ok {
			break
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			break
		}
		raw[key] = value
	}
	if len(raw) == 0 {
		return domain.State{}, false
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return domain.State{}, false
	}
	var state domain.State
	if err := json.Unmarshal(encoded, &state); err != nil {
		return domain.State{}, false
	}
	return normalize(state), true
}
