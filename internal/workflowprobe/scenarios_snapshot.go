package workflowprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/store"
)

func runSnapshotRestartRecovery() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-restart-01", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "restart.example", 2026100701, 1, 1, 80)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "restart-lease-0001", "east", 1)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 1 {
		return ensure(false, "expected exactly one lease")
	}
	probe, err := submitProbe(harness, group.LeaseIDs[0], "restart-probe-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "restart-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.Verdict != domain.VerdictConsistent {
		return ensure(false, "assessment was not consistent before restart")
	}
	var baseline domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &baseline); err != nil {
		return err
	}

	snapshotPath := filepath.Join(harness.tempDir, "state.json")
	before, err := os.ReadFile(snapshotPath)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}

	// Repeated restarts must restore identical facts without rewriting the file.
	for range 2 {
		if err := harness.restart(); err != nil {
			return err
		}
	}
	after, err := os.ReadFile(snapshotPath)
	if err != nil {
		return fmt.Errorf("read snapshot after restart: %w", err)
	}
	if !bytes.Equal(after, before) {
		return ensure(false, "restart rewrote the snapshot file")
	}

	var restored domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &restored); err != nil {
		return err
	}
	baselineJSON, err := json.Marshal(baseline)
	if err != nil {
		return fmt.Errorf("encode baseline plan: %w", err)
	}
	restoredJSON, err := json.Marshal(restored)
	if err != nil {
		return fmt.Errorf("encode restored plan: %w", err)
	}
	if !bytes.Equal(restoredJSON, baselineJSON) {
		return ensure(false, "plan facts did not survive restart")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	assessmentJSON, err := json.Marshal(assessment)
	if err != nil {
		return fmt.Errorf("encode baseline assessment: %w", err)
	}
	latestJSON, err := json.Marshal(latest)
	if err != nil {
		return fmt.Errorf("encode restored assessment: %w", err)
	}
	if !bytes.Equal(latestJSON, assessmentJSON) {
		return ensure(false, "assessment facts did not survive restart")
	}
	var nodes domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes", nil, &nodes); err != nil {
		return err
	}
	if len(nodes.Items) != 1 || nodes.Items[0].NodeName != "resolver-restart-01" || nodes.Items[0].State != domain.NodeReady {
		return ensure(false, "node facts did not survive restart")
	}

	var disk domain.State
	if err := json.Unmarshal(after, &disk); err != nil {
		return fmt.Errorf("decode snapshot for fact check: %w", err)
	}
	lease, ok := disk.Leases[group.LeaseIDs[0]]
	if !ok || lease.State != domain.LeaseReported || lease.ProbeID != probe.ID {
		return ensure(false, "lease facts did not survive restart")
	}
	if len(disk.Revisions) != 1 || disk.Revisions[revision.ID].Serial != revision.Serial {
		return ensure(false, "revision facts did not survive restart")
	}
	if len(disk.LeaseGroups) != 1 || len(disk.Probes) != 1 || len(disk.Assessments) != 1 {
		return ensure(false, "allocation, probe or assessment facts did not survive restart")
	}
	if disk.LeaseRequestKeys[plan.ID+"\x00restart-lease-0001"] != group.ID ||
		disk.AssessmentRequestKeys[plan.ID+"\x00restart-assess-0001"] != assessment.ID {
		return ensure(false, "idempotency keys did not survive restart")
	}
	if len(disk.AuditEvents) == 0 {
		return ensure(false, "audit events did not survive restart")
	}

	// A snapshot that cannot be fully parsed must fail the open and stay
	// byte-for-byte untouched, with no intermediate files left behind.
	corruptDir, err := os.MkdirTemp("", "zrc-corrupt-*")
	if err != nil {
		return fmt.Errorf("create corrupt snapshot directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(corruptDir) }()
	corruptPath := filepath.Join(corruptDir, "state.json")
	middleCut := make([]byte, 0, len(before))
	middleCut = append(middleCut, before[:len(before)/4]...)
	middleCut = append(middleCut, before[3*len(before)/4:]...)
	damaged := map[string][]byte{
		"truncated":      before[:len(before)/2],
		"missing middle": middleCut,
	}
	for name, payload := range damaged {
		if err := os.WriteFile(corruptPath, payload, 0o600); err != nil {
			return fmt.Errorf("write %s snapshot: %w", name, err)
		}
		if _, err := store.Open(corruptPath); err == nil {
			return ensure(false, "corrupt snapshot ("+name+") opened without error")
		}
		current, err := os.ReadFile(corruptPath)
		if err != nil {
			return fmt.Errorf("read %s snapshot: %w", name, err)
		}
		if !bytes.Equal(current, payload) {
			return ensure(false, "corrupt snapshot ("+name+") was modified")
		}
		entries, err := os.ReadDir(corruptDir)
		if err != nil {
			return fmt.Errorf("list corrupt snapshot directory: %w", err)
		}
		if len(entries) != 1 {
			return ensure(false, "failed start ("+name+") left intermediate files")
		}
	}

	// A recognizable older structure must still decode deterministically and
	// must not be rewritten on open.
	legacyDir, err := os.MkdirTemp("", "zrc-legacy-*")
	if err != nil {
		return fmt.Errorf("create legacy snapshot directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(legacyDir) }()
	legacyPath := filepath.Join(legacyDir, "state.json")
	legacy := []byte(`{
  "revision": 7,
  "plans": {
    "plan_legacy": {
      "id": "plan_legacy",
      "zone": "legacy.example",
      "state": "draft",
      "minimum_success_ratio": 1,
      "maximum_latency_ms": 100,
      "expected_nodes": 1,
      "version": 1,
      "created_at": "2026-09-25T12:00:00Z"
    }
  }
}`)
	if err := os.WriteFile(legacyPath, legacy, 0o600); err != nil {
		return fmt.Errorf("write legacy snapshot: %w", err)
	}
	legacyStore, err := store.Open(legacyPath)
	if err != nil {
		return fmt.Errorf("open legacy snapshot: %w", err)
	}
	legacyState, err := legacyStore.Snapshot(context.Background())
	if err != nil {
		return fmt.Errorf("snapshot legacy state: %w", err)
	}
	legacyPlan, ok := legacyState.Plans["plan_legacy"]
	if !ok || legacyPlan.Zone != "legacy.example" || legacyPlan.ExpectedNodes != 1 {
		return ensure(false, "legacy snapshot did not decode deterministically")
	}
	if legacyState.Revision != 7 || legacyState.SchemaVersion != 1 {
		return ensure(false, "legacy snapshot metadata was not normalized")
	}
	if legacyState.Nodes == nil || legacyState.Leases == nil || legacyState.Probes == nil || legacyState.Assessments == nil {
		return ensure(false, "legacy snapshot collections were not initialized")
	}
	current, err := os.ReadFile(legacyPath)
	if err != nil {
		return fmt.Errorf("read legacy snapshot: %w", err)
	}
	if !bytes.Equal(current, legacy) {
		return ensure(false, "opening a legacy snapshot rewrote the file")
	}

	// A leftover intermediate file must never be adopted as valid state; only a
	// genuinely missing snapshot initializes a blank state.
	pendingDir, err := os.MkdirTemp("", "zrc-pending-*")
	if err != nil {
		return fmt.Errorf("create pending snapshot directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(pendingDir) }()
	pendingPath := filepath.Join(pendingDir, "state.json")
	if err := os.WriteFile(filepath.Join(pendingDir, ".state.json.pending-0001"), before, 0o600); err != nil {
		return fmt.Errorf("write leftover pending snapshot: %w", err)
	}
	pendingStore, err := store.Open(pendingPath)
	if err != nil {
		return fmt.Errorf("open with leftover pending snapshot: %w", err)
	}
	pendingState, err := pendingStore.Snapshot(context.Background())
	if err != nil {
		return fmt.Errorf("snapshot pending state: %w", err)
	}
	if pendingState.Revision != 0 || len(pendingState.Plans) != 0 || len(pendingState.Probes) != 0 {
		return ensure(false, "leftover intermediate file was adopted as state")
	}
	if _, err := os.Stat(pendingPath); err != nil {
		return ensure(false, "missing snapshot was not initialized")
	}
	return nil
}
