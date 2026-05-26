-- =============================================================
-- Migration 016: cluster_member_proposals table
-- =============================================================
-- Sprint 8 V1-AdminAuthority: when Engine 1 detects a new candidate
-- matching an already-APPROVED+locked cluster, it creates a proposal
-- record instead of reverting the cluster (old Sprint 7 Task E
-- behavior was destructive: status flip + PowerObject delete + audit
-- destruction of admin's prior verdict).
--
CREATE TABLE IF NOT EXISTS cluster_member_proposals (
    evidence_pairs      JSONB,
-- Admin reviews proposals in /admin Pending Review tab:
);
--   * APPROVE → property added to cluster, member_count++,
CREATE INDEX IF NOT EXISTS idx_cmp_cluster_status

CREATE INDEX IF NOT EXISTS idx_cmp_proposed_at

--               proposal.status = APPROVED
--   * REJECT  → evidence pairs blacklisted in ai_duplicate_feedbacks
--               (engine won't re-propose), proposal.status = REJECTED
-- The target cluster's APPROVED verdict is preserved either way.

DO $$ BEGIN
    CREATE TYPE cluster_proposal_status AS ENUM ('PENDING', 'APPROVED', 'REJECTED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id          UUID NOT NULL REFERENCES property_clusters(id) ON DELETE CASCADE,
    property_id         UUID NOT NULL REFERENCES properties(id) ON DELETE CASCADE,
    ai_score            REAL,
    phash_matches       INTEGER NOT NULL DEFAULT 0,
    status              cluster_proposal_status NOT NULL DEFAULT 'PENDING',
    proposed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decided_at          TIMESTAMPTZ,
    decided_by          UUID REFERENCES agents(id) ON DELETE SET NULL,
    decision_reason     TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()

-- Prevent duplicate PENDING proposals for the same (cluster, property) pair.
-- Multiple historical APPROVED/REJECTED rows for the same pair are allowed
-- (each represents a separate admin decision over time).
CREATE UNIQUE INDEX IF NOT EXISTS cmp_unique_pending
    ON cluster_member_proposals (cluster_id, property_id)
    WHERE status = 'PENDING';

    ON cluster_member_proposals (cluster_id, status);

CREATE INDEX IF NOT EXISTS idx_cmp_property_status
    ON cluster_member_proposals (property_id, status);
    ON cluster_member_proposals (proposed_at DESC);

COMMENT ON TABLE cluster_member_proposals IS
    'Sprint 8 V1-AdminAuthority: Engine 1 proposals to add new members to APPROVED+locked clusters. Admin decides per-proposal.';
