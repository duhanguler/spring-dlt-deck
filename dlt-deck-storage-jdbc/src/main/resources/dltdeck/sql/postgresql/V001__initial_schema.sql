-- spring-dlt-deck PostgreSQL 17 Schema (V001)
-- 3-Table Hot/Cold/Deduplicated Layout + Multi-Pod Stripe Leases + Batch Merkle Audit Ledger

CREATE TABLE IF NOT EXISTS dlt_deck_fingerprints (
    fingerprint          VARCHAR(64) PRIMARY KEY,
    exception_fqcn       VARCHAR(512),
    root_cause_fqcn      VARCHAR(512),
    sample_message       TEXT,
    canonical_stacktrace TEXT,
    failure_category     VARCHAR(32) NOT NULL,
    recommendation       TEXT NOT NULL,
    first_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dlt_deck_messages (
    id                      UUID PRIMARY KEY,
    dlt_topic               VARCHAR(255) NOT NULL,
    dlt_partition           INTEGER NOT NULL,
    dlt_offset              BIGINT NOT NULL,
    original_topic          VARCHAR(255),
    original_partition      INTEGER,
    original_offset         BIGINT,
    original_consumer_group VARCHAR(255),
    message_key_hash        INTEGER NOT NULL DEFAULT 0,
    key_fingerprint         VARCHAR(64),
    payload_format          VARCHAR(32) NOT NULL,
    schema_id               INTEGER,
    payload_size_bytes      INTEGER NOT NULL DEFAULT 0,
    fingerprint             VARCHAR(64) NOT NULL REFERENCES dlt_deck_fingerprints(fingerprint),
    failure_category        VARCHAR(32) NOT NULL,
    replay_hops             INTEGER NOT NULL DEFAULT 0,
    status                  VARCHAR(32) NOT NULL DEFAULT 'NEW',
    active_plan_id          UUID,
    fencing_token           BIGINT NOT NULL DEFAULT 0,
    lease_owner             VARCHAR(128),
    lease_expires_at        TIMESTAMPTZ,
    published_partition     INTEGER,
    published_offset        BIGINT,
    ingested_at             TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_dlt_deck_coords UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);

CREATE INDEX IF NOT EXISTS idx_dlt_deck_msg_keyset
    ON dlt_deck_messages (dlt_topic, ingested_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_dlt_deck_msg_cluster
    ON dlt_deck_messages (fingerprint, status, ingested_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_dlt_deck_msg_order
    ON dlt_deck_messages (dlt_topic, original_partition, original_offset, dlt_offset)
    WHERE status IN ('NEW', 'FAILED_PUBLISH');
CREATE INDEX IF NOT EXISTS idx_dlt_deck_msg_lease
    ON dlt_deck_messages (lease_expires_at)
    WHERE status = 'CLAIMED';
CREATE INDEX IF NOT EXISTS idx_dlt_deck_msg_key_hol
    ON dlt_deck_messages (dlt_topic, key_fingerprint, status);

CREATE TABLE IF NOT EXISTS dlt_deck_payloads (
    message_id        UUID PRIMARY KEY REFERENCES dlt_deck_messages(id) ON DELETE CASCADE,
    message_key       BYTEA,
    payload           BYTEA,
    payload_encoding  VARCHAR(16) NOT NULL DEFAULT 'RAW',
    external_blob_uri VARCHAR(512),
    headers_json      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dlt_deck_replay_plans (
    id                  UUID PRIMARY KEY,
    name                VARCHAR(255) NOT NULL,
    fingerprint_filter  VARCHAR(64),
    dlt_topic_filter    VARCHAR(255),
    target_topic        VARCHAR(255) NOT NULL,
    ordering_mode       VARCHAR(32) NOT NULL,
    payload_patch_json  TEXT,
    rate_limit_per_sec  INTEGER NOT NULL,
    max_records         INTEGER NOT NULL,
    snapshot_count      INTEGER NOT NULL,
    plan_spec_hash      VARCHAR(64) NOT NULL,
    status              VARCHAR(32) NOT NULL,
    requires_approval   BOOLEAN NOT NULL,
    created_by          VARCHAR(128) NOT NULL,
    approved_by         VARCHAR(128),
    rejection_reason    TEXT,
    total_claimed       INTEGER NOT NULL DEFAULT 0,
    total_published     INTEGER NOT NULL DEFAULT 0,
    total_failed        INTEGER NOT NULL DEFAULT 0,
    total_indeterminate INTEGER NOT NULL DEFAULT 0,
    total_blocked_hol   INTEGER NOT NULL DEFAULT 0,
    total_re_dlt        INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL,
    approved_at         TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS dlt_deck_plan_stripes (
    plan_id             UUID NOT NULL REFERENCES dlt_deck_replay_plans(id) ON DELETE CASCADE,
    stripe_id           INTEGER NOT NULL,
    lease_owner         VARCHAR(128),
    lease_expires_at    TIMESTAMPTZ,
    fencing_token       BIGINT NOT NULL DEFAULT 0,
    status              VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    PRIMARY KEY (plan_id, stripe_id)
);

CREATE TABLE IF NOT EXISTS dlt_deck_audit_ledger (
    sequence_no         BIGSERIAL PRIMARY KEY,
    event_id            UUID NOT NULL UNIQUE,
    actor               VARCHAR(128) NOT NULL,
    actor_role          VARCHAR(64) NOT NULL,
    action              VARCHAR(64) NOT NULL,
    target_type         VARCHAR(64) NOT NULL,
    target_id           VARCHAR(128) NOT NULL,
    merkle_root         VARCHAR(64),
    details_json        TEXT NOT NULL,
    prev_hash           VARCHAR(64) NOT NULL,
    entry_hash          VARCHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL
);
