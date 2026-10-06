-- Tenant-scoped product domain for watchlists, explainable findings,
-- reviewed actions, and attributed feedback. Apply after 0001_source_cursors.sql.
CREATE TABLE IF NOT EXISTS public.taq_tenants (
    tenant_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.taq_api_keys (
    key_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    key_hash BYTEA PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    principal TEXT NOT NULL,
    scopes TEXT[] NOT NULL,
    requests_per_minute INTEGER NOT NULL DEFAULT 600 CHECK (requests_per_minute > 0 AND requests_per_minute <= 10000),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_taq_api_keys_tenant ON public.taq_api_keys(tenant_id);

CREATE TABLE IF NOT EXISTS public.taq_api_usage (
    key_hash BYTEA PRIMARY KEY REFERENCES public.taq_api_keys(key_hash) ON DELETE CASCADE,
    window_start TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL CHECK (request_count >= 0)
);

CREATE TABLE IF NOT EXISTS public.taq_watchlist_versions (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    version BIGINT NOT NULL CHECK (version > 0),
    created_by TEXT NOT NULL,
    change_reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_entities (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    entity_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('brand','domain','organization')),
    canonical_name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, entity_id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, version) REFERENCES public.taq_watchlist_versions(tenant_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_entity_aliases (
    tenant_id UUID NOT NULL,
    entity_id UUID NOT NULL,
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    alias_kind TEXT NOT NULL CHECK (alias_kind IN ('exact','alias','domain','negative')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, entity_id, normalized_alias, alias_kind),
    FOREIGN KEY (tenant_id, entity_id) REFERENCES public.taq_entities(tenant_id, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_taq_entity_alias_lookup
    ON public.taq_entity_aliases(tenant_id, normalized_alias) WHERE alias_kind <> 'negative';

CREATE TABLE IF NOT EXISTS public.taq_contextualized_events (
    tenant_id UUID NOT NULL,
    event_id TEXT NOT NULL,
    entity_id UUID NOT NULL,
    context_version BIGINT NOT NULL,
    match_kind TEXT NOT NULL CHECK (match_kind IN ('canonical','alias','domain')),
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id, entity_id, context_version),
    FOREIGN KEY (tenant_id, entity_id) REFERENCES public.taq_entities(tenant_id, entity_id),
    FOREIGN KEY (tenant_id, context_version) REFERENCES public.taq_watchlist_versions(tenant_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_rule_versions (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    rule_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    definition JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('candidate','shadow','active','rejected','retired')),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, rule_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_findings (
    tenant_id UUID NOT NULL,
    finding_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    entity_id UUID NOT NULL,
    rule_id TEXT NOT NULL,
    rule_version BIGINT NOT NULL,
    finding_type TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('info','low','medium','high','critical')),
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    explanation TEXT NOT NULL,
    evidence JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
      CHECK (status IN ('new','confirmed','false_positive','dismissed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, finding_id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, entity_id) REFERENCES public.taq_entities(tenant_id, entity_id),
    FOREIGN KEY (tenant_id, rule_id, rule_version)
      REFERENCES public.taq_rule_versions(tenant_id, rule_id, version)
);
CREATE INDEX IF NOT EXISTS idx_taq_findings_feed
    ON public.taq_findings(tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS public.taq_action_policies (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    policy_id TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    definition JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, policy_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_actions (
    tenant_id UUID NOT NULL,
    action_id UUID NOT NULL,
    finding_id UUID NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version BIGINT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
      CHECK (state IN ('pending','approved','rejected','expired','queued','completed','failed')),
    requested_by TEXT NOT NULL,
    approved_by TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, action_id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, finding_id) REFERENCES public.taq_findings(tenant_id, finding_id),
    FOREIGN KEY (tenant_id, policy_id, policy_version)
      REFERENCES public.taq_action_policies(tenant_id, policy_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_action_outbox (
    tenant_id UUID NOT NULL,
    action_id UUID NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','claimed','delivered','failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    last_error TEXT,
    PRIMARY KEY (tenant_id, action_id),
    FOREIGN KEY (tenant_id, action_id) REFERENCES public.taq_actions(tenant_id, action_id)
);

CREATE TABLE IF NOT EXISTS public.taq_feedback (
    tenant_id UUID NOT NULL,
    feedback_id UUID NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    finding_id UUID NOT NULL,
    finding_snapshot JSONB NOT NULL,
    label TEXT NOT NULL CHECK (label IN ('correct','false_positive','missed','needs_review')),
    reason TEXT NOT NULL,
    author TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_version BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, feedback_id),
    UNIQUE (tenant_id, idempotency_key),
    FOREIGN KEY (tenant_id, finding_id) REFERENCES public.taq_findings(tenant_id, finding_id),
    FOREIGN KEY (tenant_id, rule_id, rule_version)
      REFERENCES public.taq_rule_versions(tenant_id, rule_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_audit_log (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    actor TEXT NOT NULL,
    operation TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.taq_rule_evaluations (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    candidate_rule_id TEXT NOT NULL,
    candidate_version BIGINT NOT NULL,
    dataset_version TEXT NOT NULL,
    split TEXT NOT NULL CHECK (split IN ('gold','temporal_holdout','shadow')),
    metrics JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    guardrail_version BIGINT NOT NULL DEFAULT 0,
    evaluated_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, candidate_rule_id, candidate_version, dataset_version, split, guardrail_version),
    FOREIGN KEY (tenant_id, candidate_rule_id, candidate_version)
      REFERENCES public.taq_rule_versions(tenant_id, rule_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_rule_datasets (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    dataset_version TEXT NOT NULL,
    manifest_hash TEXT NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    sample_count BIGINT NOT NULL CHECK (sample_count > 0),
    approved_by TEXT NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, dataset_version),
    UNIQUE (tenant_id, manifest_hash)
);

CREATE TABLE IF NOT EXISTS public.taq_rule_guardrails (
    tenant_id UUID PRIMARY KEY REFERENCES public.taq_tenants(tenant_id),
    version BIGINT NOT NULL CHECK (version > 0),
    min_precision DOUBLE PRECISION NOT NULL CHECK (min_precision BETWEEN 0 AND 1),
    min_recall DOUBLE PRECISION NOT NULL CHECK (min_recall BETWEEN 0 AND 1),
    max_false_positives_per_1000 DOUBLE PRECISION NOT NULL CHECK (max_false_positives_per_1000 >= 0),
    approved_by TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.taq_rule_guardrail_history (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    version BIGINT NOT NULL CHECK (version > 0),
    min_precision DOUBLE PRECISION NOT NULL CHECK (min_precision BETWEEN 0 AND 1),
    min_recall DOUBLE PRECISION NOT NULL CHECK (min_recall BETWEEN 0 AND 1),
    max_false_positives_per_1000 DOUBLE PRECISION NOT NULL CHECK (max_false_positives_per_1000 >= 0),
    approved_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, version)
);

CREATE TABLE IF NOT EXISTS public.taq_sampled_events (
    tenant_id UUID NOT NULL REFERENCES public.taq_tenants(tenant_id),
    event_id TEXT NOT NULL,
    sampled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    label TEXT CHECK (label IN ('alertable','not_alertable','unreviewed')),
    reviewed_by TEXT,
    PRIMARY KEY (tenant_id, event_id)
);
