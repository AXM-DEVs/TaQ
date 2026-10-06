CREATE TABLE IF NOT EXISTS public.taq_source_cursors
(
    source_name TEXT PRIMARY KEY,
    cursor JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.taq_source_events_seen
(
    source_name TEXT NOT NULL,
    native_id TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_name, native_id)
);

CREATE INDEX IF NOT EXISTS idx_taq_source_events_seen_seen_at
    ON public.taq_source_events_seen (seen_at);

\i /docker-migrations/0002_product_domain.sql
\i /docker-migrations/0003_rule_evaluation_provenance.sql
\i /docker-migrations/0004_rule_dataset_samples.sql
