-- Persist the exact, tenant-approved examples evaluated by TaQ.
-- Apply after 0003_rule_evaluation_provenance.sql.
ALTER TABLE public.taq_rule_datasets
    ADD COLUMN IF NOT EXISTS format_version TEXT NOT NULL DEFAULT 'metadata_only_v0';

CREATE TABLE IF NOT EXISTS public.taq_rule_dataset_samples (
    tenant_id UUID NOT NULL,
    dataset_version TEXT NOT NULL,
    sample_index INTEGER NOT NULL CHECK (sample_index >= 0),
    split TEXT NOT NULL CHECK (split IN ('gold','temporal_holdout')),
    label TEXT NOT NULL CHECK (label IN ('alertable','not_alertable')),
    sample JSONB NOT NULL CHECK (jsonb_typeof(sample) = 'object'),
    PRIMARY KEY (tenant_id, dataset_version, sample_index),
    FOREIGN KEY (tenant_id, dataset_version)
      REFERENCES public.taq_rule_datasets(tenant_id, dataset_version)
);

CREATE INDEX IF NOT EXISTS idx_taq_rule_dataset_samples_split
    ON public.taq_rule_dataset_samples(tenant_id, dataset_version, split, sample_index);
