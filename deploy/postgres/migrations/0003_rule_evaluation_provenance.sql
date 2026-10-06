-- Mark pre-runner evaluations as non-authoritative without deleting their audit evidence.
ALTER TABLE public.taq_rule_evaluations
    ADD COLUMN IF NOT EXISTS evaluation_method TEXT NOT NULL DEFAULT 'legacy_unverified';

UPDATE public.taq_rule_evaluations
SET passed = FALSE,
    metrics = metrics || jsonb_build_object('invalidated_reason', 'caller_supplied_metrics_not_reproducible')
WHERE evaluation_method = 'legacy_unverified' AND passed;
