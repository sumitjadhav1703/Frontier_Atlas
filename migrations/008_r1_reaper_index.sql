-- R1 review fix: index the stray-ready dead-letter predicate (007 already shipped).

CREATE INDEX IF NOT EXISTS idx_tasks_ready_exhausted
    ON tasks (run_id)
    WHERE status = 'ready' AND attempt_count >= max_attempts;
