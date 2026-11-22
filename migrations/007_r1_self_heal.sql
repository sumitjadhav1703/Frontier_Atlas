-- R1 self-healing: cheap reaper scan and cached-body lookup by final URL.

CREATE INDEX IF NOT EXISTS idx_tasks_leased_expiry
    ON tasks (lease_expires)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS idx_observations_final_url_ok
    ON observations (final_url, fetched_at DESC)
    WHERE outcome = 'ok' AND content_key IS NOT NULL;
