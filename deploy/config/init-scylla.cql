CREATE KEYSPACE IF NOT EXISTS cdp
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS cdp.profiles (
    tenant_id text,
    canonical_id text,
    user_id text,
    first_seen timestamp,
    last_seen timestamp,
    total_events bigint,
    total_sessions bigint,
    events_1d bigint,
    events_7d bigint,
    events_30d bigint,
    events_90d bigint,
    sessions_1d bigint,
    sessions_7d bigint,
    sessions_30d bigint,
    sessions_90d bigint,
    avg_session_duration_sec bigint,
    current_session_active boolean,
    current_session_duration_sec bigint,
    page_views bigint,
    clicks bigint,
    logins bigint,
    feature_uses bigint,
    last_page text,
    last_country text,
    last_device text,
    last_browser text,
    top_pages list<text>,
    top_features list<text>,
    PRIMARY KEY ((tenant_id, canonical_id))
);
