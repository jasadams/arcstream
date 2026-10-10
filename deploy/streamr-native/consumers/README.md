# Application consumers for the native event integration

This directory supplies the same-origin proxy for an isolated Arcstream event UI.
The engine has no knowledge of Arcstream schemas or policies. Profiles and session
summaries remain unqualified; an events UI is not full application qualification.

The gateway resolves `native-query-api:8080` and `native-dashboard:3000` on its
container network. Mount `nginx.conf` as `/etc/nginx/nginx.conf`, and publish the
gateway's port 8080 on the chosen network-accessible port. `/graphql`, including
`/graphql/ws`, routes to the query API; other requests route to the dashboard.
The dashboard uses `QUERY_API_URL=http://native-query-api:8080/graphql` for SSR.
Select the existing FlareDB backend toggle; `FLAREDB_URL` enables that backend but
does not change the dashboard's default backend.

## Kafka subscription configuration

The query API accepts independently configurable application topics and groups:

| Variable | Default |
| --- | --- |
| `KAFKA_EVENTS_TOPIC` | `unified-events` |
| `KAFKA_EVENTS_GROUP_ID` | `query-api-events` |
| `KAFKA_PROFILES_TOPIC` | `profile-updates` |
| `KAFKA_PROFILES_GROUP_ID` | `query-api-subscriptions` |

Set `KAFKA_BROKERS` to the isolated broker. For the native integration, set
`KAFKA_EVENTS_TOPIC=arc-native-unified-events` and use an isolated event group.
Existing defaults are unchanged. A profiles topic setting does not implement a
profile emitter. The live UI feed retains its existing sampling/buffering and
commit behavior; it is not the authoritative lossless event audit.

## Storage qualification

`test/streamr-reference/compare_flaredb_events.py` compares a declared historical
Kafka TSV prefix with the analytics store. It queries the explicit event IDs
without a row limit, checks multiplicity and all 17 fields, preserves null versus
empty strings and exact properties text, and compares timestamps as UTC logical
milliseconds. It does not claim to audit the complete growing live population.

```sh
python3 test/streamr-reference/compare_flaredb_events.py \
  --expected-tsv /path/to/unified.tsv --expected-rows 1389 \
  --endpoint http://127.0.0.1:18154/query/sql \
  --output-dir /path/to/new-evidence-directory
```

A successful storage check does not establish GraphQL/WebSocket behavior,
transaction-abort visibility, full profile/session parity or capacity acceptance.


Optional event text is blank in the existing nonnullable GraphQL/UI presentation when the stored value is JSON null: anonymous_id, user_id, page_url, device_type, browser and country. Original Kafka and FlareDB values remain null; properties remains its original JSON string. Missing Event fields still reject, while LiveEventMessage retains its existing missing-optional empty defaults. Wrong nonstring values and missing/null required identity/time fields still reject. GraphQL String! nullability is unchanged.

## Recorded local qualification

See `qualification-20261010.json` for source/binary receipts, exact local gates,
network/browser results and remaining limitations. The quiet-input nullable probe
passed the native-topic WebSocket, GraphQL and exact stored-event checks; its
producer resumed afterward. The first probe under ongoing input timed out and is
retained as a failure with unresolved cause. Existing sampled, read-uncommitted UI
behavior is unchanged. Events work; profile/session output is still unqualified.
