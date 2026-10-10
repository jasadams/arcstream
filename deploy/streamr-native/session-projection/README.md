# Native session projection SQL

These Arcstream fixtures test transformations of three immutable completed-session records captured from the previously passed native SESSION query. They do not yet connect this transformation to live SESSION output or establish a production Kafka topology.

| Candidate | Exact result | Recovery scope |
| --- | --- | --- |
| `scalar-pages.sql` | Passed deduplicated pages and UTC millisecond formatting; event types remain an array | Three committed rows preserved byte-for-byte |
| `map-diagnostic.sql` | Passed a constant Map encoded as a JSON object | Three committed rows preserved byte-for-byte |
| `dynamic-counts.sql` | Planner rejects dynamic `JSON_OBJECT` keys | Not run |
| `dynamic-map-counts.sql` | Planner requires an updating sink | Not run |
| `dynamic-map-cdc.sql` | Passed all 12 fields as three exact CDC records | First record committed; restored aggregates consume the other two inputs |

Actual evidence is under `/home/jason/qa-evidence/str32-20261010-52391450/arcstream-integration/profile-session/`: scalar/map `*-native-002` and `session-projection-dynamic-map-cdc-native-002`. Independent reviews passed. First attempts with incorrect checkpoint row expectations are preserved alongside the literal-key planning and plain-sink failures.

The dynamic Map query uses existing SQL: UNNEST, COUNT, paired ordered ARRAY_AGG and MAP. Keys come from the events; no fixed event taxonomy is imposed. Both arrays use identical ordering. The inferred JSON row sink successfully encodes Map values in these tests; other connector/schema-generation paths remain unqualified.

The two updating aggregate stages retain state by completed-session identity. Exact finite values and recovery do not prove finite lifetime retention, a stateless transformation, one final application record for arbitrary timing, or production resource limits. The strict comparator requires exactly three physical records and all 12 expected final values; it does not crop intermediate or duplicate output.

Run in the required development container with absolute repository/evidence paths mounted identically, using a fresh output directory:

```sh
python3 test/streamr-reference/session-projection/prepare.py \
  --case dynamic-map-cdc \
  --evidence-dir /absolute/fresh/evidence/session-map-cdc \
  --sql-testing /home/jason/qa-evidence/str32-20261010-52391450/recovery-admission-repair/bin/sql-testing \
  --execute
```

Omit `--execute` to prepare a guarded command only. Runs are serialized by the coordinator. No rebuild is needed. The runner verifies SQL executable SHA256 `9baf16501c77a5a30bf87c50c2d3693a14986ae92d253731cda5370953e6d2ed`, clears inherited Streamr flags and imposes native/outer timeouts of 180/240 seconds. Memory/controller/batch1 uses 16 MiB execution resources. Dynamic cases explicitly enable native aggregates and two database/snapshot slots; shared queued writes are 32 MiB and decoded values 16 MiB. The CDC case checkpoints after the first input; scalar/constant Map cases checkpoint all three. Exact selected checkpoint, source prefix, committed bytes and complete outputs are verified.

Source receipts use Arcstream base `c7f4777` and the exact Arroyo DataFusion48.0.1 fork pinned at `916b45f5c28d94765ae4a6393c5e126b2ea55e1c`. Existing scalar functions were checked in that fork. No Streamr engine, SQL extension, SESSION closure policy, live admission policy or transactional projector was changed.
