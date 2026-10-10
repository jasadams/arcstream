# Native Streamr identity evaluation

These queries resolve identities through tenant-partitioned anonymous-ID and
user-ID state tables. The identity pipeline writes one capture record per input;
two downstream pipelines publish unified events and identity merges. The two
output topics commit independently.

Use an isolated broker and Streamr catalog. Create the four `arc-native-*` data
topics named in the SQL, and the three distinct recovery topics with one partition
and `cleanup.policy=compact`. Submit each query with parallelism one. The fixture
relies on the declared input order.

Run the initial fixture against that instance:

```sh
python3 deploy/streamr-native/fixture-run.py \
  --api-url http://localhost:18150/api/v1/ \
  --broker-container YOUR_BROKER_CONTAINER \
  --evidence-dir /path/to/new-attempt
```

The runner compares every output field against the reference fixture, allowing
UUID renaming while preserving identity lineage and merge direction. It verifies
the first 13 committed unified events and two merges. This initial fixture check
does not establish absence of extra records, restart correctness, sustained load,
or profile/session qualification. Recovery validation must additionally scan
complete committed topics and verify stable literal IDs across checkpoint
restoration and new input.

Run the adapter and comparator checks with:

```sh
python3 -m unittest discover -s test/streamr-reference -p 'test_*.py'
```

No existing application deployment or production topics are changed by these
files. The original Flink identity fixture remains the comparison oracle.

The reference fixture and comparator were reused from Arcstream commit
`87d20e52a5690fb7f3b6b93a44afd01f3b4495fa` (ARC-17 reference work). The native
identity body was preserved from the existing ARC-13 evaluation, with Kafka
source/sink configuration added here. Existing reference worktrees and PRs remain
separate; this increment provides the native state-table integration.
