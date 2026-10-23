# Team Configuration

- **Team key:** ARC
- **Team name:** Arcstream
- **GitHub repo:** jasadams/arcstream

## Versioning

- **Method:** semver
- **Tag format:** vMAJOR.MINOR.PATCH
- **Version source:** git tag

## Release

- **Pipelines:** Publish Docker Images (application artifacts).
- **Production URL:** https://cdp.alytic.com.au
- Version tags publish dashboard, query-api and event-producer images.
- The approved zero-UDF native profile/session graph has scoped Kafka/RocksDB
  correctness and recovery evidence. Native deployment is maintained in private
  `arcstream-cloud`: launch, qualified FlareDB-only update, then separate routing.
- Read `docs/arc-18-native-delivery-20261023.md` and the current ARC-17/18 ledger
  before acting. FLA-183 query availability currently blocks cutover; pending
  candidates and historical failed reports are not readiness evidence.
- Image publication, integration readiness, replay completion and production
  cutover are distinct. Cutover requires actual application evidence and explicit
  user authorization; do not re-request authorization already given in-session.

## Worktree

- **Port base:** 3200
- **Reserved port:** 3200 (global dev instance)
- **Port formula:** 3200 + (ticket_number % 900)
