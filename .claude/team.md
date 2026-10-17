# Team Configuration

- **Team key:** ARC
- **Team name:** Arcstream
- **GitHub repo:** jasadams/arcstream

## Versioning

- **Method:** semver
- **Tag format:** vMAJOR.MINOR.PATCH
- **Version source:** git tag

## Release

- **Pipelines:** Publish Docker Images; arcstream-cloud Deploy native demo
- **Production URL:** https://cdp.alytic.com.au
- Version tags publish the dashboard, query-api and event-producer images.
  The explicit native demo workflow in `jasadams/arcstream-cloud` coordinates
  the engine, schemas, consumers and producer; publication does not automatically
  roll app images ahead of the engine. Its manual inputs pin the source and images.

## Worktree

- **Port base:** 3200
- **Reserved port:** 3200 (global dev instance)
- **Port formula:** 3200 + (ticket_number % 900)
