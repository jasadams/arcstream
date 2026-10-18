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
- The custom native demo rollout workflow was retired under ARC-18. There is
  currently no complete native profile/session deployment graph. Do not reuse
  historical rollout instructions or treat image publication as cutover.
- Production cutover requires validated native application results and explicit
  authorization. See `docs/arc-18-native-status.md`.

## Worktree

- **Port base:** 3200
- **Reserved port:** 3200 (global dev instance)
- **Port formula:** 3200 + (ticket_number % 900)
