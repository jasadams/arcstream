# Team Configuration

- **Team key:** ARC
- **Team name:** Arcstream
- **GitHub repo:** jasadams/arcstream

## Versioning

- **Method:** semver
- **Tag format:** vMAJOR.MINOR.PATCH
- **Version source:** git tag

## Worktree

- **Port base:** 3200
- **Reserved port:** 3200 (global dev instance)
- **Port formula:** 3200 + (ticket_number % 900)
