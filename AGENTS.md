# Project instructions

Read `CLAUDE.md` and `.claude/build-test.md` for existing repository guidance.

## Streamr product validation — controlling objective

We are building Streamr as a general-purpose streaming SQL product.
Arcstream is a real application used to validate that product.

Success means Streamr's intended native capabilities execute the application
correctly. Producing equivalent application outputs through a workaround does
not satisfy that objective.

- Implement supported relational, aggregate, window, ranking, calendar and
  state-table behavior through native SQL.
- Do not replace that behavior with application UDFs, opaque serialized state
  machines, external processors or client-side computation to make tests pass.
- Application ownership is a placement constraint, not permission to bypass
  Streamr capabilities. Keeping code in Arcstream alone does not make the
  architecture acceptable.
- When a query fails, preserve the runnable SQL, explicit expected results,
  exact source version and failure evidence. Investigate supported SQL patterns
  and upstream Arroyo, then fix the demonstrated generic engine deficiency.
- Before introducing a workaround or changing the intended execution model,
  obtain explicit user approval. Schedule pressure, MVP scope, demo deployment
  permission and passing output tests do not provide that approval.
- Keep the failing native SQL case as a regression test. Validate the repaired
  capability through the real integration.
- Describe hybrid implementations explicitly. Do not use "native" to imply
  native SQL when UDFs implement the substantive behavior.

Required independent review verdict:

1. Does this change exercise the product capability we intended to validate?
2. Does it bypass or conceal an engine deficiency?
3. Is every architectural deviation explicitly authorized?

An unauthorized bypass is a blocking finding, even when tests and CI pass.
