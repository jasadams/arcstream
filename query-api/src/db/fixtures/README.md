# FlareDB HTTP profile receipts

`flaredb-profile-native-http.json` is an unchanged copy of the actual native
three-table qualification prefix SQL response:
`/home/jason/qa-evidence/arc18-flaredb-three-table-20261023/host-1/prefix-profiles-response.json`.
SHA256: `15234c3fb2045c64e077f7dde25cee8c40bdc086c0e9c5918f3766cf760b469e`.
Its two rows contain all22 approved stored profile fields, native string arrays,
actual event-time timestamps and completed-session average5.0/null. The Rust
assertions use explicit application literals from
`test/streamr-reference/graph-release-fixtures/expected-prefixes.json` prefix7,
with the graph's observed identity UUID bindings. This receipt is HTTP decoding
evidence; it does not by itself establish full application recovery or deployment.

`flaredb-profile-scalar-http.json` is preserved unchanged historical scalar-engine
evidence (SHA256 `cc171843ed9352dfec7e10ba2ccfc28475c4ba57f384042db2399894f2920719`).
It encodes profile arrays inside strings and includes old deferred metadata.
It is now a negative regression: current consumers must reject that transport,
even when the embedded text is valid JSON. No scalar receipt was rewritten or
promoted to native List evidence. `changed_fields` is outside the approved22
field contract and has no special decoder path.
