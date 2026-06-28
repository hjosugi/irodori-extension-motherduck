# MotherDuck Connector

Adds MotherDuck connectivity as an installable connector extension.

This connector is listed in the public Irodori extension marketplace.

## Connector

- Extension ID: `irodori.motherduck`
- Engine ID: `motherduck`
- Wire: `duckdb`
- Default port: `443`
- Native ABI: `irodori.connector.native.v1`
- Driver linked: `true`

A desktop adapter source snapshot is staged in `native/source/` from `db/duck.rs`.

Connector metadata lives in `connector.config.json` and `irodori.extension.json`.
The Rust code keeps native ABI exports in `src/lib.rs`, shared buffer/JSON helpers in `src/abi.rs`, and DuckDB-compatible connect/query/metadata behavior in `src/driver.rs`.

## Connection Metadata

- Endpoint modes: `motherduckService`, `localFile`, `inMemory`, `connectionString`
- Transport modes: `direct`, `sshTunnel`, `socks5Proxy`, `httpConnectProxy`, `proxyChain`
- TLS supported: `true`
- Custom driver options: `true`

| Auth method | Label | Secret purposes |
|---|---|---|
| `none` | No authentication | none |
| `connectionString` | Connection string / DSN | none |
| `motherduckToken` | MotherDuck token | `token` |
| `oauth2` | OAuth 2.0 | `token` |
| `browserSso` | Browser SSO | `token` |
| `customDriverOptions` | Custom driver options | `password`, `token`, `privateKey`, `privateKeyPassphrase` |

## ABI Calls

The scaffold handles these JSON requests today:

| Method | Response |
|---|---|
| `health` / `ping` | Connector health, engine id, ABI version, and driver link status. |
| `describe` / `capabilities` | Embedded manifest and connector config. |
| `manifest` | Raw `irodori.extension.json`. |
| `config` | Raw `connector.config.json`. |
| `connect` | Opens an in-memory/local DuckDB-compatible connection. |
| `query` | Runs SQL and returns columns, rows, and truncation status. |
| `metadata` | Returns schema/table/column metadata. |
| `close` | Closes the named connector connection. |

Driver operations return structured connector errors for invalid requests, missing connections, and backend failures.

## Development


DuckDB-linked builds share `../target` across sibling extension repositories. Normal `make check` and CI set `DUCKDB_DOWNLOAD_LIB=1` so libduckdb comes from the prebuilt upstream archive instead of a local C++ build. Run `make check-duckdb-bundled` only when a fully self-contained DuckDB build is required, because it compiles libduckdb C++ and can consume significant CPU.


```sh
make check
make build
```

Release packages place platform-specific native artifacts under `dist/native`.
