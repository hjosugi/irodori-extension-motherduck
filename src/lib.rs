//! Native connector ABI for MotherDuck.
//!
//! This connector links a real DuckDB driver and implements connect/query/
//! metadata/close over the JSON connector ABI.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

const ABI_VERSION: u32 = 1;
const ENGINE: &str = "motherduck";
const CONFIG_JSON: &str = include_str!("../connector.config.json");
const MANIFEST_JSON: &str = include_str!("../irodori.extension.json");

static CONNECTIONS: OnceLock<Mutex<HashMap<String, duckdb::Connection>>> = OnceLock::new();

#[repr(C)]
#[derive(Clone, Copy)]
pub struct IrodoriConnectorBuffer {
    pub ptr: *const u8,
    pub len: usize,
}

#[derive(Default)]
struct ObjectMeta {
    schema: String,
    name: String,
    kind: String,
    columns: Vec<Value>,
}

fn connections() -> &'static Mutex<HashMap<String, duckdb::Connection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn owned_buffer(value: String) -> IrodoriConnectorBuffer {
    let mut bytes = value.into_bytes().into_boxed_slice();
    let buffer = IrodoriConnectorBuffer {
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
    };
    std::mem::forget(bytes);
    buffer
}

fn json_buffer(value: Value) -> IrodoriConnectorBuffer {
    owned_buffer(value.to_string())
}

fn buffer_to_string(buffer: IrodoriConnectorBuffer) -> Result<String, ()> {
    if buffer.ptr.is_null() {
        return if buffer.len == 0 {
            Ok(String::new())
        } else {
            Err(())
        };
    }
    let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| ())
}

fn ok(mut payload: serde_json::Map<String, Value>) -> IrodoriConnectorBuffer {
    payload.insert("ok".to_string(), Value::Bool(true));
    json_buffer(Value::Object(payload))
}

fn error(code: &str, message: impl Into<String>) -> IrodoriConnectorBuffer {
    json_buffer(json!({
        "ok": false,
        "error": {
            "code": code,
            "message": message.into()
        }
    }))
}

fn parse_request(buffer: IrodoriConnectorBuffer) -> Result<Option<Value>, IrodoriConnectorBuffer> {
    let request = buffer_to_string(buffer).map_err(|_| {
        error(
            "connector.invalidRequest",
            "Connector request buffer must be empty or valid UTF-8 JSON.",
        )
    })?;
    let trimmed = request.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    serde_json::from_str::<Value>(trimmed)
        .map(Some)
        .map_err(|err| {
            error(
                "connector.invalidJson",
                format!("Connector request must be valid JSON: {err}"),
            )
        })
}

fn request_method(request: Option<&Value>) -> Result<&str, IrodoriConnectorBuffer> {
    match request {
        None => Ok("health"),
        Some(value) => value
            .get("method")
            .and_then(Value::as_str)
            .filter(|method| !method.trim().is_empty())
            .ok_or_else(|| {
                error(
                    "connector.invalidRequest",
                    "Connector request needs a string method.",
                )
            }),
    }
}

fn string_field<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
}

fn profile_field<'a>(request: &'a Value, field: &str) -> Option<&'a str> {
    string_field(request, field).or_else(|| {
        request
            .get("profile")
            .and_then(|profile| string_field(profile, field))
    })
}

fn connection_id(request: Option<&Value>) -> String {
    request
        .and_then(|value| {
            string_field(value, "connectionId")
                .or_else(|| string_field(value, "id"))
                .or_else(|| {
                    value
                        .get("profile")
                        .and_then(|profile| string_field(profile, "id"))
                })
        })
        .unwrap_or("default")
        .trim()
        .to_string()
}

fn max_rows(request: &Value) -> usize {
    request
        .get("maxRows")
        .or_else(|| request.get("limit"))
        .and_then(Value::as_u64)
        .unwrap_or(10_000)
        .clamp(1, 100_000) as usize
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = connection_id(Some(request));
    let database = profile_field(request, "database").or_else(|| profile_field(request, "url"));
    let conn = match database.map(str::trim) {
        None | Some("") | Some(":memory:") => duckdb::Connection::open_in_memory(),
        Some(path) => duckdb::Connection::open(path),
    };
    let conn = match conn {
        Ok(conn) => conn,
        Err(err) => return error("connector.connectFailed", format!("connect failed: {err}")),
    };
    let server_version = duckdb_version(&conn).unwrap_or_else(|| "unknown".to_string());
    if should_seed_sample(request, &connection_id) {
        if let Err(err) = seed_sample(&conn) {
            return error("connector.seedFailed", err);
        }
    }
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    guard.insert(connection_id.clone(), conn);
    ok(serde_json::Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        ("connectionId".to_string(), Value::String(connection_id)),
        ("serverVersion".to_string(), Value::String(server_version)),
        ("driverLinked".to_string(), Value::Bool(true)),
    ]))
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = connection_id(Some(request));
    let Some(sql) = string_field(request, "sql") else {
        return error(
            "connector.invalidRequest",
            "query requires a string sql field.",
        );
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(conn) = guard.get_mut(&connection_id) else {
        return error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match run_query(conn, sql, max_rows(request)) {
        Ok((columns, rows, truncated)) => ok(serde_json::Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(
                    rows.into_iter()
                        .map(|row| Value::Array(row.into_iter().collect()))
                        .collect(),
                ),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => error("connector.queryFailed", err),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let Some(conn) = guard.get_mut(&connection_id) else {
        return error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        );
    };
    match load_metadata(conn) {
        Ok(metadata) => ok(serde_json::Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => error("connector.metadataFailed", err),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    ok(serde_json::Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

fn duckdb_version(conn: &duckdb::Connection) -> Option<String> {
    conn.query_row("select version()", [], |row| row.get::<_, String>(0))
        .ok()
}

fn should_seed_sample(request: &Value, connection_id: &str) -> bool {
    request
        .get("seedSample")
        .or_else(|| {
            request
                .get("profile")
                .and_then(|profile| profile.get("seedSample"))
        })
        .and_then(Value::as_bool)
        .unwrap_or(matches!(
            connection_id,
            "duckdb-memory" | "motherduck-memory"
        ))
}

fn seed_sample(conn: &duckdb::Connection) -> Result<(), String> {
    conn.execute_batch("create table if not exists customers (id integer, name varchar);")
        .map_err(|err| format!("duckdb sample schema failed: {err}"))?;
    let existing = conn
        .query_row("select count(*) from customers", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap_or(0);
    if existing == 0 {
        conn.execute_batch("insert into customers values (1, 'Kawase Foods'), (2, 'Minato Labs');")
            .map_err(|err| format!("duckdb sample data failed: {err}"))?;
    }
    Ok(())
}

fn run_query(
    conn: &duckdb::Connection,
    sql: &str,
    cap: usize,
) -> Result<(Vec<String>, Vec<Vec<Value>>, bool), String> {
    let lead = sql.trim_start().to_ascii_lowercase();
    let is_query = [
        "select", "with", "show", "pragma", "explain", "describe", "values", "table", "call",
    ]
    .iter()
    .any(|keyword| lead.starts_with(keyword));
    if !is_query {
        conn.execute(sql, [])
            .map_err(|err| format!("query failed: {err}"))?;
        return Ok((Vec::new(), Vec::new(), false));
    }

    let mut stmt = conn
        .prepare(sql)
        .map_err(|err| format!("query failed: {err}"))?;
    let mut duck_rows = stmt
        .query([])
        .map_err(|err| format!("query failed: {err}"))?;
    let columns: Vec<String> = match duck_rows.as_ref() {
        Some(stmt) => stmt
            .column_names()
            .iter()
            .map(|column| column.to_string())
            .collect(),
        None => Vec::new(),
    };
    let column_count = columns.len();
    let mut rows = Vec::new();
    let mut truncated = false;
    while let Some(row) = duck_rows
        .next()
        .map_err(|err| format!("query failed: {err}"))?
    {
        if rows.len() >= cap {
            truncated = true;
            break;
        }
        rows.push(
            (0..column_count)
                .map(|index| cell_to_json(row, index))
                .collect(),
        );
    }
    Ok((columns, rows, truncated))
}

fn load_metadata(conn: &duckdb::Connection) -> Result<Value, String> {
    let mut objects: BTreeMap<(String, String), ObjectMeta> = BTreeMap::new();
    let mut stmt = conn
        .prepare(
            "select table_schema, table_name, table_type              from information_schema.tables              where table_schema not in ('information_schema', 'pg_catalog')              order by table_schema, table_name",
        )
        .map_err(|err| format!("metadata objects failed: {err}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|err| format!("metadata objects failed: {err}"))?;
    for row in rows {
        let (schema, name, table_type) =
            row.map_err(|err| format!("metadata objects failed: {err}"))?;
        let kind = if table_type.eq_ignore_ascii_case("VIEW") {
            "view"
        } else {
            "table"
        };
        objects.insert(
            (schema.clone(), name.clone()),
            ObjectMeta {
                schema,
                name,
                kind: kind.to_string(),
                columns: Vec::new(),
            },
        );
    }

    let mut stmt = conn
        .prepare(
            "select table_schema, table_name, column_name, data_type, is_nullable, ordinal_position              from information_schema.columns              where table_schema not in ('information_schema', 'pg_catalog')              order by table_schema, table_name, ordinal_position",
        )
        .map_err(|err| format!("metadata columns failed: {err}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i32>(5)?,
            ))
        })
        .map_err(|err| format!("metadata columns failed: {err}"))?;
    for row in rows {
        let (schema, table, name, data_type, nullable, ordinal) =
            row.map_err(|err| format!("metadata columns failed: {err}"))?;
        if let Some(object) = objects.get_mut(&(schema, table)) {
            object.columns.push(json!({
                "name": name,
                "dataType": data_type,
                "nullable": nullable.eq_ignore_ascii_case("YES"),
                "ordinal": ordinal
            }));
        }
    }

    let mut schemas: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for object in objects.into_values() {
        schemas
            .entry(object.schema.clone())
            .or_default()
            .push(json!({
                "schema": object.schema,
                "name": object.name,
                "kind": object.kind,
                "columns": object.columns
            }));
    }
    Ok(json!({
        "schemas": schemas
            .into_iter()
            .map(|(name, objects)| json!({ "name": name, "objects": objects }))
            .collect::<Vec<_>>()
    }))
}

fn cell_to_json(row: &duckdb::Row, index: usize) -> Value {
    use duckdb::types::Value as DuckValue;
    match row.get::<usize, DuckValue>(index) {
        Ok(DuckValue::Null) => Value::Null,
        Ok(DuckValue::Boolean(value)) => Value::Bool(value),
        Ok(DuckValue::TinyInt(value)) => json!(value),
        Ok(DuckValue::SmallInt(value)) => json!(value),
        Ok(DuckValue::Int(value)) => json!(value),
        Ok(DuckValue::BigInt(value)) => json!(value),
        Ok(DuckValue::UTinyInt(value)) => json!(value),
        Ok(DuckValue::USmallInt(value)) => json!(value),
        Ok(DuckValue::UInt(value)) => json!(value),
        Ok(DuckValue::UBigInt(value)) => json!(value),
        Ok(DuckValue::Float(value)) => json!(value as f64),
        Ok(DuckValue::Double(value)) => json!(value),
        Ok(DuckValue::Text(value)) => Value::String(value),
        Ok(DuckValue::Blob(value)) => Value::String(format!("\\x{}", hex_encode(&value))),
        Ok(other) => Value::String(format!("{other:?}")),
        Err(_) => Value::Null,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

#[no_mangle]
pub extern "C" fn irodori_extension_abi_version() -> u32 {
    ABI_VERSION
}

#[no_mangle]
pub extern "C" fn irodori_connector_engine_json() -> IrodoriConnectorBuffer {
    owned_buffer(ENGINE.to_string())
}

#[no_mangle]
pub extern "C" fn irodori_extension_manifest_json() -> IrodoriConnectorBuffer {
    owned_buffer(MANIFEST_JSON.to_string())
}

#[no_mangle]
pub extern "C" fn irodori_connector_config_json() -> IrodoriConnectorBuffer {
    owned_buffer(CONFIG_JSON.to_string())
}

#[no_mangle]
pub extern "C" fn irodori_connector_call_json(
    request: IrodoriConnectorBuffer,
) -> IrodoriConnectorBuffer {
    let request = match parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };
    match method {
        "health" | "ping" => ok(serde_json::Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(true)),
        ])),
        "describe" | "capabilities" => ok(serde_json::Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(true)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => owned_buffer(MANIFEST_JSON.to_string()),
        "config" => owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

#[no_mangle]
pub extern "C" fn irodori_connector_free_buffer(buffer: IrodoriConnectorBuffer) {
    if buffer.ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(buffer.ptr as *mut u8, buffer.len);
        drop(Box::from_raw(slice));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer_from_str(value: &'static str) -> IrodoriConnectorBuffer {
        IrodoriConnectorBuffer {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    fn buffer_from_bytes(value: &'static [u8]) -> IrodoriConnectorBuffer {
        IrodoriConnectorBuffer {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    fn buffer_to_json(buffer: IrodoriConnectorBuffer) -> Value {
        let bytes = unsafe { std::slice::from_raw_parts(buffer.ptr, buffer.len) };
        let value = serde_json::from_slice(bytes).unwrap();
        irodori_connector_free_buffer(buffer);
        value
    }

    fn call(request: &'static str) -> Value {
        buffer_to_json(irodori_connector_call_json(buffer_from_str(request)))
    }

    #[test]
    fn manifest_and_config_describe_the_same_connector() {
        let manifest: Value = serde_json::from_str(MANIFEST_JSON).unwrap();
        let config: Value = serde_json::from_str(CONFIG_JSON).unwrap();
        let connector = &manifest["contributes"]["connectors"][0];

        assert_eq!(manifest["id"], config["extensionId"]);
        assert_eq!(connector["engine"], ENGINE);
        assert_eq!(connector["engine"], config["connector"]["engine"]);
        assert_eq!(connector["module"], config["connector"]["module"]);
        assert_eq!(connector["connection"], config["connection"]);
        assert!(config["connection"]["authMethods"]
            .as_array()
            .is_some_and(|methods| !methods.is_empty()));
        assert!(config["connection"]["secretPurposes"]
            .as_array()
            .is_some_and(|purposes| !purposes.is_empty()));
        assert_eq!(config["runtime"]["driverLinked"], true);
        assert!(manifest["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|permission| permission == "connectors"));
    }

    #[test]
    fn call_json_reports_health_and_describes_metadata() {
        let health = call(r#"{"method":"health"}"#);
        assert_eq!(health["ok"], true);
        assert_eq!(health["engine"], ENGINE);
        assert_eq!(health["driverLinked"], true);

        let describe = call(r#"{"method":"describe"}"#);
        assert_eq!(describe["ok"], true);
        assert_eq!(
            describe["manifest"]["id"],
            describe["config"]["extensionId"]
        );
        assert_eq!(describe["config"]["connector"]["engine"], ENGINE);
    }

    #[test]
    fn connect_query_metadata_and_close_use_real_duckdb_driver() {
        let connected = call(r#"{"method":"connect","connectionId":"test","database":":memory:"}"#);
        assert_eq!(connected["ok"], true);
        assert_eq!(connected["driverLinked"], true);

        assert_eq!(
            call(
                r#"{"method":"query","connectionId":"test","sql":"create table numbers (n integer, label varchar)"}"#
            )["ok"],
            true
        );
        assert_eq!(
            call(
                r#"{"method":"query","connectionId":"test","sql":"insert into numbers values (1, 'one'), (2, 'two')"}"#
            )["ok"],
            true
        );
        let result = call(
            r#"{"method":"query","connectionId":"test","sql":"select n, label from numbers order by n","maxRows":10}"#,
        );
        assert_eq!(result["ok"], true);
        assert_eq!(result["columns"], json!(["n", "label"]));
        assert_eq!(result["rows"], json!([[1, "one"], [2, "two"]]));

        let metadata = call(r#"{"method":"metadata","connectionId":"test"}"#);
        assert_eq!(metadata["ok"], true);
        let schemas = metadata["metadata"]["schemas"].as_array().unwrap();
        assert!(schemas.iter().any(|schema| schema["objects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|object| object["name"] == "numbers")));

        assert_eq!(
            call(r#"{"method":"close","connectionId":"test"}"#)["closed"],
            true
        );
        let missing = call(r#"{"method":"query","connectionId":"test","sql":"select 1"}"#);
        assert_eq!(missing["ok"], false);
        assert_eq!(missing["error"]["code"], "connector.connectionNotFound");
    }

    #[test]
    fn query_reports_driver_errors() {
        let _ = call(r#"{"method":"connect","connectionId":"errors","database":":memory:"}"#);
        let response = call(
            r#"{"method":"query","connectionId":"errors","sql":"select * from missing_table"}"#,
        );
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"]["code"], "connector.queryFailed");
    }

    #[test]
    fn call_json_rejects_invalid_request_buffers() {
        let invalid_utf8 = buffer_to_json(irodori_connector_call_json(buffer_from_bytes(&[
            0xff, 0xfe,
        ])));
        assert_eq!(invalid_utf8["ok"], false);
        assert_eq!(invalid_utf8["error"]["code"], "connector.invalidRequest");

        let invalid_json = call("{");
        assert_eq!(invalid_json["ok"], false);
        assert_eq!(invalid_json["error"]["code"], "connector.invalidJson");

        let invalid_null = buffer_to_json(irodori_connector_call_json(IrodoriConnectorBuffer {
            ptr: std::ptr::null(),
            len: 1,
        }));
        assert_eq!(invalid_null["ok"], false);
        assert_eq!(invalid_null["error"]["code"], "connector.invalidRequest");
    }
}
