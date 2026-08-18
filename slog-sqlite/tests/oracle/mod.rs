//! Logical equality oracle: both databases pass `PRAGMA integrity_check`, have an
//! identical `sqlite_master` schema, and identical per-table content compared
//! with rows ordered by every column (placement-independent).

use std::collections::BTreeMap;
use std::path::Path;

use rusqlite::Connection;

/// Panics unless the two database files are logically identical.
pub fn assert_equal(a: &Path, b: &Path) {
    let da = dump(a);
    let db = dump(b);
    assert_eq!(da.integrity, "ok", "integrity_check failed for {}", a.display());
    assert_eq!(db.integrity, "ok", "integrity_check failed for {}", b.display());
    assert_eq!(da.schema, db.schema, "schema differs:\n{}\n---\n{}", da.schema, db.schema);
    assert_eq!(da.content, db.content, "table content differs");
}

struct Dump {
    integrity: String,
    schema: String,
    content: BTreeMap<String, String>,
}

fn dump(path: &Path) -> Dump {
    let conn = Connection::open(path).unwrap();
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    let schema: String = {
        let mut stmt = conn
            .prepare(
                "SELECT type, name, tbl_name, coalesce(sql, '') FROM sqlite_master
                 WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name, tbl_name",
            )
            .unwrap();
        let rows = stmt
            .query_map([], |r| {
                Ok(format!(
                    "{}|{}|{}|{}",
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?
                ))
            })
            .unwrap();
        rows.collect::<Result<Vec<_>, _>>().unwrap().join("\n")
    };
    let tables: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let content = tables.into_iter().map(|t| (t.clone(), table_dump(&conn, &t))).collect();
    Dump { integrity, schema, content }
}

/// All rows of `table`, one per line, ordered by every column.
fn table_dump(conn: &Connection, table: &str) -> String {
    let cols: Vec<String> = {
        let mut stmt = conn.prepare("SELECT name FROM pragma_table_info(?1)").unwrap();
        stmt.query_map([table], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let quoted = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
    let order = cols.iter().map(|c| quoted(c)).collect::<Vec<_>>().join(",");
    let mut stmt =
        conn.prepare(&format!("SELECT * FROM {} ORDER BY {order}", quoted(table))).unwrap();
    let width = stmt.column_count();
    let mut rows = stmt.query([]).unwrap();
    let mut lines = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        let rendered: Vec<String> = (0..width).map(|i| render(row.get_ref(i).unwrap())).collect();
        lines.push(rendered.join("|"));
    }
    lines.join("\n")
}

fn render(v: rusqlite::types::ValueRef) -> String {
    use rusqlite::types::ValueRef::*;
    match v {
        Null => "NULL".into(),
        Integer(i) => i.to_string(),
        Real(f) => format!("{f:?}"),
        Text(t) => format!("'{}'", String::from_utf8_lossy(t)),
        Blob(b) => format!("x'{}'", b.iter().map(|b| format!("{b:02x}")).collect::<String>()),
    }
}
