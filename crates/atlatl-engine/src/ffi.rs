//! The C API pieces duckdb-rs doesn't expose safely: opening a database we own, and an Arrow
//! streaming reader that reports mid-stream errors (duckdb-rs `stream_arrow` treats them as
//! end-of-stream).

use std::ffi::{CStr, CString};
use std::ops::ControlFlow;
use std::ptr;
use std::sync::Arc;

use arrow::array::{RecordBatch, StructArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use duckdb::ffi;

pub(crate) fn library_version() -> String {
    unsafe { CStr::from_ptr(ffi::duckdb_library_version()) }
        .to_string_lossy()
        .into_owned()
}

pub(crate) struct RawDb(ffi::duckdb_database);

// SAFETY: a duckdb_database is internally synchronised; connections are created per thread.
unsafe impl Send for RawDb {}
unsafe impl Sync for RawDb {}

impl RawDb {
    pub(crate) fn open(settings: &[(&str, String)]) -> Result<Self, String> {
        unsafe {
            let mut cfg: ffi::duckdb_config = ptr::null_mut();
            if ffi::duckdb_create_config(&mut cfg) != ffi::DuckDBSuccess {
                return Err("duckdb_create_config failed".into());
            }
            for (k, v) in settings {
                let (ck, cv) = (cstring(k)?, cstring(v)?);
                if ffi::duckdb_set_config(cfg, ck.as_ptr(), cv.as_ptr()) != ffi::DuckDBSuccess {
                    ffi::duckdb_destroy_config(&mut cfg);
                    return Err(format!("invalid engine setting {k}={v}"));
                }
            }
            let mut db = ptr::null_mut();
            let mut err = ptr::null_mut();
            let rc = ffi::duckdb_open_ext(c":memory:".as_ptr(), &mut db, cfg, &mut err);
            ffi::duckdb_destroy_config(&mut cfg);
            if rc != ffi::DuckDBSuccess {
                let msg = if err.is_null() {
                    "duckdb_open_ext failed".to_owned()
                } else {
                    let m = CStr::from_ptr(err).to_string_lossy().into_owned();
                    ffi::duckdb_free(err.cast());
                    m
                };
                return Err(msg);
            }
            Ok(Self(db))
        }
    }

    pub(crate) fn connection(&self) -> duckdb::Result<duckdb::Connection> {
        // SAFETY: the handle outlives the connection: `Inner` owns both and connections keep the
        // instance alive after `duckdb_close` anyway.
        unsafe { duckdb::Connection::open_from_raw(self.0) }
    }

    pub(crate) fn raw_connection(&self) -> Result<RawConn, String> {
        let mut con = ptr::null_mut();
        if unsafe { ffi::duckdb_connect(self.0, &mut con) } != ffi::DuckDBSuccess {
            return Err("duckdb_connect failed".into());
        }
        Ok(RawConn(con))
    }
}

impl Drop for RawDb {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_close(&mut self.0) }
    }
}

pub(crate) struct RawConn(ffi::duckdb_connection);

// SAFETY: used by one thread at a time, except `interrupt`, which DuckDB allows from any thread.
unsafe impl Send for RawConn {}
unsafe impl Sync for RawConn {}

impl Drop for RawConn {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_disconnect(&mut self.0) }
    }
}

impl RawConn {
    pub(crate) fn interrupt(&self) {
        unsafe { ffi::duckdb_interrupt(self.0) }
    }

    /// Streams `sql` as Arrow. `on_schema` runs once after execution; `on_batch` may stop early.
    /// Returns the rows delivered. Errors are DuckDB's own messages.
    pub(crate) fn stream(
        &self,
        sql: &str,
        on_schema: impl FnOnce(SchemaRef),
        mut on_batch: impl FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<u64, String> {
        let csql = cstring(sql)?;
        unsafe {
            let mut stmt = ptr::null_mut();
            if ffi::duckdb_prepare(self.0, csql.as_ptr(), &mut stmt) != ffi::DuckDBSuccess {
                let p = ffi::duckdb_prepare_error(stmt);
                let msg = if p.is_null() {
                    "prepare failed".to_owned()
                } else {
                    CStr::from_ptr(p).to_string_lossy().into_owned()
                };
                ffi::duckdb_destroy_prepare(&mut stmt);
                return Err(msg);
            }
            let mut res: ffi::duckdb_result = std::mem::zeroed();
            let rc = ffi::duckdb_execute_prepared_streaming(stmt, &mut res);
            ffi::duckdb_destroy_prepare(&mut stmt);
            let out = if rc != ffi::DuckDBSuccess {
                Err(result_error(&mut res).unwrap_or_else(|| "execute failed".into()))
            } else {
                let mut opts = ffi::duckdb_result_get_arrow_options(&mut res);
                let r = fetch_all(&mut res, opts, on_schema, &mut on_batch);
                ffi::duckdb_destroy_arrow_options(&mut opts);
                r
            };
            ffi::duckdb_destroy_result(&mut res);
            out
        }
    }
}

unsafe fn fetch_all(
    res: *mut ffi::duckdb_result,
    opts: ffi::duckdb_arrow_options,
    on_schema: impl FnOnce(SchemaRef),
    on_batch: &mut impl FnMut(RecordBatch) -> ControlFlow<()>,
) -> Result<u64, String> {
    unsafe {
        let ffi_schema = arrow_schema(res, opts)?;
        let schema = Arc::new(Schema::try_from(&ffi_schema).map_err(|e| e.to_string())?);
        on_schema(schema);
        let streaming = ffi::duckdb_result_is_streaming(*res);
        let mut rows = 0u64;
        loop {
            let mut chunk = if streaming {
                ffi::duckdb_stream_fetch_chunk(*res)
            } else {
                ffi::duckdb_fetch_chunk(*res)
            };
            if chunk.is_null() {
                break;
            }
            let mut arr = FFI_ArrowArray::empty();
            let err = ffi::duckdb_data_chunk_to_arrow(opts, chunk, (&raw mut arr).cast());
            ffi::duckdb_destroy_data_chunk(&mut chunk);
            if !err.is_null() {
                return Err(error_data(err));
            }
            let data = from_ffi(arr, &ffi_schema).map_err(|e| e.to_string())?;
            let batch = RecordBatch::from(StructArray::from(data));
            rows += batch.num_rows() as u64;
            if on_batch(batch).is_break() {
                return Ok(rows);
            }
        }
        match result_error(res) {
            Some(e) => Err(e),
            None => Ok(rows),
        }
    }
}

fn cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|_| "string contains a NUL byte".to_owned())
}

unsafe fn result_error(res: *mut ffi::duckdb_result) -> Option<String> {
    let p = unsafe { ffi::duckdb_result_error(res) };
    (!p.is_null()).then(|| unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned())
}

unsafe fn error_data(mut e: ffi::duckdb_error_data) -> String {
    let msg = unsafe { CStr::from_ptr(ffi::duckdb_error_data_message(e)) }
        .to_string_lossy()
        .into_owned();
    unsafe { ffi::duckdb_destroy_error_data(&mut e) };
    msg
}

unsafe fn arrow_schema(
    res: *mut ffi::duckdb_result,
    opts: ffi::duckdb_arrow_options,
) -> Result<FFI_ArrowSchema, String> {
    unsafe {
        let n = ffi::duckdb_column_count(res);
        let mut types: Vec<_> = (0..n)
            .map(|i| ffi::duckdb_column_logical_type(res, i))
            .collect();
        let mut names: Vec<_> = (0..n).map(|i| ffi::duckdb_column_name(res, i)).collect();
        let mut schema = FFI_ArrowSchema::empty();
        let err = ffi::duckdb_to_arrow_schema(
            opts,
            types.as_mut_ptr(),
            names.as_mut_ptr(),
            n,
            (&raw mut schema).cast(),
        );
        for t in &mut types {
            ffi::duckdb_destroy_logical_type(t);
        }
        if !err.is_null() {
            return Err(error_data(err));
        }
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `fetch_all` silently falls back to a materialised fetch when a result isn't streaming.
    #[test]
    fn prepared_results_stream() {
        let db = RawDb::open(&[]).expect("open");
        let con = db.raw_connection().expect("connect");
        let sql = cstring("SELECT i, repeat('x', 64) FROM range(10000000) t(i)").expect("sql");
        unsafe {
            let mut stmt = ptr::null_mut();
            assert_eq!(
                ffi::duckdb_prepare(con.0, sql.as_ptr(), &mut stmt),
                ffi::DuckDBSuccess
            );
            let mut res: ffi::duckdb_result = std::mem::zeroed();
            let rc = ffi::duckdb_execute_prepared_streaming(stmt, &mut res);
            ffi::duckdb_destroy_prepare(&mut stmt);
            assert_eq!(rc, ffi::DuckDBSuccess);
            let streaming = ffi::duckdb_result_is_streaming(res);
            ffi::duckdb_destroy_result(&mut res);
            assert!(streaming);
        }
    }
}
