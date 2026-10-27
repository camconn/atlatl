//! A small JSON Schema validator covering the keywords schemars emits; enough to check that
//! sample reports and events conform to the published schemas.

use serde_json::Value;

pub fn validate(schema: &Value, v: &Value) -> Result<(), String> {
    check(schema, schema, v, "$")
}

fn check(root: &Value, s: &Value, v: &Value, at: &str) -> Result<(), String> {
    let s = match s {
        Value::Bool(true) => return Ok(()),
        Value::Bool(false) => return Err(format!("{at}: schema is false")),
        Value::Object(m) => m,
        _ => return Err(format!("{at}: bad schema")),
    };
    if let Some(r) = s.get("$ref").and_then(Value::as_str) {
        let name = r
            .strip_prefix("#/$defs/")
            .ok_or_else(|| format!("{at}: unsupported $ref {r}"))?;
        let target = root
            .pointer(&format!("/$defs/{name}"))
            .ok_or_else(|| format!("{at}: missing def {name}"))?;
        check(root, target, v, at)?;
    }
    if let Some(t) = s.get("type") {
        let types: Vec<&str> = match t {
            Value::String(x) => vec![x.as_str()],
            Value::Array(a) => a.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        if !types.iter().any(|t| type_ok(t, v)) {
            return Err(format!("{at}: expected {types:?}, got {v}"));
        }
    }
    if let Some(e) = s.get("enum").and_then(Value::as_array)
        && !e.contains(v)
    {
        return Err(format!("{at}: {v} not in {e:?}"));
    }
    if let Some(c) = s.get("const")
        && c != v
    {
        return Err(format!("{at}: {v} != const {c}"));
    }
    if let (Some(min), Some(n)) = (s.get("minimum").and_then(Value::as_f64), v.as_f64())
        && n < min
    {
        return Err(format!("{at}: {n} < minimum {min}"));
    }
    if let Some(all) = s.get("allOf").and_then(Value::as_array) {
        for x in all {
            check(root, x, v, at)?;
        }
    }
    if let Some(any) = s.get("anyOf").and_then(Value::as_array)
        && !any.iter().any(|x| check(root, x, v, at).is_ok())
    {
        return Err(format!("{at}: matches no anyOf branch: {v}"));
    }
    if let Some(one) = s.get("oneOf").and_then(Value::as_array) {
        let n = one.iter().filter(|x| check(root, x, v, at).is_ok()).count();
        if n != 1 {
            let errs: Vec<String> = one
                .iter()
                .filter_map(|x| check(root, x, v, at).err())
                .collect();
            return Err(format!("{at}: matches {n} oneOf branches: {errs:?}"));
        }
    }
    if let Value::Object(obj) = v {
        let props = s.get("properties").and_then(Value::as_object);
        if let Some(req) = s.get("required").and_then(Value::as_array) {
            for k in req.iter().filter_map(Value::as_str) {
                if !obj.contains_key(k) {
                    return Err(format!("{at}: missing required `{k}`"));
                }
            }
        }
        for (k, x) in obj {
            let path = format!("{at}.{k}");
            if let Some(ps) = props
                .and_then(|p| p.get(k))
                .or(s.get("additionalProperties"))
            {
                check(root, ps, x, &path)?;
            }
        }
    }
    if let (Value::Array(a), Some(items)) = (v, s.get("items")) {
        for (i, x) in a.iter().enumerate() {
            check(root, items, x, &format!("{at}[{i}]"))?;
        }
    }
    Ok(())
}

fn type_ok(t: &str, v: &Value) -> bool {
    match t {
        "null" => v.is_null(),
        "boolean" => v.is_boolean(),
        "object" => v.is_object(),
        "array" => v.is_array(),
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => v.is_i64() || v.is_u64(),
        _ => false,
    }
}
