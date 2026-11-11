//! Parameter flattening and canonical context encoding.

use serde_json::{Map, Value};

/// Flattens nested objects into dotted keys. Arrays and scalars are leaves; an empty object is
/// kept as a leaf so it survives a round trip.
pub fn flatten(prefix: &str, value: &Value, out: &mut Vec<(String, Value)>) {
    match value {
        Value::Object(map) if !map.is_empty() => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(&key, v, out);
            }
        }
        other => out.push((prefix.to_string(), other.clone())),
    }
}

/// Rebuilds a nested object from dotted keys.
pub fn unflatten(pairs: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut root = Map::new();
    for (key, value) in pairs {
        let mut node = &mut root;
        let mut parts = key.split('.').peekable();
        while let Some(part) = parts.next() {
            if parts.peek().is_none() {
                node.insert(part.to_string(), value.clone());
                break;
            }
            let child = node
                .entry(part.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if !child.is_object() {
                *child = Value::Object(Map::new());
            }
            node = child.as_object_mut().expect("just made an object");
        }
    }
    Value::Object(root)
}

/// JSON with object keys sorted at every level, so equal contexts encode equally.
pub fn canonical(value: &Value) -> String {
    fn sort(v: &Value) -> Value {
        match v {
            Value::Object(m) => {
                let mut keys: Vec<_> = m.keys().collect();
                keys.sort();
                let mut out = Map::new();
                for k in keys {
                    out.insert(k.clone(), sort(&m[k]));
                }
                Value::Object(out)
            }
            Value::Array(a) => Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    match value {
        Value::Null => "{}".to_string(),
        v => sort(v).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip() {
        let v = json!({"lr": 0.1, "opt": {"name": "adam", "betas": [0.9, 0.99]}, "empty": {}});
        let mut flat = Vec::new();
        flatten("", &v, &mut flat);
        assert!(flat.iter().any(|(k, _)| k == "opt.name"));
        assert_eq!(unflatten(flat), v);
    }

    #[test]
    fn canonical_sorts_keys() {
        assert_eq!(
            canonical(&json!({"b": 1, "a": {"d": 2, "c": 3}})),
            r#"{"a":{"c":3,"d":2},"b":1}"#
        );
        assert_eq!(canonical(&Value::Null), "{}");
    }
}
