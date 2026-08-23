//! SemDF contract: Polarisfork `semantic.*` carried in Apache Arrow field metadata.
//!
//! Keys are `org.zndx.semdf.*`. This crate does **not** talk to Metabase, Impala,
//! or Gaius Engine. Illegal aggregations fail here (`#SL.00000002`).

use std::collections::HashMap;

use serde_json::Value;

pub const VERSION: &str = "1";

pub const SCHEMA_VERSION: &str = "org.zndx.semdf.version";
pub const SCHEMA_CATALOG: &str = "org.zndx.semdf.catalog";

pub const MEASURE_IRI: &str = "org.zndx.semdf.measure_iri";
pub const UNIT: &str = "org.zndx.semdf.unit";
pub const QUANTITY_KIND: &str = "org.zndx.semdf.quantity_kind";
pub const GRAIN: &str = "org.zndx.semdf.grain";
pub const AGGREGATIONS: &str = "org.zndx.semdf.aggregations";
pub const JOIN_KEYS: &str = "org.zndx.semdf.join_keys";
pub const ROLE: &str = "org.zndx.semdf.role";

pub const GURU_NO_METADATA: &str = "#SL.00000001.NOSEMDF";
pub const GURU_ILLEGAL_AGG: &str = "#SL.00000002.ILLEGALAGG";
pub const GURU_POLYMORPHIC: &str = "#SL.00000008.POLYVALUE";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SemdfError {
    #[error("{code} column has no SemDF field metadata")]
    NoMetadata { code: &'static str },
    #[error("{code} aggregation {op} is not legal for this measure")]
    IllegalAgg { code: &'static str, op: String },
    #[error("{code} polymorphic value column without measure binding")]
    Polymorphic { code: &'static str },
    #[error("{code} aggregations metadata is not a JSON array of strings: {detail}")]
    BadAggregations { code: &'static str, detail: String },
}

pub type FieldMeta = HashMap<String, String>;

pub fn parse_aggregations(raw: &str) -> Result<Vec<String>, SemdfError> {
    let v: Value = serde_json::from_str(raw).map_err(|e| SemdfError::BadAggregations {
        code: GURU_ILLEGAL_AGG,
        detail: e.to_string(),
    })?;
    let arr = v.as_array().ok_or_else(|| SemdfError::BadAggregations {
        code: GURU_ILLEGAL_AGG,
        detail: "expected JSON array".into(),
    })?;
    arr.iter()
        .map(|x| {
            x.as_str()
                .map(|s| s.to_ascii_uppercase())
                .ok_or_else(|| SemdfError::BadAggregations {
                    code: GURU_ILLEGAL_AGG,
                    detail: "array entries must be strings".into(),
                })
        })
        .collect()
}

/// `COUNT` and projection are always legal. Other ops must appear in `aggregations`.
pub fn check_aggregation(meta: Option<&FieldMeta>, op: &str) -> Result<(), SemdfError> {
    let op_u = op.to_ascii_uppercase();
    if op_u == "COUNT" || op_u == "COUNT_STAR" || op_u == "PROJECT" {
        return Ok(());
    }
    let meta = meta.ok_or(SemdfError::NoMetadata {
        code: GURU_NO_METADATA,
    })?;
    if !meta.contains_key(MEASURE_IRI) {
        return Err(SemdfError::Polymorphic {
            code: GURU_POLYMORPHIC,
        });
    }
    let raw = meta.get(AGGREGATIONS).ok_or(SemdfError::IllegalAgg {
        code: GURU_ILLEGAL_AGG,
        op: op_u.clone(),
    })?;
    let allowed = parse_aggregations(raw)?;
    if allowed.iter().any(|a| a == &op_u) {
        Ok(())
    } else {
        Err(SemdfError::IllegalAgg {
            code: GURU_ILLEGAL_AGG,
            op: op_u,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ricci() -> FieldMeta {
        let mut m = FieldMeta::new();
        m.insert(
            MEASURE_IRI.into(),
            "https://signals.zndx.org/sdg#Ricci".into(),
        );
        m.insert(UNIT.into(), "1".into());
        m.insert(QUANTITY_KIND.into(), "intensive".into());
        m.insert(GRAIN.into(), "graph×snapshot".into());
        m.insert(AGGREGATIONS.into(), r#"["AVG","MIN","MAX","COUNT"]"#.into());
        m
    }

    #[test]
    fn avg_legal_sum_not() {
        let m = ricci();
        assert!(check_aggregation(Some(&m), "avg").is_ok());
        let err = check_aggregation(Some(&m), "SUM").unwrap_err();
        match err {
            SemdfError::IllegalAgg { code, op } => {
                assert_eq!(code, GURU_ILLEGAL_AGG);
                assert_eq!(op, "SUM");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn count_always_legal() {
        let m = ricci();
        assert!(check_aggregation(Some(&m), "COUNT").is_ok());
        assert!(check_aggregation(None, "COUNT").is_ok());
    }

    #[test]
    fn missing_measure_is_polymorphic() {
        let m = FieldMeta::new();
        let err = check_aggregation(Some(&m), "SUM").unwrap_err();
        assert!(matches!(err, SemdfError::Polymorphic { .. }));
    }
}
