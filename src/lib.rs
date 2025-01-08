#![allow(dead_code)]

use indexmap::IndexMap;
use lazy_static::lazy_static;
use oca_sdk_rs::{
    build_from_ocafile, overlay, parse_oca_bundle_to_ocafile, Attribute,
    AttributeType, NestedAttrType, OCABox, OCABundle,
};
use polars::prelude::*;
use pyo3::{exceptions::PyValueError, prelude::*, types::PyDict};
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
mod events;
use events::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MMIOBundle {
    #[serde(rename = "oca_bundle")]
    oca: OCABundle,
    meta: HashMap<String, String>,
}

#[pyclass(name = "OCABundle")]
struct MMIOBundlePy {
    inner: MMIOBundle,
    log: ProvenanceLog,
    data: MMData,
}

impl MMIOBundlePy {
    fn new(inner: MMIOBundle) -> Self {
        let mut log = ProvenanceLog::new();
        log.add_event(Box::new(LoadBundleEvent::new(inner.clone())));

        Self {
            inner,
            log,
            data: MMData::new(),
        }
    }

    fn standard_said(standard: &str) -> Option<String> {
        lazy_static! {
            static ref STANDARDS: HashMap<String, String> = maplit::hashmap! {
                "Standard1@1.0".to_string() => "EBA3iXoZRgnJzu9L1OwR0Ke8bcTQ4B8IeJYFatiXMfh7".to_string(),
                "Standard2@1.0".to_string() => "ENnxCGDxYDGQpQw5r1u5zMc0C-u0Q_ixNGDFJ1U9yfxo".to_string()
            };
        }
        STANDARDS.get(standard).cloned()
    }
}

#[pymethods]
impl MMIOBundlePy {
    #[getter]
    fn get_events(&self) -> Vec<String> {
        self.log.events.iter().map(|e| e.get_event()).collect()
    }

    #[getter]
    fn get_data(&self) -> MMData {
        let mut d = self.data.clone();
        self.inner.oca.overlays.iter().for_each(|o| {
            if let Some(link_ov) = o.as_any().downcast_ref::<overlay::Link>() {
                d.add_links(link_ov.clone());
            }
        });
        d
    }

    fn ingest(&mut self, data: MMRecord) {
        self.data.add_record(data.clone());
        self.log.add_event(Box::new(FeedEvent::new(data)));
    }

    fn link(
        &mut self,
        standard: String,
        linkage: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let target_said =
            Self::standard_said(standard.as_str()).ok_or_else(|| {
                PyErr::new::<PyValueError, _>(format!(
                    "standard {} not found",
                    standard
                ))
            })?;

        let linkage_map: IndexMap<String, String> = linkage
            .iter()
            .map(|(k, v)| {
                (
                    k.extract::<String>().unwrap(),
                    v.extract::<String>().unwrap(),
                )
            })
            .collect();

        let mut ocafile = parse_oca_bundle_to_ocafile(&self.inner.oca);
        let mut add_link_command =
            format!("ADD LINK refs:{} ATTRS", target_said);
        linkage_map.iter().for_each(|(k, v)| {
            add_link_command.push_str(&format!(" {}={}", k, v));
        });
        ocafile.push_str(&add_link_command);

        let oca_bundle = build_from_ocafile(ocafile)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{:?}", e)))?;
        self.inner.oca = oca_bundle;

        Ok(())
    }
}

type MMRecord = PyDataFrame;

#[derive(Clone, Debug)]
#[pyclass]
struct MMData {
    records: Vec<MMRecord>,
    link_overlays: Vec<overlay::Link>,
}

impl MMData {
    fn new() -> Self {
        Self {
            records: vec![],
            link_overlays: vec![],
        }
    }

    fn add_record(&mut self, record: MMRecord) {
        self.records.push(record);
    }

    fn add_links(&mut self, link_overlay: overlay::Link) {
        self.link_overlays.push(link_overlay);
    }

    fn transform_record(
        &self,
        data: MMRecord,
        link: &overlay::Link,
    ) -> PyResult<MMRecord> {
        let new_data = link.attribute_mapping.iter().try_fold(
            data.0.clone(),
            |mut acc, (old_name, new_name)| -> Result<DataFrame, PolarsError> {
                match acc.get_column_index(new_name) {
                    Some(idx) => {
                        let s0 = acc.select_at_idx(idx).unwrap().clone();
                        let s = acc.select_series([old_name]).unwrap().clone();
                        let s1 = s[0].clone();

                        let series = match s0.dtype() {
                            DataType::String => {
                                Series::new(
                                    new_name,
                                    s0.str()
                                        .unwrap()
                                        .into_iter()
                                        .enumerate()
                                        .map(|(i, value)| match value {
                                            Some(str) => {
                                                let s1_value =
                                                    s1.get(i).unwrap();
                                                let v =
                                                    match s1_value.get_str() {
                                                        Some(s) => s,
                                                        None => &s1_value
                                                            .to_string(),
                                                    };
                                                format!("{} {}", str, v,)
                                            }
                                            None => {
                                                format!(
                                                    "{} {}",
                                                    s0.get(i).unwrap(),
                                                    s1.get(i).unwrap()
                                                )
                                                /* if value.is_integer() {
                                                    value.add(&s1.get(i).unwrap()).to_string()
                                                } else {
                                                    format!(
                                                        "{} {}",
                                                        s0.get(i).unwrap(),
                                                        s1.get(i).unwrap()
                                                    )
                                                } */
                                            }
                                        })
                                        .collect::<StringChunked>()
                                        .into_series(),
                                )
                            }
                            DataType::Int64 => Series::new(
                                new_name,
                                s0.i64()
                                    .unwrap()
                                    .into_iter()
                                    .enumerate()
                                    .map(|(i, value)| {
                                        value.map(|v| {
                                            v + s1
                                                .get(i)
                                                .unwrap()
                                                .try_extract::<i64>()
                                                .unwrap()
                                        })
                                    })
                                    .collect::<Int64Chunked>()
                                    .into_series(),
                            ),
                            _ => s1.clone(),
                        };

                        acc.replace(new_name, series)?;
                        acc = acc.drop(old_name)?;
                    }
                    None => {
                        acc.rename(old_name, new_name)?;
                    }
                }
                Ok(acc)
            },
        );
        Ok(PyDataFrame(new_data.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
        })?))
    }
}

#[pymethods]
impl MMData {
    #[getter]
    fn get_records(&self) -> Vec<MMRecord> {
        self.records.clone()
    }

    #[pyo3(name = "to")]
    fn transform(
        &mut self,
        config: HashMap<String, String>,
    ) -> PyResult<MMData> {
        let standard = config.get("standard").ok_or_else(|| {
            PyErr::new::<PyValueError, _>("standard attribute is required")
        })?;
        let target =
            MMIOBundlePy::standard_said(standard).ok_or_else(|| {
                PyErr::new::<PyValueError, _>(format!(
                    "standard {} not found",
                    standard
                ))
            })?;

        let link = self
            .link_overlays
            .iter()
            .find(|t| t.target_bundle == target.clone())
            .ok_or_else(|| {
                PyErr::new::<PyValueError, _>(
                    "target attribute not found in links",
                )
            })?;

        let mut new_data: MMData = MMData::new();
        let mut errors: Vec<PyErr> = vec![];
        self.records.iter().for_each(|d| {
            new_data.add_record(
                self.transform_record(d.clone(), link).unwrap_or_else(|e| {
                    errors.push(e);
                    d.clone()
                }),
            );
        });

        if !errors.is_empty() {
            return Err(errors.remove(0));
        }

        Ok(new_data)
    }
}

#[pymodule]
fn m2io_tmp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn open(b: String) -> PyResult<MMIOBundlePy> {
        let r = serde_json::from_str::<MMIOBundle>(&b)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;

        let bundle = MMIOBundlePy::new(r);

        Ok(bundle)
    }

    #[pyfn(m)]
    fn infer_semantics(data: MMRecord) -> PyResult<MMIOBundlePy> {
        let mut oca = OCABox::new();
        data.0.schema().iter_fields().for_each(|f| {
            let mut attr = Attribute::new(f.name().to_string());
            match f.data_type() {
                DataType::Int64 => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        AttributeType::Numeric,
                    ));
                }
                DataType::Float64 => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        AttributeType::Numeric,
                    ));
                }
                DataType::String => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        AttributeType::Text,
                    ));
                }
                _ => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        AttributeType::Text,
                    ));
                }
            }
            oca.add_attribute(attr);
        });

        let oca_bundle = oca.generate_bundle();
        let mmio_bundle = MMIOBundle {
            oca: oca_bundle,
            meta: HashMap::new(),
        };
        let bundle = MMIOBundlePy::new(mmio_bundle);
        Ok(bundle)
    }

    Ok(())
}
