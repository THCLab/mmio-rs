#![allow(dead_code)]

use indexmap::IndexMap;
use lazy_static::lazy_static;
use oca_ast_semantics::ast::NestedAttrType;
use oca_bundle_semantics::state::{
    attribute::Attribute as MechanicsAttribute,
    attribute::AttributeType as MechanicsAttributeType,
    oca::OCABox as OCAMechanicsBox, oca::OCABundle as OCAMechanics,
};
use polars::prelude::*;
use pyo3::{exceptions::PyValueError, prelude::*};
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use transformation_file::state::Transformation;
mod events;
use events::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MMIOBundle {
    mechanics: OCAMechanics,
    meta: HashMap<String, String>,
}

#[pyclass(name = "OCABundle")]
struct OCABundlePy {
    inner: MMIOBundle,
    log: ProvenanceLog,
    data: MMData,
}

impl OCABundlePy {
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

    fn create_transformation(
        &self,
        source_said: String,
        target_said: String,
        linkage: HashMap<String, String>,
    ) -> PyResult<Transformation> {
        let mut attributes = IndexMap::new();
        linkage.iter().for_each(|(k, v)| {
            attributes.insert(k.clone(), v.clone());
        });

        Ok(Transformation {
            said: None,
            source: Some(source_said),
            target: Some(target_said),
            attributes,
        })
    }
}

#[pymethods]
impl OCABundlePy {
    #[getter]
    fn get_events(&self) -> Vec<String> {
        self.log.events.iter().map(|e| e.get_event()).collect()
    }

    #[getter]
    fn get_data(&self) -> MMData {
        self.data.clone()
    }

    fn ingest(&mut self, data: MMRecord) {
        self.data.add_record(data.clone());
        self.log.add_event(Box::new(FeedEvent::new(data)));
    }

    fn import_link(&mut self, link: String) -> PyResult<()> {
        let r = serde_json::from_str::<Transformation>(&link)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;
        let source_said = match &r.source {
            Some(s) => s.clone(),
            None => {
                return Err(PyErr::new::<PyValueError, _>(
                    "source attribute is required",
                ))
            }
        };
        let mechanics_said = match &self.inner.mechanics.said {
            Some(s) => s,
            None => {
                return Err(PyErr::new::<PyValueError, _>(
                    "mechanics.said attribute is required",
                ))
            }
        };
        if source_said != mechanics_said.to_string() {
            return Err(PyErr::new::<PyValueError, _>(
                "source attribute must be equal to mechanics.said",
            ));
        }
        self.data.add_transformation(r.clone());
        Ok(())
    }

    fn link(
        &mut self,
        standard: String,
        linkage: HashMap<String, String>,
    ) -> PyResult<()> {
        let target_said =
            Self::standard_said(standard.as_str()).ok_or_else(|| {
                PyErr::new::<PyValueError, _>(format!(
                    "standard {} not found",
                    standard
                ))
            })?;

        let transformation: Transformation = self.create_transformation(
            self.inner.mechanics.said.clone().unwrap().to_string(),
            target_said.to_string(),
            linkage.clone(),
        )?;

        self.data.add_transformation(transformation.clone());

        Ok(())
    }
}

type MMRecord = PyDataFrame;

#[derive(Clone, Debug)]
#[pyclass]
struct MMData {
    records: Vec<MMRecord>,
    transformations: Vec<Transformation>,
}

impl MMData {
    fn new() -> Self {
        Self {
            records: vec![],
            transformations: vec![],
        }
    }

    fn add_record(&mut self, record: MMRecord) {
        self.records.push(record);
    }

    fn add_transformation(&mut self, transformation: Transformation) {
        self.transformations.push(transformation);
    }

    fn transform_record(
        &self,
        data: MMRecord,
        link: &Transformation,
    ) -> PyResult<MMRecord> {
        let new_data = link.attributes.iter().try_fold(
            data.0.clone(),
            |mut acc, (old_name, new_name)| -> Result<DataFrame, PolarsError> {
                acc.rename(old_name, new_name)?;
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
        let target = OCABundlePy::standard_said(standard).ok_or_else(|| {
            PyErr::new::<PyValueError, _>(format!(
                "standard {} not found",
                standard
            ))
        })?;

        let link = self
            .transformations
            .iter()
            .find(|t| t.target == Some(target.clone()))
            .ok_or_else(|| {
                PyErr::new::<PyValueError, _>(
                    "target attribute not found in transformations",
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
    fn open(b: String) -> PyResult<OCABundlePy> {
        let r = serde_json::from_str::<MMIOBundle>(&b)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;

        let bundle = OCABundlePy::new(r);

        Ok(bundle)
    }

    #[pyfn(m)]
    fn infer_semantics(data: MMRecord) -> PyResult<OCABundlePy> {
        let mut oca = OCAMechanicsBox::new();
        data.0.schema().iter_fields().for_each(|f| {
            let mut attr = MechanicsAttribute::new(f.name().to_string());
            match f.data_type() {
                DataType::Int64 => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        MechanicsAttributeType::Numeric,
                    ));
                }
                DataType::Float64 => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        MechanicsAttributeType::Numeric,
                    ));
                }
                DataType::String => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        MechanicsAttributeType::Text,
                    ));
                }
                _ => {
                    attr.set_attribute_type(NestedAttrType::Value(
                        MechanicsAttributeType::Text,
                    ));
                }
            }
            oca.add_attribute(attr);
        });

        let oca_bundle = oca.generate_bundle();
        let mmio_bundle = MMIOBundle {
            mechanics: oca_bundle,
            meta: HashMap::new(),
        };
        let bundle = OCABundlePy::new(mmio_bundle);
        Ok(bundle)
    }

    Ok(())
}
