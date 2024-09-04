#![allow(dead_code)]

use oca_bundle_semantics::state::oca::OCABundle as OCAMechanics;
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
    transformations: Vec<Transformation>,
    data: Vec<MMData>,
}

impl OCABundlePy {
    fn new(inner: MMIOBundle) -> Self {
        let mut log = ProvenanceLog::new();
        log.add_event(Box::new(LoadBundleEvent::new(inner.clone())));

        Self {
            inner,
            log,
            transformations: vec![],
            data: vec![],
        }
    }
}

type MMData = PyDataFrame;

#[pymethods]
impl OCABundlePy {
    #[getter]
    fn get_events(&self) -> Vec<String> {
        self.log.events.iter().map(|e| e.get_event()).collect()
    }

    fn feed(&mut self, data: MMData) {
        self.data.push(data.clone());
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
        self.transformations.push(r);
        Ok(())
    }

    fn transform(&mut self, target: String) -> PyResult<Vec<MMData>> {
        let link = self
            .transformations
            .iter()
            .find(|t| t.target == Some(target.clone()))
            .ok_or_else(|| {
                PyErr::new::<PyValueError, _>(
                    "target attribute not found in transformations",
                )
            })?;

        let mut new_data: Vec<MMData> = vec![];
        let mut errors: Vec<PyErr> = vec![];
        self.data.iter().for_each(|d| {
            new_data.push(self.transform_data(d.clone(), link).unwrap_or_else(
                |e| {
                    errors.push(e);
                    d.clone()
                },
            ));
        });

        if !errors.is_empty() {
            return Err(errors.remove(0));
        }

        self.log.add_event(Box::new(TransformEvent::new()));
        Ok(new_data)
    }
}

impl OCABundlePy {
    fn transform_data(
        &self,
        data: MMData,
        link: &Transformation,
    ) -> PyResult<MMData> {
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

#[pymodule]
fn m2io_tmp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn open(b: String) -> PyResult<OCABundlePy> {
        let r = serde_json::from_str::<MMIOBundle>(&b)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;

        let bundle = OCABundlePy::new(r);

        Ok(bundle)
    }

    Ok(())
}
