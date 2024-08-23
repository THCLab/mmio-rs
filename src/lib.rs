#![allow(dead_code)]

use oca_rs::facade::bundle::Bundle as OCABundle;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
mod events;
use events::*;

#[pyclass(name = "OCABundle")]
struct OCABundlePy {
    inner: OCABundle,
    log: ProvenanceLog,
    data: Vec<MMData>,
}

impl OCABundlePy {
    fn new(inner: OCABundle) -> Self {
        let mut log = ProvenanceLog::new();
        log.add_event(Box::new(LoadBundleEvent::new(inner.clone())));

        Self {
            inner,
            log,
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

    fn transform(&mut self) -> PyResult<Vec<MMData>> {
        let mut new_data: Vec<MMData> = vec![];
        let mut errors: Vec<PyErr> = vec![];
        self.data.iter().for_each(|d| {
            new_data.push(self.transform_data(d.clone()).unwrap_or_else(|e| {
                errors.push(e);
                d.clone()
            }));
        });

        if !errors.is_empty() {
            return Err(errors.remove(0));
        }

        self.log.add_event(Box::new(TransformEvent::new()));
        Ok(new_data)
    }
}

impl OCABundlePy {
    fn transform_data(&self, data: MMData) -> PyResult<MMData> {
        let new_data = self.inner.transformations.iter().try_fold(
            data.0.clone(),
            |acc, t| {
                t.attributes.iter().try_fold(
                    acc,
                    |mut acc,
                     (old_name, new_name)|
                     -> Result<DataFrame, PolarsError> {
                        acc.rename(old_name, new_name)?;
                        Ok(acc)
                    },
                )
            },
        );
        Ok(PyDataFrame(new_data.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
        })?))
    }
}

#[pymodule]
fn m2io(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn load(b: String) -> PyResult<OCABundlePy> {
        let r = serde_json::from_str::<OCABundle>(&b).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{}", e))
        })?;

        let bundle = OCABundlePy::new(r);

        Ok(bundle)
    }

    Ok(())
}
