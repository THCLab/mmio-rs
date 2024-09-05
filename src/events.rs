use crate::{MMRecord, MMIOBundle};
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3_polars::PyDataFrame;
use std::{any::type_name, time::SystemTime};

pub struct ProvenanceLog {
    pub events: Vec<EventBox>,
}

impl ProvenanceLog {
    pub fn new() -> Self {
        Self { events: vec![] }
    }

    pub fn add_event(&mut self, e: EventBox) {
        self.events.push(e);
    }
}

type EventBox = Box<dyn Event + Send + Sync + 'static>;

pub trait Event {
    fn get_event(&self) -> String;
    fn get_event_type(&self) -> String {
        let type_name = type_name::<Self>().to_string();
        type_name.split("::").last().unwrap().to_string()
    }
}

#[derive(Debug)]
struct Sys {
    user: String,
    version: String,
}

impl Sys {
    fn new() -> Self {
        let mut user = String::new();
        let mut version = String::new();

        let r = Python::with_gil(|py| -> PyResult<()> {
            let sys = py.import_bound("sys")?;
            version = sys.getattr("version")?.extract()?;

            let locals =
                [("os", py.import_bound("os")?)].into_py_dict_bound(py);
            let code =
                "os.getenv('USER') or os.getenv('USERNAME') or 'Unknown'"
                    .to_string();
            user = py.eval_bound(&code, None, Some(&locals))?.extract()?;

            Ok(())
        });

        if let Err(e) = r {
            eprintln!("Error: {:?}", e);
        }
        Sys { user, version }
    }
}

pub struct LoadBundleEvent {
    time: SystemTime,
    sys: Sys,
    bundle: MMIOBundle,
}

impl LoadBundleEvent {
    pub fn new(bundle: MMIOBundle) -> Self {
        Self {
            time: SystemTime::now(),
            sys: Sys::new(),
            bundle,
        }
    }
}

impl Event for LoadBundleEvent {
    fn get_event(&self) -> String {
        format!(
            "{}: {:?}, {:#?}",
            self.get_event_type(),
            self.sys,
            self.bundle
        )
    }
}

pub struct FeedEvent {
    time: SystemTime,
    sys: Sys,
    data: MMRecord,
}

impl FeedEvent {
    pub fn new(data: PyDataFrame) -> Self {
        Self {
            time: SystemTime::now(),
            sys: Sys::new(),
            data,
        }
    }
}

impl Event for FeedEvent {
    fn get_event(&self) -> String {
        format!(
            "{}: {:?}, {:#?}",
            self.get_event_type(),
            self.sys,
            self.data.0
        )
    }
}

pub struct TransformEvent {
    time: SystemTime,
    sys: Sys,
}

impl TransformEvent {
    pub fn new() -> Self {
        Self {
            time: SystemTime::now(),
            sys: Sys::new(),
        }
    }
}

impl Event for TransformEvent {
    fn get_event(&self) -> String {
        format!("{}: {:?}", self.get_event_type(), self.sys)
    }
}
