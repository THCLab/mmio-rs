use crate::{MMRecord, MMIOBundle};
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3_polars::PyDataFrame;
use serde::{Deserialize, Serialize};
use std::{any::type_name, time::SystemTime};

#[derive(Debug, Serialize, Deserialize)]
pub struct ProvenanceLog {
    pub events: Vec<Event>,
}

impl ProvenanceLog {
    pub fn new() -> Self {
        Self { events: vec![] }
    }

    pub fn add_event(&mut self, e: Event) {
        self.events.push(e);
    }
}

// type EventBox = Box<dyn Event + Send + Sync + 'static>;
//
// pub trait Event {
//     fn get_event(&self) -> String;
//     fn get_event_type(&self) -> String {
//         let type_name = type_name::<Self>().to_string();
//         type_name.split("::").last().unwrap().to_string()
//     }
// }


#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // Optional: for tagged serialization
pub enum Event {
    LoadBundle(LoadBundleEvent),
    Feed(FeedEvent),
    Transform(TransformEvent),
}

impl Event {
    pub fn get_event_type(&self) -> &'static str {
        match self {
            Event::LoadBundle(_) => "LoadBundleEvent",
            Event::Feed(_) => "FeedEvent",
            Event::Transform(_) => "TransformEvent",
        }
    }

    pub fn get_event(&self) -> String {
        match self {
            Event::LoadBundle(e) => e.get_event(),
            Event::Feed(e) => e.get_event(),
            Event::Transform(e) => e.get_event(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
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
    pub fn get_event(&self) -> String {
        format!("LoadBundleEvent: {:?}, {:#?}", self.sys, self.bundle)
    }
}

#[derive(Debug, Serialize)]
pub struct FeedEvent {
    time: SystemTime,
    sys: Sys,
    #[serde(skip)]
    data: MMRecord,
}

impl<'de> Deserialize<'de> for FeedEvent {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let event = FeedEvent {
            time: SystemTime::now(),
            sys: Sys::new(),
            data: dummy_record(),
        };
        Ok(event)
    }
}

fn dummy_record() -> MMRecord {
    // Create a dummy MMRecord here
        Python::with_gil(|py| {
        let pd = py.import_bound("pandas").unwrap();
        let df = pd.call_method0("DataFrame").unwrap();
        df.extract::<PyDataFrame>().unwrap()
    })
}

impl FeedEvent {
    pub fn new(data: PyDataFrame) -> Self {
        Self {
            time: SystemTime::now(),
            sys: Sys::new(),
            data,
        }
    }
    pub fn get_event(&self) -> String {
        format!("FeedEvent: {:?}, {:#?}", self.sys, self.data.0)
    }
}


#[derive(Debug, Serialize, Deserialize)]
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
    pub fn get_event(&self) -> String {
        format!("TransformEvent: {:?}", self.sys)
    }
}

