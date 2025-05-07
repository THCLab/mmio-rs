#![allow(dead_code)]

use oca_sdk_rs::OCABundle;
use pyo3::{exceptions::PyValueError, prelude::*};
use said::{sad::SAD, SelfAddressingIdentifier};
use serde::{Deserialize, Serialize};
use said::derivation::HashFunctionCode;
use said::version::format::SerializationFormats;

#[pyclass]
pub struct PySaid {
    #[pyo3(get, set)]
    pub value: String,
}

impl From<said::SelfAddressingIdentifier> for PySaid {
    fn from(said: said::SelfAddressingIdentifier) -> Self {
        PySaid { value: said.to_string() }
    }
}

#[pyclass(name = "MMIO")]
#[derive(Debug, Serialize, Deserialize, SAD, Clone)]
pub struct MMIO {
    pub version: String,
    #[said]
    pub id: Option<SelfAddressingIdentifier>,
    pub modalities: Vec<Modality>,
}

#[pyclass(name = "Modality")]
#[derive(Clone, Debug, Serialize, Deserialize, SAD)]
pub struct Modality {
    #[said]
    pub id: Option<SelfAddressingIdentifier>,
    pub modality_said: Option<SelfAddressingIdentifier>,
    pub modality_type: ModalityType,
    pub media_type: String,
    pub oca_bundle: Semantic,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ModalityType {
    Image,
    Text,
    Audio,
    Video,
    Binary,
}


impl IntoPy<PyObject> for ModalityType {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            ModalityType::Image => "image".into_py(py),
            ModalityType::Audio => "audio".into_py(py),
            ModalityType::Text => "text".into_py(py),
            ModalityType::Binary => "binary".into_py(py),
            ModalityType::Video => "video".into_py(py),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Semantic {
    Reference(SelfAddressingIdentifier),
    Bundle(OCABundle),
}

#[pyclass]
#[derive(Clone)]
pub struct PySemantic {
    inner: Semantic,
}

#[pyclass]
#[derive(Clone)]
pub struct PyOCABundle {
    inner: OCABundle,
}

impl PyOCABundle {
    pub fn into_inner(&self) -> OCABundle {
        self.inner.clone()
    }
}


#[pymethods]
impl PySemantic {
    #[staticmethod]
    pub fn reference(said: String) -> Self {
        let id: SelfAddressingIdentifier = said.parse().unwrap();
        Self { inner: Semantic::Reference(id) }
    }

    #[staticmethod]
    pub fn bundle(bundle: &PyOCABundle) -> Self {
        Self { inner: Semantic::Bundle(bundle.into_inner().clone()) }
    }

    pub fn is_reference(&self) -> bool {
        matches!(self.inner, Semantic::Reference(_))
    }

    pub fn is_bundle(&self) -> bool {
        matches!(self.inner, Semantic::Bundle(_))
    }

    pub fn __str__(&self) -> String {
        match &self.inner {
            Semantic::Reference(id) => id.to_string(),
            Semantic::Bundle(oca_bundle) => {
                let oca_bundle = serde_json::to_string(&oca_bundle).unwrap();
                oca_bundle.to_string()
            }
        }
    }
}



impl MMIO {
    fn new() -> Self {
        Self {
            version: "0.1".to_string(),
            modalities: vec![],
            id: None,
        }
    }
}

#[pymethods]
impl Modality {
    #[getter]
    fn get_id(&self) -> PySaid {
        self.id.clone().map_or_else(
            || PySaid { value: "None".to_string() },
            |id| PySaid { value: id.to_string() },
        )
    }

    #[getter]
    fn get_modality_said(&self) -> PySaid {
        self.modality_said.clone().map_or_else(
            || PySaid { value: "None".to_string() },
            |said| PySaid { value: said.to_string() },
        )
    }

    #[getter]
    fn get_modality_type(&self) -> ModalityType {
        self.modality_type.clone()
    }

    #[getter]
    fn get_media_type(&self) -> String {
        self.media_type.clone()
    }

    #[getter]
    fn get_oca_bundle(&self) -> PySemantic {
        match &self.oca_bundle {
            Semantic::Reference(said) => PySemantic::reference(said.to_string()),
            Semantic::Bundle(bundle) => PySemantic::bundle(&PyOCABundle { inner: bundle.clone() }),
        }
    }
}

#[pymethods]
impl MMIO {
    #[getter]
    fn get_modalities(&self) -> Vec<Modality> {
        self.modalities.clone()
    }

    // fn ingest(&mut self, data: MMRecord) {
    //     todo!()
    // }

    fn serialize(&self) -> PyResult<String> {
        // Serialize the MMIO to JSON
        let json = serde_json::to_string(&self)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;
        Ok(json)
    }
}

#[pymodule]
fn m2io_tmp(m: &Bound<'_, PyModule>) -> PyResult<()> {
    #[pyfn(m)]
    fn open(b: String) -> PyResult<MMIO> {
        let mmio = serde_json::from_str::<MMIO>(&b)
            .map_err(|e| PyErr::new::<PyValueError, _>(format!("{}", e)))?;

        Ok(mmio)
    }

    // #[pyfn(m)]
    // fn infer_semantics(data: MMRecord) -> PyResult<MMIO> {
    //     let mut oca = OCABox::new();
    //     data.0.schema().iter_fields().for_each(|f| {
    //         let mut attr = Attribute::new(f.name().to_string());
    //         match f.data_type() {
    //             DataType::Int64 => {
    //                 attr.set_attribute_type(NestedAttrType::Value(
    //                     AttributeType::Numeric,
    //                 ));
    //             }
    //             DataType::Float64 => {
    //                 attr.set_attribute_type(NestedAttrType::Value(
    //                     AttributeType::Numeric,
    //                 ));
    //             }
    //             DataType::String => {
    //                 attr.set_attribute_type(NestedAttrType::Value(
    //                     AttributeType::Text,
    //                 ));
    //             }
    //             _ => {
    //                 attr.set_attribute_type(NestedAttrType::Value(
    //                     AttributeType::Text,
    //                 ));
    //             }
    //         }
    //         oca.add_attribute(attr);
    //     });
    //
    //     let oca_bundle = oca.generate_bundle();
    //     let modality = Modality {
    //         id: None,
    //         modality_said: None,
    //         modality_type: ModalityType::Image,
    //         media_type: "text".to_string(),
    //         oca_bundle: Semantic::Bundle(oca_bundle),
    //     };
    //     let mmio = MMIO {
    //         version:"0.1".to_string(),
    //         id: None,
    //         data: vec![modality], };
    //     Ok(mmio)
    // }

    Ok(())
}


#[cfg(test)]
mod tests {
    use said::derivation::HashFunction;

    use super::*;

    #[test]
    fn test_create_new() {
        let mut mmio = MMIO::new();
        assert_eq!(mmio.version, "0.1");
        assert_eq!(mmio.modalities.len(), 0);
        assert_eq!(mmio.id, None);

        let oca_bundle_json = r#"{"v":"OCAS20JSON000320_","digest":"EHJ58dssK7HxXJjATIdMjXy2aoJpZRH7cIai5NXPQaZD","capture_base":{"digest":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"capture_base/2.0.0","attributes":{"first_name":"Text","hgt":"Numeric","last_name":"Text","wgt":"Numeric"}},"overlays":{"character_encoding":{"digest":"EEDz_xTwN9P8BCZcU33OfFrO_lWIry9Jl1srE9leGbwF","capture_base":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"overlay/character_encoding/2.0.0","attribute_character_encoding":{"first_name":"utf-8","hgt":"utf-8","last_name":"utf-8","wgt":"utf-8"}},"meta":[{"digest":"EP9iNoIrLu9w3YAMNW8FLWj5sP6VpOIoTIvDeC_6kvK0","capture_base":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"overlay/meta/2.0.0","language":"eng","description":"Standard 1 Patient BMI","name":"Patient BMI"}]}}"#;
        let oca_bundle: OCABundle = serde_json::from_str(oca_bundle_json).unwrap();

        let code = HashFunctionCode::Blake3_256;
        let format = SerializationFormats::JSON;

        #[derive(Serialize, Deserialize)]
        struct Bmi {
            first_name: String,
            last_name: String,
            hgt: i64,
            wgt: i64,
        }

        let d1 = Bmi {
            first_name: "John".to_string(),
            last_name: "Doe".to_string(),
            hgt: 180,
            wgt: 75,
        };

        let json = serde_json::to_string(&d1).unwrap();

        let said = HashFunction::from(code.clone()).derive(&serde_json::to_vec(&json).unwrap());


        let mut modality = Modality {
            id: None,
            modality_said: Some(said),
            modality_type: ModalityType::Image,
            media_type: "image/png".to_string(),
            oca_bundle: Semantic::Bundle(oca_bundle),
        };
        modality.compute_digest(&code, &format);
        mmio.modalities.push(modality);

        mmio.compute_digest(&code, &format);
        let computed_digest = mmio.id.as_ref();

        assert_eq!(computed_digest, Some(&"EI-TaIVg2tmtXMdjAlogb5OnmaAsdhHVnGqfhDMk4mTM".parse().unwrap()));

        println!("Serialized MMIO: {}", mmio.serialize().unwrap());

        assert_eq!(mmio.version, "0.1");
        assert_eq!(mmio.modalities.len(), 1);
        assert_eq!(mmio.modalities[0].media_type, "image/png");
    }

    #[test]
    fn test_deserialize() {
        let json = r#"{"version":"0.1","id":"EI-TaIVg2tmtXMdjAlogb5OnmaAsdhHVnGqfhDMk4mTM","data":[{"id":"EA_zIBLGGyzCo5ywVZz5asrtktgxR2dLRiegv6-wmC89","modality_said":"EK0GaxzGUPg54gPrUVOE1BkmZyRJXWXjGmQCeZCjSNeQ","modality_type":"Image","media_type":"image/png","oca_bundle":{"type":"Bundle","value":{"digest":"EHJ58dssK7HxXJjATIdMjXy2aoJpZRH7cIai5NXPQaZD","capture_base":{"digest":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"capture_base/2.0.0","attributes":{"first_name":"Text","hgt":"Numeric","last_name":"Text","wgt":"Numeric"}},"overlays":{"character_encoding":{"digest":"EEDz_xTwN9P8BCZcU33OfFrO_lWIry9Jl1srE9leGbwF","capture_base":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"overlay/character_encoding/2.0.0","attribute_character_encoding":{"first_name":"utf-8","hgt":"utf-8","last_name":"utf-8","wgt":"utf-8"}},"meta":[{"digest":"EP9iNoIrLu9w3YAMNW8FLWj5sP6VpOIoTIvDeC_6kvK0","capture_base":"EHZSMO2EFsXy8r5XogQ381-VmOiTUQYjV3WNkBfWYaCH","type":"overlay/meta/2.0.0","language":"eng","description":"Standard 1 Patient BMI","name":"Patient BMI"}]}}}}]}"#;
        let mmio: MMIO = serde_json::from_str(json).unwrap();
        assert_eq!(mmio.version, "0.1");
        assert_eq!(mmio.modalities.len(), 1);
        assert_eq!(mmio.modalities[0].media_type, "image/png");
        assert_eq!(mmio.id.as_ref(), Some(&"EI-TaIVg2tmtXMdjAlogb5OnmaAsdhHVnGqfhDMk4mTM".parse().unwrap()));
    }
}
