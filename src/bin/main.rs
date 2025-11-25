use clap::{Parser, Subcommand};
use m2io_tmp::{Modality, ModalityType, Semantic, MMIO};
use said::derivation::{HashFunction, HashFunctionCode};
use said::SelfAddressingIdentifier;
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "m2io")]
#[command(about = "Multimodal Integration Object CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new MMIO object
    Create {
        #[arg(short = 'm', long = "modalities",
            value_parser = parse_modality,
            num_args = 0..,
            help = "Specify a modality using: file=path,bundle_said=<SAID>. Repeat for multiple modalities.",
            conflicts_with = "manifest"
        )]
        modalities: Vec<Modality>,

        #[arg(short = 'f', long = "manifest",
            help = "Path to a manifest file (JSON or CSV) containing modality definitions",
            conflicts_with = "modalities"
        )]
        manifest: Option<PathBuf>,

        #[arg(short, long)]
        output: PathBuf,
    },
    Parse {
        #[arg(long = "mmio")]
        mmio: PathBuf,
    },
    Said {
        #[arg(long = "file")]
        file: PathBuf,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct ModalityManifestEntry {
    file_name: String,
    bundle_said: String,
    #[serde(default)]
    modality_type: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
}

fn modality_type_from_mime(mime: &str) -> Option<ModalityType> {
    if mime.starts_with("image/") {
        Some(ModalityType::Image)
    } else if mime.starts_with("text/") || mime == "application/json" {
        Some(ModalityType::Text)
    } else if mime.starts_with("audio/") {
        Some(ModalityType::Audio)
    } else if mime.starts_with("video/") {
        Some(ModalityType::Video)
    } else if mime == "application/octet-stream" || mime.starts_with("application/") {
        Some(ModalityType::Binary)
    } else {
        // default to text
        Some(ModalityType::Text)
    }
}

fn parse_modality(s: &str) -> Result<Modality, String> {
    let mut file = None;
    let mut bundle_said = None;
    let mut modality_type = None;
    let mut media_type = None;

    for part in s.split(',') {
        let mut kv = part.splitn(2, '=');
        match (kv.next(), kv.next()) {
            (Some("file"), Some(v)) => file = Some(v.to_string()),
            (Some("bundle_said"), Some(v)) => {
                let said: SelfAddressingIdentifier = v.parse().unwrap();
                bundle_said = Some(said);

            },
            (Some("modality_type"), Some(v)) => {
                let mt: ModalityType = v.parse().unwrap();
                modality_type = Some(mt);
            },
            (Some("media_type"), Some(v)) => media_type = Some(v.to_string()),
            _ => return Err(format!("Invalid modality format: {}", part)),
        }
    }

    match (file, bundle_said) {
        (Some(f), Some(b)) => {
            create_modality_from_file(&f, b, modality_type, media_type)
        },
        _ => Err("Both file and semantic must be provided.".to_string()),
    }
}

fn create_modality_from_file(
    file_path: &str,
    bundle_said: SelfAddressingIdentifier,
    modality_type: Option<ModalityType>,
    media_type: Option<String>,
) -> Result<Modality, String> {
    let mut buf = [0; 512];
    let mut file_reader = File::open(file_path).map_err(|e| format!("Cannot open file: {}", e))?;
    let n = file_reader.read(&mut buf).map_err(|e| format!("Read error: {}", e))?;

    let (final_media_type, final_modality_type) = if media_type.is_none() {
        // Infer MIME type from file content
        let mime = infer::get(&buf[..n])
            .map(|kind| kind.mime_type())
            .unwrap_or("application/octet-stream");
        let mt = media_type.unwrap_or_else(|| mime.to_string());
        let mod_type = modality_type.or_else(|| modality_type_from_mime(mime));
        (mt, mod_type)
    } else {
        (media_type.unwrap(), modality_type)
    };

    let final_modality_type = final_modality_type.ok_or_else(|| "Could not determine modality type".to_string())?;

    let code = HashFunctionCode::Blake3_256;
    let hash_algorithm = HashFunction::from(code.clone());

    let said = hash_algorithm.derive_from_stream(file_reader).unwrap();

    let mut modality = Modality {
        digest: None,
        modality_said: Some(said),
        modality_type: final_modality_type,
        media_type: final_media_type,
        oca_bundle: Semantic::Reference(bundle_said)
    };

    modality.compute_digest();
    Ok(modality)
}

fn load_modalities_from_manifest(manifest_path: &PathBuf) -> Result<Vec<Modality>, String> {
    let mut file = File::open(manifest_path)
        .map_err(|e| format!("Failed to open manifest file: {}", e))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|e| format!("Failed to read manifest file: {}", e))?;

    // Try to parse as JSON first
    let entries: Vec<ModalityManifestEntry> = if manifest_path.extension().and_then(|s| s.to_str()) == Some("json") {
        serde_json::from_str(&contents)
            .map_err(|e| format!("Failed to parse JSON manifest: {}", e))?
    } else if manifest_path.extension().and_then(|s| s.to_str()) == Some("csv") {
        // Parse as CSV
        let mut reader = csv::Reader::from_reader(contents.as_bytes());
        reader.deserialize()
            .collect::<Result<Vec<ModalityManifestEntry>, _>>()
            .map_err(|e| format!("Failed to parse CSV manifest: {}", e))?
    } else {
        return Err("Manifest file must have .json or .csv extension".to_string());
    };

    let mut modalities = Vec::new();
    for entry in entries {
        let bundle_said: SelfAddressingIdentifier = entry.bundle_said.parse()
            .map_err(|e| format!("Invalid bundle_said '{}': {}", entry.bundle_said, e))?;

        let modality_type = if let Some(mt_str) = entry.modality_type {
            Some(mt_str.parse::<ModalityType>()
                .map_err(|e| format!("Invalid modality_type '{}': {}", mt_str, e))?)
        } else {
            None
        };

        let modality = create_modality_from_file(
            &entry.file_name,
            bundle_said,
            modality_type,
            entry.media_type,
        )?;

        modalities.push(modality);
    }

    Ok(modalities)
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create { modalities, manifest, output } => {
            let final_modalities = if let Some(manifest_path) = manifest {
                load_modalities_from_manifest(&manifest_path)
                    .expect("Failed to load modalities from manifest")
            } else if !modalities.is_empty() {
                modalities
            } else {
                eprintln!("Error: Either --modalities or --manifest must be provided");
                std::process::exit(1);
            };

            let mut mmio = MMIO {
                version: "0.1".to_string(),
                digest: None,
                modalities: final_modalities,
            };

            mmio.compute_digest();

            let json = serde_json::to_string_pretty(&mmio).expect("Failed to serialize MMIO");
            fs::write(&output, json).expect("Failed to write MMIO file");
            println!("MMIO object created at: {}", output.display());
        }
        Commands::Parse { mmio } => {
            let mut file = File::open(&mmio).expect("Failed to open MMIO file");
            let mut contents = String::new();
            file.read_to_string(&mut contents).expect("Failed to read MMIO file");

            let mmio: MMIO = serde_json::from_str(&contents).expect("Failed to parse MMIO");
            // Verify if the SAID are valid
            for modality in &mmio.modalities {
                if let Some(said) = &modality.digest {
                    let mut  m = modality.clone();
                    m.compute_digest();
                    assert_eq!(said, &m.digest.unwrap(), "SAID mismatch for modality: \n {:?}", modality);
                } else {
                    println!("No SAID found for modality: {:?}", modality);
                }
            }
            // Verify if the SAID of MMIO is valid
            let mut mmio_clone = mmio.clone();
            mmio_clone.compute_digest();
            assert_eq!(mmio.digest, mmio_clone.digest, "SAID mismatch for MMIO");
            println!("Parsed MMIO object is valid");
        }
        Commands::Said { file } => {
            let mut file_reader = File::open(&file).expect("Failed to open file");
            let mut buf = [0; 512];
            let n = file_reader.read(&mut buf).expect("Read error");
            let mime = infer::get(&buf[..n])
                .map(|kind| kind.mime_type())
                .unwrap_or("application/octet-stream");

            let code = HashFunctionCode::Blake3_256;
            let hash_algorithm = HashFunction::from(code.clone());

            let said = hash_algorithm.derive_from_stream(file_reader).unwrap();
            println!("MIME type: {}", mime);
            println!("SAID: {}", said);
        }
    }
}

