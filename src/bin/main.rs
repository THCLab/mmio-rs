use clap::{Parser, Subcommand};
use m2io_tmp::{Modality, ModalityType, Semantic, MMIO};
use said::derivation::{HashFunction, HashFunctionCode};
use said::sad::SerializationFormats;
use said::SelfAddressingIdentifier;
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;

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
        #[arg(long = "modalities",
            value_parser = parse_modality,
            num_args = 0..,
            help = "Specify a modality using: file=path,bundle_said=<SAID>. Repeat for multiple modalities."
        )]
        modalities: Vec<Modality>,

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

fn modality_type_from_mime(mime: &str) -> ModalityType {
    if mime.starts_with("image/") {
        ModalityType::Image
    } else if mime.starts_with("text/") || mime == "application/json" {
        ModalityType::Text
    } else if mime.starts_with("audio/") {
        ModalityType::Audio
    } else if mime.starts_with("video/") {
        ModalityType::Video
    } else if mime == "application/octet-stream" || mime.starts_with("application/") {
        ModalityType::Binary
    } else {
        // Fallback
        ModalityType::Text
    }
}


fn parse_modality(s: &str) -> Result<Modality, String> {
    let mut file = None;
    let mut bundle_said = None;

    for part in s.split(',') {
        let mut kv = part.splitn(2, '=');
        match (kv.next(), kv.next()) {
            (Some("file"), Some(v)) => file = Some(v.to_string()),
            (Some("bundle_said"), Some(v)) => {
                let said: SelfAddressingIdentifier = v.parse().unwrap();
                bundle_said = Some(said);

            },
            _ => return Err(format!("Invalid modality format: {}", part)),
        }
    }

    match (file, bundle_said) {
        (Some(f), Some(b)) => {
            // Infer MIME type from file content
            let mut buf = [0; 512];
            let mut file_reader = File::open(&f).map_err(|e| format!("Cannot open file: {}", e))?;
            let n = file_reader.read(&mut buf).map_err(|e| format!("Read error: {}", e))?;
            let mime = infer::get(&buf[..n])
                .map(|kind| kind.mime_type())
                .unwrap_or("application/octet-stream");

            let code = HashFunctionCode::Blake3_256;
            let hash_algorithm = HashFunction::from(code.clone());

            let said = hash_algorithm.derive_from_stream(file_reader).unwrap();


            let modality_type = modality_type_from_mime(mime);
            let mut modality = Modality { digest: None, modality_said: Some(said), modality_type,
                media_type: mime.to_string(), oca_bundle: Semantic::Reference(b) };

            modality.compute_digest();
            Ok(modality)
        },
        _ => Err("Both file and semantic must be provided.".to_string()),
    }
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Create { modalities, output } => {
            let mut mmio = MMIO {
                version: "0.1".to_string(),
                digest: None,
                modalities,
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

