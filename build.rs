use protobuf_codegen_pure::Codegen;
use std::fs::File;
use std::path::Path;

const ORC_PROTO_PATH: &str = "proto/orc_proto.proto";
const ORC_PROTO_COMMIT: &str = "cf720d7b2333618c652445299486cb9600bdbe4f";

fn main() {
    let out_dir = std::env::var_os("OUT_DIR").unwrap().into_string().unwrap();
    let full_orc_proto_path = Path::new(&out_dir).join(ORC_PROTO_PATH);

    if !full_orc_proto_path.exists() {
        if let Some(parent) = full_orc_proto_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).expect("Could not create proto build directory");
            }
        }

        download_orc_proto(&full_orc_proto_path)
    }

    println!("cargo:rerun-if-changed={}/{}", out_dir, ORC_PROTO_PATH);

    Codegen::new()
        .out_dir("src/proto")
        .input(ORC_PROTO_PATH)
        .include("proto")
        .run()
        .expect("Protobuf codegen failed");
}

fn download_orc_proto(orc_proto_path: &Path) {
    let source_url = format!(
        "https://raw.githubusercontent.com/apache/orc/{}/proto/orc_proto.proto",
        ORC_PROTO_COMMIT
    );

    let mut response =
        reqwest::blocking::get(source_url).expect("Could not download ORC proto file");
    let mut output = File::create(orc_proto_path).expect("Could not create ORC proto file");
    std::io::copy(&mut response, &mut output).expect("Could not write ORC proto file");
}
