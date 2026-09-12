// Build script to generate gRPC client code from protos.
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("service_descriptor.bin");
    let mut includes = vec!["proto"];
    for candidate in ["/usr/include", "/usr/local/include"] {
        if std::path::Path::new(candidate).exists() {
            includes.push(candidate);
        }
    }

    // The canonical `.proto` is mirrored byte for byte from the server
    // repository, comments included, and those comments are written in
    // French. Left enabled, they become rustdoc on the generated types — and
    // `UploadRequest` is re-exported at the crate root, so that text would
    // ship on docs.rs. The SDK's own hand-written English documentation is
    // the single source of truth for callers; the `.proto` stays the source
    // of truth for the wire.
    //
    // Comments are disabled in two places because two generators emit them,
    // and the two spell "everything" differently. Messages, fields and enums
    // come from prost, which matches proto paths and takes `.` as the
    // catch-all — but `tonic_prost_build` deliberately does not forward its
    // own `disable_comments` to prost, so that one has to travel on an
    // explicit `Config` (every other builder setting is applied onto this
    // `Config` for us). The service clients come from tonic, which compares
    // fully qualified service names for equality and so has to be given each
    // one by name: a new service in the `.proto` needs a line here.
    let mut config = tonic_prost_build::Config::new();
    config.disable_comments(["."]);

    tonic_prost_build::configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .disable_comments([
            "rocia.v1.DocumentService",
            "rocia.v1.GraphService",
            "rocia.v1.FileService",
            "rocia.v1.TenantService",
        ])
        .compile_well_known_types(true)
        .build_server(false)
        .extern_path(".google.protobuf.Empty", "::pbjson_types::Empty")
        .extern_path(".google.protobuf.Timestamp", "::pbjson_types::Timestamp")
        .extern_path(".google.protobuf.Struct", "::pbjson_types::Struct")
        .file_descriptor_set_path(&descriptor_path)
        .compile_with_config(config, &["proto/upstream/v1/upstream.proto"], &includes)?;

    println!("cargo:rerun-if-changed=proto/upstream/v1/upstream.proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}
