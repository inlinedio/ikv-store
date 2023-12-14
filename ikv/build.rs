fn main() -> Result<(), Box<dyn std::error::Error>> {
    protobuf_codegen::Codegen::new()
        // Use `protoc` parser, optional.
        .protoc()
        // Use `protoc-bin-vendored` bundled protoc command, optional.
        .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
        // All inputs and imports from the inputs must reside in `includes` directories.
        .includes(&["src/proto", "src/proto/internal"])
        .inputs(&[
            "src/proto/common.proto",
            "src/proto/streaming.proto",
            "src/proto/internal/index.proto",
        ])
        // Specify output directory relative to Cargo output directory.
        .cargo_out_dir("protos")
        .run_from_script();

    tonic_build::compile_protos("src/proto/common.proto")?;
    tonic_build::compile_protos("src/proto/services.proto")?;
    Ok(())
}
