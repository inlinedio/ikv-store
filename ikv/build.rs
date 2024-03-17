fn main() -> Result<(), Box<dyn std::error::Error>> {
    // std::env::set_var("PROTOC", protobuf_src::protoc);
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());

    protobuf_codegen::Codegen::new()
        // Use `protoc` parser, optional.
        .protoc()
        // Use `protoc-bin-vendored` bundled protoc command, optional.
        .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
        // All inputs and imports from the inputs must reside in `includes` directories.
        .includes(&["../ikv-cloud/src/main/proto"])
        .inputs(&[
            "../ikv-cloud/src/main/proto/common.proto",
            "../ikv-cloud/src/main/proto/streaming.proto",
            "../ikv-cloud/src/main/proto/index.proto",
        ])
        // Specify output directory relative to Cargo output directory.
        .cargo_out_dir("protos")
        .run_from_script();

    //tonic_build::compile_protos("src/proto/common.proto")?;
    //tonic_build::compile_protos("src/proto/services.proto")?;
    Ok(())
}
