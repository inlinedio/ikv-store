//! Generated files are imported from here.
//!
//! For the demonstration we generate descriptors twice, with
//! as pure rust codegen, and with codegen dependent on `protoc` binary.
pub mod generated_proto {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

pub mod ikvserviceschemas {
    tonic::include_proto!("ikvschemas");
}
