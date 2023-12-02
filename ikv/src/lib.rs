use proto::generated_proto::common::IKVDocumentOnWire;

mod controller;
mod index;
pub mod jni;
mod kafka;
mod proto;
mod schema;

pub fn hello_world() -> String {
    let _doc: IKVDocumentOnWire; // example of using proto structs
    String::from("Hello World")
}
