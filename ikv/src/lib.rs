use proto::generated_proto::services::MultiFieldDocument;

mod controller;
mod index;
pub mod jni;
mod kafka;
mod proto;
mod schema;

pub fn hello_world() -> String {
    let _doc: MultiFieldDocument; // example of using proto structs
    String::from("Hello World")
}
