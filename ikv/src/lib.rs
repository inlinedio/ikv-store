mod controller;
mod index;
pub mod jni;
mod kafka;
mod proto;
mod schema;
mod utils;

pub fn hello_world() -> String {
    String::from("Hello World")
}
