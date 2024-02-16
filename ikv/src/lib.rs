mod controller;
pub mod ffi;
mod index;
mod kafka;
mod proto;
mod schema;
mod utils;

pub fn hello_world() -> String {
    String::from("Hello World")
}
