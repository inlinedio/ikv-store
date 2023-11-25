use super::main::Controller;

pub fn to_external_handle(controller: Controller) -> i64 {
    let boxed_controller = Box::new(controller);
    let handle: *mut Controller = Box::into_raw(boxed_controller);
    handle as i64
}

pub fn from_external_handle(handle: i64) -> &'static mut Controller {
    unsafe { &mut *(handle as *mut Controller) }
}

pub fn to_box(handle: i64) -> Box<Controller> {
    let boxed_controller_ptr = handle as *mut Controller;
    unsafe { Box::from_raw(boxed_controller_ptr) }
}
