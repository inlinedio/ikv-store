use super::main::Controller;

pub fn to_external_handle(controller: Controller) -> i64 {
    let boxed_controller = Box::new(controller);
    let handle: *mut Controller = Box::into_raw(boxed_controller);
    handle as i64
}

pub fn from_external_handle(handle: i64) -> &'static mut Controller {
    unsafe { &mut *(handle as *mut Controller) }
}

pub fn close_external_handle(handle: i64) {
    let boxed_controller_ptr = handle as *mut Controller;
    let _boxed_controller = unsafe { Box::from_raw(boxed_controller_ptr) };

    // drop on _boxed_controller will cleanup the controller from heap
}
