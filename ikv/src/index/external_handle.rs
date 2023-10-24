use super::ckv::CKVIndex;

pub fn to_external_handle(index: CKVIndex) -> i64 {
    let boxed_index = Box::new(index);
    let index_handle: *mut CKVIndex = Box::into_raw(boxed_index);
    index_handle as i64
}

pub fn from_external_handle(handle: i64) -> &'static mut CKVIndex {
    unsafe { &mut *(handle as *mut CKVIndex) }
}

pub fn close_external_handle(handle: i64) {
    let boxed_index_ptr = handle as *mut CKVIndex;
    let _boxed_index = unsafe { Box::from_raw(boxed_index_ptr) };

    // drop on _boxed_index will cleanup the index from heap
}
