extern crate core;
use core::alloc::{GlobalAlloc, Layout};

use libc::{c_int, c_void};
use tikv_jemalloc_sys as ffi;
use tracking_allocator::{AllocationGroupId, AllocationTracker};

#[cfg(all(any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mipsel",
    target_arch = "powerpc"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 8;
#[cfg(all(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "mips64",
    target_arch = "riscv64",
    target_arch = "s390x",
    target_arch = "sparc64"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 16;

fn layout_to_flags(align: usize, size: usize) -> c_int {
    if align <= ALIGNOF_MAX_ALIGN_T && align <= size {
        0
    } else {
        ffi::MALLOCX_ALIGN(align)
    }
}

// Assumes a condition that always must hold.
macro_rules! assume {
    ($e:expr) => {
        debug_assert!($e);
        if !($e) {
            core::hint::unreachable_unchecked();
        }
    };
}

fn wrapped_layout(layout: Layout) -> Layout {
    static HEADER_FLAG_LAYOUT: Layout = Layout::new::<usize>();
    static HEADER_LEN_LAYOUT: Layout = Layout::new::<usize>();

    let (actual_layout, _offset_flag_object) = HEADER_FLAG_LAYOUT
        .extend(HEADER_LEN_LAYOUT)
        .expect("wrapping layout rextend length  overflow");

    let (actual_layout, _offset_len_object) = actual_layout
        .extend(layout)
        .expect("wrapping layout extend  object");
    let actual_layout = actual_layout.pad_to_align();

    actual_layout
}

unsafe fn alloc_hook(ptr: *mut u8, layout: &Layout) -> *mut u8 {
    #[allow(clippy::cast_ptr_alignment)]
    let flag_ptr = ptr.cast::<usize>();
    flag_ptr.write(0x1234567890123456_usize);
    let ptr = ptr.wrapping_add(8);

    #[allow(clippy::cast_ptr_alignment)]
    let len_ptr = ptr.cast::<usize>();
    len_ptr.write(layout.size());
    let ptr = ptr.wrapping_add(8);

    ptr as *mut u8
}

unsafe fn free_hook(ptr: *mut u8, layout: &Layout) -> *mut u8 {
    let ptr = ptr.wrapping_sub(8);
    #[allow(clippy::cast_ptr_alignment)]
    let len = ptr.cast::<usize>().read();

    let ptr = ptr.wrapping_sub(8);
    #[allow(clippy::cast_ptr_alignment)]
    let flag = ptr.cast::<usize>().read();

    ptr.cast::<usize>().write(0x1234567890654321_usize);

    //if flag != 0x1234567890123456_usize || len != layout.size() {
    if len >= 1842415900 {
        panic!(
            "-------------- free is not right ({} {}) {:#?} ({} {} {})",
            flag,
            len,
            ptr,
            0x1234567890123456_usize,
            0x1234567890654321_usize,
            layout.size()
        );
    }

    ptr as *mut u8
}

#[derive(Copy, Clone, Default, Debug)]
pub struct DebugMemoryAlloc;

unsafe impl GlobalAlloc for DebugMemoryAlloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let layout = wrapped_layout(layout);

        assume!(layout.size() != 0);
        let flags = layout_to_flags(layout.align(), layout.size());
        let ptr = if flags == 0 {
            ffi::malloc(layout.size())
        } else {
            ffi::mallocx(layout.size(), flags)
        };

        alloc_hook(ptr as *mut u8, &layout)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let layout = wrapped_layout(layout);

        assume!(layout.size() != 0);
        let flags = layout_to_flags(layout.align(), layout.size());
        let ptr = if flags == 0 {
            ffi::calloc(1, layout.size())
        } else {
            ffi::mallocx(layout.size(), flags | ffi::MALLOCX_ZERO)
        };

        alloc_hook(ptr as *mut u8, &layout)
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // assume!(layout.size() != 0);
        // assume!(new_size != 0);
        // let flags = layout_to_flags(layout.align(), new_size);
        // let ptr = if flags == 0 {
        //     ffi::realloc(ptr as *mut c_void, new_size)
        // } else {
        //     ffi::rallocx(ptr as *mut c_void, new_size, flags)
        // };
        // ptr as *mut u8

        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        let new_ptr = unsafe { self.alloc(new_layout) };

        unsafe {
            let size = std::cmp::min(layout.size(), new_size);
            std::ptr::copy_nonoverlapping(ptr, new_ptr, size);
            self.dealloc(ptr, layout);
        }

        new_ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let layout = wrapped_layout(layout);

        assume!(!ptr.is_null());
        assume!(layout.size() != 0);

        let ptr = free_hook(ptr, &layout);

        let flags = layout_to_flags(layout.align(), layout.size());
        ffi::sdallocx(ptr as *mut c_void, layout.size(), flags);
    }
}

pub(crate) struct StdoutTracker;

// This is our tracker implementation.  You will always need to create an implementation of `AllocationTracker` in order
// to actually handle allocation events.  The interface is straightforward: you're notified when an allocation occurs,
// and when a deallocation occurs.
impl AllocationTracker for StdoutTracker {
    fn allocated(
        &self,
        addr: usize,
        object_size: usize,
        wrapped_size: usize,
        group_id: AllocationGroupId,
    ) {
        // Allocations have all the pertinent information upfront, which you may or may not want to store for further
        // analysis. Notably, deallocations also know how large they are, and what group ID they came from, so you
        // typically don't have to store much data for correlating deallocations with their original allocation.
        println!(
            "alloc -> addr=0x{:0x} object_size={} wrapped_size={} group_id={:?}",
            addr, object_size, wrapped_size, group_id
        );
    }

    fn deallocated(
        &self,
        addr: usize,
        object_size: usize,
        wrapped_size: usize,
        source_group_id: AllocationGroupId,
        current_group_id: AllocationGroupId,
    ) {
        // When a deallocation occurs, as mentioned above, you have full access to the address, size of the allocation,
        // as well as the group ID the allocation was made under _and_ the active allocation group ID.
        //
        // This can be useful beyond just the obvious "track how many current bytes are allocated by the group", instead
        // going further to see the chain of where allocations end up, and so on.
        println!(
            "free  -> addr=0x{:0x} object_size={} wrapped_size={} source_group_id={:?} current_group_id={:?}",
            addr, object_size, wrapped_size, source_group_id, current_group_id
        );
    }
}
