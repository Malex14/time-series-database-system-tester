use std::hint::black_box;

pub mod metrics;

/// This function is used to prevent the compiler from optimizing away the read of a value.
#[inline(always)]
pub fn do_not_optimize_read<T>(val: &T) {
	black_box(do_not_optimize_read_inner(val));
}

#[inline(always)]
fn do_not_optimize_read_inner<T>(val: &T) -> &T {
	val
}
