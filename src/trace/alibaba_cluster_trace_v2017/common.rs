/// Base for denormalization of normalized ram for nodes and pods
/// 128 GB equals to 1.0 of normalized value.
pub const DENORMALIZATION_BASE: i64 = 128 * 1024 * 1024 * 1024;

/// Base for converting number of cores of nodes and pods to number of millicores.
pub const CPU_BASE: i64 = 1000;
