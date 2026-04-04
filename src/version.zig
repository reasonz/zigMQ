const build_options = @import("build_options");

pub const current = build_options.version;
pub const banner = "ZigMQ " ++ current;
pub const release_tag = "v" ++ current;
