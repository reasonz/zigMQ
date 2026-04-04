const std = @import("std");

const config = @import("config.zig");
const protocol = @import("protocol.zig");
const queue = @import("queue.zig");
const connection = @import("connection.zig");
const pubsub = @import("pubsub.zig");
const server_mod = @import("server.zig");
const version = @import("version.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const app_config = config.parseArgs();

    var server = try server_mod.Server.init(allocator, app_config);
    defer server.deinit();

    try server.run();
}

test {
    _ = config;
    _ = protocol;
    _ = queue;
    _ = connection;
    _ = pubsub;
    _ = server_mod;
    _ = version;
}
