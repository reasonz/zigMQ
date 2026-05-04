const std = @import("std");

const config = @import("config.zig");
const protocol = @import("protocol.zig");
const queue = @import("queue.zig");
const connection = @import("connection.zig");
const pubsub = @import("pubsub.zig");
const server_mod = @import("server.zig");
const version = @import("version.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const app_config = config.parseArgs(args);

    var server = try server_mod.Server.init(allocator, io, app_config);
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
