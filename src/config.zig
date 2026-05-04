const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;

pub const Config = struct {
    port: u16 = 8388,
    mode: enum { standalone, master, worker } = .standalone,
    workers: u32 = 1,
    queue_capacity: usize = 10000,
    max_queue_capacity: usize = 100000,
    data_dir: ?[]const u8 = null,
};

pub fn parseArgs(args: []const [:0]const u8) Config {
    var config = Config{};

    var i: usize = 1; // skip program name
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (mem.eql(u8, arg, "--port")) {
            i += 1;
            if (i < args.len) {
                config.port = fmt.parseInt(u16, args[i], 10) catch 8388;
            }
        } else if (mem.eql(u8, arg, "--workers")) {
            i += 1;
            if (i < args.len) {
                config.workers = fmt.parseInt(u32, args[i], 10) catch 1;
            }
        } else if (mem.eql(u8, arg, "--capacity")) {
            i += 1;
            if (i < args.len) {
                config.queue_capacity = fmt.parseInt(usize, args[i], 10) catch 10000;
                if (config.queue_capacity < 2) config.queue_capacity = 10000;
            }
        } else if (mem.eql(u8, arg, "--max-capacity")) {
            i += 1;
            if (i < args.len) {
                config.max_queue_capacity = fmt.parseInt(usize, args[i], 10) catch 100000;
            }
        } else if (mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i < args.len) {
                if (mem.eql(u8, args[i], "master")) {
                    config.mode = .master;
                } else if (mem.eql(u8, args[i], "worker")) {
                    config.mode = .worker;
                } else {
                    config.mode = .standalone;
                }
            }
        } else if (mem.eql(u8, arg, "--data-dir")) {
            i += 1;
            if (i < args.len) {
                config.data_dir = args[i];
            }
        }
    }

    if (config.max_queue_capacity < config.queue_capacity) {
        config.max_queue_capacity = config.queue_capacity;
    }

    return config;
}
