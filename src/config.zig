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

pub fn parseArgs() Config {
    var args = std.process.args();
    _ = args.next();

    var config = Config{};

    while (args.next()) |arg| {
        if (mem.eql(u8, arg, "--port")) {
            if (args.next()) |value| {
                config.port = fmt.parseInt(u16, value, 10) catch 8388;
            }
        } else if (mem.eql(u8, arg, "--workers")) {
            if (args.next()) |value| {
                config.workers = fmt.parseInt(u32, value, 10) catch 1;
            }
        } else if (mem.eql(u8, arg, "--capacity")) {
            if (args.next()) |value| {
                config.queue_capacity = fmt.parseInt(usize, value, 10) catch 10000;
                if (config.queue_capacity == 0) config.queue_capacity = 10000;
            }
        } else if (mem.eql(u8, arg, "--max-capacity")) {
            if (args.next()) |value| {
                config.max_queue_capacity = fmt.parseInt(usize, value, 10) catch 100000;
            }
        } else if (mem.eql(u8, arg, "--mode")) {
            if (args.next()) |value| {
                if (mem.eql(u8, value, "master")) {
                    config.mode = .master;
                } else if (mem.eql(u8, value, "worker")) {
                    config.mode = .worker;
                } else {
                    config.mode = .standalone;
                }
            }
        } else if (mem.eql(u8, arg, "--data-dir")) {
            config.data_dir = args.next();
        }
    }

    if (config.max_queue_capacity < config.queue_capacity) {
        config.max_queue_capacity = config.queue_capacity;
    }

    return config;
}
