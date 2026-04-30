const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const testing = std.testing;

pub const Command = struct {
    kind: Operation,
    op: []const u8,
    queue: ?[]const u8,
    body: ?[]const u8,

    pub fn parse(line: []const u8) !Command {
        var parts = mem.splitScalar(u8, line, ' ');
        const op = parts.next() orelse return error.InvalidCommand;
        const queue = parts.next();
        const body = parts.rest();

        return .{
            .kind = parseOp(op),
            .op = op,
            .queue = queue,
            .body = if (body.len > 0) body else null,
        };
    }
};

pub const Operation = enum {
    ping,
    info,
    push,
    send,
    pop,
    recv,
    peek,
    len,
    queues,
    qcreate,
    mq,
    sub,
    unsub,
    @"pub",
    topics,
    subs,
    unknown,
};

pub fn parseOp(actual: []const u8) Operation {
    return switch (actual.len) {
        2 => if (isOp(actual, "mq")) .mq else .unknown,
        3 => {
            if (isOp(actual, "len")) return .len;
            if (isOp(actual, "pop")) return .pop;
            if (isOp(actual, "pub")) return .@"pub";
            if (isOp(actual, "sub")) return .sub;
            return .unknown;
        },
        4 => {
            if (isOp(actual, "ping")) return .ping;
            if (isOp(actual, "info")) return .info;
            if (isOp(actual, "push")) return .push;
            if (isOp(actual, "send")) return .send;
            if (isOp(actual, "recv")) return .recv;
            if (isOp(actual, "peek")) return .peek;
            if (isOp(actual, "subs")) return .subs;
            return .unknown;
        },
        5 => {
            if (isOp(actual, "unsub")) return .unsub;
            return .unknown;
        },
        6 => {
            if (isOp(actual, "queues")) return .queues;
            if (isOp(actual, "topics")) return .topics;
            if (isOp(actual, "qcreate")) return .qcreate;
            return .unknown;
        },
        7 => if (isOp(actual, "qcreate")) .qcreate else .unknown,
        else => .unknown,
    };
}

pub fn isOp(actual: []const u8, expected: []const u8) bool {
    return ascii.eqlIgnoreCase(actual, expected);
}

test "command parsing preserves message body" {
    const cmd = try Command.parse("send q1 hello world");
    try testing.expectEqual(Operation.send, cmd.kind);
    try testing.expectEqualStrings("send", cmd.op);
    try testing.expectEqualStrings("q1", cmd.queue.?);
    try testing.expectEqualStrings("hello world", cmd.body.?);
}

test "command matching is case insensitive" {
    try testing.expect(isOp("PING", "ping"));
    try testing.expect(isOp("Sub", "sub"));
    try testing.expect(!isOp("publish", "pub"));
}

test "operation parsing is case insensitive" {
    try testing.expectEqual(Operation.ping, parseOp("PING"));
    try testing.expectEqual(Operation.recv, parseOp("Recv"));
    try testing.expectEqual(Operation.qcreate, parseOp("qcreate"));
    try testing.expectEqual(Operation.unknown, parseOp("publish"));
}
