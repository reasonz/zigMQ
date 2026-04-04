const std = @import("std");
const ascii = std.ascii;
const mem = std.mem;
const testing = std.testing;

pub const Command = struct {
    op: []const u8,
    queue: ?[]const u8,
    body: ?[]const u8,

    pub fn parse(line: []const u8) !Command {
        var parts = mem.splitScalar(u8, line, ' ');
        const op = parts.next() orelse return error.InvalidCommand;
        const queue = parts.next();
        const body = parts.rest();

        return .{
            .op = op,
            .queue = queue,
            .body = if (body.len > 0) body else null,
        };
    }
};

pub fn isOp(actual: []const u8, expected: []const u8) bool {
    return ascii.eqlIgnoreCase(actual, expected);
}

test "command parsing preserves message body" {
    const cmd = try Command.parse("send q1 hello world");
    try testing.expectEqualStrings("send", cmd.op);
    try testing.expectEqualStrings("q1", cmd.queue.?);
    try testing.expectEqualStrings("hello world", cmd.body.?);
}

test "command matching is case insensitive" {
    try testing.expect(isOp("PING", "ping"));
    try testing.expect(isOp("Sub", "sub"));
    try testing.expect(!isOp("publish", "pub"));
}
