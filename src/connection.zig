const std = @import("std");
const atomic = std.atomic;
const fmt = std.fmt;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const testing = std.testing;

pub const Connection = struct {
    stream: net.Stream,
    buffer: [4096]u8 = undefined,
    buffer_start: usize = 0,
    buffer_end: usize = 0,
    allocator: mem.Allocator,
    subscriptions: std.ArrayList([]const u8),
    write_mutex: std.Thread.Mutex = .{},
    closed: bool = false,
    ref_count: atomic.Value(usize) = atomic.Value(usize).init(1),

    pub fn init(stream: net.Stream, allocator: mem.Allocator) Connection {
        return .{
            .stream = stream,
            .allocator = allocator,
            .subscriptions = std.ArrayList([]const u8).empty,
        };
    }

    pub fn deinit(self: *Connection) void {
        self.clearSubscriptions();
        self.subscriptions.deinit(self.allocator);
    }

    pub fn addSubscription(self: *Connection, topic: []const u8) !void {
        for (self.subscriptions.items) |existing| {
            if (mem.eql(u8, existing, topic)) return;
        }

        const copy = try self.allocator.dupe(u8, topic);
        try self.subscriptions.append(self.allocator, copy);
    }

    pub fn removeSubscription(self: *Connection, topic: []const u8) void {
        for (self.subscriptions.items, 0..) |existing, i| {
            if (mem.eql(u8, existing, topic)) {
                self.allocator.free(existing);
                _ = self.subscriptions.swapRemove(i);
                return;
            }
        }
    }

    pub fn clearSubscriptions(self: *Connection) void {
        for (self.subscriptions.items) |topic| {
            self.allocator.free(topic);
        }
        self.subscriptions.clearRetainingCapacity();
    }

    pub fn nextBufferedLine(self: *Connection) ?[]const u8 {
        const unread = self.buffer[self.buffer_start..self.buffer_end];
        if (mem.indexOfScalar(u8, unread, '\n')) |idx| {
            const line = unread[0..idx];
            self.buffer_start += idx + 1;
            if (self.buffer_start == self.buffer_end) {
                self.buffer_start = 0;
                self.buffer_end = 0;
            }
            return line;
        }
        return null;
    }

    pub fn compactUnread(self: *Connection) void {
        if (self.buffer_start == 0) return;

        const unread_len = self.buffer_end - self.buffer_start;
        if (unread_len > 0) {
            mem.copyForwards(u8, self.buffer[0..unread_len], self.buffer[self.buffer_start..self.buffer_end]);
        }
        self.buffer_start = 0;
        self.buffer_end = unread_len;
    }

    pub fn readLine(self: *Connection) !?[]const u8 {
        while (true) {
            if (self.nextBufferedLine()) |line| return line;

            if (self.buffer_end == self.buffer.len) {
                if (self.buffer_start == 0) return error.BufferFull;
                self.compactUnread();
                continue;
            }

            const n = try self.stream.read(self.buffer[self.buffer_end..]);
            if (n == 0) {
                if (self.buffer_end == self.buffer_start) return null;
                return error.ConnectionClosed;
            }
            self.buffer_end += n;
        }
    }

    pub fn write(self: *Connection, data: []const u8) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        if (self.closed) return error.ConnectionClosed;
        try self.stream.writeAll(data);
    }

    pub fn writeTopicMessage(self: *Connection, topic: []const u8, msg: []const u8) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        var iovecs = [_]posix.iovec_const{
            .{ .base = "+".ptr, .len = 1 },
            .{ .base = topic.ptr, .len = topic.len },
            .{ .base = ":".ptr, .len = 1 },
            .{ .base = msg.ptr, .len = msg.len },
            .{ .base = "\r\n".ptr, .len = 2 },
        };
        try self.stream.writevAll(&iovecs);
    }

    pub fn writeBulk(self: *Connection, body: []const u8) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        if (self.closed) return error.ConnectionClosed;

        var header_buf: [32]u8 = undefined;
        const header = try fmt.bufPrint(&header_buf, "${d}\r\n", .{body.len});
        var iovecs = [_]posix.iovec_const{
            .{ .base = header.ptr, .len = header.len },
            .{ .base = body.ptr, .len = body.len },
            .{ .base = "\r\n".ptr, .len = 2 },
        };
        try self.stream.writevAll(&iovecs);
    }

    pub fn close(self: *Connection) void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();

        if (self.closed) return;
        self.closed = true;
        self.stream.close();
    }

    pub fn retain(self: *Connection) void {
        _ = self.ref_count.fetchAdd(1, .monotonic);
    }

    pub fn release(self: *Connection, allocator: mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .acq_rel) == 1) {
            self.deinit();
            allocator.destroy(self);
        }
    }
};

test "connection buffered lines preserve pipelined commands" {
    var conn = Connection.init(undefined, testing.allocator);
    defer conn.deinit();

    const input = "PING\r\nINFO\r\n";
    mem.copyForwards(u8, conn.buffer[0..input.len], input);
    conn.buffer_end = input.len;

    const first = conn.nextBufferedLine().?;
    try testing.expectEqualStrings("PING\r", first);

    const second = conn.nextBufferedLine().?;
    try testing.expectEqualStrings("INFO\r", second);

    try testing.expect(conn.nextBufferedLine() == null);
}

test "connection compaction keeps unread data" {
    var conn = Connection.init(undefined, testing.allocator);
    defer conn.deinit();

    mem.copyForwards(u8, conn.buffer[5..10], "HELLO");
    conn.buffer_start = 5;
    conn.buffer_end = 10;
    conn.compactUnread();

    try testing.expectEqual(@as(usize, 0), conn.buffer_start);
    try testing.expectEqual(@as(usize, 5), conn.buffer_end);
    try testing.expectEqualStrings("HELLO", conn.buffer[0..5]);
}

test "closed connection rejects writes" {
    var conn = Connection.init(undefined, testing.allocator);
    defer conn.deinit();
    conn.closed = true;

    try testing.expectError(error.ConnectionClosed, conn.write("PING\r\n"));
}
