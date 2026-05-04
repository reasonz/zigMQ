const std = @import("std");
const atomic = std.atomic;
const fmt = std.fmt;
const mem = std.mem;
const testing = std.testing;

pub const Connection = struct {
    const read_buffer_capacity = 16 * 1024;
    const write_buffer_capacity = 64 * 1024;

    io: std.Io,
    stream: std.Io.net.Stream,
    read_buffer: [read_buffer_capacity]u8 = undefined,
    reader: std.Io.net.Stream.Reader,
    write_buffer: [write_buffer_capacity]u8 = undefined,
    writer: std.Io.net.Stream.Writer,
    batch_writes: bool = false,
    allocator: mem.Allocator,
    subscriptions: std.ArrayList([]const u8),
    write_mutex: std.Io.Mutex = .init,
    closed: bool = false,
    ref_count: atomic.Value(usize) = .init(1),

    pub fn init(self: *Connection, stream: std.Io.net.Stream, io: std.Io, allocator: mem.Allocator) void {
        self.io = io;
        self.stream = stream;
        self.allocator = allocator;
        self.subscriptions = std.ArrayList([]const u8).empty;
        self.write_mutex = .init;
        self.closed = false;
        self.ref_count = .init(1);
        self.batch_writes = false;
        self.reader = stream.reader(io, &self.read_buffer);
        self.writer = stream.writer(io, &self.write_buffer);
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
        const buffered = self.reader.interface.buffer[self.reader.interface.seek..self.reader.interface.end];
        if (mem.indexOfScalar(u8, buffered, '\n')) |idx| {
            const line = buffered[0..idx];
            self.reader.interface.seek += idx + 1;
            return line;
        }
        return null;
    }

    pub fn hasBufferedLine(self: *Connection) bool {
        const buffered = self.reader.interface.buffer[self.reader.interface.seek..self.reader.interface.end];
        return mem.indexOfScalar(u8, buffered, '\n') != null;
    }

    pub fn isResponseBatching(self: *const Connection) bool {
        return self.batch_writes or self.writer.interface.end > 0;
    }

    pub fn beginResponse(self: *Connection, has_more_input: bool) void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        self.batch_writes = has_more_input or self.writer.interface.end > 0;
    }

    pub fn endResponse(self: *Connection, has_more_input: bool) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (has_more_input) return;
        self.batch_writes = false;
        try self.writer.interface.flush();
    }

    pub fn readLine(self: *Connection) !?[]const u8 {
        while (true) {
            if (self.nextBufferedLine()) |line| return line;

            if (self.reader.interface.end == self.reader.interface.buffer.len) {
                if (self.reader.interface.seek == 0) return error.BufferFull;
                self.reader.interface.rebase(self.reader.interface.buffer.len - self.reader.interface.seek + 1) catch return error.BufferFull;
            }

            self.reader.interface.fillMore() catch |err| switch (err) {
                error.EndOfStream => {
                    if (self.reader.interface.end == self.reader.interface.seek) return null;
                    if (self.nextBufferedLine()) |line| return line;
                    return error.ConnectionClosed;
                },
                else => return error.ConnectionClosed,
            };
        }
    }

    pub fn flushBuffered(self: *Connection) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        try self.writer.interface.flush();
    }

    pub fn writeBuffered(self: *Connection, data: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return error.ConnectionClosed;
        if (!self.batch_writes) {
            try self.writer.interface.flush();
        }
        try self.writer.interface.writeAll(data);
    }

    pub fn write(self: *Connection, data: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return error.ConnectionClosed;
        try self.writer.interface.flush();
        try self.writer.interface.writeAll(data);
        try self.writer.interface.flush();
    }

    pub fn writeTopicMessage(self: *Connection, topic: []const u8, msg: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return error.ConnectionClosed;
        try self.writer.interface.flush();
        var parts = [_][]const u8{ "+", topic, ":", msg, "\r\n" };
        try self.writer.interface.writeVecAll(&parts);
        try self.writer.interface.flush();
    }

    pub fn writeBulkBuffered(self: *Connection, body: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return error.ConnectionClosed;

        if (!self.batch_writes) {
            try self.writer.interface.flush();
            var header_buf: [32]u8 = undefined;
            const header = try fmt.bufPrint(&header_buf, "${d}\r\n", .{body.len});
            var parts = [_][]const u8{ header, body, "\r\n" };
            try self.writer.interface.writeVecAll(&parts);
            return;
        }

        var header_buf: [32]u8 = undefined;
        const header = try fmt.bufPrint(&header_buf, "${d}\r\n", .{body.len});
        try self.writer.interface.writeAll(header);
        try self.writer.interface.writeAll(body);
        try self.writer.interface.writeAll("\r\n");
    }

    pub fn writeBulk(self: *Connection, body: []const u8) !void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return error.ConnectionClosed;
        try self.writer.interface.flush();

        var header_buf: [32]u8 = undefined;
        const header = try fmt.bufPrint(&header_buf, "${d}\r\n", .{body.len});
        var parts = [_][]const u8{ header, body, "\r\n" };
        try self.writer.interface.writeVecAll(&parts);
    }

    pub fn close(self: *Connection) void {
        self.write_mutex.lockUncancelable(self.io);
        defer self.write_mutex.unlock(self.io);
        if (self.closed) return;
        self.closed = true;
        self.stream.close(self.io);
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
    var conn: Connection = undefined;
    conn.init(undefined, testing.io, testing.allocator);
    defer conn.deinit();

    const input = "PING\r\nINFO\r\n";
    @memcpy(conn.reader.interface.buffer[0..input.len], input);
    conn.reader.interface.end = input.len;

    const first = conn.nextBufferedLine().?;
    try testing.expectEqualStrings("PING\r", first);

    const second = conn.nextBufferedLine().?;
    try testing.expectEqualStrings("INFO\r", second);

    try testing.expect(conn.nextBufferedLine() == null);
}

test "closed connection rejects writes" {
    var conn: Connection = undefined;
    conn.init(undefined, testing.io, testing.allocator);
    defer conn.deinit();
    conn.closed = true;

    try testing.expectError(error.ConnectionClosed, conn.write("PING\r\n"));
}
