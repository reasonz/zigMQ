const std = @import("std");
const mem = std.mem;
const testing = std.testing;
const time = std.time;

pub const OverflowPolicy = enum {
    block,
    reject,
    drop_oldest,
    drop_new,
};

pub const Message = struct {
    id: u64,
    body: []u8,
    timestamp: i64,

    pub fn init(allocator: mem.Allocator, id: u64, body: []const u8) !Message {
        const body_copy = try allocator.dupe(u8, body);
        return .{
            .id = id,
            .body = body_copy,
            .timestamp = time.timestamp(),
        };
    }

    pub fn deinit(self: Message, allocator: mem.Allocator) void {
        allocator.free(self.body);
    }
};

pub const RingBuffer = struct {
    messages: []Message,
    capacity: usize,
    head: usize = 0,
    tail: usize = 0,
    len: usize = 0,
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator, capacity: usize) !RingBuffer {
        if (capacity == 0) return error.InvalidCapacity;

        const messages = try allocator.alloc(Message, capacity);
        for (messages) |*msg| {
            msg.* = undefined;
        }

        return .{
            .messages = messages,
            .capacity = capacity,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RingBuffer) void {
        var i: usize = 0;
        while (i < self.len) : (i += 1) {
            const idx = (self.head + i) % self.capacity;
            self.messages[idx].deinit(self.allocator);
        }
        self.allocator.free(self.messages);
    }

    pub fn push(self: *RingBuffer, msg: Message) !void {
        if (self.len == self.capacity) return error.QueueFull;
        self.pushAssumeCapacity(msg);
    }

    pub fn pushAssumeCapacity(self: *RingBuffer, msg: Message) void {
        self.messages[self.tail] = msg;
        self.tail = (self.tail + 1) % self.capacity;
        self.len += 1;
    }

    pub fn grow(self: *RingBuffer, new_capacity: usize) !void {
        if (new_capacity <= self.capacity) return error.InvalidCapacity;

        const new_messages = try self.allocator.alloc(Message, new_capacity);
        errdefer self.allocator.free(new_messages);

        for (new_messages[self.len..]) |*msg| {
            msg.* = undefined;
        }

        var i: usize = 0;
        while (i < self.len) : (i += 1) {
            const old_idx = (self.head + i) % self.capacity;
            new_messages[i] = self.messages[old_idx];
        }

        self.allocator.free(self.messages);
        self.messages = new_messages;
        self.capacity = new_capacity;
        self.head = 0;
        self.tail = self.len;
    }

    pub fn pop(self: *RingBuffer) ?Message {
        if (self.len == 0) return null;

        const msg = self.messages[self.head];
        self.head = (self.head + 1) % self.capacity;
        self.len -= 1;
        return msg;
    }

    pub fn peek(self: *RingBuffer) ?*Message {
        if (self.len == 0) return null;
        return &self.messages[self.head];
    }
};

pub const Queue = struct {
    buffer: RingBuffer,
    overflow: OverflowPolicy,
    name: []const u8,
    max_capacity: usize,

    pub fn init(allocator: mem.Allocator, name: []const u8, capacity: usize, max_capacity: usize) !Queue {
        if (max_capacity < capacity) return error.InvalidCapacity;

        return .{
            .buffer = try RingBuffer.init(allocator, capacity),
            .overflow = .reject,
            .name = try allocator.dupe(u8, name),
            .max_capacity = max_capacity,
        };
    }

    pub fn deinit(self: *Queue) void {
        self.buffer.deinit();
        self.buffer.allocator.free(self.name);
    }

    pub fn push(self: *Queue, allocator: mem.Allocator, msg: Message) !void {
        if (self.buffer.len == self.buffer.capacity and self.buffer.capacity < self.max_capacity) {
            try self.grow();
        }

        switch (self.overflow) {
            .reject, .block, .drop_new => try self.buffer.push(msg),
            .drop_oldest => {
                if (self.buffer.len == self.buffer.capacity) {
                    if (self.buffer.pop()) |oldest| {
                        oldest.deinit(allocator);
                    }
                }
                self.buffer.pushAssumeCapacity(msg);
            },
        }
    }

    pub fn grow(self: *Queue) !void {
        if (self.buffer.capacity >= self.max_capacity) return error.QueueFull;

        const doubled = if (self.buffer.capacity > self.max_capacity / 2)
            self.max_capacity
        else
            self.buffer.capacity * 2;

        const target_capacity = if (doubled > self.max_capacity) self.max_capacity else doubled;
        if (target_capacity <= self.buffer.capacity) return error.QueueFull;

        try self.buffer.grow(target_capacity);
    }
};

test "queue rejects when full" {
    var queue = try Queue.init(testing.allocator, "jobs", 1, 1);
    defer queue.deinit();

    const first = try Message.init(testing.allocator, 1, "first");
    try queue.push(testing.allocator, first);

    const second = try Message.init(testing.allocator, 2, "second");
    try testing.expectError(error.QueueFull, queue.push(testing.allocator, second));
    second.deinit(testing.allocator);
}

test "queue can drop oldest safely" {
    var queue = try Queue.init(testing.allocator, "jobs", 1, 1);
    defer queue.deinit();
    queue.overflow = .drop_oldest;

    const first = try Message.init(testing.allocator, 1, "first");
    try queue.push(testing.allocator, first);

    const second = try Message.init(testing.allocator, 2, "second");
    try queue.push(testing.allocator, second);

    const msg = queue.buffer.pop().?;
    defer msg.deinit(testing.allocator);

    try testing.expectEqualStrings("second", msg.body);
}

test "ring buffer grow preserves FIFO order" {
    var rb = try RingBuffer.init(testing.allocator, 2);
    defer rb.deinit();

    try rb.push(try Message.init(testing.allocator, 1, "first"));
    try rb.push(try Message.init(testing.allocator, 2, "second"));
    const removed = rb.pop().?;
    removed.deinit(testing.allocator);
    try rb.push(try Message.init(testing.allocator, 3, "third"));

    try rb.grow(4);

    const first = rb.pop().?;
    defer first.deinit(testing.allocator);
    const second = rb.pop().?;
    defer second.deinit(testing.allocator);

    try testing.expectEqualStrings("second", first.body);
    try testing.expectEqualStrings("third", second.body);
}

test "queue auto expands until max capacity" {
    var queue = try Queue.init(testing.allocator, "jobs", 2, 8);
    defer queue.deinit();

    try queue.push(testing.allocator, try Message.init(testing.allocator, 1, "one"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 2, "two"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 3, "three"));

    try testing.expectEqual(@as(usize, 4), queue.buffer.capacity);
    try testing.expectEqual(@as(usize, 3), queue.buffer.len);
}

test "queue stops expanding at max capacity" {
    var queue = try Queue.init(testing.allocator, "jobs", 2, 4);
    defer queue.deinit();

    try queue.push(testing.allocator, try Message.init(testing.allocator, 1, "one"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 2, "two"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 3, "three"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 4, "four"));

    const fifth = try Message.init(testing.allocator, 5, "five");
    try testing.expectError(error.QueueFull, queue.push(testing.allocator, fifth));
    fifth.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 4), queue.buffer.capacity);
}
