const std = @import("std");
const atomic = std.atomic;
const mem = std.mem;
const testing = std.testing;

pub const OverflowPolicy = enum {
    block,
    reject,
    drop_oldest,
    drop_new,
};

pub const Message = struct {
    pub const inline_body_capacity = 32;

    id: u64,
    body_len: usize,
    timestamp: i64,
    storage: Storage,

    const Storage = union(enum) {
        inline_body: [inline_body_capacity]u8,
        heap: []u8,
    };

    pub fn init(allocator: mem.Allocator, id: u64, body_bytes: []const u8) !Message {
        if (body_bytes.len <= inline_body_capacity) {
            var msg = Message{
                .id = id,
                .body_len = body_bytes.len,
                .timestamp = 0,
                .storage = .{ .inline_body = undefined },
            };
            @memcpy(msg.storage.inline_body[0..body_bytes.len], body_bytes);
            return msg;
        }

        return .{
            .id = id,
            .body_len = body_bytes.len,
            .timestamp = 0,
            .storage = .{ .heap = try allocator.dupe(u8, body_bytes) },
        };
    }

    pub fn bytes(self: *const Message) []const u8 {
        return switch (self.storage) {
            .inline_body => |*inline_body| inline_body[0..self.body_len],
            .heap => |heap_body| heap_body,
        };
    }

    pub fn deinit(self: Message, allocator: mem.Allocator) void {
        switch (self.storage) {
            .inline_body => {},
            .heap => |heap_body| allocator.free(heap_body),
        }
    }
};

const Slot = struct {
    sequence: atomic.Value(usize),
    message: Message,
};

pub const RingBuffer = struct {
    slots: []Slot,
    capacity: usize,
    head: atomic.Value(usize),
    tail: atomic.Value(usize),
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator, capacity: usize) !RingBuffer {
        if (capacity < 2) return error.InvalidCapacity;

        const slots = try allocator.alloc(Slot, capacity);
        for (slots, 0..) |*slot, i| {
            slot.* = .{
                .sequence = atomic.Value(usize).init(i),
                .message = undefined,
            };
        }

        return .{
            .slots = slots,
            .capacity = capacity,
            .head = atomic.Value(usize).init(0),
            .tail = atomic.Value(usize).init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RingBuffer) void {
        const hd = self.head.load(.monotonic);
        const tl = self.tail.load(.monotonic);
        var i = hd;
        while (i < tl) : (i += 1) {
            self.slots[i % self.capacity].message.deinit(self.allocator);
        }
        self.allocator.free(self.slots);
    }

    pub fn push(self: *RingBuffer, msg: Message) !void {
        var pos = self.tail.load(.monotonic);
        while (true) {
            const slot_idx = pos % self.capacity;
            const seq = self.slots[slot_idx].sequence.load(.acquire);

            if (seq < pos) {
                const hd = self.head.load(.acquire);
                if (hd + self.capacity <= pos) return error.QueueFull;
                pos = self.tail.load(.monotonic);
                continue;
            }

            if (seq == pos) {
                if (self.tail.cmpxchgWeak(pos, pos + 1, .monotonic, .monotonic)) |actual| {
                    pos = actual;
                    continue;
                }
                self.slots[slot_idx].message = msg;
                self.slots[slot_idx].sequence.store(pos + 1, .release);
                return;
            }

            pos = self.tail.load(.monotonic);
        }
    }

    pub fn pushDirect(self: *RingBuffer, msg: Message) void {
        const pos = self.tail.load(.monotonic);
        const slot_idx = pos % self.capacity;
        self.slots[slot_idx].message = msg;
        self.slots[slot_idx].sequence.store(pos + 1, .release);
        _ = self.tail.fetchAdd(1, .monotonic);
    }

    pub fn pop(self: *RingBuffer) ?Message {
        var pos = self.head.load(.monotonic);
        while (true) {
            const tl = self.tail.load(.acquire);
            if (pos >= tl) return null;

            const slot_idx = pos % self.capacity;
            const seq = self.slots[slot_idx].sequence.load(.acquire);

            if (seq == pos + 1) {
                if (self.head.cmpxchgWeak(pos, pos + 1, .monotonic, .monotonic)) |actual| {
                    pos = actual;
                    continue;
                }
                const msg = self.slots[slot_idx].message;
                self.slots[slot_idx].sequence.store(pos + self.capacity, .release);
                return msg;
            }

            pos = self.head.load(.monotonic);
        }
    }

    pub fn popDirect(self: *RingBuffer) ?Message {
        const hd = self.head.load(.monotonic);
        const tl = self.tail.load(.acquire);
        if (hd >= tl) return null;

        const slot_idx = hd % self.capacity;
        const msg = self.slots[slot_idx].message;
        self.slots[slot_idx].sequence.store(hd + self.capacity, .release);
        _ = self.head.fetchAdd(1, .monotonic);
        return msg;
    }

    pub fn grow(self: *RingBuffer, new_capacity: usize) !void {
        if (new_capacity <= self.capacity) return error.InvalidCapacity;

        const hd = self.head.load(.acquire);
        const tl = self.tail.load(.acquire);
        const occupied = tl -| hd;

        const new_slots = try self.allocator.alloc(Slot, new_capacity);
        errdefer self.allocator.free(new_slots);

        for (new_slots, 0..) |*slot, i| {
            slot.* = .{
                .sequence = atomic.Value(usize).init(i),
                .message = undefined,
            };
        }

        var i = hd;
        while (i < tl) : (i += 1) {
            const old_idx = i % self.capacity;
            const new_idx = i - hd;
            new_slots[new_idx].message = self.slots[old_idx].message;
            new_slots[new_idx].sequence.store(new_idx + 1, .monotonic);
        }

        self.allocator.free(self.slots);
        self.slots = new_slots;
        self.capacity = new_capacity;
        self.head.store(0, .monotonic);
        self.tail.store(occupied, .monotonic);
    }

    pub fn peekCopy(self: *RingBuffer, allocator: mem.Allocator) ![]u8 {
        const hd = self.head.load(.acquire);
        const tl = self.tail.load(.acquire);
        if (hd >= tl) return error.Empty;

        const slot_idx = hd % self.capacity;
        const seq = self.slots[slot_idx].sequence.load(.acquire);
        if (seq != hd + 1) return error.Empty;

        const body = try allocator.dupe(u8, self.slots[slot_idx].message.bytes());

        const seq2 = self.slots[slot_idx].sequence.load(.acquire);
        if (seq2 != hd + 1) {
            allocator.free(body);
            return error.Empty;
        }
        return body;
    }

    pub fn len(self: *RingBuffer) usize {
        const tl = self.tail.load(.acquire);
        const hd = self.head.load(.acquire);
        return tl -| hd;
    }
};

pub const Queue = struct {
    buffer: atomic.Value(*RingBuffer),
    overflow: OverflowPolicy,
    name: []const u8,
    max_capacity: usize,
    allocator: mem.Allocator,
    grow_mutex: std.Thread.Mutex = .{},
    old_buffers: std.ArrayList(*RingBuffer),

    pub fn init(allocator: mem.Allocator, name: []const u8, initial_capacity: usize, max_capacity: usize) !Queue {
        if (max_capacity < initial_capacity) return error.InvalidCapacity;

        const rb = try allocator.create(RingBuffer);
        errdefer allocator.destroy(rb);
        rb.* = try RingBuffer.init(allocator, initial_capacity);
        errdefer rb.deinit();

        return .{
            .buffer = atomic.Value(*RingBuffer).init(rb),
            .overflow = .reject,
            .name = try allocator.dupe(u8, name),
            .max_capacity = max_capacity,
            .allocator = allocator,
            .old_buffers = std.ArrayList(*RingBuffer).empty,
        };
    }

    pub fn deinit(self: *Queue) void {
        const rb = self.buffer.load(.monotonic);
        rb.deinit();
        self.allocator.destroy(rb);

        for (self.old_buffers.items) |old_rb| {
            old_rb.deinit();
            self.allocator.destroy(old_rb);
        }
        self.old_buffers.deinit(self.allocator);
        self.allocator.free(self.name);
    }

    pub fn push(self: *Queue, allocator: mem.Allocator, msg: Message) !void {
        _ = allocator;
        while (true) {
            const rb = self.buffer.load(.acquire);
            if (rb.push(msg)) |_| {
                return;
            } else |err| {
                if (err != error.QueueFull) return err;
            }

            if (rb.capacity < self.max_capacity) {
                try self.grow();
                continue;
            }

            switch (self.overflow) {
                .reject, .block, .drop_new => return error.QueueFull,
                .drop_oldest => return self.dropOldestAndPush(msg),
            }
        }
    }

    fn dropOldestAndPush(self: *Queue, msg: Message) !void {
        self.grow_mutex.lock();
        defer self.grow_mutex.unlock();

        const rb = self.buffer.load(.acquire);
        if (rb.pop()) |oldest| {
            oldest.deinit(self.allocator);
        }
        return rb.push(msg);
    }

    pub fn grow(self: *Queue) !void {
        self.grow_mutex.lock();
        defer self.grow_mutex.unlock();

        const old_rb = self.buffer.load(.acquire);
        if (old_rb.capacity >= self.max_capacity) return error.QueueFull;

        const doubled = if (old_rb.capacity > self.max_capacity / 2)
            self.max_capacity
        else
            old_rb.capacity * 2;

        const new_capacity = @min(doubled, self.max_capacity);
        if (new_capacity <= old_rb.capacity) return error.QueueFull;

        const new_rb = try self.allocator.create(RingBuffer);
        errdefer self.allocator.destroy(new_rb);
        new_rb.* = try RingBuffer.init(self.allocator, new_capacity);
        errdefer new_rb.deinit();

        while (old_rb.popDirect()) |m| {
            new_rb.pushDirect(m);
        }

        self.buffer.store(new_rb, .release);

        var spins: usize = 0;
        while (spins < 100) : (spins += 1) {
            std.atomic.spinLoopHint();
            if (old_rb.popDirect()) |late_msg| {
                new_rb.pushDirect(late_msg);
                spins = 0;
            }
        }

        try self.old_buffers.append(self.allocator, old_rb);
    }

    pub fn pop(self: *Queue) ?Message {
        while (true) {
            const rb = self.buffer.load(.acquire);
            if (rb.pop()) |msg| return msg;
            if (self.buffer.load(.acquire) == rb) return null;
        }
    }

    pub fn peekCopy(self: *Queue, allocator: mem.Allocator) ![]u8 {
        self.grow_mutex.lock();
        defer self.grow_mutex.unlock();
        const rb = self.buffer.load(.acquire);
        return rb.peekCopy(allocator);
    }

    pub fn len(self: *Queue) usize {
        const rb = self.buffer.load(.acquire);
        return rb.len();
    }

    pub fn capacity(self: *Queue) usize {
        return self.buffer.load(.acquire).capacity;
    }
};

test "message stores small bodies inline" {
    const msg = try Message.init(testing.allocator, 1, "short");
    defer msg.deinit(testing.allocator);

    try testing.expectEqualStrings("short", msg.bytes());
    switch (msg.storage) {
        .inline_body => {},
        .heap => try testing.expect(false),
    }
}

test "message stores large bodies on heap" {
    var body = [_]u8{'x'} ** (Message.inline_body_capacity + 1);
    const msg = try Message.init(testing.allocator, 1, &body);
    defer msg.deinit(testing.allocator);

    try testing.expectEqualStrings(&body, msg.bytes());
    switch (msg.storage) {
        .inline_body => try testing.expect(false),
        .heap => {},
    }
}

test "ring buffer lock-free push pop FIFO" {
    var rb = try RingBuffer.init(testing.allocator, 4);
    defer rb.deinit();

    try rb.push(try Message.init(testing.allocator, 1, "first"));
    try rb.push(try Message.init(testing.allocator, 2, "second"));
    try rb.push(try Message.init(testing.allocator, 3, "third"));
    try rb.push(try Message.init(testing.allocator, 4, "fourth"));

    try testing.expectError(error.QueueFull, rb.push(try Message.init(testing.allocator, 5, "fifth")));
    // Clean up the rejected message
    (try Message.init(testing.allocator, 5, "fifth")).deinit(testing.allocator);

    const m1 = rb.pop().?;
    defer m1.deinit(testing.allocator);
    try testing.expectEqualStrings("first", m1.bytes());

    const m2 = rb.pop().?;
    defer m2.deinit(testing.allocator);
    try testing.expectEqualStrings("second", m2.bytes());

    const m3 = rb.pop().?;
    defer m3.deinit(testing.allocator);
    try testing.expectEqualStrings("third", m3.bytes());

    const m4 = rb.pop().?;
    defer m4.deinit(testing.allocator);
    try testing.expectEqualStrings("fourth", m4.bytes());

    try testing.expect(rb.pop() == null);
}

test "ring buffer grow preserves messages" {
    var rb = try RingBuffer.init(testing.allocator, 2);
    defer rb.deinit();

    try rb.push(try Message.init(testing.allocator, 1, "first"));
    try rb.push(try Message.init(testing.allocator, 2, "second"));
    const removed = rb.pop().?;
    removed.deinit(testing.allocator);
    try rb.push(try Message.init(testing.allocator, 3, "third"));

    // Queue is full (head=1, tail=3, capacity=2)
    try rb.grow(4);

    try testing.expectEqual(@as(usize, 4), rb.capacity);
    try testing.expectEqual(@as(usize, 2), rb.len());

    const m1 = rb.pop().?;
    defer m1.deinit(testing.allocator);
    const m2 = rb.pop().?;
    defer m2.deinit(testing.allocator);

    try testing.expectEqualStrings("second", m1.bytes());
    try testing.expectEqualStrings("third", m2.bytes());
    try testing.expect(rb.pop() == null);
}

test "queue rejects when full" {
    var queue = try Queue.init(testing.allocator, "jobs", 2, 2);
    defer queue.deinit();

    const first = try Message.init(testing.allocator, 1, "first");
    try queue.push(testing.allocator, first);
    const second = try Message.init(testing.allocator, 2, "second");
    try queue.push(testing.allocator, second);

    const third = try Message.init(testing.allocator, 3, "third");
    try testing.expectError(error.QueueFull, queue.push(testing.allocator, third));
    third.deinit(testing.allocator);
}

test "queue can drop oldest safely" {
    var queue = try Queue.init(testing.allocator, "jobs", 2, 2);
    defer queue.deinit();
    queue.overflow = .drop_oldest;

    const first = try Message.init(testing.allocator, 1, "first");
    try queue.push(testing.allocator, first);
    const second = try Message.init(testing.allocator, 2, "second");
    try queue.push(testing.allocator, second);

    const third = try Message.init(testing.allocator, 3, "third");
    try queue.push(testing.allocator, third);

    const m1 = queue.pop().?;
    defer m1.deinit(testing.allocator);
    try testing.expectEqualStrings("second", m1.bytes());

    const m2 = queue.pop().?;
    defer m2.deinit(testing.allocator);
    try testing.expectEqualStrings("third", m2.bytes());

    try testing.expect(queue.pop() == null);
}

test "queue auto expands until max capacity" {
    var queue = try Queue.init(testing.allocator, "jobs", 2, 8);
    defer queue.deinit();

    try queue.push(testing.allocator, try Message.init(testing.allocator, 1, "one"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 2, "two"));
    try queue.push(testing.allocator, try Message.init(testing.allocator, 3, "three"));

    try testing.expectEqual(@as(usize, 4), queue.capacity());
    try testing.expectEqual(@as(usize, 3), queue.len());
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

    try testing.expectEqual(@as(usize, 4), queue.capacity());
}

test "ring buffer peek copy reads head message" {
    var rb = try RingBuffer.init(testing.allocator, 4);
    defer rb.deinit();

    try rb.push(try Message.init(testing.allocator, 1, "hello"));
    const body = try rb.peekCopy(testing.allocator);
    defer testing.allocator.free(body);
    try testing.expectEqualStrings("hello", body);

    try testing.expectEqual(@as(usize, 1), rb.len());
}

test "ring buffer peek copy returns empty when empty" {
    var rb = try RingBuffer.init(testing.allocator, 4);
    defer rb.deinit();
    try testing.expectError(error.Empty, rb.peekCopy(testing.allocator));
}
