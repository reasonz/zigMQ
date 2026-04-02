const std = @import("std");
const ascii = std.ascii;
const fmt = std.fmt;
const heap = std.heap;
const mem = std.mem;
const net = std.net;
const testing = std.testing;
const time = std.time;
const atomic = std.atomic;

const Config = struct {
    port: u16 = 6379,
    mode: enum { standalone, master, worker } = .standalone,
    workers: u32 = 1,
    queue_capacity: usize = 10000,
    max_queue_capacity: usize = 100000,
    data_dir: ?[]const u8 = null,
};

const OverflowPolicy = enum {
    block,
    reject,
    drop_oldest,
    drop_new,
};

const Message = struct {
    id: u64,
    body: []u8,
    timestamp: i64,

    fn init(allocator: mem.Allocator, id: u64, body: []const u8) !Message {
        const body_copy = try allocator.dupe(u8, body);
        return .{
            .id = id,
            .body = body_copy,
            .timestamp = time.timestamp(),
        };
    }

    fn deinit(self: Message, allocator: mem.Allocator) void {
        allocator.free(self.body);
    }
};

const RingBuffer = struct {
    messages: []Message,
    capacity: usize,
    head: usize = 0,
    tail: usize = 0,
    len: usize = 0,
    allocator: mem.Allocator,

    fn init(allocator: mem.Allocator, capacity: usize) !RingBuffer {
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

    fn deinit(self: *RingBuffer) void {
        var i: usize = 0;
        while (i < self.len) : (i += 1) {
            const idx = (self.head + i) % self.capacity;
            self.messages[idx].deinit(self.allocator);
        }
        self.allocator.free(self.messages);
    }

    fn push(self: *RingBuffer, msg: Message) !void {
        if (self.len == self.capacity) return error.QueueFull;
        self.pushAssumeCapacity(msg);
    }

    fn pushAssumeCapacity(self: *RingBuffer, msg: Message) void {
        self.messages[self.tail] = msg;
        self.tail = (self.tail + 1) % self.capacity;
        self.len += 1;
    }

    fn grow(self: *RingBuffer, new_capacity: usize) !void {
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

    fn pop(self: *RingBuffer) ?Message {
        if (self.len == 0) return null;

        const msg = self.messages[self.head];
        self.head = (self.head + 1) % self.capacity;
        self.len -= 1;
        return msg;
    }

    fn peek(self: *RingBuffer) ?*Message {
        if (self.len == 0) return null;
        return &self.messages[self.head];
    }
};

const Queue = struct {
    buffer: RingBuffer,
    overflow: OverflowPolicy,
    name: []const u8,
    max_capacity: usize,

    fn init(allocator: mem.Allocator, name: []const u8, capacity: usize, max_capacity: usize) !Queue {
        if (max_capacity < capacity) return error.InvalidCapacity;

        return .{
            .buffer = try RingBuffer.init(allocator, capacity),
            .overflow = .reject,
            .name = try allocator.dupe(u8, name),
            .max_capacity = max_capacity,
        };
    }

    fn deinit(self: *Queue) void {
        self.buffer.deinit();
        self.buffer.allocator.free(self.name);
    }

    fn push(self: *Queue, allocator: mem.Allocator, msg: Message) !void {
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

    fn grow(self: *Queue) !void {
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

const Connection = struct {
    stream: net.Stream,
    buffer: [4096]u8 = undefined,
    buffer_start: usize = 0,
    buffer_end: usize = 0,
    allocator: mem.Allocator,
    subscriptions: std.ArrayList([]const u8),
    write_mutex: std.Thread.Mutex = .{},
    ref_count: atomic.Value(usize) = atomic.Value(usize).init(1),

    fn init(stream: net.Stream, allocator: mem.Allocator) Connection {
        return .{
            .stream = stream,
            .allocator = allocator,
            .subscriptions = std.ArrayList([]const u8).empty,
        };
    }

    fn deinit(self: *Connection) void {
        self.clearSubscriptions();
        self.subscriptions.deinit(self.allocator);
    }

    fn addSubscription(self: *Connection, topic: []const u8) !void {
        for (self.subscriptions.items) |existing| {
            if (mem.eql(u8, existing, topic)) return;
        }

        const copy = try self.allocator.dupe(u8, topic);
        try self.subscriptions.append(self.allocator, copy);
    }

    fn removeSubscription(self: *Connection, topic: []const u8) void {
        for (self.subscriptions.items, 0..) |existing, i| {
            if (mem.eql(u8, existing, topic)) {
                self.allocator.free(existing);
                _ = self.subscriptions.swapRemove(i);
                return;
            }
        }
    }

    fn clearSubscriptions(self: *Connection) void {
        for (self.subscriptions.items) |topic| {
            self.allocator.free(topic);
        }
        self.subscriptions.clearRetainingCapacity();
    }

    fn nextBufferedLine(self: *Connection) ?[]const u8 {
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

    fn compactUnread(self: *Connection) void {
        if (self.buffer_start == 0) return;

        const unread_len = self.buffer_end - self.buffer_start;
        if (unread_len > 0) {
            mem.copyForwards(u8, self.buffer[0..unread_len], self.buffer[self.buffer_start..self.buffer_end]);
        }
        self.buffer_start = 0;
        self.buffer_end = unread_len;
    }

    fn readLine(self: *Connection) !?[]const u8 {
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

    fn write(self: *Connection, data: []const u8) !void {
        self.write_mutex.lock();
        defer self.write_mutex.unlock();
        try self.stream.writeAll(data);
    }

    fn retain(self: *Connection) void {
        _ = self.ref_count.fetchAdd(1, .monotonic);
    }

    fn release(self: *Connection, allocator: mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .acq_rel) == 1) {
            self.deinit();
            allocator.destroy(self);
        }
    }
};

const PublishSnapshot = struct {
    allocator: mem.Allocator,
    topic_name: []u8,
    subscribers: []*Connection,

    fn deinit(self: *PublishSnapshot) void {
        for (self.subscribers) |conn| {
            conn.release(self.allocator);
        }
        self.allocator.free(self.subscribers);
        self.allocator.free(self.topic_name);
    }
};

const Topic = struct {
    name: []const u8,
    subscribers: std.ArrayList(*Connection),
    allocator: mem.Allocator,

    fn init(allocator: mem.Allocator, name: []const u8) !Topic {
        return .{
            .name = try allocator.dupe(u8, name),
            .subscribers = std.ArrayList(*Connection).empty,
            .allocator = allocator,
        };
    }

    fn deinit(self: *Topic) void {
        self.subscribers.deinit(self.allocator);
        self.allocator.free(self.name);
    }

    fn addSubscriber(self: *Topic, conn: *Connection) !void {
        for (self.subscribers.items) |existing| {
            if (existing == conn) return;
        }
        try self.subscribers.append(self.allocator, conn);
    }

    fn removeSubscriber(self: *Topic, conn: *Connection) void {
        for (self.subscribers.items, 0..) |existing, i| {
            if (existing == conn) {
                _ = self.subscribers.swapRemove(i);
                return;
            }
        }
    }

    fn snapshot(self: *Topic, allocator: mem.Allocator) !PublishSnapshot {
        const topic_name = try allocator.dupe(u8, self.name);
        errdefer allocator.free(topic_name);

        const subscribers = try allocator.alloc(*Connection, self.subscribers.items.len);
        errdefer allocator.free(subscribers);

        for (self.subscribers.items, 0..) |conn, i| {
            conn.retain();
            subscribers[i] = conn;
        }

        return .{
            .allocator = allocator,
            .topic_name = topic_name,
            .subscribers = subscribers,
        };
    }
};

const TopicManager = struct {
    topics: std.StringHashMap(*Topic),
    allocator: mem.Allocator,

    fn init(allocator: mem.Allocator) TopicManager {
        return .{
            .topics = std.StringHashMap(*Topic).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *TopicManager) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.topics.deinit();
    }

    fn getOrCreate(self: *TopicManager, name: []const u8) !*Topic {
        if (self.topics.get(name)) |topic| {
            return topic;
        }

        const topic = try self.allocator.create(Topic);
        errdefer self.allocator.destroy(topic);

        topic.* = try Topic.init(self.allocator, name);
        errdefer topic.deinit();

        try self.topics.put(topic.name, topic);
        return topic;
    }

    fn unsubscribe(self: *TopicManager, topic_name: []const u8, conn: *Connection) void {
        if (self.topics.get(topic_name)) |topic| {
            topic.removeSubscriber(conn);
            if (topic.subscribers.items.len == 0) {
                if (self.topics.fetchRemove(topic_name)) |entry| {
                    entry.value.deinit();
                    self.allocator.destroy(entry.value);
                }
            }
        }
    }
};

const QueueManager = struct {
    queues: std.StringHashMap(*Queue),
    allocator: mem.Allocator,

    fn init(allocator: mem.Allocator) QueueManager {
        return .{
            .queues = std.StringHashMap(*Queue).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *QueueManager) void {
        var it = self.queues.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.queues.deinit();
    }

    fn getOrCreate(self: *QueueManager, name: []const u8, capacity: usize, max_capacity: usize) !*Queue {
        if (self.queues.get(name)) |queue| {
            return queue;
        }

        const queue = try self.allocator.create(Queue);
        errdefer self.allocator.destroy(queue);

        queue.* = try Queue.init(self.allocator, name, capacity, max_capacity);
        errdefer queue.deinit();

        try self.queues.put(queue.name, queue);
        return queue;
    }
};

const Command = struct {
    op: []const u8,
    queue: ?[]const u8,
    body: ?[]const u8,

    fn parse(line: []const u8) !Command {
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

const SharedState = struct {
    allocator: mem.Allocator,
    queue_mutex: std.Thread.Mutex = .{},
    topic_mutex: std.Thread.Mutex = .{},
    queue_manager: QueueManager,
    topic_manager: TopicManager,

    fn init(allocator: mem.Allocator) SharedState {
        return .{
            .allocator = allocator,
            .queue_manager = QueueManager.init(allocator),
            .topic_manager = TopicManager.init(allocator),
        };
    }

    fn deinit(self: *SharedState) void {
        self.queue_manager.deinit();
        self.topic_manager.deinit();
    }
};

var global_msg_id: atomic.Value(u64) = atomic.Value(u64).init(0);

fn nextMsgId() u64 {
    return global_msg_id.fetchAdd(1, .monotonic) + 1;
}

fn isOp(actual: []const u8, expected: []const u8) bool {
    return ascii.eqlIgnoreCase(actual, expected);
}

const Server = struct {
    allocator: mem.Allocator,
    state: SharedState,
    config: Config,
    listener: net.Server,

    fn init(allocator: mem.Allocator, config: Config) !Server {
        const addr = try net.Address.parseIp("0.0.0.0", config.port);
        const listener = try addr.listen(.{ .reuse_address = true });

        return .{
            .allocator = allocator,
            .state = SharedState.init(allocator),
            .config = config,
            .listener = listener,
        };
    }

    fn deinit(self: *Server) void {
        self.state.deinit();
    }

    fn cleanupConnection(self: *Server, conn: *Connection) void {
        self.state.topic_mutex.lock();
        defer self.state.topic_mutex.unlock();

        for (conn.subscriptions.items) |topic_name| {
            self.state.topic_manager.unsubscribe(topic_name, conn);
        }
    }

    fn connectionThreadMain(self: *Server, conn: *Connection) void {
        self.handleConnection(conn) catch |err| {
            std.debug.print("connection error: {}\n", .{err});
        };
    }

    fn handleConnection(self: *Server, conn: *Connection) !void {
        defer {
            self.cleanupConnection(conn);
            conn.stream.close();
            conn.release(self.allocator);
        }

        while (true) {
            const line = conn.readLine() catch |err| switch (err) {
                error.BufferFull => {
                    conn.write("-ERR line too long\r\n") catch {};
                    break;
                },
                error.ConnectionClosed => break,
                else => return err,
            };

            const raw = line orelse break;
            const trimmed = mem.trimRight(u8, raw, "\r");
            if (trimmed.len == 0) continue;

            const cmd = Command.parse(trimmed) catch {
                try conn.write("-ERR invalid command\r\n");
                continue;
            };

            try self.handleCommand(conn, cmd);
        }
    }

    fn buildQueuesResponse(self: *Server) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        try out.append(self.allocator, '+');

        var it = self.state.queue_manager.queues.iterator();
        if (self.state.queue_manager.queues.count() == 0) {
            try out.appendSlice(self.allocator, "\r\n");
            return try out.toOwnedSlice(self.allocator);
        }

        while (it.next()) |entry| {
            try out.appendSlice(self.allocator, entry.key_ptr.*);
            try out.appendSlice(self.allocator, "\r\n");
        }

        return try out.toOwnedSlice(self.allocator);
    }

    fn buildTopicsResponse(self: *Server) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        try out.append(self.allocator, '+');

        var it = self.state.topic_manager.topics.iterator();
        if (self.state.topic_manager.topics.count() == 0) {
            try out.appendSlice(self.allocator, "\r\n");
            return try out.toOwnedSlice(self.allocator);
        }

        while (it.next()) |entry| {
            const topic = entry.value_ptr.*;
            try out.writer(self.allocator).print("{s}({d})\r\n", .{ entry.key_ptr.*, topic.subscribers.items.len });
        }

        return try out.toOwnedSlice(self.allocator);
    }

    fn buildSubscriptionsResponse(self: *Server, conn: *Connection) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        try out.append(self.allocator, '+');

        if (conn.subscriptions.items.len == 0) {
            try out.appendSlice(self.allocator, "\r\n");
            return try out.toOwnedSlice(self.allocator);
        }

        for (conn.subscriptions.items) |topic| {
            try out.appendSlice(self.allocator, topic);
            try out.appendSlice(self.allocator, "\r\n");
        }

        return try out.toOwnedSlice(self.allocator);
    }

    fn broadcastSnapshot(self: *Server, snapshot: *PublishSnapshot, msg: []const u8) !usize {
        const payload = try fmt.allocPrint(self.allocator, "+{s}:{s}\r\n", .{ snapshot.topic_name, msg });
        defer self.allocator.free(payload);

        for (snapshot.subscribers) |conn| {
            conn.write(payload) catch {};
        }

        return snapshot.subscribers.len;
    }

    fn handleCommand(self: *Server, conn: *Connection, cmd: Command) !void {
        if (isOp(cmd.op, "ping")) {
            try conn.write("+PONG\r\n");
            return;
        }

        if (isOp(cmd.op, "info")) {
            self.state.queue_mutex.lock();
            const queue_count = self.state.queue_manager.queues.count();
            self.state.queue_mutex.unlock();

            self.state.topic_mutex.lock();
            const topic_count = self.state.topic_manager.topics.count();
            self.state.topic_mutex.unlock();

            var buf: [320]u8 = undefined;
            const response = try fmt.bufPrint(&buf, "+ZigMQ 0.3.0\r\nqueues:{d}\r\ntopics:{d}\r\ninitial_capacity:{d}\r\nmax_capacity:{d}\r\n", .{
                queue_count,
                topic_count,
                self.config.queue_capacity,
                self.config.max_queue_capacity,
            });
            try conn.write(response);
            return;
        }

        if (isOp(cmd.op, "push") or isOp(cmd.op, "send")) {
            if (cmd.queue == null or cmd.body == null) {
                try conn.write("-ERR need queue and body\r\n");
                return;
            }

            const msg = Message.init(self.allocator, nextMsgId(), cmd.body.?) catch {
                try conn.write("-ERR out of memory\r\n");
                return;
            };

            self.state.queue_mutex.lock();
            const result: enum { ok, create_failed, queue_full, out_of_memory } = blk: {
                const queue = self.state.queue_manager.getOrCreate(cmd.queue.?, self.config.queue_capacity, self.config.max_queue_capacity) catch {
                    break :blk .create_failed;
                };

                queue.push(self.allocator, msg) catch |err| switch (err) {
                    error.QueueFull => break :blk .queue_full,
                    error.OutOfMemory => break :blk .out_of_memory,
                    else => break :blk .create_failed,
                };

                break :blk .ok;
            };
            self.state.queue_mutex.unlock();

            switch (result) {
                .ok => try conn.write("+OK\r\n"),
                .queue_full => {
                    msg.deinit(self.allocator);
                    try conn.write("-ERR queue full\r\n");
                },
                .out_of_memory => {
                    msg.deinit(self.allocator);
                    try conn.write("-ERR out of memory\r\n");
                },
                .create_failed => {
                    msg.deinit(self.allocator);
                    try conn.write("-ERR create queue failed\r\n");
                },
            }
            return;
        }

        if (isOp(cmd.op, "pop") or isOp(cmd.op, "recv")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }

            self.state.queue_mutex.lock();
            const result: union(enum) {
                queue_not_found,
                empty,
                message: Message,
            } = blk: {
                const queue = self.state.queue_manager.queues.get(cmd.queue.?) orelse break :blk .queue_not_found;
                if (queue.buffer.pop()) |msg| break :blk .{ .message = msg };
                break :blk .empty;
            };
            self.state.queue_mutex.unlock();

            switch (result) {
                .queue_not_found => try conn.write("-ERR queue not found\r\n"),
                .empty => try conn.write("-ERR empty\r\n"),
                .message => |msg| {
                    defer msg.deinit(self.allocator);
                    const response = try fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ msg.body.len, msg.body });
                    defer self.allocator.free(response);
                    try conn.write(response);
                },
            }
            return;
        }

        if (isOp(cmd.op, "peek")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }

            self.state.queue_mutex.lock();
            const result: union(enum) {
                queue_not_found,
                empty,
                out_of_memory,
                body: []u8,
            } = blk: {
                const queue = self.state.queue_manager.queues.get(cmd.queue.?) orelse break :blk .queue_not_found;
                if (queue.buffer.peek()) |msg| {
                    const body_copy = self.allocator.dupe(u8, msg.body) catch break :blk .out_of_memory;
                    break :blk .{ .body = body_copy };
                }
                break :blk .empty;
            };
            self.state.queue_mutex.unlock();

            switch (result) {
                .queue_not_found => try conn.write("-ERR queue not found\r\n"),
                .empty => try conn.write("-ERR empty\r\n"),
                .out_of_memory => try conn.write("-ERR out of memory\r\n"),
                .body => |body| {
                    defer self.allocator.free(body);
                    const response = try fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ body.len, body });
                    defer self.allocator.free(response);
                    try conn.write(response);
                },
            }
            return;
        }

        if (isOp(cmd.op, "len")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }

            self.state.queue_mutex.lock();
            const queue_len = if (self.state.queue_manager.queues.get(cmd.queue.?)) |queue| queue.buffer.len else null;
            self.state.queue_mutex.unlock();

            if (queue_len) |len| {
                var buf: [32]u8 = undefined;
                const response = try fmt.bufPrint(&buf, "+{d}\r\n", .{len});
                try conn.write(response);
            } else {
                try conn.write("-ERR queue not found\r\n");
            }
            return;
        }

        if (isOp(cmd.op, "queues")) {
            self.state.queue_mutex.lock();
            const response = try self.buildQueuesResponse();
            self.state.queue_mutex.unlock();
            defer self.allocator.free(response);

            try conn.write(response);
            return;
        }

        if (isOp(cmd.op, "qcreate") or isOp(cmd.op, "mq")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }

            self.state.queue_mutex.lock();
            const created = self.state.queue_manager.getOrCreate(cmd.queue.?, self.config.queue_capacity, self.config.max_queue_capacity) catch null;
            self.state.queue_mutex.unlock();

            if (created != null) {
                try conn.write("+OK\r\n");
            } else {
                try conn.write("-ERR create queue failed\r\n");
            }
            return;
        }

        if (isOp(cmd.op, "sub")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need topic name\r\n");
                return;
            }

            self.state.topic_mutex.lock();
            const result: enum { ok, failed } = blk: {
                const topic = self.state.topic_manager.getOrCreate(cmd.queue.?) catch break :blk .failed;
                conn.addSubscription(cmd.queue.?) catch break :blk .failed;
                topic.addSubscriber(conn) catch {
                    conn.removeSubscription(cmd.queue.?);
                    break :blk .failed;
                };
                break :blk .ok;
            };
            self.state.topic_mutex.unlock();

            switch (result) {
                .ok => try conn.write("+OK\r\n"),
                .failed => try conn.write("-ERR subscribe failed\r\n"),
            }
            return;
        }

        if (isOp(cmd.op, "unsub")) {
            self.state.topic_mutex.lock();
            if (cmd.queue) |topic_name| {
                self.state.topic_manager.unsubscribe(topic_name, conn);
                conn.removeSubscription(topic_name);
            } else {
                for (conn.subscriptions.items) |topic_name| {
                    self.state.topic_manager.unsubscribe(topic_name, conn);
                }
                conn.clearSubscriptions();
            }
            self.state.topic_mutex.unlock();

            try conn.write("+OK\r\n");
            return;
        }

        if (isOp(cmd.op, "pub")) {
            if (cmd.queue == null or cmd.body == null) {
                try conn.write("-ERR need topic and message\r\n");
                return;
            }

            self.state.topic_mutex.lock();
            const publish_result: union(enum) {
                failed,
                snapshot: PublishSnapshot,
            } = blk: {
                const topic = self.state.topic_manager.getOrCreate(cmd.queue.?) catch break :blk .failed;
                const snapshot = topic.snapshot(self.allocator) catch break :blk .failed;
                break :blk .{ .snapshot = snapshot };
            };
            self.state.topic_mutex.unlock();

            switch (publish_result) {
                .failed => try conn.write("-ERR publish failed\r\n"),
                .snapshot => |snapshot| {
                    var owned_snapshot = snapshot;
                    defer owned_snapshot.deinit();

                    const count = self.broadcastSnapshot(&owned_snapshot, cmd.body.?) catch {
                        try conn.write("-ERR publish failed\r\n");
                        return;
                    };
                    var buf: [32]u8 = undefined;
                    const response = try fmt.bufPrint(&buf, "+OK {d}\r\n", .{count});
                    try conn.write(response);
                },
            }
            return;
        }

        if (isOp(cmd.op, "topics")) {
            self.state.topic_mutex.lock();
            const response = try self.buildTopicsResponse();
            self.state.topic_mutex.unlock();
            defer self.allocator.free(response);

            try conn.write(response);
            return;
        }

        if (isOp(cmd.op, "subs")) {
            const response = try self.buildSubscriptionsResponse(conn);
            defer self.allocator.free(response);

            try conn.write(response);
            return;
        }

        try conn.write("-ERR unknown command\r\n");
    }

    fn run(self: *Server) !void {
        std.debug.print("ZigMQ listening on port {d}\n", .{self.config.port});

        while (true) {
            const incoming = try self.listener.accept();
            const conn = try self.allocator.create(Connection);
            errdefer self.allocator.destroy(conn);

            conn.* = Connection.init(incoming.stream, self.allocator);

            const thread = std.Thread.spawn(.{}, Server.connectionThreadMain, .{ self, conn }) catch |err| {
                incoming.stream.close();
                conn.release(self.allocator);
                return err;
            };
            thread.detach();
        }
    }
};

fn parseArgs() Config {
    var args = std.process.args();
    _ = args.next();

    var config = Config{};

    while (args.next()) |arg| {
        if (mem.eql(u8, arg, "--port")) {
            if (args.next()) |value| {
                config.port = fmt.parseInt(u16, value, 10) catch 6379;
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

pub fn main() !void {
    const allocator = heap.page_allocator;
    const config = parseArgs();

    var server = try Server.init(allocator, config);
    defer server.deinit();

    try server.run();
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

test "topic snapshot retains subscribers until released" {
    var topic = try Topic.init(testing.allocator, "news");
    defer topic.deinit();

    var conn_a = Connection.init(undefined, testing.allocator);
    defer conn_a.deinit();
    var conn_b = Connection.init(undefined, testing.allocator);
    defer conn_b.deinit();

    try topic.addSubscriber(&conn_a);
    try topic.addSubscriber(&conn_b);

    var snapshot = try topic.snapshot(testing.allocator);

    try testing.expectEqual(@as(usize, 2), snapshot.subscribers.len);
    try testing.expectEqual(@as(usize, 2), conn_a.ref_count.load(.seq_cst));
    try testing.expectEqual(@as(usize, 2), conn_b.ref_count.load(.seq_cst));

    snapshot.deinit();
    try testing.expectEqual(@as(usize, 1), conn_a.ref_count.load(.seq_cst));
    try testing.expectEqual(@as(usize, 1), conn_b.ref_count.load(.seq_cst));
}
