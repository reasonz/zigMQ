const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const net = std.net;
const testing = std.testing;

const config_mod = @import("config.zig");
const protocol = @import("protocol.zig");
const queue_mod = @import("queue.zig");
const connection_mod = @import("connection.zig");
const pubsub = @import("pubsub.zig");
const version = @import("version.zig");

const Config = config_mod.Config;
const Command = protocol.Command;
const Message = queue_mod.Message;
const Queue = queue_mod.Queue;
const Connection = connection_mod.Connection;
const PublishSnapshot = pubsub.PublishSnapshot;
const TopicManager = pubsub.TopicManager;

const queue_shard_count = 16;
const topic_shard_count = 16;

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

fn shardIndex(name: []const u8, comptime count: usize) usize {
    return @as(usize, @intCast(std.hash.Wyhash.hash(0, name) % count));
}

const QueuePushResult = enum { ok, create_failed, queue_full, out_of_memory };
const QueuePopResult = union(enum) {
    queue_not_found,
    empty,
    message: Message,
};
const QueuePeekResult = union(enum) {
    queue_not_found,
    empty,
    out_of_memory,
    body: []u8,
};

const QueueShard = struct {
    mutex: std.Thread.Mutex = .{},
    manager: QueueManager,
};

const ShardedQueueManager = struct {
    shards: [queue_shard_count]QueueShard,

    fn init(allocator: mem.Allocator) ShardedQueueManager {
        var shards: [queue_shard_count]QueueShard = undefined;
        for (&shards) |*shard| {
            shard.* = .{ .manager = QueueManager.init(allocator) };
        }
        return .{ .shards = shards };
    }

    fn deinit(self: *ShardedQueueManager) void {
        for (&self.shards) |*shard| {
            shard.manager.deinit();
        }
    }

    fn shardFor(self: *ShardedQueueManager, name: []const u8) *QueueShard {
        return &self.shards[shardIndex(name, queue_shard_count)];
    }

    fn push(
        self: *ShardedQueueManager,
        allocator: mem.Allocator,
        name: []const u8,
        msg: Message,
        capacity: usize,
        max_capacity: usize,
    ) QueuePushResult {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        const queue = shard.manager.getOrCreate(name, capacity, max_capacity) catch {
            shard.mutex.unlock();
            return .create_failed;
        };
        shard.mutex.unlock();

        queue.push(allocator, msg) catch |err| switch (err) {
            error.QueueFull => return .queue_full,
            error.OutOfMemory => return .out_of_memory,
            else => return .create_failed,
        };

        return .ok;
    }

    fn pop(self: *ShardedQueueManager, name: []const u8) QueuePopResult {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        const queue = shard.manager.queues.get(name) orelse {
            shard.mutex.unlock();
            return .queue_not_found;
        };
        shard.mutex.unlock();

        if (queue.pop()) |msg| return .{ .message = msg };
        return .empty;
    }

    fn peekCopy(self: *ShardedQueueManager, allocator: mem.Allocator, name: []const u8) QueuePeekResult {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        const queue = shard.manager.queues.get(name) orelse {
            shard.mutex.unlock();
            return .queue_not_found;
        };
        shard.mutex.unlock();

        const body = queue.peekCopy(allocator) catch |err| switch (err) {
            error.Empty => return .empty,
            error.OutOfMemory => return .out_of_memory,
        };
        return .{ .body = body };
    }

    fn len(self: *ShardedQueueManager, name: []const u8) ?usize {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        const queue = shard.manager.queues.get(name) orelse {
            shard.mutex.unlock();
            return null;
        };
        shard.mutex.unlock();

        return queue.len();
    }

    fn ensureQueue(self: *ShardedQueueManager, name: []const u8, capacity: usize, max_capacity: usize) bool {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        defer shard.mutex.unlock();

        return shard.manager.getOrCreate(name, capacity, max_capacity) catch null != null;
    }

    fn count(self: *ShardedQueueManager) usize {
        var total: usize = 0;
        for (&self.shards) |*shard| {
            shard.mutex.lock();
            total += shard.manager.queues.count();
            shard.mutex.unlock();
        }
        return total;
    }

    fn appendNames(self: *ShardedQueueManager, allocator: mem.Allocator, out: *std.ArrayList(u8)) !usize {
        var total: usize = 0;
        for (&self.shards) |*shard| {
            {
                shard.mutex.lock();
                defer shard.mutex.unlock();
                var it = shard.manager.queues.iterator();
                while (it.next()) |entry| {
                    try out.appendSlice(allocator, entry.key_ptr.*);
                    try out.appendSlice(allocator, "\r\n");
                    total += 1;
                }
            }
        }
        return total;
    }
};

const TopicSubscribeResult = enum { ok, failed };
const TopicPublishResult = union(enum) {
    out_of_memory,
    no_subscribers,
    snapshot: PublishSnapshot,
};

const TopicShard = struct {
    mutex: std.Thread.Mutex = .{},
    manager: TopicManager,
};

const ShardedTopicManager = struct {
    shards: [topic_shard_count]TopicShard,

    fn init(allocator: mem.Allocator) ShardedTopicManager {
        var shards: [topic_shard_count]TopicShard = undefined;
        for (&shards) |*shard| {
            shard.* = .{ .manager = TopicManager.init(allocator) };
        }
        return .{ .shards = shards };
    }

    fn deinit(self: *ShardedTopicManager) void {
        for (&self.shards) |*shard| {
            shard.manager.deinit();
        }
    }

    fn shardFor(self: *ShardedTopicManager, name: []const u8) *TopicShard {
        return &self.shards[shardIndex(name, topic_shard_count)];
    }

    fn subscribe(self: *ShardedTopicManager, name: []const u8, conn: *Connection) TopicSubscribeResult {
        const shard = self.shardFor(name);
        shard.mutex.lock();
        defer shard.mutex.unlock();

        const topic = shard.manager.getOrCreate(name) catch return .failed;
        conn.addSubscription(name) catch return .failed;
        topic.addSubscriber(conn) catch {
            conn.removeSubscription(name);
            return .failed;
        };
        return .ok;
    }

    fn unsubscribe(self: *ShardedTopicManager, topic_name: []const u8, conn: *Connection) void {
        const shard = self.shardFor(topic_name);
        shard.mutex.lock();
        defer shard.mutex.unlock();

        shard.manager.unsubscribe(topic_name, conn);
    }

    fn snapshotForPublish(self: *ShardedTopicManager, allocator: mem.Allocator, topic_name: []const u8) TopicPublishResult {
        const shard = self.shardFor(topic_name);
        shard.mutex.lock();
        defer shard.mutex.unlock();

        const snapshot = shard.manager.snapshotSubscribers(allocator, topic_name) catch {
            return .out_of_memory;
        };
        if (snapshot) |owned| {
            return .{ .snapshot = owned };
        }
        return .no_subscribers;
    }

    fn count(self: *ShardedTopicManager) usize {
        var total: usize = 0;
        for (&self.shards) |*shard| {
            shard.mutex.lock();
            total += shard.manager.topics.count();
            shard.mutex.unlock();
        }
        return total;
    }

    fn appendTopicStats(self: *ShardedTopicManager, allocator: mem.Allocator, out: *std.ArrayList(u8)) !usize {
        var total: usize = 0;
        for (&self.shards) |*shard| {
            {
                shard.mutex.lock();
                defer shard.mutex.unlock();
                var it = shard.manager.topics.iterator();
                while (it.next()) |entry| {
                    const topic = entry.value_ptr.*;
                    try out.writer(allocator).print("{s}({d})\r\n", .{ entry.key_ptr.*, topic.subscribers.items.len });
                    total += 1;
                }
            }
        }
        return total;
    }
};

const SharedState = struct {
    queue_manager: ShardedQueueManager,
    topic_manager: ShardedTopicManager,

    fn init(allocator: mem.Allocator) SharedState {
        return .{
            .queue_manager = ShardedQueueManager.init(allocator),
            .topic_manager = ShardedTopicManager.init(allocator),
        };
    }

    fn deinit(self: *SharedState) void {
        self.queue_manager.deinit();
        self.topic_manager.deinit();
    }
};

pub const Server = struct {
    allocator: mem.Allocator,
    state: SharedState,
    config: Config,
    listener: net.Server,

    pub fn init(allocator: mem.Allocator, config: Config) !Server {
        const addr = try net.Address.parseIp("0.0.0.0", config.port);
        const listener = try addr.listen(.{ .reuse_address = true });

        return .{
            .allocator = allocator,
            .state = SharedState.init(allocator),
            .config = config,
            .listener = listener,
        };
    }

    pub fn deinit(self: *Server) void {
        self.state.deinit();
    }

    fn cleanupConnection(self: *Server, conn: *Connection) void {
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
            conn.close();
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
            if (trimmed.len == 0) {
                if (conn.isResponseBatching()) try conn.endResponse(conn.hasBufferedLine());
                continue;
            }

            const has_more_input = conn.hasBufferedLine();
            if (has_more_input or conn.isResponseBatching()) conn.beginResponse(has_more_input);
            const cmd = Command.parse(trimmed) catch {
                try conn.writeBuffered("-ERR invalid command\r\n");
                if (has_more_input or conn.isResponseBatching()) try conn.endResponse(conn.hasBufferedLine());
                continue;
            };

            try self.handleCommand(conn, cmd);
            if (has_more_input or conn.isResponseBatching()) try conn.endResponse(conn.hasBufferedLine());
        }
    }

    fn buildQueuesResponse(self: *Server) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        try out.append(self.allocator, '+');
        const count = try self.state.queue_manager.appendNames(self.allocator, &out);
        if (count == 0) {
            try out.appendSlice(self.allocator, "\r\n");
        }

        return try out.toOwnedSlice(self.allocator);
    }

    fn buildTopicsResponse(self: *Server) ![]u8 {
        var out = std.ArrayList(u8).empty;
        errdefer out.deinit(self.allocator);

        try out.append(self.allocator, '+');
        const count = try self.state.topic_manager.appendTopicStats(self.allocator, &out);
        if (count == 0) {
            try out.appendSlice(self.allocator, "\r\n");
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

    fn broadcastSnapshot(self: *Server, snapshot: *PublishSnapshot, topic_name: []const u8, msg: []const u8) usize {
        _ = self;
        var delivered: usize = 0;
        for (snapshot.items()) |conn| {
            conn.writeTopicMessage(topic_name, msg) catch continue;
            delivered += 1;
        }

        return delivered;
    }

    fn handleCommand(self: *Server, conn: *Connection, cmd: Command) !void {
        if (cmd.kind == .ping) {
            try conn.writeBuffered("+PONG\r\n");
            return;
        }

        if (cmd.kind == .info) {
            const queue_count = self.state.queue_manager.count();
            const topic_count = self.state.topic_manager.count();

            var out = std.ArrayList(u8).empty;
            defer out.deinit(self.allocator);
            try out.writer(self.allocator).print("+{s}\r\nqueues:{d}\r\ntopics:{d}\r\ninitial_capacity:{d}\r\nmax_capacity:{d}\r\n", .{
                version.banner,
                queue_count,
                topic_count,
                self.config.queue_capacity,
                self.config.max_queue_capacity,
            });
            try conn.writeBuffered(out.items);
            return;
        }

        if (cmd.kind == .push or cmd.kind == .send) {
            if (cmd.queue == null or cmd.body == null) {
                try conn.writeBuffered("-ERR need queue and body\r\n");
                return;
            }

            const msg = Message.init(self.allocator, 0, cmd.body.?) catch {
                try conn.writeBuffered("-ERR out of memory\r\n");
                return;
            };

            switch (self.state.queue_manager.push(
                self.allocator,
                cmd.queue.?,
                msg,
                self.config.queue_capacity,
                self.config.max_queue_capacity,
            )) {
                .ok => try conn.writeBuffered("+OK\r\n"),
                .queue_full => {
                    msg.deinit(self.allocator);
                    try conn.writeBuffered("-ERR queue full\r\n");
                },
                .out_of_memory => {
                    msg.deinit(self.allocator);
                    try conn.writeBuffered("-ERR out of memory\r\n");
                },
                .create_failed => {
                    msg.deinit(self.allocator);
                    try conn.writeBuffered("-ERR create queue failed\r\n");
                },
            }
            return;
        }

        if (cmd.kind == .pop or cmd.kind == .recv) {
            if (cmd.queue == null) {
                try conn.writeBuffered("-ERR need queue name\r\n");
                return;
            }

            switch (self.state.queue_manager.pop(cmd.queue.?)) {
                .queue_not_found => try conn.writeBuffered("-ERR queue not found\r\n"),
                .empty => try conn.writeBuffered("-ERR empty\r\n"),
                .message => |msg| {
                    defer msg.deinit(self.allocator);
                    try conn.writeBulkBuffered(msg.bytes());
                },
            }
            return;
        }

        if (cmd.kind == .peek) {
            if (cmd.queue == null) {
                try conn.writeBuffered("-ERR need queue name\r\n");
                return;
            }

            switch (self.state.queue_manager.peekCopy(self.allocator, cmd.queue.?)) {
                .queue_not_found => try conn.writeBuffered("-ERR queue not found\r\n"),
                .empty => try conn.writeBuffered("-ERR empty\r\n"),
                .out_of_memory => try conn.writeBuffered("-ERR out of memory\r\n"),
                .body => |body| {
                    defer self.allocator.free(body);
                    try conn.writeBulkBuffered(body);
                },
            }
            return;
        }

        if (cmd.kind == .len) {
            if (cmd.queue == null) {
                try conn.writeBuffered("-ERR need queue name\r\n");
                return;
            }

            if (self.state.queue_manager.len(cmd.queue.?)) |len| {
                var buf: [32]u8 = undefined;
                const response = try fmt.bufPrint(&buf, "+{d}\r\n", .{len});
                try conn.writeBuffered(response);
            } else {
                try conn.writeBuffered("-ERR queue not found\r\n");
            }
            return;
        }

        if (cmd.kind == .queues) {
            const response = try self.buildQueuesResponse();
            defer self.allocator.free(response);

            try conn.writeBuffered(response);
            return;
        }

        if (cmd.kind == .qcreate or cmd.kind == .mq) {
            if (cmd.queue == null) {
                try conn.writeBuffered("-ERR need queue name\r\n");
                return;
            }

            if (self.state.queue_manager.ensureQueue(cmd.queue.?, self.config.queue_capacity, self.config.max_queue_capacity)) {
                try conn.writeBuffered("+OK\r\n");
            } else {
                try conn.writeBuffered("-ERR create queue failed\r\n");
            }
            return;
        }

        if (cmd.kind == .sub) {
            if (cmd.queue == null) {
                try conn.writeBuffered("-ERR need topic name\r\n");
                return;
            }

            switch (self.state.topic_manager.subscribe(cmd.queue.?, conn)) {
                .ok => try conn.writeBuffered("+OK\r\n"),
                .failed => try conn.writeBuffered("-ERR subscribe failed\r\n"),
            }
            return;
        }

        if (cmd.kind == .unsub) {
            if (cmd.queue) |topic_name| {
                self.state.topic_manager.unsubscribe(topic_name, conn);
                conn.removeSubscription(topic_name);
            } else {
                for (conn.subscriptions.items) |topic_name| {
                    self.state.topic_manager.unsubscribe(topic_name, conn);
                }
                conn.clearSubscriptions();
            }

            try conn.writeBuffered("+OK\r\n");
            return;
        }

        if (cmd.kind == .@"pub") {
            if (cmd.queue == null or cmd.body == null) {
                try conn.writeBuffered("-ERR need topic and message\r\n");
                return;
            }

            switch (self.state.topic_manager.snapshotForPublish(self.allocator, cmd.queue.?)) {
                .out_of_memory => try conn.writeBuffered("-ERR publish failed\r\n"),
                .no_subscribers => try conn.writeBuffered("+OK 0\r\n"),
                .snapshot => |snapshot| {
                    var owned_snapshot = snapshot;
                    defer owned_snapshot.deinit();

                    try conn.flushBuffered();
                    const count = self.broadcastSnapshot(&owned_snapshot, cmd.queue.?, cmd.body.?);
                    var buf: [32]u8 = undefined;
                    const response = try fmt.bufPrint(&buf, "+OK {d}\r\n", .{count});
                    try conn.writeBuffered(response);
                },
            }
            return;
        }

        if (cmd.kind == .topics) {
            const response = try self.buildTopicsResponse();
            defer self.allocator.free(response);

            try conn.writeBuffered(response);
            return;
        }

        if (cmd.kind == .subs) {
            const response = try self.buildSubscriptionsResponse(conn);
            defer self.allocator.free(response);

            try conn.writeBuffered(response);
            return;
        }

        try conn.writeBuffered("-ERR unknown command\r\n");
    }

    pub fn run(self: *Server) !void {
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

test "broadcast count reflects successful deliveries" {
    var server = Server{
        .allocator = testing.allocator,
        .state = SharedState.init(testing.allocator),
        .config = Config{},
        .listener = undefined,
    };
    defer server.state.deinit();

    var conn = Connection.init(undefined, testing.allocator);
    defer conn.deinit();
    conn.closed = true;

    conn.retain();
    const subscribers = try testing.allocator.alloc(*Connection, 1);
    subscribers[0] = &conn;

    var snapshot = PublishSnapshot{
        .allocator = testing.allocator,
        .subscriber_count = 1,
        .heap_subscribers = subscribers,
    };
    defer snapshot.deinit();

    const count = server.broadcastSnapshot(&snapshot, "news", "hello");
    try testing.expectEqual(@as(usize, 0), count);
}
