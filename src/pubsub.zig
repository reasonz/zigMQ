const std = @import("std");
const mem = std.mem;
const testing = std.testing;

const Connection = @import("connection.zig").Connection;

pub const PublishSnapshot = struct {
    allocator: mem.Allocator,
    subscriber_count: usize = 0,
    heap_subscribers: ?[]*Connection = null,
    inline_subscribers: [inline_capacity]*Connection = undefined,

    pub const inline_capacity = 8;

    pub fn items(self: *PublishSnapshot) []*Connection {
        return if (self.heap_subscribers) |subscribers| subscribers else self.inline_subscribers[0..self.subscriber_count];
    }

    pub fn deinit(self: *PublishSnapshot) void {
        for (self.items()) |conn| {
            conn.release(self.allocator);
        }
        if (self.heap_subscribers) |subscribers| {
            self.allocator.free(subscribers);
        }
    }
};

pub const Topic = struct {
    name: []const u8,
    subscribers: std.ArrayList(*Connection),
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator, name: []const u8) !Topic {
        return .{
            .name = try allocator.dupe(u8, name),
            .subscribers = std.ArrayList(*Connection).empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Topic) void {
        self.subscribers.deinit(self.allocator);
        self.allocator.free(self.name);
    }

    pub fn addSubscriber(self: *Topic, conn: *Connection) !void {
        for (self.subscribers.items) |existing| {
            if (existing == conn) return;
        }
        try self.subscribers.append(self.allocator, conn);
    }

    pub fn removeSubscriber(self: *Topic, conn: *Connection) void {
        for (self.subscribers.items, 0..) |existing, i| {
            if (existing == conn) {
                _ = self.subscribers.swapRemove(i);
                return;
            }
        }
    }

    pub fn snapshot(self: *Topic, allocator: mem.Allocator) !PublishSnapshot {
        var publish_snapshot = PublishSnapshot{
            .allocator = allocator,
            .subscriber_count = self.subscribers.items.len,
        };
        if (publish_snapshot.subscriber_count > PublishSnapshot.inline_capacity) {
            publish_snapshot.heap_subscribers = try allocator.alloc(*Connection, publish_snapshot.subscriber_count);
        }

        const subscribers = publish_snapshot.items();
        for (self.subscribers.items, 0..) |conn, i| {
            conn.retain();
            subscribers[i] = conn;
        }

        return publish_snapshot;
    }
};

pub const TopicManager = struct {
    topics: std.StringHashMap(*Topic),
    allocator: mem.Allocator,

    pub fn init(allocator: mem.Allocator) TopicManager {
        return .{
            .topics = std.StringHashMap(*Topic).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TopicManager) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.topics.deinit();
    }

    pub fn getOrCreate(self: *TopicManager, name: []const u8) !*Topic {
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

    pub fn pruneIfEmpty(self: *TopicManager, topic_name: []const u8) void {
        if (self.topics.get(topic_name)) |topic| {
            if (topic.subscribers.items.len == 0) {
                if (self.topics.fetchRemove(topic_name)) |entry| {
                    entry.value.deinit();
                    self.allocator.destroy(entry.value);
                }
            }
        }
    }

    pub fn snapshotSubscribers(self: *TopicManager, allocator: mem.Allocator, topic_name: []const u8) !?PublishSnapshot {
        const topic = self.topics.get(topic_name) orelse return null;
        if (topic.subscribers.items.len == 0) {
            self.pruneIfEmpty(topic_name);
            return null;
        }

        return try topic.snapshot(allocator);
    }

    pub fn unsubscribe(self: *TopicManager, topic_name: []const u8, conn: *Connection) void {
        if (self.topics.get(topic_name)) |topic| {
            topic.removeSubscriber(conn);
            self.pruneIfEmpty(topic_name);
        }
    }
};

test "topic snapshot retains subscribers until released" {
    var topic = try Topic.init(testing.allocator, "news");
    defer topic.deinit();

    var conn_a: Connection = undefined;
    conn_a.init(undefined, testing.io, testing.allocator);
    defer conn_a.deinit();
    var conn_b: Connection = undefined;
    conn_b.init(undefined, testing.io, testing.allocator);
    defer conn_b.deinit();

    try topic.addSubscriber(&conn_a);
    try topic.addSubscriber(&conn_b);

    var snapshot = try topic.snapshot(testing.allocator);

    try testing.expectEqual(@as(usize, 2), snapshot.items().len);
    try testing.expectEqual(@as(usize, 2), conn_a.ref_count.load(.seq_cst));
    try testing.expectEqual(@as(usize, 2), conn_b.ref_count.load(.seq_cst));

    snapshot.deinit();
    try testing.expectEqual(@as(usize, 1), conn_a.ref_count.load(.seq_cst));
    try testing.expectEqual(@as(usize, 1), conn_b.ref_count.load(.seq_cst));
}

test "topic manager prunes empty topics before publish snapshot" {
    var manager = TopicManager.init(testing.allocator);
    defer manager.deinit();

    _ = try manager.getOrCreate("ghost");
    try testing.expectEqual(@as(usize, 1), manager.topics.count());

    const snapshot = try manager.snapshotSubscribers(testing.allocator, "ghost");
    try testing.expect(snapshot == null);
    try testing.expectEqual(@as(usize, 0), manager.topics.count());
}

test "topic snapshot stores small subscriber lists inline" {
    var topic = try Topic.init(testing.allocator, "news");
    defer topic.deinit();

    var conns: [PublishSnapshot.inline_capacity]Connection = undefined;
    for (&conns, 0..) |*conn, i| {
        conn.init(undefined, testing.io, testing.allocator);
        defer conn.deinit();
        try topic.addSubscriber(conn);
        try testing.expectEqual(@as(usize, 1), conn.ref_count.load(.seq_cst));
        _ = i;
    }

    var snapshot = try topic.snapshot(testing.allocator);
    defer snapshot.deinit();

    try testing.expect(snapshot.heap_subscribers == null);
    try testing.expectEqual(@as(usize, PublishSnapshot.inline_capacity), snapshot.items().len);
}
