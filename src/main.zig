const std = @import("std");
const net = std.net;
const heap = std.heap;
const mem = std.mem;
const fmt = std.fmt;
const time = std.time;

const Config = struct {
    port: u16 = 6379,
    mode: enum { standalone, master, worker } = .standalone,
    workers: u32 = 1,
    queue_capacity: usize = 10000,
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

    fn push(self: *RingBuffer, msg: Message) void {
        if (self.len == self.capacity) return;
        self.messages[self.tail] = msg;
        self.tail = (self.tail + 1) % self.capacity;
        self.len += 1;
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

    fn init(allocator: mem.Allocator, name: []const u8, capacity: usize) !Queue {
        return .{
            .buffer = try RingBuffer.init(allocator, capacity),
            .overflow = .block,
            .name = name,
        };
    }

    fn deinit(self: *Queue) void {
        self.buffer.deinit();
    }
};

// Forward declaration for Connection
const Connection = struct {
    stream: net.Stream,
    buffer: [4096]u8 = undefined,
    pos: usize = 0,
    allocator: mem.Allocator,
    subscriptions: std.ArrayList([]const u8),
    subscribed: bool = false,

    fn init(stream: net.Stream, allocator: mem.Allocator) Connection {
        return .{
            .stream = stream,
            .allocator = allocator,
            .subscriptions = std.ArrayList([]const u8).empty,
        };
    }

    fn deinit(self: *Connection) void {
        self.subscriptions.deinit(self.allocator);
    }

    fn addSubscription(self: *Connection, topic: []const u8) !void {
        for (self.subscriptions.items) |t| {
            if (mem.eql(u8, t, topic)) return;
        }
        const copy = try self.allocator.dupe(u8, topic);
        try self.subscriptions.append(self.allocator, copy);
    }

    fn removeSubscription(self: *Connection, topic: []const u8) void {
        for (self.subscriptions.items, 0..) |t, i| {
            if (mem.eql(u8, t, topic)) {
                self.allocator.free(t);
                _ = self.subscriptions.swapRemove(i);
                return;
            }
        }
    }

    fn clearSubscriptions(self: *Connection) void {
        for (self.subscriptions.items) |t| {
            self.allocator.free(t);
        }
        self.subscriptions.clearRetainingCapacity();
    }

    fn readLine(self: *Connection) !?[]const u8 {
        while (true) {
            if (mem.indexOfScalar(u8, self.buffer[0..self.pos], '\n')) |idx| {
                const line = self.buffer[0..idx];
                const remaining = self.pos - idx - 1;
                for (0..remaining) |i| {
                    self.buffer[i] = self.buffer[idx + 1 + i];
                }
                self.pos = remaining;
                return line;
            }
            if (self.pos > 0) {
                if (self.pos > self.buffer.len / 2) {
                    for (0..self.pos) |i| {
                        self.buffer[i] = self.buffer[self.pos + i];
                    }
                }
            }
            if (self.pos >= self.buffer.len) {
                return error.BufferFull;
            }
            const n = try self.stream.read(self.buffer[self.pos .. self.buffer.len]);
            if (n == 0) return null;
            self.pos += n;
        }
    }

    fn write(self: *Connection, data: []const u8) !void {
        try self.stream.writeAll(data);
    }
};

// ============ Pub/Sub ============

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
        for (self.subscribers.items) |c| {
            if (c == conn) return;
        }
        try self.subscribers.append(self.allocator, conn);
    }

    fn removeSubscriber(self: *Topic, conn: *Connection) void {
        for (self.subscribers.items, 0..) |c, i| {
            if (c == conn) {
                _ = self.subscribers.swapRemove(i);
                return;
            }
        }
    }

    fn broadcast(self: *Topic, msg: []const u8) !void {
        const payload = try fmt.allocPrint(self.allocator, "+{s}:{s}\r\n", .{ self.name, msg });
        defer self.allocator.free(payload);
        for (self.subscribers.items) |sub| {
            sub.stream.writeAll(payload) catch {};
        }
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
        }
        self.topics.deinit();
    }

    fn getOrCreate(self: *TopicManager, name: []const u8) !*Topic {
        // First try to find existing topic
        if (self.topics.get(name)) |topic| {
            return topic;
        }
        // Not found, create new one
        // Duplicate name to store in Topic (Topic owns this copy)
        const name_dup = try self.allocator.dupe(u8, name);
        const topic = try self.allocator.create(Topic);
        topic.* = try Topic.init(self.allocator, name_dup);
        errdefer self.allocator.destroy(topic);
        // Store topic pointer in HashMap - the key is the name_dup that Topic owns
        try self.topics.put(topic.name, topic);
        return topic;
    }

    fn removeSubscriberAll(self: *TopicManager, conn: *Connection) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.removeSubscriber(conn);
        }
    }

    fn subscriberCount(self: *TopicManager) usize {
        return self.topics.count();
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
        }
        self.queues.deinit();
    }

    fn getOrCreate(self: *QueueManager, name: []const u8, capacity: usize) !*Queue {
        if (self.queues.get(name)) |q| {
            return q;
        }
        const name_copy = try self.allocator.dupe(u8, name);
        const queue = try self.allocator.create(Queue);
        queue.* = try Queue.init(self.allocator, name_copy, capacity);
        try self.queues.put(name_copy, queue);
        return queue;
    }
};

const Command = struct {
    op: []const u8,
    queue: ?[]const u8,
    body: ?[]const u8,

    fn parse(line: []const u8) !Command {
        var parts = std.mem.splitScalar(u8, line, ' ');
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

var global_msg_id: u64 = 0;
fn nextMsgId() u64 {
    global_msg_id += 1;
    return global_msg_id;
}

const Server = struct {
    allocator: mem.Allocator,
    queue_manager: QueueManager,
    topic_manager: TopicManager,
    config: Config,
    listener: net.Server,

    fn init(allocator: mem.Allocator, config: Config) !Server {
        const addr = try net.Address.parseIp("0.0.0.0", config.port);
        const listener = try addr.listen(.{ .reuse_address = true });

        return .{
            .allocator = allocator,
            .queue_manager = QueueManager.init(allocator),
            .topic_manager = TopicManager.init(allocator),
            .config = config,
            .listener = listener,
        };
    }

    fn deinit(self: *Server) void {
        self.queue_manager.deinit();
        self.topic_manager.deinit();
    }

    fn handleConnection(self: *Server, stream: net.Server.Connection) !void {
        const client_stream = stream.stream;
        var conn = Connection.init(client_stream, self.allocator);
        defer {
            self.topic_manager.removeSubscriberAll(&conn);
            conn.clearSubscriptions();
            conn.deinit();
            client_stream.close();
        }

        while (true) {
            const line = conn.readLine() catch break;
            const l = line orelse break;

            const trimmed = mem.trimRight(u8, l, "\r\n");
            if (trimmed.len == 0) continue;

            const cmd = Command.parse(trimmed) catch {
                try conn.write("-ERR invalid command\r\n");
                continue;
            };

            try self.handleCommand(&conn, cmd);
        }
    }

    fn handleCommand(self: *Server, conn: *Connection, cmd: Command) !void {
        const qm = &self.queue_manager;

        if (mem.eql(u8, cmd.op, "PING")) {
            try conn.write("+PONG\r\n");
            return;
        }

        if (mem.eql(u8, cmd.op, "INFO")) {
            var buf: [256]u8 = undefined;
            const s = try fmt.bufPrint(&buf, "+ZigMQ 0.1.0\r\nqueues:{d}\r\n", .{qm.queues.count()});
            try conn.write(s);
            return;
        }

        if (mem.eql(u8, cmd.op, "PUSH") or mem.eql(u8, cmd.op, "send")) {
            if (cmd.body == null or cmd.queue == null) {
                try conn.write("-ERR need body\r\n");
                return;
            }
            const queue = qm.getOrCreate(cmd.queue.?, self.config.queue_capacity) catch {
                try conn.write("-ERR create queue failed\r\n");
                return;
            };

            const msg = Message.init(self.allocator, nextMsgId(), cmd.body.?) catch {
                try conn.write("-ERR out of memory\r\n");
                return;
            };
            queue.buffer.push(msg);
            try conn.write("+OK\r\n");
            return;
        }

        if (mem.eql(u8, cmd.op, "POP") or mem.eql(u8, cmd.op, "recv")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }
            const queue = qm.queues.get(cmd.queue.?) orelse {
                try conn.write("-ERR queue not found\r\n");
                return;
            };

            if (queue.buffer.pop()) |msg| {
                const body_len = msg.body.len;
                const resp = try fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ body_len, msg.body });
                msg.deinit(self.allocator);
                try conn.write(resp);
                self.allocator.free(resp);
            } else {
                try conn.write("-ERR empty\r\n");
            }
            return;
        }

        if (mem.eql(u8, cmd.op, "PEEK") or mem.eql(u8, cmd.op, "peek")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }
            const queue = qm.queues.get(cmd.queue.?) orelse {
                try conn.write("-ERR queue not found\r\n");
                return;
            };

            if (queue.buffer.peek()) |msg| {
                const resp = try fmt.allocPrint(self.allocator, "${d}\r\n{s}\r\n", .{ msg.body.len, msg.body });
                try conn.write(resp);
                self.allocator.free(resp);
            } else {
                try conn.write("-ERR empty\r\n");
            }
            return;
        }

        if (mem.eql(u8, cmd.op, "LEN") or mem.eql(u8, cmd.op, "len")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }
            const queue = qm.queues.get(cmd.queue.?) orelse {
                try conn.write("-ERR queue not found\r\n");
                return;
            };
            var buf: [32]u8 = undefined;
            const s = try fmt.bufPrint(&buf, "+{d}\r\n", .{queue.buffer.len});
            try conn.write(s);
            return;
        }

        if (mem.eql(u8, cmd.op, "QUEUES") or mem.eql(u8, cmd.op, "queues")) {
            var it = qm.queues.iterator();
            var buf: [1024]u8 = undefined;
            var pos: usize = 0;
            buf[pos] = '+';
            pos += 1;
            while (it.next()) |entry| {
                const name = entry.key_ptr.*;
                if (pos + name.len + 2 > buf.len) break;
                @memcpy(buf[pos..pos + name.len], name);
                pos += name.len;
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            if (pos == 1) {
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            try conn.write(buf[0..pos]);
            return;
        }

        if (mem.eql(u8, cmd.op, "QCREATE") or mem.eql(u8, cmd.op, "mq")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need queue name\r\n");
                return;
            }
            _ = qm.getOrCreate(cmd.queue.?, self.config.queue_capacity) catch {
                try conn.write("-ERR create queue failed\r\n");
                return;
            };
            try conn.write("+OK\r\n");
            return;
        }

        // ============ Pub/Sub Commands ============

        // SUB <topic> - 订阅主题
        if (mem.eql(u8, cmd.op, "SUB") or mem.eql(u8, cmd.op, "sub")) {
            if (cmd.queue == null) {
                try conn.write("-ERR need topic name\r\n");
                return;
            }
            const tm = &self.topic_manager;
            const topic = tm.getOrCreate(cmd.queue.?) catch {
                try conn.write("-ERR create topic failed\r\n");
                return;
            };
            topic.addSubscriber(conn) catch {
                try conn.write("-ERR subscribe failed\r\n");
                return;
            };
            conn.addSubscription(cmd.queue.?) catch {
                try conn.write("-ERR track subscription failed\r\n");
                return;
            };
            conn.subscribed = true;
            try conn.write("+OK\r\n");
            return;
        }

        // UNSUB [topic] - 取消订阅（省略topic则取消全部）
        if (mem.eql(u8, cmd.op, "UNSUB") or mem.eql(u8, cmd.op, "unsub")) {
            const tm = &self.topic_manager;
            if (cmd.queue) |topic_name| {
                // Only remove from the specific topic
                if (tm.topics.get(topic_name)) |topic| {
                    topic.removeSubscriber(conn);
                }
                conn.removeSubscription(topic_name);
            } else {
                // Remove from all topics
                tm.removeSubscriberAll(conn);
                conn.clearSubscriptions();
            }
            if (conn.subscriptions.items.len == 0) {
                conn.subscribed = false;
            }
            try conn.write("+OK\r\n");
            return;
        }

        // PUB <topic> <msg> - 发布消息
        if (mem.eql(u8, cmd.op, "PUB") or mem.eql(u8, cmd.op, "pub")) {
            if (cmd.queue == null or cmd.body == null) {
                try conn.write("-ERR need topic and message\r\n");
                return;
            }
            const tm = &self.topic_manager;
            const topic = tm.getOrCreate(cmd.queue.?) catch {
                try conn.write("-ERR create topic failed\r\n");
                return;
            };
            try topic.broadcast(cmd.body.?);
            const cnt = topic.subscribers.items.len;
            var buf: [32]u8 = undefined;
            const s = try fmt.bufPrint(&buf, "+OK {d}\r\n", .{cnt});
            try conn.write(s);
            return;
        }

        // TOPICS - 列出所有主题及订阅数
        if (mem.eql(u8, cmd.op, "TOPICS") or mem.eql(u8, cmd.op, "topics")) {
            const tm = &self.topic_manager;
            var it = tm.topics.iterator();
            var buf: [2048]u8 = undefined;
            var pos: usize = 0;
            buf[pos] = '+';
            pos += 1;
            while (it.next()) |entry| {
                const name = entry.key_ptr.*;
                const cnt = entry.value_ptr.*.subscribers.items.len;
                if (pos + name.len + 32 > buf.len) break;
                @memcpy(buf[pos..pos + name.len], name);
                pos += name.len;
                buf[pos] = '(';
                pos += 1;
                const cnt_str = fmt.allocPrint(self.allocator, "{d}", .{cnt}) catch break;
                defer self.allocator.free(cnt_str);
                @memcpy(buf[pos..pos + cnt_str.len], cnt_str);
                pos += cnt_str.len;
                buf[pos] = ')';
                pos += 1;
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            if (pos == 1) {
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            try conn.write(buf[0..pos]);
            return;
        }

        // SUBS - 查看当前连接的订阅列表
        if (mem.eql(u8, cmd.op, "SUBS") or mem.eql(u8, cmd.op, "subs")) {
            var buf: [1024]u8 = undefined;
            var pos: usize = 0;
            buf[pos] = '+';
            pos += 1;
            for (conn.subscriptions.items) |topic| {
                if (pos + topic.len + 2 > buf.len) break;
                @memcpy(buf[pos..pos + topic.len], topic);
                pos += topic.len;
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            if (pos == 1) {
                buf[pos] = '\r';
                pos += 1;
                buf[pos] = '\n';
                pos += 1;
            }
            try conn.write(buf[0..pos]);
            return;
        }

        try conn.write("-ERR unknown command\r\n");
    }

    fn run(self: *Server) !void {
        std.debug.print("ZigMQ listening on port {d}\n", .{self.config.port});

        while (true) {
            const stream = try self.listener.accept();
            self.handleConnection(stream) catch |err| {
                std.debug.print("connection error: {}\n", .{err});
            };
        }
    }
};

fn parseArgs() Config {
    var args = std.process.args();
    _ = args.next();

    var config = Config{};

    while (args.next()) |arg| {
        if (mem.eql(u8, arg, "--port")) {
            if (args.next()) |v| {
                config.port = fmt.parseInt(u16, v, 10) catch 6379;
            }
        } else if (mem.eql(u8, arg, "--workers")) {
            if (args.next()) |v| {
                config.workers = fmt.parseInt(u32, v, 10) catch 1;
            }
        } else if (mem.eql(u8, arg, "--capacity")) {
            if (args.next()) |v| {
                config.queue_capacity = fmt.parseInt(usize, v, 10) catch 10000;
            }
        } else if (mem.eql(u8, arg, "--mode")) {
            if (args.next()) |v| {
                if (mem.eql(u8, v, "master")) {
                    config.mode = .master;
                } else if (mem.eql(u8, v, "worker")) {
                    config.mode = .worker;
                } else {
                    config.mode = .standalone;
                }
            }
        } else if (mem.eql(u8, arg, "--data-dir")) {
            config.data_dir = args.next();
        }
    }

    return config;
}

pub fn main() !void {
    var gpa = heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = parseArgs();

    var server = try Server.init(allocator, config);
    defer server.deinit();

    try server.run();
}
