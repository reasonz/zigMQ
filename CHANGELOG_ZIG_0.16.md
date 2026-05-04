# ZigMQ 升级到 Zig 0.16.0 变更记录

## 基本信息

- **升级前版本**: Zig 0.15.2
- **升级后版本**: Zig 0.16.0
- **升级日期**: 2026-05-03
- **涉及文件**: 8 个文件，203 行新增，257 行删除

## API 适配变更

### 1. Juicy Main (`src/main.zig`)

Zig 0.16 引入 `std.process.Init`，替代手动 GPA 初始化。

```zig
// 0.15.2
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    // ...
}

// 0.16.0
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    // ...
}
```

### 2. 构建系统 (`build.zig`)

- `std.fs.cwd().readFileAlloc` → `@embedFile("VERSION")`（避免 build-time Io 依赖）
- `std.mem.trimRight` → `std.mem.trimEnd`

### 3. I/O 系统 (`src/connection.zig`, `src/server.zig`, `src/queue.zig`)

Zig 0.16 的标志性变更：所有 I/O 操作需要显式传入 `io: std.Io` 参数。

| 0.15.2 | 0.16.0 |
|--------|--------|
| `std.net.Stream` | `std.Io.net.Stream` |
| `std.net.Server` | `std.Io.net.Server` |
| `std.net.IpAddress.parse("0.0.0.0", port)` | `ionet.IpAddress.parseIp4("0.0.0.0", port)` |
| `stream.reader()` | `stream.reader(io, &buffer)` |
| `stream.writer()` | `stream.writer(io, &buffer)` |
| `addr.listen(.{...})` | `addr.listen(io, .{...})` |
| `listener.accept()` | `listener.accept(io)` |
| `stream.close()` | `stream.close(io)` |
| `reader.fill()` (无参数) | `reader.fillMore()` (单次非阻塞读取) |

### 4. 互斥锁 (`src/connection.zig`, `src/server.zig`, `src/queue.zig`)

```zig
// 0.15.2
mutex: std.Thread.Mutex = .{},
mutex.lock();
mutex.unlock();

// 0.16.0
mutex: std.Io.Mutex = .init,
mutex.lockUncancelable(io);
mutex.unlock(io);
```

### 5. ArrayList 非托管 (`src/server.zig`)

Zig 0.16 中 `std.ArrayList` 不再内部存储 Allocator，所有方法需要显式传入。

```zig
// 0.15.2
var out = std.ArrayList(u8).init(allocator);
try out.append('+');
try out.appendSlice("\r\n");
return out.toOwnedSlice();

// 0.16.0
var out: std.ArrayList(u8) = .empty;
try out.append(allocator, '+');
try out.appendSlice(allocator, "\r\n");
return out.toOwnedSlice(allocator);
```

### 6. writeVecAll 变更 (`src/connection.zig`)

```zig
// 0.15.2
try self.writer.writeVecAll(&.{ header, body, "\r\n" });

// 0.16.0
var parts = [_][]const u8{ header, body, "\r\n" };
try self.writer.interface.writeVecAll(&parts);
```

`Stream.Writer` 在 0.16 中是薄封装，所有写操作需通过 `.interface`：
- `self.writer.flush()` → `self.writer.interface.flush()`
- `self.writer.writeAll(data)` → `self.writer.interface.writeAll(data)`

### 7. Server 监听 (`src/server.zig`)

```zig
// 0.15.2
const addr = try net.IpAddress.parse("0.0.0.0", config.port);
const listener = try addr.listen(.{ .reuse_address = true });

// 0.16.0
const addr = try ionet.IpAddress.parseIp4("0.0.0.0", config.port);
const listener = try addr.listen(io, .{ .reuse_address = true });
```

### 8. Thread 启动

`std.Thread.spawn(.{}, ...)` 和 `thread.detach()` 在 0.16 中保持不变。

## 关键 Bug 修复

### Bug 1: 堆内存损坏（栈指针逃逸）

**问题**: `Connection.init()` 在栈上创建 Connection 结构体，`reader.interface.buffer` 指向栈上的 `self.read_buffer`。通过 `conn.* = init(...)` 拷贝到堆上后，buffer 指针仍然指向已释放的栈内存，导致主题名称等数据被下一个 `accept()` 调用覆盖。

**修复**: 将 `init` 改为接收 `*Connection` 指针，直接在堆内存上初始化：

```zig
// 修复前
pub fn init(...) Connection {
    var self: Connection = undefined;
    self.reader = stream.reader(io, &self.read_buffer); // 指向栈地址
    return self;
}
conn.* = Connection.init(stream, io, allocator); // 拷贝后指针悬空

// 修复后
pub fn init(self: *Connection, ...) void {
    self.reader = stream.reader(io, &self.read_buffer); // 指向堆地址
}
conn.init(stream, io, allocator);
```

### Bug 2: 管道命令死循环

**问题**: 当 reader buffer 满（16KB）且剩余数据不包含完整行（无 `\n`）时，`rebase(1)` 检查到 `seek` 之前有空闲空间就不做搬移，然后 `continue` 跳回循环顶部，既不调用 `fillMore` 读取新数据也不释放空间，造成死循环。

**修复**: rebase 后不再 `continue`，改为 fall through 到 `fillMore`，确保实际读取新数据。

## 测试结果

### 单元测试
全部通过。

### 协议测试 (5/5)
- queue_basics — 队列基本操作
- queue_growth_and_fifo — 队列扩容与顺序
- pubsub_flow — 发布订阅流程
- pipelining — 命令管道
- empty_topic_cleanup — 空主题清理

### 性能测试 (5/5)
| 测试场景 | 吞吐量 |
|----------|--------|
| queue_contention | 51,496 ops/s |
| independent_queue_roundtrips | 47,680 ops/s |
| pubsub_fanout | 35,456 deliveries/s |
| pipelined_send | 399,002 ops/s |
| pipelined_recv | 550,307 ops/s |
