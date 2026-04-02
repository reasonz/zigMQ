# ZigMQ: 极简消息队列 + 发布订阅

> 用 Zig 语言实现的轻量级消息队列服务，支持队列和发布/订阅两种模式。当前版本聚焦于多连接可用性、明确的队列满语义和更稳健的内存管理。

## 目录

1. [设计文档](#1-设计文档)
2. [开发调试过程](#2-开发调试过程)
3. [使用说明](#3-使用说明)
4. [理论性能指标](#4-理论性能指标)

---

## 1. 设计文档

### 1.1 核心目标

| 目标 | 描述 |
|------|------|
| **代码极简** | 单文件实现，保持结构紧凑 |
| **部署极简** | 单二进制文件，零外部依赖 |
| **上手极简** | 5 分钟即可熟练使用 |
| **模式融合** | 队列与发布/订阅和谐共存 |

### 1.2 数据结构

#### Ring Buffer（环形缓冲区）

采用固定容量数组实现 FIFO 队列：

```zig
const RingBuffer = struct {
    messages: []Message,
    capacity: usize,   // 固定容量
    head: usize = 0,   // 读指针
    tail: usize = 0,   // 写指针
    len: usize = 0,    // 当前长度
    allocator: mem.Allocator,
};
```

**优势：**
- O(1) 的入队/出队操作
- 预分配连续内存，零动态分配碎片
- 无 GC 停顿，内存布局紧凑

#### Message（消息结构）

```zig
const Message = struct {
    id: u64,              // 全局唯一 ID
    body: []u8,           // 消息体（独立分配）
    timestamp: i64,        // 时间戳
};
```

### 1.3 协议设计

采用类 Memcached 的极简文本协议。

#### 命令格式
```
COMMAND [ARG1] [ARG2]\r\n
```

#### 响应格式
```
成功: +OK\r\n
错误: -ERR <message>\r\n
消息: $<length>\r\n<Body>\r\n
发布: +<topic>:<message>\r\n
```

### 1.4 统一命令集

| 命令 | 格式 | 说明 | 别名 |
|------|------|------|------|
| **队列操作** ||||
| `send` | `send <queue> <msg>` | 发送消息到队列 | PUSH |
| `recv` | `recv <queue>` | 接收/弹出消息 | POP |
| `peek` | `peek <queue>` | 查看队列头部 | PEEK |
| `len` | `len <queue>` | 队列长度 | LEN |
| `queues` | `queues` | 列出所有队列 | QUEUES |
| `mq` | `mq <queue>` | 创建队列 | QCREATE |
| **发布/订阅** ||||
| `sub` | `sub <topic>` | 订阅主题 | SUB |
| `unsub` | `unsub [topic]` | 取消订阅（省略=全部） | UNSUB |
| `pub` | `pub <topic> <msg>` | 发布消息 | PUB |
| `topics` | `topics` | 列出所有主题 | TOPICS |
| `subs` | `subs` | 查看当前订阅列表 | SUBS |
| **系统** ||||
| `ping` | `ping` | 心跳检测 | PING |
| `info` | `info` | 服务器信息 | INFO |

### 1.5 架构设计

当前实现补充说明：

- 命令大小写不敏感，`ping` / `PING` / `PiNg` 都可用
- 连接模型为“每连接一个线程 + queue/topic 分离锁”
- 队列在写满时会按 2x 自动扩容，直到 `max_queue_capacity`
- 队列默认溢出策略为 `reject`，满队列时返回 `-ERR queue full`
- `PUB` 会先快照订阅者列表，再在锁外广播，避免慢订阅者长时间占住全局状态锁
- 全局消息 ID 使用原子自增，避免所有生产者争用同一把 ID 锁
- `peek` / `topics` / `queues` / `subs` 的响应不再依赖固定大小缓冲

```
┌─────────────────────────────────────────────────────────────┐
│                      ZigMQ Server                           │
│                                                             │
│  ┌─────────┐    ┌─────────────────┐    ┌──────────────┐  │
│  │ Listener│───>│ ConnectionHandler│───>│CommandDispatch│  │
│  └─────────┘    └─────────────────┘    └───────┬───────┘  │
│                                                │           │
│                        ┌───────────────────────┼───────────┤
│                        ▼                       ▼           │
│              ┌─────────────────┐    ┌─────────────────┐   │
│              │   QueueManager   │    │  TopicManager   │   │
│              │   (HashMap)      │    │   (HashMap)     │   │
│              └────────┬─────────┘    └────────┬────────┘   │
│                       │                       │             │
│        ┌──────────────┼───────────────┬──────┘             │
│        ▼              ▼               ▼                     │
│  ┌──────────┐  ┌──────────┐   ┌──────────────┐            │
│  │ Queue A  │  │ Queue B  │   │ Topic: news  │            │
│  │ [RB]     │  │ [RB]     │   │  ├─ Conn-A   │            │
│  └──────────┘  └──────────┘   │  ├─ Conn-B   │            │
│                                │  └─ Conn-C   │            │
│                                └──────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

### 1.6 发布/订阅特性

- **多端订阅**：多个客户端可同时订阅同一主题
- **广播发布**：发布消息自动广播给所有订阅者
- **主题无需创建**：`PUB` 自动创建主题，零配置
- **自动清理**：连接断开时自动移除所有订阅
- **消息格式**：`+<topic>:<message>` 前缀便于解析来源

---

## 2. 开发调试过程

### 2.1 环境问题与解决

#### Zig 版本兼容性

**问题：** Zig 0.16.0-dev 版本标准库不稳定，`std.net` 模块不存在。

**解决：**
```bash
curl -L https://ziglang.org/download/0.15.2/zig-aarch64-macos-0.15.2.tar.xz -o /tmp/zig-0.15.2.tar.xz
tar -xf /tmp/zig-0.15.2.tar.xz -C /tmp
/tmp/zig-aarch64-macos-0.15.2/zig build
```

### 2.2 核心 Bug 排查

#### Bug 1: StringHashMap 键查找失败

**现象：** PUSH 成功后 LEN 报错 queue not found。

**根因：** StringHashMap 要求键内存有效，Command 解析出的 `[]const u8` 指向临时 buffer。

**解决：** 在 QueueManager.getOrCreate 中先复制键字符串：
```zig
const name_copy = try allocator.dupe(u8, name);
try queues.put(name_copy, queue);
```

#### Bug 2: TCP 流协议数据覆盖

**现象：** PUSH 成功但 PEEK 数据错位。

**根因：** Message.body 指向 connection buffer，但 buffer 会被后续命令覆盖。

**解决：** Message 存储独立分配的数据副本。

#### Bug 3: UNSUB 命令误删订阅者

**现象：** SUB 成功后 topics 显示订阅数为 0。

**根因：** UNSUB 处理中错误调用 `removeSubscriberAll(conn)` 从所有主题移除订阅者。

**解决：** 改为只从指定主题移除。

### 2.3 Zig 0.15 API 变化

| 旧 API | 新 API |
|--------|--------|
| `std.ArrayList(T).init()` | `std.ArrayList(T).empty` |
| `arrayList.append(item)` | `arrayList.append(allocator, item)` |
| `arrayList.deinit()` | `arrayList.deinit(allocator)` |

---

## 3. 使用说明

### 3.1 安装

```bash
# 克隆项目
git clone <repo>
cd zigMQ

# 需要 Zig 0.15.2
zig version

# 构建
zig build

# 运行单元测试
zig build test

./zig-out/bin/zigmq
```

### 3.2 启动

```bash
# 默认配置（端口 6379）
./zig-out/bin/zigmq

# 自定义端口
./zig-out/bin/zigmq --port 8080

# 小队列 + 自动扩容上限
./zig-out/bin/zigmq --capacity 1024 --max-capacity 16384
```

### 3.3 队列操作

```python
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 6379))

def cmd(c):
    s.sendall(c.encode() + b'\r\n')
    return s.recv(1024).decode().strip()

# 发送消息
cmd("send q1 hello")      # +OK

# 查看长度
cmd("len q1")             # +1

# 接收消息
cmd("recv q1")            # $5\r\nhello

# 队列会自动扩容；达到 max-capacity 后才会显式报错
# cmd("send q1 overflow")  # -ERR queue full

# 列出队列
cmd("queues")            # +q1
```

### 3.4 发布/订阅

```python
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 6379))

def cmd(c):
    s.sendall(c.encode() + b'\r\n')
    return s.recv(1024).decode().strip()

# 订阅主题
cmd("sub news")          # +OK
cmd("sub tech")          # +OK

# 查看订阅
cmd("subs")              # +news\r\ntech

# 查看所有主题
cmd("topics")            # +news(1)\r\ntech(1)

# 发布消息（所有订阅者都会收到）
cmd("pub news Hello")    # +news:Hello\r\n+OK 1
```

### 3.5 交互示例

```bash
# 终端 A - 订阅者
$ nc localhost 6379
sub news
+OK
+news:Zig 1.0 released!    # 收到发布的消息

# 终端 B - 发布者
$ nc localhost 6379
pub news Zig 1.0 released!
+news:Zig 1.0 released!
+OK 1                       # 1 个订阅者收到
```

### 3.6 协议详解

#### SUB - 订阅主题

```
请求:  SUB <topic>\r\n
响应:  +OK\r\n
```

#### UNSUB - 取消订阅

```
请求:  UNSUB [topic]\r\n
响应:  +OK\r\n
（省略 topic 则取消所有订阅）
```

#### PUB - 发布消息

```
请求:  PUB <topic> <message>\r\n
响应:  +<topic>:<message>\r\n   # 广播给所有订阅者
       +OK <count>\r\n          # 发布者确认，count=订阅者数量
```

#### TOPICS - 列出主题

```
请求:  TOPICS\r\n
响应:  +<topic1>(<count1>)\r\n<topic2>(<count2>)\r\n...
```

#### INFO - 查看当前容量策略

```
请求:  INFO\r\n
响应:  +ZigMQ 0.3.0\r\n
       queues:<count>\r\n
       topics:<count>\r\n
       initial_capacity:<n>\r\n
       max_capacity:<n>\r\n
```

---

## 4. 理论性能指标

说明：当前仓库没有提交正式 benchmark 脚本和基准数据，下面的内容只能看作复杂度层面的直觉估计，不应该拿来替代真实压测结论。

### 4.1 延迟分析

```
单次 Ring Buffer 操作延迟（最佳情况，L1 缓存命中）:
  - 指令数：~20-30 条 CPU 指令
  - 耗时：~10-30 ns（3GHz 处理器）

端到端请求延迟（包含网络，本地 loopback）:
  - TCP 握手      : ~200-500 ns
  - 数据复制      : ~100-200 ns (64B)
  - 命令解析      : ~50-100 ns
  - Ring Buffer   : ~50-100 ns
  - 响应发送      : ~100-200 ns
  总计：~500-1100 ns / 请求
```

### 4.2 吞吐量预测

| 场景 | 预估 QPS | 说明 |
|------|----------|------|
| Echo (64B) | 50-80 万/s | CPU + 网络受限 |
| PUSH (1KB) | 30-50 万/s | 内存带宽受限 |
| POP (1KB) | 30-50 万/s | 同上 |
| PUB (广播) | 取决于订阅者数量 | O(n) 复杂度 |

### 4.3 内存占用（粗略估算）

```
固定 Ring Buffer（10,000 条 × 1KB/条）: 10 MB
Topic subscribers 指针数组: 可忽略
进程基础开销: < 1 MB
---------------------------------
总计: 取决于消息体大小、连接数和主题数量
```

### 4.4 与 Redis 的关系

| 指标 | ZigMQ | Redis (单线程) | 说明 |
|------|-------|----------------|------|
| **定位** | 教学 / 原型 / 小工具 | 生产级数据结构服务器 | ZigMQ 追求小而直白 |
| **并发模型** | 每连接一线程 | 单线程事件循环 | 取舍完全不同 |
| **可靠性** | 当前为内存态 | 更成熟 | ZigMQ 还在演进 |
| **生态能力** | 极少 | 很丰富 | 不应直接替代 Redis |

---

## 5. 未来规划

- [ ] AOF 持久化
- [ ] 内存 + 磁盘分层（内存队列溢写到磁盘）
- [ ] 多 Worker 分片
- [ ] 阻塞式 POP/Recv
- [ ] 简单监控端点
- [ ] 配置文件支持
- [ ] 交叉编译（Linux static）

---

## 6. 许可证

MIT License
