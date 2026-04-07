# ZigMQ

[English](README.md)

ZigMQ 是一个使用 Zig 编写的轻量级内存消息队列与发布订阅服务。它以单二进制形式交付，协议简单直接，适合快速部署到脚本、原型系统、内部工具和小型服务中。

## 项目优势

- 一个服务同时支持两种消息模式：队列和发布订阅。
- 单二进制、零外部依赖，部署成本低，上手快。
- 队列和主题都做了分片锁，发布路径采用订阅者快照广播，并发边界更清晰。
- 队列容量支持自动扩容，并且对“队列满”有明确可预期的错误语义。
- 自带单测、协议回归、并发压力测试和 benchmark 脚手架，便于持续优化。

## 它适合什么场景

| 场景 | ZigMQ 的价值 |
| --- | --- |
| 快速部署 | 一个二进制、一个端口，不依赖额外服务 |
| 协议调试 | 文本协议，人眼可读，可直接用 `nc` 或原始 socket 调试 |
| 消息模型 | FIFO 队列和 Pub/Sub 共存，适合轻量异步任务与广播通知 |
| 性能路径 | 热路径低分配、队列自动扩容、状态锁分片 |
| 可运维性 | 提供 `PING`、`INFO`、`QUEUES`、`TOPICS`、`SUBS` 等基础观测命令 |
| 可维护性 | 代码已按模块拆分，适合继续扩展和重构 |

## 安装方式

### 方式一：使用 release 包

当前 release 资产包含：

- `zigmq-v0.4.1-linux-x86_64.tar.gz`
- `zigmq-v0.4.1-macos-aarch64.tar.gz`
- `SHA256SUMS.txt`

下载解压后直接运行 `./zigmq` 即可。

### 方式二：从源码构建

环境要求：

- Zig `0.15.2`

```bash
git clone https://github.com/reasonz/zigMQ.git
cd zigMQ
zig build -Doptimize=ReleaseFast
./zig-out/bin/zigmq
```

## 使用说明

启动服务：

```bash
./zig-out/bin/zigmq --port 8388 --capacity 1024 --max-capacity 16384
```

默认端口已经调整为 `8388`，所以如果你直接运行 `./zig-out/bin/zigmq`，服务也会监听在 `8388`，除非你用 `--port` 显式覆盖。

队列示例：

```text
send jobs hello world
+OK

peek jobs
$11
hello world

recv jobs
$11
hello world
```

发布订阅示例：

```text
sub news
+OK

pub news shipped
+news:shipped
+OK 1
```

## 命令速查

| 命令 | 示例 | 说明 |
| --- | --- | --- |
| `send <queue> <msg>` | `send jobs hello` | 向队列写入一条消息 |
| `recv <queue>` | `recv jobs` | 取出并弹出队列头消息 |
| `peek <queue>` | `peek jobs` | 查看队列头消息但不移除 |
| `len <queue>` | `len jobs` | 查询队列长度 |
| `mq <queue>` | `mq jobs` | 显式创建队列 |
| `queues` | `queues` | 列出所有队列 |
| `sub <topic>` | `sub news` | 订阅当前连接到某个主题 |
| `unsub [topic]` | `unsub news` | 取消某个主题或当前全部订阅 |
| `pub <topic> <msg>` | `pub news shipped` | 向主题广播消息 |
| `topics` | `topics` | 列出当前主题及订阅数 |
| `subs` | `subs` | 查看当前连接的订阅列表 |
| `ping` | `ping` | 健康检查 |
| `info` | `info` | 查看版本和容量配置 |

协议响应格式：

- 成功：`+OK\r\n`
- 错误：`-ERR <message>\r\n`
- 消息体：`$<length>\r\n<body>\r\n`
- 广播：`+<topic>:<message>\r\n`

## 测试与校验

```bash
zig build test
zig build protocol-test
zig build stress-test
zig build benchmark
```

分别覆盖：

- `test`：队列、连接、发布订阅、服务内部逻辑单测
- `protocol-test`：协议端到端正确性回归
- `stress-test`：并发竞争场景回归
- `benchmark`：本地吞吐基线脚手架

## 性能指标

以下数据来自 2026 年 4 月 4 日本地基线测试：

- 机器：Apple Silicon macOS
- Zig：`0.15.2`
- 构建模式：`ReleaseFast`
- 网络：本机 loopback TCP
- 命令：`zig build benchmark`

| 场景 | 结果 |
| --- | --- |
| `queue_contention` | `45,659 ops/s`，`21.9 us/op` |
| `independent_queue_roundtrips` | `46,002 ops/s`，`21.7 us/op` |
| `pubsub_fanout publish_rate` | `12,765 msg/s` |
| `pubsub_fanout delivery_rate` | `76,589 deliveries/s` |

这些数据更适合作为“同一台机器、同一套脚本下的版本对比基线”，而不是严格的实验室 benchmark。

## 打包与发布

生成 Linux 和 macOS release 包：

```bash
python3 scripts/package_release.py
```

发布 GitHub Release：

```bash
python3 scripts/publish_release.py
```

打包产物会输出到 `dist/release/`，同时生成 `SHA256SUMS.txt`。

## 当前定位

ZigMQ 的定位不是替代 Redis 或 NATS，而是做一个小而清晰、容易读懂、性能足够好、工程边界明确的消息服务。它非常适合学习、内部系统、自动化任务后端，以及需要快速落地的小型产品能力。
