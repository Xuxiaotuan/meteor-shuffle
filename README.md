# Meteor Shuffle

> 🌠 基于 Scala + Pekko 的远程 Shuffle 服务，对标 Apache Celeborn

## 架构

```
                            控制面 (Pekko Artery)
                            ═══════════════════
┌──────────────────────────────────────────────────────────────┐
│                     Master (Pekko Cluster)                   │
│  - Worker 注册/心跳/下线检测                                    │
│  - Shuffle 分区 slot 分配 (round-robin / free-space-aware)    │
│  - Revive 故障转移 (携带 replica 地址)                          │
│  - Event Sourcing 持久化 (Pekko Persistence)                   │
└──────┬───────────────────┬───────────────────┬───────────────┘
       │                   │                   │
  ┌────▼─────┐        ┌────▼─────┐        ┌────▼─────┐
  │ Worker#1 │        │ Worker#2 │        │ Worker#3 │
  │ NVMe x4  │        │ NVMe x4  │        │ NVMe x4  │
  │ +DiskMon │        │ +DiskMon │        │ +DiskMon │
  └────┬─────┘        └────┬─────┘        └────┬─────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              数据面 (Netty TCP 直连)
              Push / Fetch / Replicate
              ═══════════════════════
```

**控制面**：Pekko Artery — Worker 注册、心跳、Revive、Shuffle 生命周期管理
**数据面**：Netty TCP 直连 — PushData / FetchData / ReplicateData，绕过 Pekko 序列化

## 模块

| 模块 | 职责 | 行数 |
|------|------|------|
| `meteor-common` | 协议定义、压缩 (LZ4/Snappy/Zstd)、配置、Prometheus 指标、服务注册键 | 707 |
| `meteor-master` | 控制面：Worker 管理、slot 分配、Revive 故障转移、事件溯源 | 1,911 |
| `meteor-worker` | 数据面：PartitionWriter (mmap + 分块)、副本同步、优雅关闭、磁盘监控 | 1,142 |
| `meteor-client` | 嵌入 Flink TM 的 shuffle 客户端 (LifecycleManager + DataPusher) | 813 |
| `meteor-transport` | Netty 4.1 传输层：二进制帧编解码、连接池、异步请求/响应 | 702 |
| `meteor-flink-spi` | Flink 1.19 ShuffleServiceFactory SPI 适配器 | 768 |

**总计**: 32 个源文件，~6,043 行代码，44 个测试用例

## 数据流

```
Mapper.emitRecord()
  → MeteorResultPartitionWriter.buffer
  → finish() → ShuffleClient.push()
  → [压缩: LZ4/Snappy/Zstd]
  → PushData 帧
  → TransportClient.push(host, port)     ← Netty 直连
  → TransportServer.handlePush()
  → PartitionWriterStorageAdapter.write()
  → PartitionWriter (mmap + fsync)
  → [副本同步: TransportClient.replicate()]

Reducer.requestPartitions()
  → ShuffleClient.fetch()
  → TransportClient.fetch(host, port)    ← Netty 直连
  → TransportServer.handleFetch()
  → PartitionWriter.readAll() (mmap, zero-copy)
  → NetworkBuffer 入 resultQueue
  → pollNext() → BufferOrEvent
```

## 功能清单

### 核心功能
- [x] 协议定义 (PushData / FetchData / Revive / Replicate / Recover)
- [x] Master Acter — Worker 注册、心跳、下线检测
- [x] Worker Actor — PushData / FetchData、磁盘存储
- [x] ShuffleClient — 位置缓存 + Revive 故障转移
- [x] Worker ↔ Worker 副本同步
- [x] Partition Split — 倾斜分区自动拆分
- [x] Pekko Persistence Event Sourcing — Master 状态持久化

### 生产就绪 (P1)
- [x] 多盘 free-space-aware 负载均衡 (DiskMonitor + 缓存)
- [x] 优雅关闭 (GracefulShutdown → UnregisterWorker → 拒新 shuffle)
- [x] 分区数据自动清理 (CheckShuffleCleanup, 5 分钟间隔)
- [x] Prometheus 指标打点 (Master 6 指标 + Worker 9 指标)

### 高级特性 (P2)
- [x] Memory-mapped I/O — 写 RandomAccessFile + FileChannel，读 mmap zero-copy
- [x] Chunked Transfer — 64MB 分块存储，避免 Pekko 2MB 消息帧上限
- [x] Replica Recovery — 副本恢复协议, Master Revive 返回 replica 地址
- [x] E2E 全链路集成测试 — 12 场景 (压缩/校验/分块/副本/并发)
- [x] CRC32 校验和 — PushData 携带校验和，Worker 侧验证
- [x] Netty 直连传输 — 数据面 Netty TCP，控制面 Pekko Artery

### Flink 集成
- [x] Flink 1.19 ShuffleServiceFactory SPI (Scala 2.12)
- [x] MeteorResultPartitionWriter (emitRecord 缓冲 → finish 推送)
- [x] MeteorIndexedInputGate (fetch Worker → poll queue → BufferOrEvent)
- [x] Local in-memory shuffle (同 TM 内 ConcurrentLinkedQueue 直连)
- [x] 端到端集成测试: Source(1)→FlatMap(2)→KeyedReduce(2)→Sink(1), 80 条数据

## 快速开始

```bash
# 编译
sbt compile

# 运行全部测试 (44 个, 所有模块)
sbt test

# 启动 Master
sbt "meteorMaster/run"

# 启动 Worker (启用 Netty 数据面)
sbt "meteorWorker/run -Dmeteor.worker.data.port=9090"
```

### Flink 1.19 本地集成测试

```bash
# 1. 打包 SPI fat jar
sbt meteorFlinkSpi/assembly

# 2. 复制到 Flink lib
cp meteor-flink-spi/target/scala-2.12/meteor-flink-spi-assembly-0.1.0-SNAPSHOT.jar \
   ~/flink-1.19.3/lib/

# 3. 配置 Flink (conf/config.yaml)
# shuffle-service-factory.class: cn.xuyinyin.meteor.spi.MeteorShuffleServiceFactory
# taskmanager.numberOfTaskSlots: 4

# 4. 启动 Flink 集群
~/flink-1.19.3/bin/start-cluster.sh

# 5. 提交测试 Job
~/flink-1.19.3/bin/flink run \
  -c cn.xuyinyin.meteor.spi.job.ShuffleIntegrationJob \
  meteor-flink-spi/target/scala-2.12/meteor-flink-spi-assembly-0.1.0-SNAPSHOT.jar

# 6. 查看结果
cat ~/flink-1.19.3/log/flink-*-taskexecutor-*.out
```

## 配置

核心配置项 (对齐 Celeborn 命名风格):

| Meteor 配置 | Celeborn 配置 | 默认值 |
|-------------|---------------|--------|
| `meteor.client.push.replicate` | `celeborn.client.push.replicate.enabled` | true |
| `meteor.client.push.max-reqs-in-flight` | `celeborn.client.push.maxReqsInFlight.perWorker` | 32 |
| `meteor.shuffle.partition.split-threshold` | `celeborn.client.shuffle.partition.split.threshold` | 256MB |
| `meteor.worker.data.port` | N/A (Netty 数据面端口) | 0 (禁用) |
| `meteor.shuffle.io.mode` | `celeborn.storage.ioMode` | mmap |
| `meteor.shuffle.io.chunk-size` | N/A (分块大小) | 64MB |

配置文件: `meteor-shared.conf`、`master.conf`、`worker.conf`

## 性能基准

> 测试环境: MacBook Air M2, 单 JVM, 内存文件系统

```
PushData:   13,537 ops/s, 52.9 MB/s   (p50=41µs,  p99=159µs)
FetchData:  44,138 ops/s, 172.4 MB/s  (p50=14µs,  p99=30µs)
Concurrent: 456,838 ops/s, 446.1 MB/s (4 writers, 0 errors)
```

## 测试覆盖

```
测试文件                             测试数
──────────────────────────────────────────
meteor-common/CodecSpec              11  — 压缩/解压/CRC32/帧格式
meteor-master/WorkerE2ESpec           8  — Worker 注册/心跳/Push/Fetch
meteor-master/ShuffleIntegrationSpec  3  — Master↔Worker 集成
meteor-master/FullPipelineE2ESpec    10  — 全链路: 压缩/校验/分块/副本/并发
meteor-transport/TransportCodecSpec   5  — 帧编解码 / body 格式
meteor-transport/ClientServerSpec     3  — Push/Fetch/Replicate 直连
meteor-worker/StorageAdapterSpec      4  — PartitionWriter↔StorageAdapter
──────────────────────────────────────────
总计                                  44
```

### Flink 1.19 集成测试报告 (2026-05-28)

**环境**:
- Flink 1.19.3, Standalone 单节点
- Scala 2.12.20 + Pekko 1.1.3
- macOS (Apple Silicon), Java 21
- 模式: Local in-memory shuffle (Meteor Master 不可用，自动降级)

**结果**: ✅ 全部 80 条数据正确处理，reduce 聚合准确

#### 测试架构

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Flink Standalone (单节点)                            │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                     TaskManager (4 slots)                             │  │
│  │                                                                       │  │
│  │   ┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌──────────┐  │  │
│  │   │  Source   │───→│ FlatMap  │───→│ KeyedReduce   │───→│   Sink   │  │  │
│  │   │  (1/1)   │    │ (1/2)    │    │   (1/2)       │    │  (1/1)   │  │  │
│  │   └──────────┘    ├──────────┤    ├───────────────┤    └──────────┘  │  │
│  │                   │ FlatMap  │───→│ KeyedReduce   │                   │  │
│  │                   │ (2/2)    │    │   (2/2)       │                   │  │
│  │                   └──────────┘    └───────────────┘                   │  │
│  │                                                                       │  │
│  │   ┌────────────────────────────────────────────────────────────────┐  │  │
│  │   │          Meteor Shuffle Plugin (in-memory)                     │  │  │
│  │   │                                                                │  │
│  │   │  MeteorShuffleMaster ← registerPartitionWithProducer()         │  │
│  │   │  MeteorShuffleEnvironment ← createInputGates()                │  │
│  │   │  MeteorResultPartitionWriter[] ← subpartitionQueues (CLQ)     │  │
│  │   │  MeteorIndexedInputGate[] ← pollNext() → BufferOrEvent        │  │
│  │   └────────────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                     JobManager                                         │  │
│  │   shuffle-service-factory.class = MeteorShuffleServiceFactory         │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### Job 拓扑

```
Source(1) ──forward──→ FlatMap(2) ──keyBy──→ KeyedReduce(2) ──forward──→ Sink(1)
         (chained)              (shuffle)                   (chained)

Dataset: 80 items (apple×10, banana×10, ..., honeydew×10)
Output:  apple-1→1, banana-2→2, cherry-3→3, date-4→4, ...
```

#### Local In-Memory Shuffle 数据流

```
                           ┌─────────────────────────────────────┐
                           │   MeteorResultPartitionWriter       │
                           │                                     │
 WriterTask                │   subpartitionQueues[0] ── CLQ ──┐  │
 (e.g. FlatMap)            │   subpartitionQueues[1] ── CLQ ──┤  │
      │                    │                                   │  │
      │ emitRecord()       │   BufferBuilder[0] ──────────────┤  │
      │ ───────────────→   │   BufferBuilder[1] ──────────────┤  │
      │                    │                                   │  │
      │ finishCurrentBuffer│         notifyDataAvailable()     │  │
      │ ───────────────→   │   ──→ listener.notifyDataAvailable│  │
      │                    │       │                           │  │
      │                    └───────┼───────────────────────────┘  │
      │                            │                              │
      │                            ▼                              │
      │                    ┌───────────────────────────────────────┤
      │                    │   MeteorIndexedInputGate             │
      │                    │                                      │
      │                    │   drainView()                        │
      │                    │     view.getNextBuffer()             │
      │                    │       └→ queue[subIdx].poll()        │
      │                    │     → resultQueue.add(BufferOrEvent) │
      │                    │                                      │
 ReaderTask                │   pollNext()                         │
 (e.g. KeyedReduce)       │     └→ resultQueue.poll()            │
      │                    │        → Optional<BufferOrEvent>     │
      │                    └───────────────────────────────────────┘
      │
      │  processElement()
      ▼
```

#### SPI 加载流程

```
Flink JobManager 启动
  │
  ├─ SPI: ServiceLoader.load(ShuffleServiceFactory)
  │    └─ META-INF/services/org.apache.flink.runtime.shuffle.ShuffleServiceFactory
  │         └─ cn.xuyinyin.meteor.spi.MeteorShuffleServiceFactory
  │
  ├─ MeteorShuffleServiceFactory.createShuffleMaster()
  │    └─ new MeteorShuffleMaster()
  │         └─ 注册到 descriptorCache: TrieMap[JobID → MeteorShuffleDescriptor]
  │
  └─ MeteorShuffleServiceFactory.createShuffleEnvironment()
       └─ new MeteorShuffleEnvironment()
            ├─ registerResultPartitionWriters():
            │    ├─ new MeteorResultPartitionWriter(partitionId, numSubs)
            │    ├─ MeteorPluginContext.registerLocalPartition(rpid, writer)
            │    └─ return writer
            │
            └─ createInputGates():
                 ├─ getShuffleDescriptors → MeteorShuffleDescriptor[]
                 ├─ MeteorPluginContext.getLocalPartition(rpid) → writer
                 ├─ writer.createSubpartitionView(subIdx, listener)
                 │    └─ new MeteorSubpartitionView(writer, listener, subIdx)
                 └─ new MeteorIndexedInputGate(views, channelInfos)
                      └─ pollNext(): resultQueue.poll() → BufferOrEvent
```

#### 关键修复

| # | 问题 | 根因 | 修复方案 |
|---|------|------|----------|
| 1 | `MeteorSubpartitionView.getNextBuffer()` 读错 queue | view 从 `subpartitionQueues(0)` 读取，但数据在 `subpartitionQueues(1)` | 按 `subpartitionIndex` 读取正确的 queue |
| 2 | 下游不知道消费哪个 subpartition | InputGate 的 `numPartitions` 是上游总分区数，不是消费索引 | 用 `InputGateDeploymentDescriptor.getConsumedSubpartitionIndex()` 确定 |
| 3 | 小 record 永远不被 flush | `emitRecord` 只在 buffer 满 (32KB) 时 flush，record ~20B | 每次 `emitRecord` 后立即 `finishCurrentBuffer` + `notifyDataAvailable` |
| 4 | `createSubpartitionView` OOB | 传入 `numPartitions` (2) 作为 index，创建 range [2,2] | 改用 `new ResultSubpartitionIndexSet(subIdx)` |
| 5 | Scala 2.13/2.12 运行时不兼容 | Flink 1.19 绑定 Scala 2.12，`ScalaRunTime$.wrapRefArray` 签名不同 | 全项目迁移到 Scala 2.12.20 |

## Roadmap

- [x] **v0.1** 协议 + Master + Worker + Client 核心
- [x] **v0.2** Pekko HTTP REST API + Prometheus 指标 + Pekko Persistence
- [x] **v0.3** Partition Split + 副本同步 + Flink SPI
- [x] **v0.4** 生产就绪 (P1): 多盘负载、优雅关闭、数据清理、Prometheus 打点
- [x] **v0.5** 高级特性 (P2): mmap I/O、Chunked Transfer、副本恢复、CRC32、Netty 直连
- [x] **v0.6** Flink 1.19 集成测试 (Scala 2.12 迁移 + SPI 端到端验证)
- [ ] **v0.7** 远程 Shuffle (Meteor Master + 多 TM 跨节点)
- [ ] **v0.8** 滚动升级 / 蓝绿部署
- [ ] **v1.0** 生产验证

## 免责声明

本项目为学习性质，参考 Apache Celeborn 的设计思路，不旨在替代或复刻 Celeborn。
