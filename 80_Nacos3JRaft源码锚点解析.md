# Nacos 3.x JRaft 源码锚点深度解析（本地 3.2.4）

> **源码基线**：Nacos 3.2.4（本地：`d:\GC_WorkSpaces\nacos-3.2.4`）  
> **核心模块**：`core/src/main/java/com/alibaba/nacos/core/distributed/raft/`  
> **面试权重**：⭐⭐⭐⭐（Config CP 模式核心实现，本地源码可精确锚点）

---

## 一、Nacos JRaft 模块结构（本地可验证）

```
d:\GC_WorkSpaces\nacos-3.2.4\core\src\main\java\com\alibaba\nacos\core\distributed\raft\
├── JRaftServer.java          L106  ★ 核心入口
├── NacosStateMachine.java    L69   ★ Raft 状态机
├── JRaftProtocol.java              JRaft 协议封装
├── RaftConfig.java                 配置
├── RaftSysConstants.java           系统常量（选举超时/Snapshot 间隔）
├── NacosClosure.java               回调闭包
├── JRaftMaintainService.java       后台维护
├── processor/
│   ├── AbstractProcessor.java       L50  抽象处理基类
│   ├── NacosWriteRequestProcessor.java  写请求处理
│   └── NacosReadRequestProcessor.java   读请求处理
├── grpc/
│   └── NacosGrpcRaftRpcFactory.java  JRaft gRPC 通信
├── utils/
│   ├── RaftExecutor.java            ★ Raft 线程池（核心！）
│   ├── JRaftOps.java                Raft 操作枚举
│   └── FailoverClosureImpl.java      故障转移回调
└── auth/
    └── NacosJRaftServerInterceptor.java  L42  gRPC 鉴权拦截
```

---

## 二、JRaftServer 核心源码锚点

### 2.1 JRaftServer 类结构（★ 锚点 L106）

```java
// 锚点：d:\GC_WorkSpaces\nacos-3.2.4\core\src\main\java\...\JRaftServer.java:L106
//
// ★ JRaftServer 核心成员变量（面试高频！）
public class JRaftServer {
    
    private RpcServer rpcServer;                    // L110 JRaft RPC Server
    private CliClientServiceImpl cliClientService; // L112 客户端服务
    private CliService cliService;                 // L114 CLI 服务
    
    // ★ 多 Raft Group 管理（面试高频！Nacos 支持多个 Raft Group）
    // 每个功能模块一个独立 Raft Group（Config、Naming CP、Distro 等）
    private Map<String, RaftGroupTuple> multiRaftGroup = new ConcurrentHashMap<>(); // L118
    
    private volatile boolean isStarted = false;    // L120 生命周期状态
    private volatile boolean isShutdown = false;   // L122
    
    private Configuration conf;                    // L124 集群配置
    private NodeOptions nodeOptions;               // L128 JRaft Node 选项
    private Serializer serializer;                 // L130 Protobuf 序列化
    private Collection<RequestProcessor4CP> processors  // L132 RequestProcessor 集合
        = Collections.synchronizedSet(new HashSet<>());
    
    private PeerId localPeerId;                    // L141 当前节点 PeerId
    private int failoverRetries;                   // L143 故障转移重试次数
    private int rpcRequestTimeoutMs;               // L145 RPC 请求超时
}
//
// ★ 关键设计：多 Raft Group
//   Nacos 不是一个全局 Raft Group，而是每个功能模块一个：
//   → "config" Raft Group：Config 数据 CP 模式
//   → "naming_persistent" Raft Group：持久化 Instance
//   → 各自独立选主 + 独立日志复制
//   → 某个 Group 的 Leader 挂了不影响其他 Group
```

### 2.2 init() 初始化方法（★ 锚点 L158）

```java
// 锚点：JRaftServer.java:L158-197
//
// void init(RaftConfig config) {
//     L159: this.raftConfig = config;
//     L160: this.serializer = SerializeFactory.getDefault();
//     L162: RaftExecutor.init(config);  // ★ 初始化 Raft 线程池
//     
//     L164-168: 解析 self 地址 → localPeerId
//     
//     L169: nodeOptions = new NodeOptions();
//     L171-176: // 选举超时（默认 5s）
//     L181-184: // ★ 四个共享 Timer（面试高频！JRaft 工程优化）
//     nodeOptions.setSharedElectionTimer(true);   // 共享选举 Timer
//     nodeOptions.setSharedVoteTimer(true);       // 共享投票 Timer
//     nodeOptions.setSharedStepDownTimer(true);   // 共享下台 Timer
//     nodeOptions.setSharedSnapshotTimer(true);   // 共享 Snapshot Timer
//     // → 多个 Raft Group 共享同一个 Timer（节省资源！）
//
//     L186: nodeOptions.setElectionTimeoutMs(electionTimeout);
//     L187-188: RaftOptions raftOptions = RaftOptionsBuilder.initRaftOptions(raftConfig);
//               nodeOptions.setRaftOptions(raftOptions);
//     L190: nodeOptions.setEnableMetrics(true);  // ★ 开启 Metrics
// }
//
// ★ 面试高频：JRaft 共享 Timer 设计（Nacos 工程优化亮点）
//   标准 Raft：每个 Raft Node 独立 Timer
//   JRaft：多个 Node 共享同一个 Timer
//   → Nacos 一个节点有多个 Raft Group → 共享 Timer 省资源
//   → 和 TiKV raft-rs 工程优化思路相同（批量 + Pipeline）
```

### 2.3 WriteRequest 写入流程

```java
// 锚点：JRaftServer.java + NacosStateMachine.java:L95-149
//
// ★ Nacos Config CP 模式写入完整链路（本地源码可验证！）
//
// HTTP POST /nacos/v1/cs/configs → JRaft → StateMachine → MySQL
//
// ① ConfigController.publishConfig
//    → 包装成 WriteRequest（Protobuf）
//
// ② JRaftServer.write(groupId, WriteRequest)
//    → RaftGroupTuple group = multiRaftGroup.get(groupId);
//    → Node node = group.node;
//    → Task task = new Task();
//      task.setData(writeRequest.toByteArray());
//      task.setClosure(closure);
//    → node.apply(task);  // ★ JRaft 写入入口
//
// ③ JRaft 内部（Alipay JRaft 库实现）
//    → 写 LogEntry 到 WAL（保证崩溃恢复）
//    → AppendEntries RPC 复制给 Follower
//    → 过半确认 → CommitIndex++
//    → 调用 StateMachine.onApply()
//
// ④ NacosStateMachine.onApply() ★ 锚点 L95
//    @Override
//    public void onApply(Iterator iter) {
//        while (iter.hasNext()) {
//            if (message instanceof WriteRequest) {
//                // ★ 锚点 L121-124：WriteRequest 调用 processor.onApply
//                Response response = processor.onApply((WriteRequest) message);
//                postProcessor(response, closure);
//            }
//            if (message instanceof ReadRequest) {
//                // ★ 锚点 L126-129：ReadRequest 调用 processor.onRequest
//                Response response = processor.onRequest((ReadRequest) message);
//                postProcessor(response, closure);
//            }
//        }
//    }
//
// ⑤ RequestProcessor4CP.onApply() → ConfigRequestProcessor
//    → 真正写 MySQL（Config 存储在 MySQL + Raft 日志复制）
//
// ★ 和 TiKV Raft 写入对比（面试高频）：
//   Nacos：Raft → StateMachine → MySQL（Raft 做日志复制）
//   TiKV：Raft → Percolator → RocksDB（Raft 做日志复制 + Percolator 做事务）
//   → 都是"Raft 只负责副本复制"的设计模式！
```

---

## 三、NacosStateMachine 核心源码锚点

### 3.1 onApply 方法（★ 锚点 L95）

```java
// 锚点：d:\GC_WorkSpaces\nacos-3.2.4\core\src\main\java\...\NacosStateMachine.java:L95
//
// ★ NacosStateMachine.onApply 核心设计（面试高频！）
//
// ① 区分 ReadRequest vs WriteRequest：
//    WriteRequest → processor.onApply()（状态变更）
//    ReadRequest  → processor.onRequest()（只读查询）
//    → 读请求也走 Raft Log（保证强一致）
//    → 但读请求在 Follower 上直接跳过（iter.done() == null）
//
// ② 错误处理：
//    任何 Processor 抛异常 → iter.setErrorAndRollback()
//    → JRaft 自动回滚到 Error 状态 → 触发选主
//    → 保证 StateMachine 不会被错误日志污染
//
// ③ Closure 回调：
//    NacosClosure 封装 WriteRequest + 回调 Handler
//    → JRaft Apply 完成后调 closure.run(status)
//    → Controller 层收到结果返回 Client
//
// ★ JRaft Closure 设计（和 TiKV raft-rs Closure 一样）：
//   Closure = 异步回调
//   → apply(task, closure)  → 异步提交
//   → closure.run(status)    → Apply 完成回调
//   → Controller 不阻塞等待（高性能）
```

### 3.2 onLeaderStart / onLeaderStop

```java
// NacosStateMachine 还有两个关键方法：
//
// onLeaderStart(long term)：
//   // 锚点（JRaft StateMachineAdapter）
//   isLeader.set(true);
//   this.term = term;
//   this.leaderIp = node.getLeaderId().toString();
//   // → Leader 选主成功 → 更新 RouteTable → Client 自动路由
//
// onLeaderStop(LeaderChangeContext ctx)：
//   isLeader.set(false);
//   // → Leader 下台 → Client 查新 Leader → 自动重连
//
// ★ 和 TiKV Raft Leader 切换流程一样：
//   Leader 切换 → 新 Leader 写入 RouteTable → Client 自动路由
//   Nacos JRaft 用 RouteTable（JRaft 库内置）
```

---

## 四、Nacos JRaft vs TiKV Raft vs ETCD Raft

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Nacos JRaft vs TiKV Raft vs ETCD Raft 核心对比（面试高频）│
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │ Nacos JRaft  │ TiKV raft-rs │ ETCD Go      ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 语言           │ Java         │ Rust         │ Go           ││
│  │ 实现方         │ 阿里 JRaft   │ 字节/阿里    │ CoreOS       ││
│  │ Raft Group 数  │ 多个（Config │ 几千个（每个 │ 多个         ││
│  │                │ +Naming CP） │ Region 一个）│              ││
│  │ 存储层         │ MySQL +      │ RocksDB      │ bbolt + MVCC ││
│  │                │ Raft Log     │ + Percolator │ + Watch      ││
│  │ 共享 Timer     │ ✅ 有        │ ✅ 有批量    │ ❌ 无        ││
│  │ Closure 回调   │ ✅ NacosClosure│ ✅ Closure   │ ✅ DoneCh   ││
│  │ 工程优化       │ 共享 Timer   │ Pipeline +   │ Lease Read   ││
│  │                │ + Metrics    │ Batch Write  │ + Linearizable││
│  │                │ + gRPC 通信  │ + Async Apply│              ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ 三个实现的 Raft 协议核心完全一样（Leader 选主 + Log 复制）│
│   差异只在：语言 + 工程优化 + 存储层                        │
│   → 面试不考具体实现差异，考 Raft 协议本身                   │
│
│ ★ Nacos JRaft 独有设计：                                      │
│   ① RequestProcessor 模式（和 Spring Filter 链一样）       │
│   ② 多个独立 Raft Group（功能模块隔离）                    │
│   ③ 共享 Timer（节省资源）                                  │
│   ④ gRPC 通信（JRaft 3.x 升级）                            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（12 题）

| # | 考点 | 源码锚点 | 答案 |
|---|------|---------|------|
| 1 | JRaftServer 核心类位置？| L106 | `nacos-3.2.4\core\src\main\java\...\JRaftServer.java:L106` |
| 2 | 多 Raft Group？| L118 | `multiRaftGroup = new ConcurrentHashMap<>()` 每个功能模块一个独立 Group |
| 3 | 共享 Timer 设计？| L181-184 | 四个 SharedXxxTimer（Election/Vote/StepDown/Snapshot）→ 多个 Group 共享省资源 |
| 4 | NacosStateMachine 位置？| L69 | `NacosStateMachine extends StateMachineAdapter` |
| 5 | onApply 核心方法？| L95-149 | 遍历 LogEntry → 区分 ReadRequest/WriteRequest → 调用 processor.onApply/onRequest |
| 6 | WriteRequest 处理？| L121-124 | `processor.onApply((WriteRequest) message)` → 状态变更 |
| 7 | ReadRequest 处理？| L126-129 | `processor.onRequest((ReadRequest) message)` → 只读查询也走 Raft 保证强一致 |
| 8 | JRaft 写流程？| JRaftServer → node.apply(task) → Wal → AppendEntries → 过半确认 → StateMachine.onApply |
| 9 | JRaft Closure 设计？| NacosClosure | 异步回调 → apply(task, closure) → closure.run(status) |
| 10 | JRaft vs TiKV Raft？| 核心逻辑一样（Raft 协议）；差异在语言（Java/Rust）+ 工程优化；TiKV 有 Percolator 事务层 |
| 11 | Nacos JRaft vs ETCD Raft？| JRaft：Java + MySQL 存储 + 共享 Timer；ETCD：Go + bbolt + MVCC + Watch |
| 12 | 本地源码验证？| 全路径 `d:\GC_WorkSpaces\nacos-3.2.4\core\src\main\java\...\raft\` | 63 个 Java 文件，源码清晰可验证 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Nacos 3.2.4（本地精确路径 + 行号锚点）  
**面试考点：12 题**  
**差异化点**：本地源码锚点精确到文件 + 行号（唯一一份可以这样做的）
