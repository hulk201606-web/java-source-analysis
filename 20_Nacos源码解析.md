# Nacos 源码深度解析（注册中心 + 配置中心）

> **源码基线**：Nacos 3.2.4（本地：`d:\GC_WorkSpaces\nacos-3.2.4`）  
> **核心仓库**：nacos-group/nacos  
> **大纲来源**：19_SpringCloud源码调研大纲.md P0-1  
> **面试权重**：⭐⭐⭐⭐⭐（国内互联网必问）

---

## 一、Nacos 整体架构

### 1.1 四大模块职责

```
┌─────────────────────────────────────────────────────────────┐
│  Nacos 四大模块                                              │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │  nacos-api    │ nacos-core    │ nacos-naming  │ nacos-config  ││
│  │ API 定义      │ 核心通用能力  │ 服务发现      │ 配置中心      ││
│  │ Client/Server │ 一致性协议    │ Instance注册   │ 配置CRUD      ││
│  │ HTTP/gRPC     │ Distro/Raft   │ 心跳/查询      │ 长轮询/gRPC   ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│  Nacos 2.0+ 通信协议：HTTP（旧）+ **gRPC（新，双向流）**     │
│  → Naming 服务发现默认 AP 模式（Distro 协议）               │
│  → Config 配置中心默认 CP 模式（Raft 协议）                 │
│                                                               │
│ ★ CP vs AP 切换：                                           │
│   clusterMode: standalone → 单机（无 Raft/Distro）         │
│   clusterMode: cluster    → 集群（CP 默认）                │
│   naming 配置中设置 CP：naming.data.useInstanceWithBeat=false │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Naming 服务发现核心

### 2.1 Instance 注册流程（★ 大纲锚点：InstanceController.registerInstance L110-180）

```java
// ★ InstanceController.registerInstance
// 源码：nacos-naming/src/main/java/com/alibaba/nacos/naming/controllers/InstanceController.java
// 行号：约 L110-180（3.2.4 需实际验证）
//
// 完整注册链路：
//
// HTTP POST /nacos/v1/ns/instance
//   → InstanceController.registerInstance(namespaceId, serviceName, instance)
//     → 校验参数（serviceName/ip/port 非空）
//     → ServiceManager.registerInstance(namespaceId, serviceName, instance)
//
// ★ ServiceManager.registerInstance（大纲锚点 L180-230）
//   → 检查 Service 是否存在
//   → 不存在则创建 Service（clusterMap + ephemeralInstances + persistentInstances）
//   → 写入 Instance 到内存 Map（ConcurrentHashMap）
//   → **持久化**（AP 模式写 Distro DataStore；CP 模式写 MySQL/Raft）
//   → Distro 协议广播给集群其他节点（DistroDataProcessor）
//
// 关键数据结构：
//   ServiceManager.services = ConcurrentHashMap<String, ServiceManager.Service>
//   Service.clusterMap = ConcurrentHashMap<String, Cluster>
//   Cluster.ephemeralInstances = ConcurrentHashMap<String, Instance> // 临时实例
//   Cluster.persistentInstances = ConcurrentHashMap<String, Instance> // 持久实例
//
// ★ 临时 vs 持久实例（面试高频）：
//   Ephemeral Instance：**有心跳自动剔除**（默认 Nacos Client 注册）
//   Persistent Instance：手动注册，**不会被心跳剔除**（需要手动删除）
```

### 2.2 心跳机制（★ 大纲锚点：ClientBeatCheckTask.run L50-90）

```java
// ★ Nacos 心跳三阶段（面试必背！）
//
// 客户端心跳：
//   ClientBeatProcessor.onReceiveData → 接收心跳包
//   → 更新 Instance.lastBeatTime
//   → 返回 OK
//   → 客户端 5 秒发送一次（BeatTask）
//
// 服务端心跳检查：
//   ClientBeatCheckTask.run（定时任务，5 秒执行一次）
//   → 遍历所有 Ephemeral Instance
//   → 当前时间 - lastBeatTime > 15 秒 → 标记 unhealthy
//   → 当前时间 - lastBeatTime > 30 秒 → **剔除实例**
//
// ★ 三阶段常量（源码里写死）：
//   HEART_BEAT_INTERVAL = 5000ms    // 5 秒发送一次心跳
//   HEART_BEAT_TIMEOUT = 15000ms   // 15 秒没心跳标记 unhealthy
//   IP_DELETE_TIMEOUT = 30000ms     // 30 秒没心跳剔除
//
// ★ 为什么 15/30？
//   15 秒 = 3 个心跳周期 → 容忍偶尔丢失
//   30 秒 = 6 个心跳周期 → 确认挂了才剔除
//   → 和 Eureka 的 90s/60s 相比更激进（Nacos 更灵敏）
```

### 2.3 Instance 查询流程

```java
// ★ InstanceController.listInstances（大纲锚点 L320-380）
//
// HTTP GET /nacos/v1/ns/instance/list?serviceName=xxx
//   → InstanceController.listInstances(namespaceId, serviceName, ...)
//     → ServiceManager.selectInstances(namespaceId, serviceName, groupName, clusters, healthyOnly)
//       → 从内存 Map 取 Instance（ConcurrentHashMap.get）
//       → healthyOnly=true → 过滤 unhealthy
//       → 排序返回
//
// ★ Distro 协议数据同步（大纲锚点 DistroDataProcessor.process L100-150）
//
// Distro = Nacos AP 模式的数据同步协议
//   → 每个节点只负责自己 hash 到的 Service（node.key = hash(IP:PORT)）
//   → 客户端写（Push）到负责的节点
//   → 其他节点从负责节点拉（Pull）数据
//   → 最终一致性（几秒内收敛）
//
// ★ Distro vs Raft vs Eureka Server-Server：
//   Distro：AP + 无中心化 + 最终一致（Nacos 默认）
//   Raft：CP + Leader 强推 + 强一致（Nacos Config）
//   Eureka：Server-Server Peer Replicate（AP，无中心化，比 Distro 简单）
```

### 2.4 Distro 协议完整源码（★ 大纲核心考点）

```java
// ★ Distro 协议（Nacos 原创 AP 协议）
//
// 核心设计：
//   ① 节点 hash 分配：每个 Service 归属于 hash(IP:PORT) 最小的节点
//   ② 客户端 Push：注册实例 → 推给负责的节点
//   ③ 服务端 Pull：非负责节点定时从负责节点拉数据
//   ④ 数据校验：MD5 校验每个 Service 的 Instance 列表
//
// Distro 数据存储（DataStore，大纲锚点 L80-120）：
//   DataStore.data = ConcurrentHashMap<String, Object>
//   → key = Service 的 fullName（namespace/group/serviceName）
//   → value = Service 对象（含所有 Instance）
//   → AP 模式无持久化（内存 + 定时磁盘快照）
//
// DistroDataProcessor（大纲锚点 L100-150）：
//   处理其他节点发来的 DistroData 包
//   → 更新本地 DataStore
//   → 校验 MD5
//   → 返回 ACK
//
// ★ Distro vs Raft 选择：
//   Naming 服务发现 → **默认 Distro（AP，最终一致，可用优先）**
//   Config 配置中心 → **默认 Raft（CP，强一致，数据不能错）**
//   → 为什么？服务发现短暂不一致没事（有的节点看不到新实例）；配置错了就炸了
```

### 2.5 Raft 协议（CP 模式，Config 核心）

```java
// ★ Nacos CP 模式用简化版 Raft（叫 JRaft）
//
// JRaft 三角色：
//   Leader（唯一写）+ Follower（只读）+ Candidate（选主）
//
// 选主流程：
//   节点心跳超时 → 转 Candidate → 投票给自己 → 请求其他节点投票
//   → 获得超过半数票 → 成为 Leader → 广播 heartbeat
//
// 日志复制：
//   Client 请求 → Leader → 写本地 WAL → 复制给 Follower
//   → 超过半数确认 → Commit → 返回 Client
//
// ★ Nacos 2.x 用 JRaft（百度开源），不是自己实现的 Raft
```

---

## 三、Config 配置中心核心

### 3.1 配置发布流程（★ 大纲锚点：ConfigController.publishConfig L150-200）

```java
// ★ ConfigController.publishConfig
// 源码：nacos-config/src/main/java/com/alibaba/nacos/config/server/controller/ConfigController.java
// 行号：约 L150-200
//
// HTTP POST /nacos/v1/cs/configs
//   → ConfigController.publishConfig(namespaceId, dataId, group, content, type)
//     → 校验参数
//     → **持久化**（CP 模式写 MySQL + Raft；单机写 Derby）
//     → 更新内存 Cache（LocalData）
//     → **通知所有客户端**（Config 长轮询 + gRPC 推送）
//
// ★ Config 数据三级缓存：
//   ① 内存（LocalData）→ 最快
//   ② 文件（本地磁盘 snapshot）→ 快
//   ③ MySQL → 慢（兜底）
//
// ConfigController.getConfig（大纲锚点 L100-140）：
//   GET /nacos/v1/cs/configs?dataId=xxx&group=xxx
//   → 内存查 → 文件查 → MySQL 查
//   → 三级缓存命中返回
```

### 3.2 配置变更通知（★ 面试高频）

```java
// ★ Nacos 配置变更通知机制
//
// 1. 长轮询（HTTP，兼容老客户端）
//    ConfigClientWorker.checkConfigInfo（大纲锚点 L120-180）
//    → POST /nacos/v1/cs/configs/listener（Long Polling）
//    → 服务端 hold 住请求最多 30 秒
//    → 如果期间有配置变更 → 立即返回变更 dataId
//    → 客户端收到变更 → GET 拉最新配置
//    → 继续下一轮长轮询
//
// 2. gRPC 推送（Nacos 2.0+，实时性更好）
//    BiRequestStream 双向流
//    → 服务端主动推送变更（不需要客户端轮询）
//    → 延迟更低（毫秒级）
//
// ★ 和 Spring Cloud Config 刷新对比：
//   Nacos：**内置长轮询/gRPC 推送**（Config 自己实现）
//   Spring Cloud Config + Bus：**Git 刷新 + RabbitMQ 广播**
//   → Nacos 更简单（一个组件搞定注册+配置+通知）
```

### 3.3 Nacos 2.0 gRPC 升级

```java
// ★ Nacos 2.0 gRPC 通信升级
//
// 1. 双向流（BiRequestStream）
//    → Client 和 Server 之间建立一个长连接
//    → 双向发送 HTTP-like 请求/响应
//    → 复用连接（减少 TCP 握手）
//
// 2. RequestProcessor 分发
//    → Server 端按请求类型分发到不同 Processor
//    → InstanceRequestProcessor / ConfigRequestProcessor / ...
//
// 3. 性能提升：
//    → 连接复用 + 二进制协议 → 比 HTTP 1.1 快 5-10x
//    → 支持 gRPC 健康检查（比 HTTP 更标准）
```

---

## 四、面试考点（20 题，★ 大纲要求）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Nacos CP/AP 怎么切换？| `clusterMode` 配置 + `naming.data.useInstanceWithBeat=false`（Naming 切 CP）；Config 默认 CP（Raft）；Naming 默认 AP（Distro）|
| 2 | Distro 和 Raft 区别？| **Distro：AP + 无中心化 + 最终一致**（Naming 默认）；**Raft：CP + Leader + 强一致**（Config 默认）；选哪个看业务容忍度 |
| 3 | 心跳机制三阶段？| **5 秒发送 / 15 秒 unhealthy / 30 秒剔除**；Ephemeral Instance（临时）才有心跳；Persistent 手动注册不剔除 |
| 4 | 配置变更通知原理？| **长轮询（HTTP，hold 30s）+ gRPC 推送（2.0+ 实时）**；ConfigController 发布后触发通知 |
| 5 | Nacos 数据三级缓存？| **内存 LocalData → 文件 snapshot → MySQL**；内存最快，MySQL 兜底 |
| 6 | 集群节点 hash 分配？| **node.key = hash(IP:PORT)** → 每个 Service 归属于 hash 最小的节点；Distro 协议核心 |
| 7 | 临时 vs 持久实例？| Ephemeral：默认 Client 注册，有心跳，30s 无心跳剔除；Persistent：手动注册，**不会被心跳剔除** |
| 8 | Distro 协议完整流程？| 节点 hash 分配 → 客户端 Push 到负责节点 → 非负责节点 Pull → MD5 校验 → 最终一致 |
| 9 | Nacos 2.0 gRPC 升级？| **双向流（BiRequestStream）+ 二进制协议** → 比 HTTP 快 5-10x；RequestProcessor 按类型分发 |
| 10 | Instance 注册完整链路？| HTTP POST → InstanceController.registerInstance → ServiceManager.registerInstance → 写内存+持久化 → Distro 广播 |
| 11 | ConfigController.publishConfig？| POST /cs/configs → 校验 → MySQL 持久化 → 更新内存 Cache → **通知客户端**（长轮询 + gRPC）|
| 12 | JRaft 是什么？| 百度开源简化版 Raft（Nacos CP 模式用）→ Leader/Follower/Candidate + 日志复制 + 选主 |
| 13 | Nacos vs Eureka？| Nacos：**CP/AP 可选 + 配置中心 + gRPC**；Eureka：纯 AP + 只有注册中心；Nacos 2.0 性能更好 |
| 14 | Nacos vs ZooKeeper？| Nacos：**AP+CP 可选 + 配置中心 + Web UI**；ZK：纯 CP（ZAB）+ 分布式锁/KV；企业选 Nacos |
| 15 | Nacos 健康检查？| HTTP 端口默认 8848；gRPC 端口默认 9848；gRPC 端口 = HTTP + 1000 |
| 16 | Nacos 配置文件？| server.properties（端口/集群/存储）+ jvm（JVM 参数）；集群必须指定 node.ip 不写 127.0.0.1 |
| 17 | Nacos 集群最少节点？| **3 个（奇数，Raft 选主需要 Quorum）**；2 个无法选主 |
| 18 | Nacos 持久化？| CP 模式（Config）：MySQL/Derby；AP 模式（Naming）：内存 + 定时磁盘快照（不强制持久化）|
| 19 | Nacos Client 配置？| `spring.cloud.nacos.discovery.server-addr=host:8848`；`spring.cloud.nacos.config.server-addr=host:8848` |
| 20 | Nacos 学习路径？| Docker 起单机 → HTTP API 实验 → Spring Boot 集成 → 集群部署（3 节点）→ gRPC 调试 → Distro/Raft 原理 |

---

## 五、核心源码锚点清单（★ 大纲要求，需在 `d:\GC_WorkSpaces\nacos-3.2.4` 中验证）

| 类 | 方法 | 核心功能 |
|----|------|---------|
| InstanceController | registerInstance | 处理服务注册 HTTP 请求 |
| ServiceManager | registerInstance | 注册 + 持久化 + 内存更新 |
| ServiceManager | selectInstances | 查询实例列表 |
| ClientBeatCheckTask | run | 心跳检查定时任务（5/15/30 三阶段）|
| DataStore | put / get | AP 模式 Distro 数据存储 |
| DistroDataProcessor | process | Distro 协议处理（接收/校验）|
| ConfigController | publishConfig | 配置发布 |
| ConfigController | getConfig | 配置获取（三级缓存）|
| ClientWorker | checkConfigInfo | 客户端长轮询 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Nacos 3.2.4（本地 `d:\GC_WorkSpaces\nacos-3.2.4`）  
**面试考点：20 题**  
**大纲来源**：19_SpringCloud源码调研大纲.md P0-1
