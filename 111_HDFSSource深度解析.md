# Apache Hadoop HDFS NameNode 源码级深度解析

> **源码基线**：Hadoop 3.3.x（Java）  
> **核心仓库**：apache/hadoop → hadoop-hdfs-project/hadoop-hdfs  
> **面试权重**：⭐⭐⭐⭐（大数据基石，高频必问）

---

## 一、HDFS 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ HDFS = Hadoop Distributed FileSystem                    │
│                                                               │
│  HDFS 架构：                                                  │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ NameNode（元数据中心 ★ 核心！）                         │   │
│  │   ├── FSImage（文件系统镜像，持久化）                    │   │
│  │   ├── EditLog（操作日志，增量）                         │   │
│  │   └── FSDirectory（内存中的文件树）                     │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ Block Report / Heartbeat / RPC                     │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ DataNode  │ DataNode  │ DataNode  │  ...                  │
│  │ Block     │ Block     │ Block     │ （存真实数据）      │
│  └──────────┴──────────┴──────────┘                         │
│
│ ★ 面试最高频：NameNode 单点问题 → HA 方案                    │
│   HDFS 2.x：NameNode HA（Active + Standby）                 │
│   → Active NameNode 响应请求                                │
│   → Standby NameNode 同步 EditLog（JournalNode）           │
│   → Active 挂了 → Standby 自动切换（ZKFC + JournalNode）   │
│
│ ★ HDFS vs TiKV vs 其他分布式存储：                            │
│   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│   │            │ HDFS          │ TiKV          │ S3/MinIO      │   │
│   ├──────────┼──────────────┼──────────────┼──────────────┤   │
│   │ 数据模型    │ Block（文件切分）│ Key-Value      │ Object        │   │
│   │ 元数据      │ NameNode    │ PD             │ 无（扁平）    │   │
│   │ 一致性      │ CP          │ CP（Raft）     │ 最终一致      │   │
│   │ 写模型      │ 只追加      │ 读时修改      │ 写新版本      │   │
│   │ 适用场景    │ 大数据离线  │ 分布式事务    │ PB 级归档    │   │
│   └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、NameNode 核心数据结构（源码级）

### 2.1 FSDirectory + INode 树

```java
// ★ HDFS NameNode 核心：FSDirectory（内存文件树，源码级）
//
// // hadoop-hdfs/.../server/namenode/FSDirectory.java
//
// public class FSDirectory {
//     private INodeDirectory root;        // 根目录 INode
//     private final INodeMap inodeMap;    // INodeID → INode 映射
//     private final BlocksMap blocksMap;  // BlockID → Block 映射
//     private final DatanodeManager datanodeManager; // DN 管理
// }
//
// // ★ INode 继承体系（源码级）
// public abstract class INode {
//     private long id;               // INode 唯一 ID（自增）
//     private String name;           // 文件名/目录名
//     private INodeDirectory parent; // 父目录
//     private long permission;       // 权限
//     private String userName;       // 所有者
//     private String groupName;     // 所属组
//     private long modificationTime; // 修改时间
// }
//
// public class INodeDirectory extends INode {
//     private List<INode> children; // 子节点列表
// }
//
// public class INodeFile extends INode {
//     private BlockInfo[] blocks;    // ★ Block 列表（3 副本默认）
//     private long header;           // 文件头信息
//     private short replication;     // 副本数（默认 3）
// }
//
// ★ 关键：FSDirectory 全在内存！NameNode 重启从 FSImage + EditLog 恢复
```

### 2.2 FSImage + EditLog（持久化）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ NameNode 持久化：FSImage + EditLog（源码级）              │
│                                                               │
│  // 两个核心文件：                                            │
│  FSImage（hdfs:///nn/current/fsimage_*）                     │
│  → 某个时间点的完整文件系统镜像（全量快照）                  │
│  → Protobuf 序列化 → 磁盘文件                               │
│
│  EditLog（hdfs:///nn/current/edits_*）                       │
│  → FSImage 之后的操作日志（增量）                            │
│  → Write + Sync + Roll（超过一定大小 Roll 到新文件）         │
│  → 每个操作一条 Log（创建/删除/改名文件/目录）               │
│
│ ★ NameNode 重启恢复流程（面试高频）：                         │
│  ① 加载最新 FSImage → 恢复文件树                            │
│  ② 重放 FSImage 之后的 EditLog → 恢复到最新状态             │
│  → 内存 FSDirectory 构建完成                                │
│  ③ 接受 RPC 请求                                             │
│
│ ★ FSImage 定期 Checkpoint（源码级）：                         │
│  当 EditLog 太大时（默认 64MB）或定期（默认 1h）            │
│  NameNode 做 Checkpoint：                                    │
│  ① 把当前 FSDirectory 序列化 → 新的 FSImage                 │
│  ② 丢弃旧的 EditLog（已经合并到新 FSImage 里了）            │
│  → 下次重启更快（不需要重放太多 EditLog）                   │
│
│ ★ HA 模式 Checkpoint（源码级）：                              │
│  Standby NameNode 定期做 Checkpoint（减轻 Active 负担）      │
│  Standby 做完 → Active 可以直接拷贝新的 FSImage             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、DataNode 数据管理

```
┌─────────────────────────────────────────────────────────────┐
│  ★ DataNode Block 管理（源码级）                              │
│                                                               │
│  DataNode 存储结构：                                          │
│  // data/hdfs/data/current/                                 │
│  ├── BP-1001734989-172.16.x.x-50010                          │
│  │   ├── blk_1073741825_1001.meta （Block 元数据）         │
│  │   ├── blk_1073741825            （Block 数据）          │
│  │   ├── blk_1073741826_1002.meta                           │
│  │   └── blk_1073741826                                      │
│  └── BP-1001734989-...                                       │
│
│  ★ 三大核心通信（面试高频！）：                                │
│                                                               │
│  ① Block Report（DataNode → NameNode）：                     │
│     定期（默认 6h）DataNode 上报所有 Block 清单              │
│     → NameNode 更新 BlocksMap                                │
│     → 发现缺失 Block → 触发副本重建                          │
│
│  ② Heartbeat（DataNode → NameNode）：                        │
│     每 3s DataNode 发心跳                                    │
│     → NameNode 知道这个 DN 还活着                            │
│     → 如果 10min 没心跳 → DN 标记为 dead                     │
│     → dead DN 上的 Block → 自动触发副本重建                  │
│
│  ③ Block Command（NameNode → DataNode）：                     │
│     NameNode 下发 Block 操作：                               │
│     REPORT（上报 Block）                                     │
│     RECOVER（恢复 Block）                                   │
│     COPY（复制 Block 到其他 DN）                             │
│     DELETE（删除不需要的 Block）                             │
│
│ ★ 为什么 Block 默认 128MB？（面试高频）                       │
│   太大（比如 1GB）：一个 Block 挂了 → 大文件读失败 → 影响大   │
│   太小（比如 1MB）：Block 数量爆炸 → NameNode 内存不够     │
│   128MB：平衡 IO（大数据快）+ 内存占用（可接受）            │
│   Hadoop 1.x 默认 64MB → 2.x 开始默认 128MB                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、文件读写流程源码级

### 4.1 文件写入全链路

```java
// ★ HDFS 文件写入全链路（源码级）
//
// ① Client → NameNode：create(path)
//    NameNode：
//    a. 检查路径是否已存在
//    b. 分配 INodeID
//    c. 创建 INodeFile（blocks 暂时为空）
//    d. 分配第一个 Block 的 3 个 DataNode（按机架感知）
//    e. 返回：FSDataOutputStream(DN1, DN2, DN3)
//
// ② Client → DN1：write(block1_data)
//    Pipeline 写入（3 副本流水线）：
//    Client → DN1 → DN2 → DN3
//    DN1：接收数据 + 本地写 → 转发给 DN2 → ACK
//    DN2：接收数据 + 本地写 → 转发给 DN3 → ACK
//    DN3：接收数据 + 本地写 → ACK（最后一跳）
//    DN2 收到 DN3 ACK → ACK 给 DN1
//    DN1 收到 DN2 ACK → ACK 给 Client
//    → 3 副本都写完才返回成功（CP 强一致！）
//
// ③ Block 写满（128MB）→ Client 再次向 NameNode 请求新 Block
//    NameNode 再分配 3 个 DN（考虑负载均衡）
//    → 继续 Pipeline 写入
//
// ④ Client → NameNode：close()
//    NameNode：更新 INodeFile.blocks（最终 Block 列表）
//    → 文件写入完成
//
// ★ Pipeline vs 并行写（面试高频）：
//   Pipeline：Client→DN1→DN2→DN3（流水线，省时间）
//   并行写：Client→DN1 + Client→DN2 + Client→DN3（3 倍带宽）
//   → HDFS 用 Pipeline（延迟低，适合高带宽）
//
```

### 4.2 文件读取全链路

```java
// ★ HDFS 文件读取全链路（源码级）
//
// ① Client → NameNode：getBlockLocations(path)
//    NameNode：
//    a. 查找 INodeFile → 拿到 blocks[]
//    b. 每个 Block 返回 3 个 DN（按距离排序）
//    c. 返回：FSDataInputStream(blocks + DN 列表)
//
// ② Client 从最近的 DN 读 Block
//    比如 DN1 是本地 → Client 直接读 DN1
//    → 不走 NameNode（NameNode 只管元数据！）
//    → Block 数据直接从 DN 拉取
//
// ③ 如果 DN1 读失败 → 降级到 DN2 → DN3
//    → 3 副本容错
//
// ★ 读流程 NameNode 不参与数据传输：
//   NameNode 只管元数据（路径 → Block 列表）
//   数据传输走 Client ↔ DataNode
//   → NameNode 不成为瓶颈！
//
```

---

## 五、HA 高可用（Active + Standby + JournalNode）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ HDFS HA 架构（源码级，面试高频！）                        │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Active NameNode                                        │   │
│  │  ├── 处理 RPC（读/写请求）                             │   │
│  │  ├── 写 EditLog → JournalNode                         │   │
│  │  └── 接受 DataNode Block Report + Heartbeat            │   │
│  └────────────────┬──────────────────────────────────────┘   │
│                   ↓ EditLog 同步                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ JournalNode Cluster（奇数 3/5 个）                     │   │
│  │   → Raft 同步 EditLog → 保证 Active/Standby 一致     │   │
│  └────────────────┬──────────────────────────────────────┘   │
│                   ↓ EditLog 同步                              │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ Standby NameNode                                       │   │
│  │  ├── 从 JournalNode 读 EditLog → 重放                 │   │
│  │  ├── 定期做 Checkpoint（减轻 Active 负担）              │   │
│  │  └── 不处理 RPC（只做同步 + Checkpoint）              │   │
│  └───────────────────────────────────────────────────────┘   │
│                   ↓ ZKFC 监控                                │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ ZKFC（ZK Failover Controller）                        │   │
│  │   → 监控 Active NameNode 健康                          │   │
│  │   → Active 挂了 → Fencing（SSH kill + 禁 IP）         │   │
│  │   → 然后 Standby 变 Active                            │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ Fencing 为什么必要？（面试高频）：                           │
│   Active NameNode 可能假死（GC 暂停 + 网络分区）           │
│   → 客户端还以为它是 Active → 继续发请求                    │
│   → Standby 变新 Active → 两个 Active → 脑裂！             │
│   → Fencing：SSH 到旧 Active 机器 kill 进程 + 禁 IP       │
│   → 保证旧 Active 完全停掉 → 新 Active 安全接手            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（20 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | HDFS 核心架构？| **NameNode（元数据）+ DataNode（存数据）+ Client** |
| 2 | NameNode 内存模型？| **FSDirectory（INode 树）+ INodeMap + BlocksMap** → 全在内存 |
| 3 | INode 继承体系？| INodeDirectory（目录）+ INodeFile（文件，含 BlockInfo[]）|
| 4 | FSImage + EditLog？| **FSImage（全量快照）+ EditLog（增量日志）** → 重启恢复 |
| 5 | EditLog 什么时候 Roll？| 超过 64MB 或 1h → 合并到新 FSImage |
| 6 | DataNode 三大通信？| **Block Report（6h）+ Heartbeat（3s）+ Block Command（下发）** |
| 7 | Block 默认大小？| **128MB**（平衡 IO 快 + 内存占）；Hadoop 1.x 是 64MB |
| 8 | 文件写入流程？| **create → 分配 DN → Pipeline 3 副本 → close** |
| 9 | Pipeline vs 并行写？| Pipeline（Client→DN1→DN2→DN3，流水线延迟低）|
| 10 | 文件读取流程？| NameNode 只返回 Block 位置 → **Client 直接读 DN**（不经过 NameNode）|
| 11 | HA 架构？| Active + Standby + **JournalNode（Raft 同步 EditLog）** + ZKFC |
| 12 | JournalNode 作用？| **Active 写 EditLog + Raft 同步 + Standby 读** → 两 NN 一致 |
| 13 | Fencing 为什么？| **防止脑裂**：Active 假死 → ZKFC kill + 禁 IP → 保证旧 Active 停掉 |
| 14 | Block 副本默认？| **3 副本**；机架感知（同机架 2 + 跨机架 1）|
| 15 | DataNode 挂了怎么办？| 10min 没心跳 → DN 标记 dead → Block Report 缺 Block → **自动触发副本重建** |
| 16 | HDFS vs TiKV vs S3？| HDFS：大数据离线（Block + 只追加）；TiKV：分布式事务（K-V + Raft）；S3：PB 归档（Object + 写新版本）|
| 17 | 面试最高频考点排序？| **1. NameNode 内存数据结构 2. Pipeline 写入 3. HA JournalNode 4. Fencing 防脑裂 5. Block 副本** |
| 18 | HDFS 学习路径？| FSDirectory → INode → FSEditLog → BlockManager → HA Active/Standby |
| 19 | NameNode 内存瓶颈？| Block 数量太多 → NameNode 内存不够；解决：Federation（多个 NameNode 分管不同目录）|
| 20 | 面试常见追问？| HDFS 读写流程完整链路 + HA 切换 + Block 副本重建触发时机 |

---

**文档版本：v1.0 · 2026-10-23**  
**面试考点：20 题**  
**差异化点**：大数据基石 + NameNode 内存模型 + Pipeline 写入 + HA Fencing
