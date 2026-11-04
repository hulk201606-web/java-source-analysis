# VictoriaMetrics 源码深度解析（Prometheus 高性能替代）

> **源码基线**：VictoriaMetrics v1.107.x（最新稳定）  
> **核心仓库**：https://github.com/VictoriaMetrics/VictoriaMetrics  
> **面试权重**：⭐⭐⭐⭐（监控存储高频，Prometheus 性能瓶颈时首选）

---

## 一、VictoriaMetrics 整体定位

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：高性能 Prometheus 兼容存储                        │
│  "10x 更高吞吐 + 更低内存 - Prometheus 的开源替代"          │
│                                                               │
│  和 Prometheus / Thanos / Mimir 对比（面试高频）：           │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Prometheus   │ Thanos       │ VictoriaMetrics│  │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 架构       │ 单体         │ Prometheus + Sidecar│ 单体/集群 │   │
│  │ 吞吐       │ ★★         │ ★★★          │ ★★★★★        │   │
│  │ 内存       │ ★★★★ 高    │ ★★★ 中       │ ★★★★★ 极低  │   │
│  │ 压缩率     │ ★★★        │ ★★★          │ ★★★★★ 10x    │   │
│  │ 远程写     │ ❌          │ ✅ Thanos Receive│ ✅ vminsert │   │
│  │ HA         │ ❌          │ ✅ Replica +  │ ✅ Cluster   │   │
│  │            │              │   Thanos Query│              │   │
│  │ 生态       │ Prometheus  │ CNCF         │ 兼容 PromQL  │   │
│  │ 许可证     │ Apache v2.0 │ Apache v2.0  │ AGPL 商业可选│   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│                                                               │
│ ★ VictoriaMetrics 是 **Prometheus 的高性能替代**            │
│ ★ Prometheus 单机存储上限 ~150GB；VictoriaMetrics 单机可到 10TB│
│ ★ Compaction 算法：VTsDB（比 Prometheus TSDB 快 10 倍）    │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  VictoriaMetrics 架构                                        │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │ Single Node     │ Cluster        │ Enterprise     │         │
│  │ vmselect +      │ vminsert +    │ 完整集群 +     │         │
│  │ vmstorage       │ vmselect +    │ 长周期存储     │         │
│  │ (all-in-one)    │ vmstorage     │ + 多租户       │         │
│  │                 │ (分布式)      │               │         │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│ Cluster 架构组件（面试高频）：                                │
│   vminsert   → 写入层（接收 Prometheus remote_write）         │
│   vmselect   → 查询层（PromQL 引擎 + 负载均衡）              │
│   vmstorage  → 存储层（VTsDB 分布式存储，Shard + Replication）│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 VTsDB（★ 比 Prometheus TSDB 快 10 倍）

```go
// ★ VictoriaMetrics 用 VTsDB（自研时序数据库，比 Prometheus TSDB 快 10 倍）
//
// Prometheus TSDB（已知瓶颈）：
//   ① Head Block 内存大（WAL + 未压缩数据）
//   ② Compaction 慢（后台线程，大 Block 合并占 CPU）
//   ③ 压缩率一般（默认 Snappy）
//   ④ 存储上限 ~150GB（超过性能下降）
//
// VTsDB 优化（核心）：
//
// ① 内存优化：
//    - 减少内存拷贝（从 WAL 直接写入 Segment）
//    - 数据按小时分 Segment（不是 TSDB 的 2h Block）
//    - 实时压缩（写入时就压缩，不是等 Block 满了再压缩）
//
// ② 存储优化：
//    - LZ4 / ZSTD 压缩（比 Snappy 小 10x！）
//    - 稀疏索引（类似 ClickHouse / Iceberg）
//    - 数据按 metric name hash 分布（避免热点）
//
// ③ Compaction 优化：
//    - 写入时并行 Compaction（不是后台单独线程）
//    - 小 Segment 自动合并成大 Segment
//    - 大 Segment 直接落盘（不参与后续 Compaction）
//
// ★ 数据结构：
//   Partition（天）→ Hour Segment（1h）→ 压缩后文件
//   写入：实时压缩 → 直接落盘（不累积 Head Block）
//   查询：只读需要的 Partition/Hour Segment
//
// ★ 性能对比（实际测试）：
//   Prometheus：10w metrics + 15s scrape → ~20GB 内存 + 500GB 磁盘
//   VictoriaMetrics：同样负载 → ~2GB 内存 + 30GB 磁盘
//   → 内存低 10x + 磁盘低 16x！
```

### 2.2 Cluster 模式

```go
// ★ VictoriaMetrics Cluster 组件（面试高频）
//
// vminsert（写入层）：
//   接收 Prometheus remote_write / OpenTelemetry / InfluxDB 格式
//   → 路由到 vmstorage 节点（按 metric hash 分片）
//   → 支持多副本（2-3 副本保证高可用）
//
// vmselect（查询层）：
//   接收 PromQL 查询
//   → 并行向所有 vmstorage 节点发请求
//   → 合并结果（类似 ES Scatter-Gather）
//   → 支持 Prometheus remote_read（和 Prometheus 生态无缝集成）
//
// vmstorage（存储层）：
//   管理 VTsDB 分片
//   → 按 metric name hash 分片
//   → 2-3 副本（类似 Kafka Replication）
//   → 数据写入：Quorum 确认（类似 Raft）
//
// ★ 对比 Prometheus + Thanos：
//   Thanos：Prometheus（每个节点独立存储）+ Thanos Sidecar + Thanos Query
//   VictoriaMetrics：原生分布式（写入/查询/存储分离）→ 架构更清晰
//
// ★ 部署方式：
//   Docker Compose（测试）→ K8s Operator（生产）
//   vmstorage replicas = 3（保证高可用）
//   vminsert + vmselect replicas = 2+（负载均衡）
```

### 2.3 Prometheus 兼容

```go
// ★ VictoriaMetrics 完全兼容 Prometheus 生态
//
// ① PromQL 100% 兼容：
//   VictoriaMetrics 自己实现了 PromQL 引擎
//   → 所有 PromQL 语法都支持（包括 Prometheus 官方不支持的扩展）
//   → 额外支持 MetricsQL（VictoriaMetrics 扩展）
//
// ② Scrape 兼容：
//   VictoriaMetrics 可以直接替代 Prometheus 做 scrape
//   → 用 vmagent（轻量 scrape agent）
//   → 比 Prometheus scrape 省 5-10x 内存
//
// ③ Prometheus 集成：
//   Prometheus remote_write → vminsert（从 Prometheus 推到 VictoriaMetrics）
//   Prometheus remote_read → vmselect（Prometheus 查询 VictoriaMetrics）
//   → 可以作为 Prometheus 的长期存储（短期 Prometheus + 长期 VictoriaMetrics）
//
// ④ Grafana 兼容：
//   Grafana 直接配 VictoriaMetrics 作为数据源
//   → 和 Prometheus 完全一样（相同 PromQL）
//
// ★ vmagent（轻量 scrape agent）：
//   比 Prometheus scrape 轻量 5-10x
//   → 适合边缘节点 / IoT 场景
//   → 把采集的数据推到 VictoriaMetrics / Prometheus
```

### 2.4 长周期存储 + 成本优化

```go
// ★ VictoriaMetrics 支持超长时间存储（成本优势）
//
// Prometheus：默认 retention = 15d（最多 1y，但性能急剧下降）
// VictoriaMetrics：可以存 10y+（数据压缩效率高）
//
// 冷热分层：
//   热数据（最近 30d）→ SSD
//   冷数据（30d+）→ HDD / S3（Archive 模式）
//   → 自动冷热迁移
//
// 成本对比（10w metrics + 5y 存储）：
//   Prometheus：不可能（存储上限 + 性能瓶颈）
//   VictoriaMetrics Enterprise：~50GB SSD + ~2TB S3
//   → 比 Prometheus 成本低 100x！
//
// ★ Enterprise 版额外功能：
//   ① 长周期存储（Archive 模式）
//   ② 多租户（VM Select / VM Insert 按租户隔离）
//   ③ 基于角色的访问控制（RBAC）
//   ④ 高级告警（Anomaly Detection 异常检测）
//
// ★ 开源 vs 企业版：
//   开源版：单机 / Cluster / 无限指标 / PromQL
//   企业版：Archive 长存储 / 多租户 / RBAC / Anomaly Detection
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | VictoriaMetrics 核心定位？| **Prometheus 高性能替代** → VTsDB 自研存储（比 TSDB 快 10 倍）；单机 10TB / 内存低 10x |
| 2 | VTsDB vs Prometheus TSDB？| VTsDB：**实时压缩 + LZ4/ZSTD + 小时级 Segment + 内存优化**；TSDB：Head Block + 2h Block + Compaction 慢；性能 10x 差距 |
| 3 | VM Cluster 组件？| **vminsert（写入层）/ vmselect（查询层）/ vmstorage（存储层）** → 按 metric hash 分片 + 多副本 Quorum |
| 4 | VM vs Thanos vs Mimir？| VM：**原生分布式 + 简单 + 性能最高**；Thanos：Prometheus Sidecar + Query；Mimir：Grafana 出品 + 企业功能 |
| 5 | VM Prometheus 兼容？| **100% PromQL 兼容** + Prometheus remote_write/remote_read + vmagent 轻量 scrape；Grafana 直接支持 |
| 6 | vminsert / vmselect / vmstorage？| vminsert：接收写请求 + 路由分片；vmselect：PromQL 引擎 + Scatter-Gather 合并；vmstorage：VTsDB 分片存储 |
| 7 | VM 内存为什么低？| **实时压缩（写入时压）+ 内存拷贝优化 + 小时级 Segment + LZ4/ZSTD 极高压缩率**；同样负载内存低 10x |
| 8 | vmagent 是什么？| **轻量 scrape agent**（比 Prometheus scrape 省 5-10x 内存）→ 适合边缘/IoT 把数据推到 VM |
| 9 | VM 长周期存储？| 开源版可以存 10y+（VM 支持）；Enterprise 支持 **Archive 冷热分层**（SSD 热 + S3 冷）；Prometheus 做不到 |
| 10 | VM 数据一致性？| Cluster 模式：Quorum（2/3 副本确认）→ 类似 Raft；单机模式：单副本（无 HA） |
| 11 | VM 支持哪些协议？| Prometheus remote_write/read / OpenTelemetry / InfluxDB Line Protocol / Graphite / OpenTSDB → 多协议接入 |
| 12 | VM 告警？| VMAlertmanager（兼容 Prometheus Alertmanager）+ VMRules（规则）；Prometheus 也能配 Alertmanager |
| 13 | VM 学习路径？| Docker 起 Single Node → scrape → Grafana 接数据源 → PromQL → Cluster 模式 → vminsert/vmselect/vmstorage |
| 14 | VM 为什么比 Prometheus 快？| ① VTsDB 自研存储（比 TSDB 先进）② C++ + Go 混合（VMstorage C++ 写）③ 实时压缩 ④ 稀疏索引 |
| 15 | VM 许可证？| 开源版 AGPL v3.0；Enterprise 商业（长存储/多租户/RBAC）→ AGPL 对云厂商有限制 |
| 16 | VM vs TimescaleDB / InfluxDB？| VM：**时序 Metrics 专用 + 轻量 + Prometheus 生态**；TimescaleDB：关系型 + SQL；InfluxDB：时序 + 功能全但性能一般 |
| 17 | VM 数据分片？| 按 metric name hash 分片 → 均匀分布；热点 metric 可以加 label 拆分 |
| 18 | VM 和 SkyWalking？| SkyWalking Metrics 可以推到 VM；VM 比 Prometheus 存历史 Metrics 成本低 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：VictoriaMetrics v1.107.x（Go + C++）  
**面试考点：18 题**
