# Prometheus 源码深度解析（监控标配）

> **源码基线**：Prometheus 2.55.x / 3.0（最新稳定）  
> **核心仓库**：https://github.com/prometheus/prometheus  
> **面试权重**：⭐⭐⭐⭐⭐（可观测性高频，SkyWalking 互补）

---

## 一、Prometheus 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：开源监控系统（Metrics 采集 + 存储 + 查询 + 告警）  │
│  为什么监控用 Prometheus？                                    │
│  → 云原生（K8s 标配）+ 拉模型（Pull）+ PromQL 强大查询       │
│  + 指标类型丰富 + 社区生态好                                  │
│                                                               │
│  和 SkyWalking 对比（互补不是替代）：                         │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │                │  Prometheus  │  SkyWalking   │             │
│  ├──────────────┼──────────────┼──────────────┤             │
│  │ 监控对象       │ Metrics（指标）│ Tracing（链路） │             │
│  │ 数据获取       │ Pull（拉模型）│ Java Agent（注入）│             │
│  │ 存储           │ TSDB          │ ES/BanyanDB   │             │
│  │ 告警           │ AlertManager  │ 内置告警      │             │
│  │ UI             │ Prometheus UI  │ SkyWalking UI │             │
│  │ 互补关系       │ Metrics 存   │ SkyWalking 的数据  │             │
│  │                │ Prometheus    │ 也能推到 Prometheus│             │
│  └──────────────┴──────────────┴──────────────┘             │
│                                                               │
│ ★ 典型组合：Prometheus（Metrics）+ Grafana（可视化）+ AlertManager（告警）│
│   + SkyWalking（Tracing）→ 完整可观测性三件套                │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Prometheus 架构组件                                          │
│                                                               │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │   Prometheus  │ AlertManager  │ Pushgateway  │             │
│  │  核心组件      │ 告警路由/静默  │ Push 模式入口 │             │
│  │  Scrape + TSDB │ 邮件/钉钉     │ 短生命周期任务 │             │
│  │  PromQL 查询   │              │ 推 Metrics    │             │
│  └──────┬───────┴──────┬───────┴──────┬───────┘             │
│         ↓              ↓              ↓                       │
│   目标 Exporter     Receiver       Pushgateway                 │
│   (Node/MysQL/JVM)  (Tracing)     接收 Push Metrics            │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 TSDB（★★★★★ 存储核心）

```go
// ★ TSDB 是 Prometheus 自己实现的时序数据库（不是通用时序 DB 如 InfluxDB）
//
// TSDB 数据模型：
//   TimeSeries = Metric Name + Labels（标签集合）
//   每条 TimeSeries 是一个时间序列（timestamp + value 对的列表）
//   存储在：chunks（时间块，默认 2h 一个）+ index（倒排索引）
//
// TSDB 三层结构（面试高频）：
//
// ① Head Block（热数据）
//   内存 + WAL（Write-Ahead Log）
//   → 最新 2h 数据在内存（查询快）
//   → WAL 保证崩溃恢复（和 MySQL Redo Log 一样）
//
// ② Persistent Blocks（冷数据）
//   磁盘上的 immutable block（2h 一个）
//   → 每个 block 包含：
//     chunks（数据块，snappy 压缩）
//     index（倒排索引：Label → TimeSeries）
//     meta.json（block 元信息）
//
// ③ Compaction（压缩合并）
//   后台 goroutine 把多个小 block 合并成大 block
//   → 类似 ES Merge / TiKV RocksDB Compaction
//   → 删除过期数据（retention 配置，默认 15 天）
//
// ★ TSDB 写入流程：
//   scrape 到样本 → Append（timestamp + value + labels）
//     → WAL 追加（保证崩溃恢复）
//     → Head Block 内存追加
//     → Head 满 2h → 写入新 Persistent Block
//
// ★ TSDB vs 通用时序 DB：
//   Prometheus TSDB：**简单高效**（只有一种数据模型 + 一种压缩）
//   InfluxDB / TimescaleDB：功能更全（SQL 查询 + 多租户）但性能不如 TSDB
```

### 2.2 Scrape（★★★★ 数据采集核心）

```go
// ★ Prometheus 用 Pull 模型拉 Metrics
//   每个 scrape_config 定义一组 targets
//   → 定时 scrape_interval（默认 15s）拉一次
//
// Scrape 核心类：
//
// ① Manager（总管理器）
//   管理所有 scrape pool
//
// ② Pool（一个 scrape_config 对应一个 pool）
//   管理一组 targets
//
// ③ Scraper（单个 target 采集）
//   发 HTTP GET /metrics → 解析文本 → Append 到 TSDB
//
// ★ Service Discovery（服务发现，面试高频）：
//   Prometheus 支持动态发现 targets（不用硬编码 IP）
//   - static_configs：静态配置
//   - file_sd_configs：从文件读 JSON/YAML
//   - dns_sd_configs：DNS 查询
//   - kubernetes_sd_configs：K8s API Server 发现 Pod/Service
//   - consul_sd_configs：Consul 注册中心发现
//
// ★ K8s 服务发现（最常用）：
//   kubernetes_sd_configs:
//     - role: pod
//       relabel_configs:
//         - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
//           action: keep
//           regex: true
//   → 自动发现带 annotation prometheus.io/scrape=true 的 Pod
//   → Relabel Config 可以改 label（面试高频！）
//
// ★ Relabel Config（标签重写）：
//   scrape 前后可以改 labels：drop / keep / replace / hashmod / labelmap
//   → 常用场景：
//     - 只采集有特定 annotation 的 Pod
//     - 把 K8s metadata（pod_namespace）改成业务 label
//     - 多租户隔离（hashmod tenant_id 做分片）
```

### 2.3 PromQL（★★★★ 查询核心）

```
// ★ PromQL 是 Prometheus 独有的查询语言（类似 SQL 但面向时序数据）
//
// 四种指标类型（面试高频！）：
//
// ① Counter（计数器）：单调递增（重置时归 0）
//   适用：请求总数、错误总数、订单总数
//   PromQL: rate(http_requests_total[5m]) → 5 分钟平均速率
//
// ② Gauge（仪表）：可增可减
//   适用：CPU 使用率、内存使用率、队列长度、当前在线用户
//   PromQL: node_cpu_seconds_total → 直接拿当前值
//
// ③ Histogram（直方图）：采样分布 + 桶
//   适用：请求延迟、响应时间
//   PromQL: histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))
//   → 99 分位延迟（面试高频！）
//
// ④ Summary（摘要）：分位数 + count + sum
//   适用：请求延迟（客户端计算分位数）
//   和 Histogram 区别：Summary 客户端算分位数（精确）；Histogram 服务端算（近似但可聚合）
//
// ★ PromQL 核心函数（面试高频）：
//   rate(metric[5m])      → 平均增长速率（Counter 专用）
//   irate(metric[5m])     → 瞬时增长速率（Counter 专用）
//   histogram_quantile()  → 计算分位数（Histogram 专用）
//   topk(10, metric)      → Top 10
//   sort(metric)          → 排序
//   label_replace()       → 改 label
//   increase(metric[5m])  → 5 分钟内增长值
//
// ★ PromQL 常用查询模式：
//   # 1. QPS（每秒请求数）
//   sum(rate(http_requests_total[5m])) by (service)
//
//   # 2. 错误率
//   sum(rate(http_requests_total{status=~"5.."}[5m])) by (service)
//   / sum(rate(http_requests_total[5m])) by (service)
//
//   # 3. P99 延迟
//   histogram_quantile(0.99,
//     sum(rate(http_request_duration_seconds_bucket[5m])) by (service, le))
//
//   # 4. CPU 使用率
//   1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) by (instance)
```

### 2.4 AlertManager（告警核心）

```yaml
// ★ AlertManager 负责告警路由/分组/抑制/静默
//   Prometheus 只负责触发告警规则 → 推给 AlertManager
//
// alertmanager.yml 核心配置：
//
// global:
//   resolve_timeout: 5m                    # 告警恢复等待时间
//   smtp_smarthost: 'smtp.example.com:587' # 邮件服务器
//   smtp_auth_username: 'alert@example.com'
//   smtp_auth_password: 'xxx'
//
// route:                                    # 告警路由规则（树状）
//   group_by: ['alertname', 'service']      # 分组（相同 alertname+service 合成一条）
//   group_wait: 10s                         # 分组内第一个告警等 10s 再发
//   group_interval: 10s                     # 同组新告警间隔多久发一次
//   repeat_interval: 1h                     # 未恢复告警多久重复发一次
//   receiver: 'default-receiver'           # 默认接收者
//   routes:
//     - matchers:
//         - severity="critical"             # 严重告警走 PagerDuty
//       receiver: 'pagerduty-critical'
//     - matchers:
//         - severity="warning"              # 警告走钉钉/邮件
//       receiver: 'dingtalk-warning'
//
// receivers:                                # 接收者（多种渠道）
//   - name: 'default-receiver'
//     email_configs:
//       - to: 'ops@example.com'
//   - name: 'pagerduty-critical'
//     pagerduty_configs:
//       - routing_key: 'xxx'
//   - name: 'dingtalk-warning'
//     webhook_configs:
//       - url: 'https://oapi.dingtalk.com/robot/send?access_token=xxx'
//
// ★ 告警抑制（Inhibition）：
//   - source_matchers: severity="critical"  # critical 触发时
//   - target_matchers: severity="warning"   # 抑制同组 warning
//   → Critical 和 Warning 同时触发 → 只发 Critical
//
// ★ 告警静默（Silence）：
//   临时沉默某些告警（维护窗口用）
//   - matchers:
//       - alertname="HighErrorRate"
//   - starts_at: "2024-10-01T00:00:00Z"
//   - ends_at: "2024-10-02T00:00:00Z"
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Prometheus 四种指标类型？| **Counter（单调递增）/ Gauge（可增可减）/ Histogram（桶分布+近似分位）/ Summary（分位数+count+sum）**|
| 2 | TSDB 三层存储？| **Head Block（内存 + WAL，最新 2h）/ Persistent Blocks（磁盘 immutable block，2h 一个）/ Compaction（后台合并 + 删除过期）**|
| 3 | Prometheus Pull vs Push？| 默认 **Pull 模型**（定时 HTTP GET /metrics）；Pushgateway 接受短生命周期任务 Push；K8s 用 SD 自动发现 targets |
| 4 | PromQL rate vs irate？| **rate**（5m 平均增长速率，Counter 专用）；**irate**（瞬时增长速率，取最后 2 个点算变化率）|
| 5 | Histogram vs Summary？| Histogram：**桶分布 + 服务端近似分位**（可跨机器聚合）；Summary：**客户端精确分位**（不能聚合）；生产推荐 Histogram |
| 6 | K8s 服务发现？| **kubernetes_sd_configs**（role=pod/service/node）+ Relabel Config；自动发现带 annotation 的 Pod |
| 7 | AlertManager 核心功能？| **告警路由（树状）/ 分组（group_by）/ 抑制（Inhibition）/ 静默（Silence）/ 多渠道**（邮件/钉钉/PagerDuty/Webhook）|
| 8 | Prometheus vs SkyWalking？| Prometheus：**Metrics 监控（Pull 模型 + PromQL）**；SkyWalking：**Tracing 链路追踪（Java Agent 注入）**；互补不是替代 → 完整可观测性三件套 = Prometheus + Grafana + SkyWalking |
| 9 | Prometheus 和 Grafana？| Prometheus：**存 Metrics + PromQL 查询**；Grafana：**可视化 + 告警通知**；Grafana 后端可以接 Prometheus / InfluxDB / ES 等 |
| 10 | TSDB WAL 作用？| **崩溃恢复** → 写入内存前先写 WAL；Prometheus 重启后回放 WAL 恢复 Head Block |
| 11 | Relabel Config？| scrape 前后改 labels：**drop / keep / replace / hashmod / labelmap**；常用场景：只采集特定 Pod、改写 K8s metadata label |
| 12 | 常见 Exporter？| **Node Exporter（机器）/ mysqld_exporter（MySQL）/ JMX Exporter（JVM）/ Spring Boot Actuator（/actuator/prometheus）** |
| 13 | Prometheus 高可用？| **Prometheus HA**（两个 Prometheus + Thanos/VictoriaMetrics 联邦查询）；不要直接 Prometheus 做长期存储（推荐 VictoriaMetrics/Mimir）|
| 14 | 告警规则配置？| Prometheus rule_files 下定义：`alert: HighErrorRate if rate(http_requests_total{status=~"5.."}[5m]) > 0.05 for 5m labels: {severity="critical"}` |
| 15 | Scrape 慢查询？| scrape_timeout（默认 10s）→ 目标 /metrics 接口 10s 内必须返回；如果 Metrics 计算慢要优化（避免在 /metrics 接口跑复杂查询）|
| 16 | TSDB 压缩？| 后台 goroutine 把多个小 block 合并 → 减少文件数量 + 删除过期数据；类似 ES Merge / TiKV RocksDB Compaction |
| 17 | Prometheus 3.0 新特性？| **TSDB 格式升级 / 内存效率更高 / OpenTelemetry 原生支持**；v2.55 是 v2 最新稳定 |
| 18 | Prometheus 学习路径？| 写一个 /metrics 端点 → Prometheus 配置 scrape → PromQL 查 → AlertManager 配告警 → Grafana 做 Dashboard |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Prometheus 2.55.x / 3.0  
**面试考点：18 题**
