# 分布式全局 ID 生成 Snowflake + Leaf + Tinyid 源码级深度解析

> **源码基线**：Twitter Snowflake + 美团 Leaf + 滴滴 Tinyid（Java）  
> **面试权重**：⭐⭐⭐⭐⭐（全局 ID 高频必问，订单号/分库分表必备）

---

## 一、全局 ID 核心要求

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式全局 ID 六大要求（面试必背！）                       │
│                                                               │
│  ① 全局唯一：不能重复（最基本）                              │
│  ② 趋势递增：MySQL InnoDB 聚簇索引需要                        │
│  ③ 高性能：生成延迟低（本地生成最好）                         │
│  ④ 高可用：服务挂了还能发号                                  │
│  ⑤ 信息安全：不能被遍历（订单号连续会被竞品爬数据）          │
│  ⑥ 接入简单：最好不依赖中间件                                │
│
│ ★ 五大方案全景：                                              │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │ 方案       │ 性能         │ 依赖          │ 典型          │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ UUID       │ 高（本地）    │ 无            │ 简单场景      │        │
│  │ 数据库自增  │ 低           │ MySQL        │ 小系统        │        │
│  │ 号段模式    │ 高           │ MySQL        │ Leaf-segment  │        │
│  │ Redis INCR │ 中高         │ Redis        │ 简单分布式     │        │
│  │ Snowflake ★│ 极高（本地） │ 时钟+workerId │ Twitter/美团  │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
│ ★ UUID 为什么不适合做订单 ID？                                │
│   太长（36 字符）+ 无序（InnoDB 聚簇索引页分裂严重）         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Snowflake 雪花算法源码级

### 2.1 64bit 结构

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Snowflake 64bit 结构（面试必背！）                        │
│                                                               │
│  0 ┃ 41 bit 时间戳        ┃ 10 bit 机器 ┃ 12 bit 序列号      │
│  符号┃ ms 级（可用 69 年） ┃ workerId    ┃ 同毫秒自增         │
│                                                               │
│  详细：                                                        │
│  ┌─┬───────────────────────────┬──────────┬──────────────┐  │
│  │0│  41 bit 毫秒时间戳          │ 10 bit   │ 12 bit       │  │
│  │符│ (now - epoch)             │ workerId │ sequence     │  │
│  │号│ 2^41ms ≈ 69 年            │ 0~1023   │ 0~4095       │  │
│  └─┴───────────────────────────┴──────────┴──────────────┘  │
│                                                               │
│  ★ 单机每毫秒可生成 4096 个 ID                                │
│  ★ 1024 个 worker 节点                                        │
│  ★ 理论 QPS：4096 × 1000ms = 409w/s                          │
│
```

### 2.2 Snowflake 核心源码

```java
// ★ Twitter Snowflake 核心源码（面试核心！）
//
// public class SnowflakeIdWorker {
//     // ★ 起始时间戳（2020-01-01，自己定义，决定可用 69 年起点）
//     private final long twepoch = 1577808000000L;
//
//     // ★ 各部分位数
//     private final long workerIdBits = 10L;   // 机器 10 bit
//     private final long sequenceBits = 12L;   // 序列 12 bit
//
//     // ★ 最大值
//     private final long maxWorkerId = ~(-1L << workerIdBits);  // 1023
//     private final long sequenceMask = ~(-1L << sequenceBits); // 4095
//
//     // ★ 位移
//     private final long workerIdShift = sequenceBits;          // 12
//     private final long timestampLeftShift = sequenceBits + workerIdBits; // 22
//
//     private long workerId;
//     private long sequence = 0L;
//     private long lastTimestamp = -1L;
//
//     public synchronized long nextId() {
//         long timestamp = timeGen();
//
//         // ★ ① 时钟回拨处理（面试高频！）
//         if (timestamp < lastTimestamp) {
//             long offset = lastTimestamp - timestamp;
//             if (offset <= 5) {
//                 // 回拨 ≤5ms：等待追平
//                 try { wait(offset << 1); } catch (Exception e) {}
//                 timestamp = timeGen();
//             } else {
//                 // 回拨 >5ms：直接抛异常（或用备用位）
//                 throw new RuntimeException("Clock moved backwards");
//             }
//         }
//
//         // ★ ② 同一毫秒：序列号 +1
//         if (lastTimestamp == timestamp) {
//             sequence = (sequence + 1) & sequenceMask;
//             if (sequence == 0) {
//                 // 序列号用完 4096 → 自旋等下一毫秒
//                 timestamp = tilNextMillis(lastTimestamp);
//             }
//         } else {
//             // ★ ③ 新毫秒：序列号归零
//             sequence = 0L;
//         }
//
//         lastTimestamp = timestamp;
//
//         // ★ ④ 位运算拼接
//         return ((timestamp - twepoch) << timestampLeftShift)
//             | (workerId << workerIdShift)
//             | sequence;
//     }
// }
//
// ★ 优点：本地生成（无网络开销）+ 趋势递增 + 性能极高
// ★ 缺点：①时钟回拨 ②workerId 分配问题 ③ID 连续可预测
//
```

---

## 三、Leaf 源码级（美团）

### 3.1 Leaf 双模式

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 美团 Leaf 两种模式（面试高频！）                           │
│                                                               │
│  模式一：Leaf-Segment（号段模式）                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ MySQL 表：leaf_alloc                                  │  │
│  │  biz_tag | max_id | step | version                    │  │
│  │  order    | 1000  | 1000|   ...                       │  │
│  │                                                         │  │
│  │ Leaf Server 一次取一个号段 [1001, 2000] 放内存        │  │
│  │ → 应用从内存拿 ID（无 DB 压力）                       │  │
│  │ → 号段用到 10% 时异步预加载下一段（双 buffer）        │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  // 取号段 SQL（乐观锁 UPDATE）                                │
│  // UPDATE leaf_alloc SET max_id = max_id + step             │
│  // WHERE biz_tag = 'order' AND version = ?                  │
│
│  模式二：Leaf-Snowflake（Snowflake + ZooKeeper）             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ 解决 Snowflake workerId 分配问题：                      │  │
│  │ → Leaf 启动时向 ZK 注册顺序节点                        │  │
│  │ → 用 znode 序号作为 workerId（持久化，重启复用）        │  │
│  │ → 定时上报自身时间到 ZK                                │  │
│  │ → 时钟回拨：检查 ZK 上其他节点时间，落后太多则拒绝启动  │  │
│  └───────────────────────────────────────────────────────┘  │
│
│ ★ 双 buffer 优化（面试高频）：                                │
│   当前号段消耗到 10%（不是用完）                              │
│   → 异步线程预取下一号段                                     │
│   → 当前段用完时下一段已在内存 → 应用无感知、无等待          │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Segment 源码级

```java
// ★ Leaf Segment 核心源码（简化）
//
// public class Segment {
//     private AtomicLong value = new AtomicLong(0);  // 当前值
//     private long max;                               // 号段最大值
//     private int step;                               // 步长
// }
//
// public class SegmentBuffer {
//     private Segment[] segments = new Segment[2];   // ★ 双 buffer
//     private volatile int currentPos;               // 当前使用哪个
//     private volatile boolean nextReady;            // 下一个号段是否就绪
//     private volatile boolean threadRunning;        // 预取线程是否在跑
// }
//
// // ★ 获取 ID
// public long nextId(String key) {
//     SegmentBuffer buffer = buffers.get(key);
//     synchronized (buffer) {
//         Segment segment = buffer.getCurrent();
//
//         // ★ 用到 10% 且下一段没准备 → 异步预取
//         if (!buffer.isNextReady()
//                 && (segment.getIdle() < 0.9 * segment.getStep())
//                 && !buffer.isThreadRunning()) {
//             buffer.setThreadRunning(true);
//             service.execute(() -> updateNextSegment(buffer));  // 异步加载
//         }
//
//         long value = segment.getValue().incrementAndGet();
//         if (value < segment.getMax()) {
//             return value;  // 当前号段内直接返回
//         }
//
//         // ★ 当前号段用完 → 切换到下一段
//         waitUntilNextReady(buffer);
//         buffer.switchPos();
//         return nextId(key);
//     }
// }
//
```

---

## 四、Tinyid（滴滴）+ 方案对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 滴滴 Tinyid（号段模式增强版）                              │
│                                                               │
│  和 Leaf-Segment 类似，但增强：                               │
│   ① 双 buffer 号段                                            │
│   ② 支持 DB 多主（多个发号 DB，性能更高）                    │
│   ③ 批量获取 ID（一次拿一批，减少调用）                      │
│   ④ client SDK 本地缓存号段（比 HTTP 调 Leaf Server 更快）  │
│
│ ★ Snowflake vs Leaf-Segment vs Tinyid（面试必背表）：        │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ Snowflake    │ Leaf-Segment │ Tinyid       │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 生成位置    │ 本地内存      │ Leaf Server  │ Client 本地  │        │
│  │ 依赖        │ 时钟+workerId│ MySQL+ZK     │ MySQL        │        │
│  │ 趋势递增    │ ✅            │ ✅            │ ✅            │        │
│  │ ID 连续性   │ 不连续         │ 连续（号段内）│ 连续          │        │
│  │ 时钟回拨    │ ⚠️ 需处理     │ ✅ 无影响     │ ✅ 无影响     │        │
│  │ DB 宕机影响 │ 无            │ 有（双buffer缓冲）│ 有（缓存缓冲）│        │
│  │ 安全性      │ 可预测        │ 步长内连续    │ 步长内连续    │        │
│  │ 性能        │ ⭐⭐⭐⭐⭐    │ ⭐⭐⭐⭐      │ ⭐⭐⭐⭐      │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
│ ★ 面试选型建议：                                              │
│   超高并发 + 能接受时钟问题 → Snowflake（本地生成）           │
│   简单可靠 + 依赖 MySQL → 号段模式（Leaf/Tinyid）            │
│   防遍历（订单号）→ Snowflake 或号段 + 随机扰动              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 全局 ID 六要求？| 唯一 + 趋势递增 + 高性能 + 高可用 + 信息安全 + 接入简单 |
| 2 | Snowflake 64bit？| **1 符号 + 41 时间戳（69年）+ 10 workerId（1024节点）+ 12 序列（4096/ms）** |
| 3 | Snowflake 理论 QPS？| 4096 × 1000 = **409w/s 单节点** |
| 4 | Snowflake 时钟回拨？| **≤5ms 等待追平；>5ms 抛异常/备用位/切换 worker** |
| 5 | 同毫秒序列用完？| sequence 到 4096 → **自旋等下一毫秒**（tilNextMillis）|
| 6 | Leaf 两模式？| **Leaf-Segment（号段）+ Leaf-Snowflake（ZK 分配 workerId）** |
| 7 | 号段模式？| DB 存 max_id + step；Server 取一段 [1001,2000] 放内存发号 |
| 8 | 双 buffer 优化？| 用到 10% 异步预取下一段；用完切换无等待 |
| 9 | 取号段 SQL？| **乐观锁 UPDATE max_id=max_id+step WHERE version=?** |
| 10 | Tinyid vs Leaf？| Tinyid 号段模式 + Client 本地缓存 + DB 多主；Leaf 有 Snowflake 模式 |
| 11 | 面试最高频考点排序？| **1. Snowflake 位结构 2. 时钟回拨 3. 号段双 buffer 4. workerId 分配 5. 方案选型** |
| 12 | 学习路径？| SnowflakeIdWorker.nextId → 时钟回拨分支 → Leaf SegmentBuffer → 双 buffer 预取 |
| 13 | 面试常见追问？| workerId 怎么分配？→ ZK 顺序节点/配置中心/手动；号段 DB 挂了？→ 内存号段还能撑 |
| 14 | UUID 为什么不用？| 36 字符太长 + 无序导致 InnoDB 聚簇索引页分裂 |
| 15 | 为什么要趋势递增？| InnoDB 主键聚簇索引，顺序插入避免 B+树频繁分裂 |
| 16 | workerId 重复后果？| 两个节点可能生成相同 ID（灾难）→ 必须保证唯一 |
| 17 | Snowflake 安全问题？| ID 可预测（能算出订单量）→ 加密/随机位/不用纯 Snowflake |
| 18 | 百度 UidGenerator？| Snowflake 变种：RingBuffer 缓存 ID + 借用未来时间解决回拨 |
| 19 | 号段模式 ID 连续缺点？| 竞品可遍历订单 → 随机步长或跳号 |
| 20 | 面试高频场景？| 订单号 + 分库分表主键 + 消息 ID + 支付流水号 |
| 21 | 时钟回拨根因？| NTP 校时/手动改时间/虚拟机迁移；41 位时间戳依赖墙上时钟 |
| 22 | 面试陷阱？| 说 Snowflake 完全不依赖任何组件 → ❌ 需要分配 workerId（ZK/配置中心）|

---

**文档版本：v1.0 · 2026-10-23**  
**核心来源**：Twitter Snowflake + 美团 Leaf + 滴滴 Tinyid  
**面试考点：22 题**  
**差异化点**：64bit 位运算源码 + 时钟回拨处理 + Leaf 双 buffer + 三方案选型表
