# Apache Flink Watermark + EventTime + 窗口触发源码级深度解析

> **源码基线**：Flink 1.19.x（Java）  
> **核心仓库**：apache/flink → flink-streaming-java/  
> **面试权重**：⭐⭐⭐⭐⭐（流处理时间语义核心，高频必问）

---

## 一、三种时间语义

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink 三种时间语义（面试必背！）                          │
│                                                               │
│  ① Event Time（事件时间 ★ 生产最常用）                        │
│     数据本身携带的时间（事件发生时间，存在 Kafka 消息里）    │
│     → 用 Watermark 处理乱序                                  │
│
│  ② Ingestion Time（摄入时间，已废弃）                        │
│     数据进入 Flink Source 的时间                             │
│     → Flink 1.12+ 已移除                                     │
│
│  ③ Processing Time（处理时间）                              │
│     算子实际处理数据的机器时间                               │
│     → 最简单但结果不确定（重跑结果不一样）                   │
│
│  例子：用户行为日志                                           │
│    10:00:00 用户点击（event time，写在日志里）              │
│    10:00:05 进入 Kafka（网络延迟）                           │
│    10:00:08 Flink 处理（processing time）                    │
│
│ ★ 为什么需要 Event Time + Watermark？                        │
│   分布式系统网络延迟乱序：                                    │
│   event1(t=1) event3(t=3) event2(t=2) ← t=2 迟到了          │
│   → 不能每来一条就触发窗口（t=2 的数据会丢）                │
│   → Watermark 表示"我认为 ≤ t 的数据都到齐了"              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Watermark 源码级

### 2.1 Watermark 核心机制

```java
// ★ Flink Watermark 核心（源码级）
//
// // flink-streaming-java/.../streaming/api/watermark/Watermark.java
//
// public final class Watermark extends StreamElement {
//     // Watermark = 一个时间戳
//     // 含义：event time ≤ timestamp 的数据理论上都到齐了
//     private final long timestamp;
//
//     public static final Watermark MAX_WATERMARK =
//         new Watermark(Long.MAX_VALUE);  // 最大水位（结束标记）
// }
//
// // ★ Watermark 生成策略（Source 端或算子端生成）
// // flink-streaming-java/.../api/functions/source/datagen/...
//
// // 常用：BoundedOutOfOrdernessWatermarks（允许固定乱序）
// public class BoundedOutOfOrdernessWatermarks<T> implements WatermarkGenerator<T> {
//     private long maxTimestamp;      // 目前看到的最大 event time
//     private final long outOfOrdernessMillis;  // 最大允许乱序时间
//
//     // ★ 每来一条数据调用
//     @Override
//     public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
//         // 更新最大时间戳
//         maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
//     }
//
//     // ★ 周期性触发（默认 200ms）生成 Watermark
//     @Override
//     public void onPeriodicEmit(WatermarkOutput output) {
//         // watermark = 最大事件时间 - 最大乱序时间
//         output.emitWatermark(
//             new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
//     }
// }
//
// ★ 例子：                                                       │
//   允许乱序 5s，目前最大事件时间 10:00:30                        │
//   watermark = 10:00:30 - 5s = 10:00:25                         │
//   → 表示"10:00:25 之前的数据都到齐了"                          │
//
```

### 2.2 Watermark 传播机制

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 多并行度下 Watermark 传播（面试高频！）                    │
│                                                               │
│  Source-1 watermark=10:00:25                                 │
│  Source-2 watermark=10:00:20  ──→ 下游算子                    │
│  Source-3 watermark=10:00:28                                 │
│                                                               │
│  下游算子收到多个上游的 Watermark：                           │
│  ★ 取所有输入 Watermark 的【最小值】作为自己的 Watermark     │
│    （木桶效应：以最慢的 Source 为准）                        │
│                                                               │
// // flink-streaming-java/.../io/network/.../StatusWatermarkValve.java
// public class StatusWatermarkValve {
//     // 每个输入通道当前的 watermark
//     private long[] channelWatermarks;
//
//     public void markWatermark(Watermark mark, int channelIndex) {
//         channelWatermarks[channelIndex] = mark.getTimestamp();
//         long newMin = Long.MAX_VALUE;
//         for (long wm : channelWatermarks) {
//             newMin = Math.min(newMin, wm);  // ★ 取最小值
//         }
//         if (newMin > lastEmittedWatermark) {
//             lastEmittedWatermark = newMin;
//             output.emitWatermark(new Watermark(newMin));
//         }
//     }
// }
//
│ ★ 面试坑：某个分区没有数据（idle partition）                 │
│   它的 watermark 一直不前进 → 拖慢整个算子                    │
│   → 解决：withIdleness(Duration.ofMinutes(1))                 │
│   → 空闲分区超过 1min 不参与 min 计算                         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、窗口触发源码级

### 3.1 窗口核心组件

```java
// ★ Flink Window 三大核心组件（面试必背！）
//
// ① WindowAssigner：数据分配到哪个窗口
//    TumblingEventTimeWindows（滚动）/ SlidingEventTimeWindows（滑动）
//    / EventTimeSessionWindows（会话）
//
// ② Trigger：什么时候触发窗口计算
//    EventTimeTrigger（Watermark 越过窗口结束时间触发）
//
// ③ WindowFunction：触发后如何计算
//    ReduceFunction / AggregateFunction / ProcessWindowFunction
//
// // flink-streaming-java/.../windowing/triggers/EventTimeTrigger.java
//
// public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
//
//     // ★ 每来一条元素
//     @Override
//     public TriggerResult onElement(Object element, long timestamp,
//                                    TimeWindow window, TriggerContext ctx) {
//         // 注册一个定时器：当 watermark ≥ 窗口最大时间戳时触发
//         if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
//             return TriggerResult.FIRE;  // watermark 已超过 → 立即触发
//         } else {
//             ctx.registerEventTimeTimer(window.maxTimestamp());  // ★ 注册定时器
//             return TriggerResult.CONTINUE;
//         }
//     }
//
//     // ★ Event Time 定时器触发
//     @Override
//     public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
//         return time == window.maxTimestamp()
//             ? TriggerResult.FIRE       // ★ 触发计算
//             : TriggerResult.CONTINUE;
//     }
//
//     @Override
//     public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
//         return TriggerResult.CONTINUE;  // 不处理 Processing Time
//     }
// }
//
// ★ 触发本质：Watermark 越过 window.maxTimestamp() → 定时器触发 → FIRE
//
```

### 3.2 三类窗口对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink 三类窗口（面试必背！）                               │
│                                                               │
│  ① 滚动窗口 Tumbling：窗口不重叠                              │
│   |--[0-10s)--|--[10-20s)--|--[20-30s)--|                   │
│   每条数据只属于一个窗口                                      │
│
│  ② 滑动窗口 Sliding：窗口重叠                                │
│   |--[0-10s)------|                                          │
│      |--[5-15s)------|                                       │
│         |--[10-20s)------|                                   │
│   窗口大小 10s，滑动步长 5s → 一条数据属于 2 个窗口          │
│
│  ③ 会话窗口 Session：按活动间隔动态划分                      │
│   |--[1-3s)--gap-|--[8-15s)--------gap--|                   │
│   两条数据间隔 < gap → 同一会话；≥ gap → 新会话              │
│   窗口开始/结束时间不固定（需要合并窗口）                    │
│
│ ★ TriggerResult 四种动作：                                    │
│   CONTINUE：什么都不做                                        │
│   FIRE：触发计算（保留窗口内容）                              │
│   PURGE：清空窗口内容（不计算）                              │
│   FIRE_AND_PURGE：触发计算 + 清空                            │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 迟到数据处理

```java
// ★ Watermark 越过窗口后的迟到数据怎么处理？（面试高频！）
//
// 三种策略：
//
// ① allowedLateness（允许迟到，默认 0）
//    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//    .allowedLateness(Time.seconds(5))   // watermark 超过后再等 5s
//    → 窗口在 [end, end+5s) 内状态不清理
//    → 迟到数据进来会再次触发（更新结果）
//
// ② sideOutputLateData（侧输出流收集迟到数据）
//    OutputTag<T> lateTag = new OutputTag<T>("late"){};
//    .sideOutputLateData(lateTag)
//    → 彻底迟到的数据进入侧流，单独补偿处理
//
// ③ 直接丢弃（默认）
//
// // 窗口状态清理逻辑
// public void clearWindowState(long watermark) {
//     // watermark > window.end + allowedLateness → 真正清除窗口状态
//     if (watermark > window.maxTimestamp() + allowedLateness) {
//         windowState.clear(window);  // 彻底清理
//     }
// }
//
```

---

## 四、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 三种时间语义？| **Event Time（事件时间）+ Ingestion Time（已废弃）+ Processing Time（处理时间）** |
| 2 | Watermark 是什么？| **"event time ≤ t 的数据理论上到齐了"的进度标记**，用于处理乱序 |
| 3 | Watermark 怎么生成？| BoundedOutOfOrderness：**watermark = 最大事件时间 - 最大乱序时间 - 1**；默认 200ms 周期生成 |
| 4 | 多并行度 Watermark 传播？| **取所有上游 watermark 的最小值**（木桶效应，以最慢 Source 为准）|
| 5 | idle partition 问题？| 空闲分区 watermark 不前进拖慢全算子 → **withIdleness 让空闲分区不参与 min** |
| 6 | 窗口三大组件？| **WindowAssigner（分配）+ Trigger（触发）+ WindowFunction（计算）** |
| 7 | EventTimeTrigger 原理？| onElement 注册 event time 定时器；**watermark ≥ window.maxTimestamp → FIRE** |
| 8 | 三类窗口？| **滚动（不重叠）+ 滑动（重叠）+ 会话（按 gap 动态合并）** |
| 9 | TriggerResult 四动作？| CONTINUE / FIRE / PURGE / FIRE_AND_PURGE |
| 10 | 迟到数据三策略？| **allowedLateness（再等）+ sideOutputLateData（侧流补偿）+ 丢弃（默认）** |
| 11 | 面试最高频考点排序？| **1. Watermark 生成机制 2. 多并行度取 min 3. 窗口触发原理 4. 迟到处理 5. 三类窗口** |
| 12 | 学习路径？| Watermark.java → BoundedOutOfOrdernessWatermarks → StatusWatermarkValve → EventTimeTrigger |
| 13 | 面试常见追问？| Watermark 会倒退吗？→ 不会（只单调递增）；乱序超了怎么办？→ 算迟到数据 |
| 14 | Watermark 本质？| 一条特殊的流元素（StreamElement），和数据一起在算子间流动 |
| 15 | Processing Time 窗口？| ProcessingTimeTrigger 按系统时间触发，不需要 Watermark |
| 16 | Watermark 延迟 vs 正确性？| 乱序时间设太大：延迟高但不丢数据；太小：延迟低但丢数据 → 权衡 |
| 17 | Watermark=MAX？| Long.MAX_VALUE 表示流结束，触发所有剩余窗口 |
| 18 | 会话窗口怎么实现？| 来新数据若与已有窗口间隔 < gap 则合并窗口（MergingWindowAssigner）|
| 19 | Kafka 多分区 Watermark？| 每个分区一个 watermark，多分区取 min；KafkaSource 按分区跟踪 |
| 20 | 面试高频场景？| 实时统计每分钟 PV（滚动窗口）+ 用户会话时长（会话窗口）+ 大屏延迟监控 |
| 21 | Watermark 在哪生成？| 可以在 Source 生成，也可以在任意算子 assignTimestampsAndWatermarks |
| 22 | Flink Watermark vs Spark Structured Streaming？| 都有 watermark 概念；Flink 更成熟（侧输出、allowedLateness）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：flink-streaming-java/.../watermark/ + windowing/triggers/EventTimeTrigger.java  
**面试考点：22 题**  
**差异化点**：Watermark 生成公式 + 多并行度取 min + 窗口定时器触发 + 迟到三策略
