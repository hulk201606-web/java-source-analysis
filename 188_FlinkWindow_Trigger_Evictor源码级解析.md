# Flink Window Tumbling/Sliding/Session + Trigger/Evictor 源码级深度解析

> **源码基线**：Flink 1.18+（Java）  
> **核心位置**：flink-runtime/.../windowing + flink-table/flink-table-planner  
> **面试权重**：⭐⭐⭐⭐⭐（流处理窗口核心，补全 162/184 Flink 生态）

---

## 一、Window 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Flink Window = 流处理时间窗口（面试必背三件套）            │
│                                                               │
│  ★ 核心三件套（面试高频）：                                    │
│   1. WindowAssigner（什么数据进哪个窗口）                     │
│   2. Trigger（什么时候触发计算）                               │
│   3. Evictor（触发后清理窗口内的数据）                        │
│                                                               │
│  ★ 四种核心 WindowAssigner（面试必背）：                     │
│                                                               │
│   ┌───────────────────────────────────────────────────┐      │
│   │ ① Tumbling（滚动窗口）：                          │      │
│   │   time: [0, 5), [5, 10), [10, 15)...             │      │
│   │   → 窗口不重叠；每个事件只进一个窗口              │      │
│   │   → 最常用（如每 5 分钟统计订单量）                │      │
│   ├───────────────────────────────────────────────────┤      │
│   │ ② Sliding（滑动窗口）：                          │      │
│   │   size=10, slide=5                               │      │
│   │   [0,10), [5,15), [10,20), [15,25)...            │      │
│   │   → 窗口重叠；每个事件进多个窗口                  │      │
│   │   → 适合需要更细粒度更新的场景                    │      │
│   ├───────────────────────────────────────────────────┤      │
│   │ ③ Session（会话窗口）：                          │      │
│   │   gap=30min（一段时间没数据则关闭窗口）            │      │
│   │   Session 1: [10:00, 10:25]（有连续数据）         │      │
│   │   Session 2: [11:00, 11:45]（有连续数据）         │      │
│   │   → 窗口大小动态；按数据活跃度切分                │      │
│   │   → 适合用户行为分析（一次会话内的行为聚合）      │      │
│   ├───────────────────────────────────────────────────┤      │
│   │ ④ Global（全局窗口）：                           │      │
│   │   所有数据进同一个窗口（永不触发）                │      │
│   │   → 必须自定义 Trigger 才能触发计算              │      │
│   │   → 适合窗口无限大 + 特殊触发条件的场景           │      │
│   └───────────────────────────────────────────────────┘      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、WindowAssigner 源码级

### 2.1 Tumbling + Sliding 窗口分配

```java
// ★ Flink WindowAssigner 源码级
//
// flink-runtime/.../windowing/assigner/
// flink-runtime/.../windowing/assigner/TumblingEventTimeWindows.java
// flink-runtime/.../windowing/assigner/SlidingEventTimeWindows.java
// flink-runtime/.../windowing/assigner/EventTimeSessionWindows.java
//
// // ★ WindowAssigner 核心接口（源码级）：
// public abstract class WindowAssigner<W extends Window> {
//     // 给定事件和时间戳 → 分配到哪些窗口
//     public abstract Collection<W> assignElements(
//         Iterable<TimestampedValue<STREAM>> elements,
//         long timestamp,
//         WindowAssignerContext context
//     );
//
//     // Window Serializer（用于 Checkpoint）
//     public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);
// }
//
// // ★ TumblingEventTimeWindows 实现：
// // 每个事件只进一个窗口
// public class TumblingEventTimeWindows extends WindowAssigner<TimeWindow> {
//     private final long size;     // 窗口大小
//     private final long offset;   // 偏移（对齐方式）
//
//     @Override
//     public Collection<TimeWindow> assignElements(...) {
//         // 窗口起始时间 = timestamp - ((timestamp - offset) % size)
//         long start = timestamp - ((timestamp - offset) % size);
//         long end = start + size;
//         return Collections.singletonList(new TimeWindow(start, end));
//         // 例：size=5min, offset=0
//         //   event ts=2024-01-01 10:03:27
//         //   start = 10:03:27 - (10:03:27 % 5min) = 10:00:00
//         //   end = 10:05:00
//         //   → [10:00:00, 10:05:00)
//     }
// }
//
// // ★ SlidingEventTimeWindows 实现：
// // 每个事件进多个重叠窗口
// public class SlidingEventTimeWindows extends WindowAssigner<TimeWindow> {
//     private final long size;      // 窗口大小
//     private final long slide;     // 滑动步长
//
//     @Override
//     public Collection<TimeWindow> assignElements(...) {
//         List<TimeWindow> windows = new ArrayList<>();
//         // ★ 关键：最后一个包含这个事件的窗口
//         long lastStart = TimeWindow.getWindowStartWithOffset(
//             timestamp - size + 1, offset, slide
//         );
//         // 往前推 size/slide 个窗口
//         for (long start = lastStart; start > timestamp - size; start -= slide) {
//             windows.add(new TimeWindow(start, start + size));
//         }
//         return windows;
//         // 例：size=10min, slide=5min
//         //   event ts=10:07:00
//         //   lastStart = 10:07:00 - 10min + 1 = 10:02:01... 再对齐
//         //   → [0,10), [5,15) → 两个窗口
//     }
// }
//
```

### 2.2 Event Time vs Processing Time

```java
// ★ Event Time vs Processing Time（面试必背）
//
// // Event Time：事件自身带的时间戳（数据里的时间）
// //   优点：不受机器延迟影响；乱序数据能正确处理
// //   缺点：需要 Watermark + Window 等待迟到数据
// //
// // Processing Time：Flink 处理数据的系统时间
// //   优点：快；不需要 Watermark
// //   缺点：受系统延迟/GC 影响；乱序数据错误归因到错误窗口
// //
// // ★ Event Time WindowAssigner 需要 Watermark：
// //   Watermark = 当前 Event Time 进度 - 允许乱序（outOfOrderness）
// //   Watermark 超过窗口结束时间 + allowed lateness → 窗口关闭
// //
// // Window 关闭流程（Event Time）：
// //   1. Watermark 推进到窗口 end + allowedLateness
// //   2. Flink 触发窗口计算（Trigger.fire()）
// //   3. Evictor 清理数据
// //   4. 如果 allowedLateness=0 → 窗口完全销毁
// //      否则保留到 Watermark 超过 end + allowedLateness
// //
// // ★ Processing Time WindowAssigner 不需要 Watermark：
// //   用 ProcessingTimeService.timerRegister() 注册定时器
// //   定时器触发 → 窗口计算
// //
```

---

## 三、Trigger + Evictor 源码级

### 3.1 Trigger 内置实现

```java
// ★ Flink Trigger 源码级（面试核心）
//
// flink-runtime/.../windowing/triggers/
// flink-runtime/.../windowing/triggers/EventTimeTrigger.java
// flink-runtime/.../windowing/triggers/CountTrigger.java
//
// // Trigger 核心接口（源码级）：
// public abstract class Trigger<W extends Window> {
//     // 窗口里每条数据来的时候调用
//     public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx);
//
//     // 系统定时器（Event Time / Processing Time）
//     public abstract TriggerResult onTime(TriggerTime triggerTime, W window, TriggerContext ctx);
//
//     // 窗口合并时调用（Session Window）
//     public abstract void onMerge(W window, OnMergeContext ctx);
// }
//
// // ★ TriggerResult 三种返回值：
// //   CONTINUE：继续等，不触发
// //   FIRE：触发窗口计算（但不清理窗口数据）
// //   PURGE：清理窗口数据（但不计算）
// //   FIRE_AND_PURGE：触发计算 + 清理数据（最常用）
//
// // ★ 三个内置 Trigger（面试高频）：
// //
// // 1. EventTimeTrigger（默认，Event Time Window 自动用）
// //    注册定时器在 window.end() + allowedLateness
// //    定时器触发 → FIRE_AND_PURGE
// //    // 源码：onTime() 时返回 FIRE_AND_PURGE
// //
// // 2. ProcessingTimeTrigger（默认，Processing Time Window 自动用）
// //    注册 Processing Time 定时器
// //    定时器触发 → FIRE_AND_PURGE
// //
// // 3. CountTrigger（自定义，按条数触发）
// //    onElement() 时计数；达到 count → FIRE
// //    // 例：每 100 条触发一次聚合（非时间驱动）
//
```

### 3.2 Evictor 源码级

```java
// ★ Flink Evictor 源码级
//
// flink-runtime/.../windowing/evictors/
//
// // Evictor 核心接口：
// public abstract class Evictor<W extends Window> {
//     // 窗口计算前清理数据
//     public void evictBefore(Iterable<TimestampedValue<Stream>> elements, int size, W window, EvictorContext ctx);
//     // 窗口计算后清理数据
//     public void evictAfter(Iterable<TimestampedValue<Stream>> elements, int size, W window, EvictorContext ctx);
// }
//
// // 三个内置 Evictor：
// //
// // 1. TimeEvictor（时间 Evict）
// //    evictBefore：保留最近 windowSize 时间的数据
// //    适合：Sliding Window + 只看最近 N 秒数据
// //
// // 2. CountEvictor（条数 Evict）
// //    evictBefore：只保留最近 maxCount 条
// //    适合：大数据量窗口限制内存
// //
// // 3. DeltaEvictor（自定义 Delta 计算）
// //    实现 deltaFunction → 满足条件的 Evict
// //    适合：业务自定义清理条件
//
// // ★ Trigger + Evictor 执行顺序（面试核心）：
// //
// //   1. 数据来 → WindowAssigner.assignElements → 加入窗口状态
// //   2. Trigger.onElement() → 判断是否触发计算
// //   3. 触发后 → Evictor.evictBefore()（计算前清理）
// //   4. 窗口函数（Aggregate / Process）执行
// //   5. Evictor.evictAfter()（计算后清理）
// //   6. Trigger 返回 FIRE_AND_PURGE → 窗口状态销毁
//
```

---

## 四、迟到数据 + allowedLateness 源码级

```java
// ★ Flink 迟到数据处理（面试高频，生产痛点）
//
// flink-runtime/.../windowing/WindowOperator.java
//
// // 默认行为：迟到数据被丢弃（dropped）
// // 配置 allowedLateness → 允许迟到数据在一定时间内仍可加入窗口
// //
// // 源码级流程：
// //
// // 1. 事件到达 WindowOperator
// //    event.timestamp = 10:03:00
// //    watermark = 10:04:00（已经推进到这个时间点）
// //    window.end = 10:00:00（这个窗口本该在 watermark > 10:00 时关闭）
// //
// // 2. 判断：event.timestamp < window.end + allowedLateness？
// //    如果 allowedLateness = 5min
// //    event.ts = 10:03:00 < window.end(10:00:00) + 5min(10:05:00)
// //    → 还在允许迟到范围内 → 重新打开窗口！
// //    → 触发窗口计算（可能多次触发同一个窗口）
// //
// // 3. 如果 allowedLateness = 0
// //    event.ts = 10:03:00 > watermark - outOfOrderness
// //    → 被丢弃（dropped）
// //
// // ★ Side Output（旁路输出）处理迟到数据：
// //   允许迟到数据通过 Side Output 流输出
// //   → 下游可以消费迟到数据做补偿
// //   // 代码示例：
// //   OutputTag<Order> lateTag = new OutputTag<>("late-orders");
// //   DataStream<Order> main = orders.keyBy(Order::userId)
// //       .window(TumblingEventTimeWindows.of(Time.minutes(5)))
// //       .allowedLateness(Time.minutes(2))
// //       .sideOutputLateData(lateTag)
// //       .aggregate(...)
// //       .getSideOutput(lateTag);  // 拿到迟到数据流
//
// // ★ allowedLateness 默认值？
// //   默认 0（不允许迟到）
// //   Event Time Window 一般设 allowedLateness（根据业务数据乱序程度）
// //   Processing Time Window 没有迟到数据概念
//
```

---

## 五、Flink Window vs Spark SS Window 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：Flink Window vs Spark SS Window 对比             │
│  ┌──────────────┬──────────────────┬──────────────────┐    │
│  │ 维度            │ Flink Window      │ Spark SS Window   │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ WindowAssigner │ ✅ Tumbling/Sliding│ ✅ Tumbling/Sliding│    │
│  │                │ Session/Global    │ Session/Global    │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ Trigger        │ ✅ 内置+自定义     │ ❌ 无 Trigger      │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ Evictor        │ ✅ 内置+自定义     │ ❌ 无 Evictor      │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ 迟到数据处理     │ ★ allowedLateness │ late data drop  │    │
│  │                │ + Side Output    │ + watermark drop   │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ 状态管理         │ ★ Flink State    │ StreamingQuery    │    │
│  │                │ Backend (RocksDB) │ Engine State      │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ Checkpoint      │ ★ 支持（184 文档）│ 支持（MicroBatch） │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ 低延迟           │ ★ 毫秒级（触发器）│ 秒级（MicroBatch） │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ 灵活性           │ ★ 高（Trigger+Evictor）│ 中（SQL 固定） │    │
│  ├──────────────┼──────────────────┼──────────────────┤    │
│  │ 面试高频         │ ★★★★★          │ ★★★★            │    │
│  └──────────────┴──────────────────┴──────────────────┘    │
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   需要低延迟 + 灵活触发 → **Flink Window**（Trigger+Evictor）│
│   需要 SQL 友好 + 简单场景 → Spark SS Window                 │
│   混合：Flink Window 做主处理 + Side Output 迟到数据给 Spark 补全│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Flink Window 三件套？| **WindowAssigner（分配）+ Trigger（触发）+ Evictor（清理）** |
| 2 | ★ 四种 WindowAssigner？| **Tumbling（滚动）+ Sliding（滑动）+ Session（会话）+ Global（全局）** |
| 3 | Tumbling 窗口特点？| 不重叠；每个事件只进一个窗口；最常用 |
| 4 | Sliding 窗口特点？| 重叠；每个事件进多个窗口；size=10, slide=5 → 重叠 50% |
| 5 | Session 窗口特点？| 动态大小；gap 内没数据 → 关闭窗口；按数据活跃度切分 |
| 6 | Global 窗口特点？| 所有数据进一个窗口；必须自定义 Trigger 触发 |
| 7 | Event Time vs Processing Time？| Event Time 是数据时间戳（需 Watermark）；Processing Time 是系统时间（快但受延迟影响）|
| 8 | ★ TriggerResult 四种返回值？| **CONTINUE + FIRE + PURGE + FIRE_AND_PURGE**（最常用：FIRE_AND_PURGE）|
| 9 | 三个内置 Trigger？| EventTimeTrigger（默认 Event Time）+ ProcessingTimeTrigger（默认 Processing Time）+ CountTrigger（按条数）|
| 10 | ★ Trigger + Evictor 执行顺序？| 数据来 → WindowAssigner → Trigger.onElement → 触发后 **Evictor.evictBefore → 窗口函数 → Evictor.evictAfter → 清理** |
| 11 | Evictor 三个内置实现？| TimeEvictor（按时间）+ CountEvictor（按条数）+ DeltaEvictor（自定义）|
| 12 | ★ 迟到数据处理核心？| **allowedLateness**（默认 0）；Event Time Window 可重开窗口；Side Output 旁路输出迟到数据 |
| 13 | Watermark 怎么推进窗口关闭？| Watermark = currentMaxEventTime - outOfOrderness；**Watermark > window.end + allowedLateness → 窗口关闭** |
| 14 | Flink Window vs Spark SS Window 核心差异？| 见第五节对比表（9 维度）|
| 15 | 面试最高频考点排序？| **1. WindowAssigner 四种 2. TriggerResult 四种返回 3. allowedLateness + Side Output 4. Event Time vs Processing Time 5. Trigger+Evictor 执行顺序** |
| 16 | 学习路径？| WindowAssigner → Trigger → Evictor → 迟到数据 → Watermark → Checkpoint |
| 17 | 面试常见追问？| Session Window 怎么处理窗口合并？| 新事件来了 → 可能同时进两个相邻 Session 窗口（gap 不够）→ **SessionWindowAssigner.merge() 合并两个窗口** |
| 18 | 面试常见追问？| Sliding Window 为什么更耗内存？| 每个事件进多个窗口 → 窗口状态更大；且多个窗口同时可能触发计算 |
| 19 | 面试陷阱？| 说 "Flink Window 关闭后数据立刻清理" → ❌ Event Time Window 要看 Watermark 和 allowedLateness；Processing Time Window 定时器触发后才关 |
| 20 | 面试陷阱？| 说 "Trigger 必须触发 Purge 才清理窗口" → ❌ Flink Event Time Window 到期后会自动清理（即使 Trigger 没返回 PURGE）|
| 21 | allowedLateness 默认值？| **0**（不允许迟到）；生产 Event Time Window 一般设 allowedLateness=5min 左右 |
| 22 | Window 状态和 Checkpoint？| Window 状态（每个窗口的数据）存在 Flink State Backend 里；Checkpoint 时一起持久化；恢复时按 Watermark 重新触发未关闭的窗口 |
| 23 | 典型生产用法？| **订单统计**：TumblingEventTimeWindows.of(Time.minutes(5)) + allowedLateness(Time.minutes(2)) + Side Output 迟到数据；Trigger 默认 EventTimeTrigger |
| 24 | CountTrigger 和 EventTimeTrigger 怎么配合？| 自定义 Trigger 组合：CountTrigger 到 100 条先触发 + EventTimeTrigger 到窗口结束一定触发（兜底）|
| 25 | Global Window 什么时候用？| 全局聚合（如累计 PV 统计）+ 自定义 Trigger（如每小时触发一次计算）|
| 26 | Tumbling vs Sliding 内存？| Sliding 重叠 → 内存≈ Tumbling × (size/slide)；size=10, slide=5 → 2 倍内存 |
| 27 | Window 和 Checkpoint 对齐？| Flink Checkpoint Barrier 会触发窗口状态 Checkpoint；窗口计算和 Checkpoint 是独立的两阶段 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：flink-runtime/.../windowing/assigner/TumblingEventTimeWindows.java + triggers/EventTimeTrigger.java + WindowOperator.java  
**面试考点：27 题**  
**差异化点**：WindowAssigner 四种源码级、TriggerResult 四种返回值、**Trigger→Evictor 执行顺序**、allowedLateness + Side Output 迟到数据、Flink Window vs Spark SS Window 对比
