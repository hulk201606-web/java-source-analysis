# RocksDB LSM Tree + Compaction 策略源码级深度解析

> **源码基线**：RocksDB 8.x（C++）  
> **核心位置**：db/ + table/ + db/compaction  
> **面试权重**：⭐⭐⭐⭐⭐（TiKV/Flink/Kafka/ClickHouse 都用，跨系统高频）

---

## 一、LSM Tree 全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ LSM Tree = Log-Structured Merge Tree（面试必背）          │
│                                                               │
│  写入模型优势：                                                │
│   传统 B+ Tree：随机写 → 磁盘随机 IO → 慢                   │
│   LSM Tree：Append-Only 写 → 顺序 IO → 快                  │
│   后台 Compaction 合并 → 消除重复/过期 → 查询加速          │
│                                                               │
│  核心数据流（面试高频）：                                      │
│   ┌──────────┐ 顺序写  ┌──────────┐ flush  ┌──────────┐    │
│   │   WAL     │ ──────→ │ MemTable │ ──────→ │  SST File │   │
│   │ (WriteAhead)        │ (内存)    │        │ (磁盘不可变)│   │
│   └──────────┘          └──────────┘        └─────┬────┘   │
│                                                     │         │
│                                                     │ Compaction │
│                                                     ▼         │
│                                               ┌──────────┐   │
│                                               │ 新 SST   │   │
│                                               │ (合并后)  │   │
│                                               └──────────┘   │
│                                                               │
│  ★ 多级 LSM（Leveled）：                                      │
│   SST 按 Level 分：L0 → L1 → L2 → ... → Ln                 │
│   每层 SST 数量递增（L1 10 个 → L2 100 个）                  │
│   每层内部 SST 按 key 有序、不重叠                          │
│   Compaction 主要是 L0 → L1，L1 → L2，逐级合并             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心组件源码级

### 2.1 WAL + MemTable + Immutable MemTable

```cpp
// ★ WAL + MemTable 核心（源码级）
//
// db/write_batch.cc + db/db_impl.cc + db/memtable.cc
//
// // 写入流程（Put/Delete）：
// Status DBImpl::Put(...) {
//     // ① ★ 先写 WAL（WriteAhead Log）
//     //    // db/wal_manager.cc
//     //    WAL = 顺序追加的日志文件（db/CURRENT → db/MANIFEST → db/000012.log）
//     //    作用：崩溃恢复时重放 WAL → 恢复 MemTable 状态
//     //    WAL 先写 → 成功才更新 MemTable
//     //    崩溃恢复：WAL 重放是可靠写入的核心保证
//     status = WriteToWAL(write_batch);
//
//     // ② ★ 再写 MemTable（内存中的有序跳表）
//     //    db/memtable.cc
//     //    MemTable = SkipList<InternalKey, Value>
//     //    InternalKey = UserKey + SequenceNumber + ValueType(Put/Delete/Merge)
//     //    写入是内存操作，快速
//     status = memtable_->Add(key, value);
//
//     // ③ MemTable 满了（write_buffer_size 默认 64MB）：
//     //    变成 Immutable MemTable（只读，不再接受新写入）
//     //    创建新的 Active MemTable
//     //    后台线程把 Immutable MemTable flush 成 SST
//     if (memtable_->ApproximateMemoryUsage() > write_buffer_size) {
//         SwitchMemtable();  // 创建新 MemTable，旧的变 Immutable
//         MaybeScheduleFlushOrCompaction();  // 后台 flush
//     }
// }
//
// // ★ Immutable MemTable Flush 成 SST：
// // db/memtable.cc → MemTable::Flush()
// //    遍历 Immutable MemTable 的 SkipList（有序）
// //    按 key 顺序写到磁盘 SST 文件
// //    SST = Sorted String Table（Block 结构，按 Block 分块 + BloomFilter + Index）
// //    SST 是不可变文件（Append-Only 架构核心）
// //    Flush 完成后 Immutable MemTable 内存释放
//
// ★ WAL 作用的关键理解（面试高频）：
//   MemTable 是内存的 → 进程崩溃丢了 → 靠 WAL 恢复
//   WAL 先写（fsync）+ MemTable 后写
//   这就是"Write-Ahead"的含义：先写日志再更新内存状态
//   WAL fsync 可以批量（group commit）提高吞吐
//
```

### 2.2 SST File 内部结构

```cpp
// ★ SST File 内部结构（源码级，面试高频）
//
// table/block_based/block_based_table_builder.cc
//
// // SST = BlockBasedTable（RocksDB 默认）
// // 内部 Block 结构：
//
// ┌────────────────────────────────────────────────────┐
// │ SST File                                            │
// │ ┌────────────────────────────────────────────────┐  │
// │ │ ① Data Block #0                                │  │
// │ │   ├── Block Header (checksum, type)            │  │
// │ │   ├── Entry 0: [key_len, key, value_len, value]│  │
// │ │   ├── Entry 1: ...                             │  │
// │ │   └── BloomFilter（快速判断 key 是否在 block 内）│  │
// │ ├────────────────────────────────────────────────┤  │
// │ │ ② Data Block #1                                │  │
// │ │   └── ...                                      │  │
// │ ├────────────────────────────────────────────────┤  │
// │ │ ③ Meta Block #0 (Filter Block)                 │  │
// │ │   └── 每个 Data Block 的 BloomFilter 汇总       │  │
// │ ├────────────────────────────────────────────────┤  │
// │ │ ④ Meta Block #1 (Index Block)                  │  │
// │ │   └── Data Block → key 起始位置映射             │  │
// │ ├────────────────────────────────────────────────┤  │
// │ │ ⑤ Properties Block                             │  │
// │ │   └── 统计信息（min/max key、数据大小、删除标记数）│  │
// │ ├────────────────────────────────────────────────┤  │
// │ │ ⑥ Footer Block（固定大小）                     │  │
// │ │   └── Index Block offset + Filter Block offset  │  │
// │ └────────────────────────────────────────────────┘  │
// └────────────────────────────────────────────────────┘
//
// ★ 关键优化：
//   - BloomFilter：O(1) 时间判断 key 是否可能在 SST 里（减少磁盘读）
//   - 多个 Level 之间 BloomFilter 叠加查询
//   - Block 压缩：Snappy / Zstd / LZ4（默认不压缩或 Snappy）
//   - Block Cache：LRU 缓存热点 Data Block（避免重复磁盘读）
//     // block_cache_size 默认 8MB；TiKV 生产设 1-4GB
//
```

### 2.3 多级 LSM + Compaction 触发

```cpp
// ★ Leveled LSM 树结构（源码级，面试核心）
//
// db/db_impl.cc → Level Compaction 调度
// db/compaction/level_compaction_picker.cc
//
// // 多级 LSM：
// // L0：Flush 上来的 SST；**key 范围可能重叠**（重要！）
// // L1：Merge 后的有序 SST；**key 范围不重叠**
// // L2：更大的有序 SST；**key 范围不重叠**
// // ...
// // L0 特殊：来自多个 Immutable MemTable 的 SST，key 范围随机
// //        查询 L0 时要遍历所有 SST（因为重叠）
// // L1+ 规则：每层 max_file_num 个 SST；再多触发 Compaction
//
// ★ 三种 Compaction 策略（面试必背）：
//
// 策略 1：Leveled（RocksDB 默认）
//   每层 max_file_num 超限 → 触发 L(n) → L(n+1) Compaction
//   // 示例：L1 最大 10 个 → 有 11 个时 → 把 L1 某些 SST 和 L2 重叠 SST 合并
//   // 写放大：中（每次合并量中等）
//   // 读放大：低（每层不重叠，查 1 个 SST/层）
//   // 空间放大：中（每层大小约 10x 递增）
//
// 策略 2：Tiered Compaction（Universal Compaction）
//   按 SST 大小分层：小的一层，大的一层
//   每层超过阈值 → 整体合并成更大层
//   // 每个 SST 最终都被合并到最大层（Sorted Run）
//   // 写放大：高（反复合并多次）
//   // 读放大：最低（每层只有一个大 SST，完全有序）
//   // 空间放大：高（需要更大磁盘）
//   // 适合：写入量小、读多写少场景
//
// 策略 3：FIFO Compaction（DeleteFile）
//   按时间/大小直接删旧 SST，不合并
//   // 适合：纯日志场景（只追加、不更新、时间窗口固定）
//   // 写放大：无（不合并）
//   // 读放大：高（多层重叠）
//   // 空间放大：最低（直接删，无临时文件）
//
```

---

## 三、Compaction 触发 + 放大因子

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Compaction 触发条件（面试高频）                           │
│                                                               │
│  1. Write Buffer 满 → Flush 成 SST → L0 又多了一个 → 可能触发│
│  2. 后台线程定时检查每层 SST 数量 → 超限触发 Level Compaction  │
│  3. 删除标记太多 → 强制 Compaction 清理过期数据              │
│  4. 磁盘空间不够 → 紧急 Compaction（可能降低写速）           │
│  5. Manual Compaction / CompactRange                         │
│                                                               │
│  ★ 三大放大因子（面试必背，生产优化核心）                      │
│                                                               │
│  ┌──────────────┬──────────────────────────────┐              │
│  │ 放大因子        │ 含义                            │              │
│  ├──────────────┼──────────────────────────────┤              │
│  │ 写放大 Write   │ 每写 1 字节，实际磁盘写多少字节 │              │
│  │ Amplification  │ L0→L1→L2→L3 每层合并都重写   │              │
│  │                │ 典型 2-40x；越高越耗 IO       │              │
│  ├──────────────┼──────────────────────────────┤              │
│  │ 读放大 Read    │ 一次 Get 要查多少 SST          │              │
│  │ Amplification  │ 每层都有重叠 → 每层都要查      │              │
│  │                │ Leveled：每层 1 个 → O(L)     │              │
│  │                │ L0 要遍历所有                  │              │
│  ├──────────────┼──────────────────────────────┤              │
│  │ 空间放大 Space │ 实时数据量 vs 磁盘占用量       │              │
│  │ Amplification  │ 包含 WAL + MemTable + Compaction│              │
│  │                │ 临时文件 + 多 Level 旧版本    │              │
│  │                │ 典型 1.1-10x                  │              │
│  └──────────────┴──────────────────────────────┘              │
│                                                               │
│  ★ 三种 Compaction 策略放大因子对比：                        │
│  ┌──────────┬──────────┬──────────┬──────────┐              │
│  │ 策略       │ 写放大    │ 读放大    │ 空间放大  │              │
│  ├──────────┼──────────┼──────────┼──────────┤              │
│  │ Leveled    │ 中（3-10）│ 低（O(L)）│ 中       │              │
│  │ Tiered     │ ★ 高      │ ★ 最低    │ ★ 高     │              │
│  │ FIFO       │ ★ 0       │ ★ 最高    │ ★ 最低   │              │
│  └──────────┴──────────┴──────────┴──────────┘              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、跨系统 RocksDB 调优对比（TiKV / Flink / Kafka Streams）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 生产级 RocksDB 调优对比表（面试高频，跨系统）             │
│                                                               │
│  ┌──────────────────┬────────────┬────────────┬────────────┐│
│  │ 参数              │ TiKV 默认    │ Flink State │ Kafka Streams││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ write_buffer_size │ 128MB      │ 64MB        │ 64MB        ││
│  │                   │ (Percolator │ (Checkpoint │ (State Store││
│  │                   │  高写入)    │  批量写)    │  增量写)    ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ max_write_buffer │ 8          │ 4           │ 4           ││
│  │ _number          │ (高写入多   │ (Checkpoint │             ││
│  │                   │  buffer)    │  有限)      │             ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ level_compaction │ ✅ Leveled  │ ✅ Leveled  │ ✅ Leveled  ││
│  │                   │ (默认)      │ (默认)      │ (默认)      ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ block_cache_size │ 1-4GB      │ 256MB       │ 512MB       ││
│  │                   │ (大数据量   │ (State 读多 │ (State 读多 ││
│  │                   │  ReadPath) │  写少)      │  写少)      ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ compression      │ LZ4 + Zstd │ Snappy      │ Snappy      ││
│  │                   │ (冷热分层) │ (轻量)      │ (轻量)      ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ bloom_filter     │ ✅ 前缀 BF  │ ✅ 标准 BF  │ ✅ 标准 BF  ││
│  │                   │ (TiKV 按   │             │             ││
│  │                   │  Table 分区)│             │             ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ compaction_thread│ 4-8        │ 2-4         │ 2-4         ││
│  │ s                │ (高频 Compa │ (Checkpoint │ (后台后台   ││
│  │                   │  ction)    │  低频)      │  低频)      ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ WAL fsync 策略    │ fsync_batch │ async       │ async       ││
│  │                   │ (批量 fsync│ (不实时 fsync│ (不实时)    ││
│  │                   │  高吞吐)   │  Checkpoint │             ││
│  │                   │            │  才 sync)   │             ││
│  ├──────────────────┼────────────┼────────────┼────────────┤│
│  │ delete_range 优化 │ ✅ 支持    │ ❌ 不常用   │ ❌ 不常用   ││
│  │                   │ (TiKV GC   │             │             ││
│  │                   │  大量删)   │             │             ││
│  └──────────────────┴────────────┴────────────┴────────────┘│
│                                                               │
│  ★ TiKV 调优特点（面试延伸）：                                │
│   - Percolator 高写入 → 大 write_buffer_size + 多 buffer     │
│   - 冷热分层压缩（热层 LZ4、冷层 Zstd）                     │
│   - Prefix BloomFilter（按 Table 前缀分桶）                  │
│   - DeleteRange 高频（GC 清理旧 MVCC 版本）                   │
│                                                               │
│  ★ Flink State Backend 特点：                                 │
│   - Checkpoint 批量写入 → 小 buffer + 异步 WAL              │
│   - State 恢复时按 Key Group 分片加载                        │
│   - RocksDB Checkpoint 利用 SST File 硬链接（快速 Checkpoint）│
│                                                               │
│  ★ Kafka Streams State Store：                                │
│   - 与 Flink 类似（增量写 + Checkpoint）                     │
│   - 写入 Kafka Topic 作为 Checkpoint + 容错                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、读路径优化：Block Cache + BloomFilter + Get

```cpp
// ★ RocksDB 读路径源码级（面试高频）
//
// db/db_impl.cc → DBImpl::Get()
// table/block_based/block_based_table_reader.cc
//
// // Get(key) 读单值核心路径：
// Status DBImpl::Get(const ReadOptions& opt, const Slice& key, std::string* value) {
//     // ① 先查 MemTable（内存，最快）
//     if (memtable_->Get(key, value)) return OK;
//
//     // ② 再查 Immutable MemTable（还没 flush 的内存）
//     for (auto imm : immutables_) {
//         if (imm->Get(key, value)) return OK;
//     }
//
//     // ③ 查 SST 文件（磁盘，最慢）
//     //    按 Level 顺序查：L0 → L1 → L2 → ... → Ln
//     //    // 对每个 Level：
//     for (auto* lsm_file : lsm_files_[level]) {
//         // ★ a. BloomFilter 快速排除
//         //    如果 BloomFilter 说 key 不在这个 SST → 跳过（无磁盘 IO！）
//         if (!lsm_file->MayMatch(key)) continue;
//
//         // ★ b. Block Cache 命中检查
//         //    // SST 的 Data Block 已经在 Block Cache 里？
//         //    // Block Cache = LRU Cache（典型 1-4GB）
//         //    // 命中 → 零磁盘 IO
//         //    // 未命中 → 读磁盘
//         auto data_block = cache_->Lookup(block_cache_key);
//         if (data_block) {
//             // Cache Hit！快速在内存 Block 里二分查找
//         } else {
//             // Cache Miss → 磁盘读 Data Block → 放入 Cache
//         }
//
//         // ★ c. Block 内二分查找（Block 是有序数组）
//         //    Block 内 Entry 按 key 有序
//         //    BinarySearch → 找到精确 key → 返回 value
//         auto iter = data_block.Find(key);
//         if (iter != data_block.End()) {
//             // ★ d. 处理 Delete 标记
//             //    InternalKey 包含 SequenceNumber + ValueType(Put/Delete/Merge)
//             //    找到的 entry 如果是 Delete 类型 → 继续找旧版本（跳过）
//             if (iter->value_type == ValueType::kTypeValue) {
//                 *value = iter->value;
//                 return OK;
//             }
//             // 如果是 Delete/RangeDelete → 表示 key 已删除
//         }
//     }
//
//     // 全 Level 查完没找到
//     return NotFound;
// }
//
// ★ 读路径三级过滤（性能关键）：
//   1. BloomFilter 排除（O(1)，无磁盘 IO）
//   2. Block Cache 命中（内存，零磁盘 IO）
//   3. Block 内二分（内存操作）
//   只有三级都没拦住才真的读磁盘 Data Block
//
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | LSM Tree 核心优势？| **Append-Only 顺序写（快）+ 后台 Compaction（查快）** |
| 2 | 写入核心流程？| **WAL fsync → MemTable 写 → MemTable 满变 Immutable → Flush 成 SST** |
| 3 | WAL 作用？| **崩溃恢复保证**；MemTable 是内存，进程崩溃丢了靠 WAL 重放恢复 |
| 4 | Immutable MemTable 是什么？| MemTable 满后变只读（不可再写）；后台线程把它 flush 成 SST |
| 5 | SST File 内部结构？| **Data Blocks（BloomFilter + 有序 Entry）+ Index Block + Filter Block + Footer** |
| 6 | L0 和 L1+ 核心区别？| L0 来自多个 Immutable MemTable → **key 范围重叠**；L1+ Compaction 合并后 **key 范围不重叠** |
| 7 | ★ 三种 Compaction 策略？| **Leveled（默认，写放大中）+ Tiered（写放大高读放大最低）+ FIFO（写放大 0 读放大最高）** |
| 8 | Compaction 触发条件？| 每层 SST 超限 + 删除标记太多 + 磁盘空间紧张 + 手动 CompactRange |
| 9 | ★ 三大放大因子？| **写放大（2-40x）+ 读放大（O(Level)）+ 空间放大（WAL+临时文件+旧版本）** |
| 10 | 读路径核心优化？| **BloomFilter O(1) 排除 → Block Cache LRU 内存命中 → Block 内二分查找** |
| 11 | Block Cache 作用？| 内存 LRU 缓存热点 Data Block；命中时零磁盘 IO；TiKV 生产设 1-4GB |
| 12 | BloomFilter 作用？| 判断 key 可能在 SST 里吗？**false-positive 但不会漏**；减少磁盘读 |
| 13 | Delete 标记怎么处理？| 写入 Delete 类型 Entry（不是真删）；Compaction 时才真正清理；Get 时跳过 Delete 标记 |
| 14 | ★ TiKV vs Flink vs Kafka Streams RocksDB 调优对比？| 见第四节对比表（10 个参数维度）|
| 15 | TiKV 调优特点？| 大 write_buffer_size + LZ4/Zstd 冷热分层 + Prefix BloomFilter + DeleteRange 高频 |
| 16 | Flink State Backend 调优特点？| 小 buffer + 异步 WAL + RocksDB Checkpoint 硬链接 + Key Group 分片 |
| 17 | Flink RocksDB Checkpoint 怎么加速？| **Checkpoint 用 SST File 硬链接**（Linux hardlink 不复制文件，O(1)）→ 极快 Checkpoint |
| 18 | Leveled Compaction 每层 SST 数量？| 默认 max_file_num 10；超限触发和下一层 Compaction |
| 19 | 面试最高频考点排序？| **1. WAL+MemTable+SST 写入链 2. 三种 Compaction 策略放大因子 3. 读路径三级优化 4. TiKV/Flink 调优对比 5. 三大放大因子** |
| 20 | 学习路径？| WAL → MemTable → Immutable MemTable → SST → Level Compaction → Get 读路径 |
| 21 | 面试常见追问？| Compaction 会阻塞写吗？| 不会！Compaction 是后台线程独立执行；写线程可以继续写 WAL + MemTable |
| 22 | 面试常见追问？| SST Block 压缩用什么？| 默认 Snappy（快但压缩率低）；可以选 Zstd/LZ4（压缩率高但慢）；TiKV 冷热分层：热 LZ4、冷 Zstd |
| 23 | 面试陷阱？| 说 RocksDB 是 "Java 实现的 Redis 兼容层" → ❌ RocksDB 是 Facebook 开源的 C++ LSM 库，TiKV/Flink/Kafka Streams 都用它 |
| 24 | 面试陷阱？| 说 Compaction 就是 "合并两个 SST" → ❌ 可以合并多个 Level 的多个 SST（L0 可能同时和多个 L1+ SST 合并）|
| 25 | Block Cache 和 LSM Level 的关系？| L0 的 SST 最热 → 最容易命中 Block Cache；深层 SST 冷 → 命中率低 |
| 26 | Flink RocksDB vs Kafka Streams RocksDB 区别？| Flink Checkpoint 外部写 SST 硬链接；Kafka Streams Checkpoint 写 Kafka Topic；其他基本相同 |
| 27 | 为什么 TiKV 要大 write_buffer_size？| Percolator 事务高写入 → 大 MemTable 减少 Flush 次数 → L0 SST 少 → L0→L1 Compaction 压力小 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：db/db_impl.cc + db/memtable.cc + db/wal_manager.cc + db/compaction/level_compaction_picker.cc  
**面试考点：27 题**  
**差异化点**：WAL→MemTable→Immutable→SST 写入链、三种 Compaction 策略放大因子对比、读路径三级优化（BloomFilter→Block Cache→二分）、**TiKV vs Flink vs Kafka Streams 跨系统调优对比表**
