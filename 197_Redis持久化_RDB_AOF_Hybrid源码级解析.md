# Redis 持久化 RDB vs AOF vs Hybrid 源码级深度解析

> **源码基线**：Redis 7.x / 8.0（C）  
> **核心位置**：src/server.c + src/bgsave.c + src/aof.c  
> **面试权重**：⭐⭐⭐⭐⭐（Redis 最高频面试题之一）

---

## 一、Redis 持久化全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis 三种持久化方式（面试必背对比）                        │
│                                                               │
│  ┌──────────────┬──────────────────┬──────────────────┐     │
│  │ 方式            │ RDB              │ AOF              │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 原理            │ ★ 快照 Dump     │ ★ 命令追加日志    │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 文件格式         │ 二进制（压缩）  │ 文本格式（Redis 协议）│  │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 崩溃恢复数据丢失 │ ★ 可能丢几分钟  │ ★ 最多丢 1s      │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 文件大小         │ ★ 小（压缩快照）│ 大（命令日志膨胀） │     │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ I/O 开销         │ ★ 低（Dump 快）│ 中（每次写 append）│    │
│  ├──────────────┼──────────────────┼──────────────────┤     │
│  │ 恢复速度         │ ★ 快（直接加载  │ 慢（重放所有命令）│     │
│  └──────────────┴──────────────────┴──────────────────┘     │
│                                                               │
│  ★ Redis 4.0+ Hybrid（混合持久化）：同时开 RDB + AOF           │
│   ① 先写 RDB 快照（快速）                                     │
│   ② 后面的命令追加到 AOF                                       │
│   → 恢复时：先加载 RDB 快照 → 再重放 AOF 增量                   │
│   → 既有 RDB 快速恢复 → 又有 AOF 低数据丢失                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、RDB 源码级

### 2.1 BGSAVE + Fork + COW

```c
// ★ RDB 核心源码级（面试核心）
//
// src/server.c → rdbSaveBackground()
// src/bgsave.c → rdbSave()
//
// // RDB 触发两种方式：
// // 1. SAVE：阻塞 Redis 主进程同步快照
// // 2. BGSAVE：fork() 子进程异步快照（生产推荐）
//
// // ★ BGSAVE 核心流程：
// int rdbSaveBackground(char *filename, rdbSaveInfo *rsi) {
//     // ① ★ fork() 子进程
//     //    // src/server.c → fork()
//     //    // Linux fork() = Copy-On-Write（COW）
//     //    // fork 瞬间子进程和父进程共享同一块内存
//     //    // 父进程继续服务；子进程 Dump 内存到 RDB
//     //
//     pid_t childpid = fork();
//     if (childpid == 0) {
//         // ② 子进程：Dump 内存到临时 RDB 文件
//         rdbSave(filename, rsi);
//         //    // rdbSave 内部遍历所有 DB 的 Key
//         //    // 按 RDB 二进制格式序列化
//         //    // 写完后 rename 原子替换旧文件
//         rename(filename, tempfile);
//         exit(0);
//     } else {
//         // ③ 父进程：继续服务
//         // 记录 BGSAVE 开始时间
//         // 注册 exit handler 等子进程结束
//         server.stat_rdb_save_time = time(NULL);
//         return C_OK;
//     }
// }
//
// // ★ RDB 二进制格式（简化）：
// struct RDBFile {
//     char magic[5];      // "REDIS"
//     int version;        // RDB 版本号
//     // 每 DB 的数据：
//     SELECT db_num;
//     // 每个 Key：
//     TYPE = REDIS_STRING;
//     Key = "user:1001";
//     Value = "Tom";
//     EXPIRE_TIME = (可选)
//     ...
//     EOF_MARKER
//     Checksum CRC64
// }
//
```

### 2.2 Fork + COW 内存影响（面试高频）

```c
// ★ Fork + COW 内存影响（面试必踩坑）
//
// // COW（Copy-On-Write）原理：
// //   fork() 瞬间：父子进程共享所有物理内存页（零额外内存）
// //   父进程写数据时：写操作触发 Page Fault → 内核复制这个页
// //   → 被写的页才复制，没被写的继续共享
// //
// // 问题：Redis 内存 10GB
// //   BGSAVE 过程中如果写操作多（10GB 全部被写一遍）
// //   → COW 复制了 10GB 新内存
// //   → 总内存变 20GB！
// //   → 如果物理内存只有 16GB → OOM Kill
// //
// // ★ 生产缓解（面试高频答案）：
// // 1. 避免高峰期 BGSAVE（低峰定时执行）
// // 2. 用 info memory 检查 mem_fragmentation_ratio（碎片率）
// //    - ratio > 1.5 说明有碎片；> 2.0 需要重启
// // 3. 设置 save 规则稀疏一点
// // 4. Redis 7.0+ 支持 Forkless RDB（异步 Dump，不用 fork）
// //    // src/server.c → asyncSAVE()
// //    // 用 thread pool 异步 Dump（减少 COW 开销）
// //    // 但仍有内存压力（遍历期间内存分配）
// //
// // ★ RDB 自动触发规则（save 参数）：
// //   save 900 1    # 900s 内 1 次写 → BGSAVE
// //   save 300 10   # 300s 内 10 次写 → BGSAVE
// //   save 60 10000 # 60s 内 10000 次写 → BGSAVE
// //   // 三者 OR 关系（满足任一触发）
// //   // 如果开了 Hybrid + RDB → save 规则只影响 RDB
//
```

---

## 三、AOF 源码级

### 3.1 AOF 三种 fsync 策略

```c
// ★ AOF 核心源码级（面试核心）
//
// src/aof.c → feedAppendOnlyFile()
// src/server.c → preparePropagate()
//
// // AOF 原理：把写命令追加到 AOF 文件
// //   Redis 重启时重放 AOF 文件里的命令 → 恢复状态
// //
// // // AOF 追加流程：
// void feedAppendOnlyFile(client *c) {
//     // 命令格式：RESP 协议（和客户端通信格式一致）
//     // 例：*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
//     // 追加到 AOF 缓冲区
//     aof_buf.append(cmd_protocol);
//
//     // ★ 根据 appendfsync 决定什么时候落盘：
//     switch (appendfsync) {
//         case ALWAYS:
//             // ★ 每条命令都 fsync（最安全，最慢）
//             // 写完 AOF Buffer → 立刻 fsync 到磁盘
//             // 数据丢 0 条；但每次写都磁盘 IO → 吞吐极低
//             aofFlush();
//             break;
//         case EVERYSEC:  // ★ Redis 默认
//             // 每秒 fsync（平衡安全和性能）
//             // 写命令到 AOF Buffer；后台线程每秒 fsync 一次
//             // 崩溃最多丢 1s 数据；吞吐基本无损
//             // src/aof.c → aofBgFlushCron()（每秒定时触发）
//             break;
//         case NO:
//             // 让 OS 自己决定什么时候 fsync（最快，最不安全）
//             // write() 只是写到页缓存；OS 定期刷盘
//             // 数据可能丢几秒甚至更多
//             break;
//     }
// }
//
// ★ AOF 三种 fsync 性能对比（面试高频）：
//   ALWAYS：     < 10000 ops/s（每次 fsync 阻塞）
//   EVERYSEC：   ~ 50000 ops/s（后台批量 fsync）★ 默认
//   NO：         ~ 100000+ ops/s（OS 自动刷盘）
//
```

### 3.2 AOF Rewrite 源码级

```c
// ★ AOF Rewrite（自动压缩膨胀的 AOF 文件）
//
// src/aof.c → rewriteAppendOnlyFileBackground()
//
// // AOF 文件膨胀问题（面试核心）：
// //   如果同一个 Key 被 SET 10000 次
// //   → AOF 里有 10000 条命令
// //   → 恢复时也要重放 10000 次 → 慢！
// //   → 文件也很大
// //
// // ★ AOF Rewrite：重新 Dump 当前内存到新 AOF 文件
// //   和 BGSAVE 一样 fork() + COW
// //   → 只包含最终状态（不包含中间 SET）
// //   → 新 AOF 文件 = 当前 RDB 快照 + 后续增量
// //
// // // Rewrite 触发参数：
// //   auto-aof-rewrite-percentage = 100  # AOF 大小是上次 Rewrite 的 2x 时触发
// //   auto-aof-rewrite-min-size = 64mb   # 最小 Rewrite 阈值
// //
// // // Rewrite 流程（源码级）：
// void rewriteAppendOnlyFileBackground() {
//     pid_t childpid = fork();
//     if (childpid == 0) {
//         // ① 子进程：遍历当前内存 → 直接写新 AOF 文件
//         rewriteAppendOnlyFile();
//         //    // 只写 Key 当前状态对应的命令
//         //    // SET key final_value （不是中间 10000 条 SET）
//         exit(0);
//     } else {
//         // ② 父进程：继续服务
//         //   Rewrite 期间的写命令：
//         //   a. 同时追加到旧 AOF（正常）
//         //   b. 也追加到 Rewrite Buffer（临时内存区）
//         //   // src/aof.c → aofRewriteBufferAppend()
//         //
//         // ③ 子进程完成后：
//         //   把 Rewrite Buffer 里的命令也追加到新 AOF
//         //   rename 新 AOF → 替换旧 AOF
//     }
// }
//
```

---

## 四、Hybrid（混合持久化）源码级

```c
// ★ Hybrid 混合持久化（Redis 4.0+，生产推荐）
//
// src/server.c → rdbSaveBackground() 配合 aofRewrite
//
// // 原理：AOF Rewrite 时先写 RDB 格式头，再追加增量命令
// //   新 AOF 文件 = RDB 二进制头 + AOF 命令尾巴
// //   → 恢复时：先解析 RDB 头 → 再重放 AOF 命令
// //
// // ★ Hybrid Rewrite 源码级（和普通 AOF Rewrite 的区别）：
// void hybridRewriteAppendOnlyFile() {
//     pid_t childpid = fork();
//     if (childpid == 0) {
//         // ① ★ 先写 RDB 格式（当前全量内存快照）
//         rdbSaveHeader();
//         rdbSaveBody();
//         //    // RDB 二进制格式（快且压缩）
//         //
//         // ② 再追加 AOF 命令尾巴
//         aofRewriteBufferAppend();
//         //    // 只有 Rewrite 期间的增量命令（非常短）
//         //    // 恢复时先加载 RDB 头 → 再重放这小段 AOF
//     }
// }
//
// // Hybrid 配置：
// //   aof-use-rdb-preamble yes  // 开启 Hybrid
// //   // 默认 yes（Redis 5.0+ 默认开启）
//
// ★ 恢复流程（源码级）：
// //   aof-load-truncated yes  // 加载截断的 AOF
// //   Redis 启动时：
// //   1. 检查持久化方式：优先 Hybrid AOF → 否则纯 AOF → 否则 RDB
// //   2. 解析 AOF 文件：
// //      a. 如果是 Hybrid → 先读 RDB Preamble → 再重放尾部 AOF
// //      b. 如果是纯 AOF → 从头到尾重放所有命令
// //   3. 如果都没有持久化文件 → 空数据库
//
```

---

## 五、三种持久化全维度对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频：Redis 三种持久化全维度对比                        │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │ 维度            │ RDB            │ AOF            │ Hybrid（推荐）││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 原理            │ 二进制快照     │ 命令追加日志   │ RDB 头 + AOF  ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 崩溃数据丢失     │ ★ 可能丢几分钟  │ ★ 最多丢 1s    │ ★ 最多丢 1s  ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 文件大小         │ ★ 小（压缩）  │ 大（命令膨胀） │ 中（RDB压缩） ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 恢复速度         │ ★ 最快（直接加载）│ 慢（重放命令）│ 快（先 RDB 后 AOF）││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 写性能           │ ★ 低（fork 快）│ 中（追加 fsync）│ 中（同 AOF） ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 自动维护         │ save 规则触发  │ auto-aof-rewrite│ Hybrid rewrite││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 运维复杂度       │ 低            │ 中（关注 Rewrite）│ 中（同 AOF）││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 推荐场景         │ 冷备份/迁移    │ 强持久化需求   │ ★ 生产首选    ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 面试高频         │ ★★★★★        │ ★★★★★        │ ★★★★★       ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   **生产必开 Hybrid**（aof-use-rdb-preamble=yes + appendfsync=everysec）│
│   → 最多丢 1s 数据 + 恢复快 + 文件小                         │
│   纯 RDB：仅用于冷备份（如每天迁移到新节点）                   │
│   纯 AOF：一般不用（恢复慢、文件膨胀）                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（27 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | RDB 核心原理？| **BGSAVE fork() 子进程 + COW + 内存 Dump 成二进制快照** |
| 2 | ★ Fork + COW 内存影响？| fork 瞬间父子共享内存；父进程写触发 COW 复制 → **最坏情况总内存翻倍** |
| 3 | RDB 自动触发规则？| save 900 1 / save 300 10 / save 60 10000（三者 OR）|
| 4 | AOF 核心原理？| **把写命令追加到 AOF 日志文件**；恢复时重放 |
| 5 | ★ AOF 三种 fsync 策略？| **ALWAYS（每条都 fsync，丢 0）+ EVERYSEC（每秒 fsync，丢最多 1s）+ NO（OS 决定，丢几秒）** |
| 6 | AOF Rewrite 作用？| 压缩膨胀的 AOF 文件；重新 Dump 当前内存状态 → 替换旧文件 |
| 7 | AOF Rewrite 期间的写命令怎么处理？| 同时追加到旧 AOF + **Rewrite Buffer（临时内存区）**；子进程完成后 Buffer 里的也追加到新 AOF |
| 8 | ★ Hybrid 混合持久化原理？| AOF Rewrite 时**先写 RDB 二进制头（快且压缩）+ 后追加 AOF 增量尾巴**；恢复先加载 RDB 头再重放 AOF |
| 9 | Hybrid 默认开启？| **Redis 5.0+ 默认开启**：aof-use-rdb-preamble=yes |
| 10 | 三种持久化崩溃恢复数据丢失量？| RDB：丢 Save 周期内的数据；**AOF（everysec）+ Hybrid：丢最多 1s** |
| 11 | 恢复时三种方式优先级？| **Hybrid AOF > 纯 AOF > RDB** |
| 12 | info memory 碎片率？| mem_fragmentation_ratio；**> 1.5 有碎片；> 2.0 需要重启** |
| 13 | 生产推荐配置？| **appendonly=yes + appendfsync=everysec + aof-use-rdb-preamble=yes（Hybrid）** |
| 14 | ★ RDB vs AOF vs Hybrid 全维度对比？| 见第五节对比表（9 维度）|
| 15 | 面试最高频考点排序？| **1. Fork+COW 内存影响 2. Hybrid 混合原理 3. AOF 三种 fsync 4. AOF Rewrite 5. 三种对比** |
| 16 | 学习路径？| BGSAVE fork+COW → AOF append → fsync 三种策略 → AOF Rewrite → Hybrid |
| 17 | 面试常见追问？| BGSAVE 和 AOF Rewrite 都 fork → 同时开会有问题吗？| Redis 会串行执行（不能同时 fork）；排队等上一个完成再 fork；避免内存叠加 |
| 18 | 面试常见追问？| 为什么 RDB 恢复比 AOF 快？| RDB 是**二进制快照（直接 load）**；AOF 要重放所有命令（每行解析+执行）→ 慢几倍 |
| 19 | 面试陷阱？| 说 "appendfsync=always 数据完全不丢" → ❌ 如果 Redis OS 崩溃（断电）+ fsync 还没完成 → 还是可能丢；**fsync 只保证写磁盘文件，不保证文件系统刷盘** |
| 20 | 面试陷阱？| 说 "Hybrid 是 RDB + AOF 同时开两个独立文件" → ❌ **Hybrid 是一个 AOF 文件里先放 RDB 头再放 AOF 命令** |
| 21 | auto-aof-rewrite-percentage 默认？| 100（AOF 文件大小增长到上次 Rewrite 的 2x 时触发）；min-size=64MB |
| 22 | Redis 7.0+ Forkless RDB？| asyncSAVE()；用 thread pool 异步 Dump；减少 COW 开销；但内存压力仍在 |
| 23 | RDB 文件 checksum？| CRC64 校验；恢复时校验文件完整性；损坏则恢复失败 |
| 24 | AOF 截断修复？| aof-load-truncated=yes；Redis 启动时检测 AOF 尾部损坏 → 自动截断到最后一个完整命令 |
| 25 | Redis Cluster 持久化？| **每个 Master 节点独立持久化**（Hybrid 推荐）；Replica 也开持久化（升级 Master 时不需要从零同步）|
| 26 | 典型生产用法？| Redis Cluster 6 节点 + **Hybrid（aof-use-rdb-preamble=yes + appendfsync=everysec）+ AOF Rewrite 自动**；定时 RDB 冷备份到 OSS |
| 27 | 冷备份和热备份？| RDB → 冷备份（停机或 BGSAVE 后复制 .rdb 文件）；热备份：Redis CLUSTER REPLICAS → 等副本同步后复制 |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：src/server.c → rdbSaveBackground() + src/aof.c → feedAppendOnlyFile() + rewriteAppendOnlyFileBackground()  
**面试考点：27 题**  
**差异化点**：Fork+COW 内存影响（OOM 风险）、AOF 三种 fsync 策略、AOF Rewrite 流程、**Hybrid（RDB 头 + AOF 增量）核心原理、三种持久化全维度对比**
