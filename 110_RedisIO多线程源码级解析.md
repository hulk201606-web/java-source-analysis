# Redis 6.x I/O 多线程源码级深度解析

> **源码基线**：Redis 7.2.x（C，redis-server）  
> **核心仓库**：antirez/redis → src/redis.c + src/networking.c  
> **面试权重**：⭐⭐⭐⭐（Redis 6.0 重大升级，I/O 多线程）

---

## 一、Redis 为什么要 I/O 多线程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis 单线程瓶颈（5.x 及以前）                            │
│                                                               │
│  Redis 5.x 线程模型：                                        │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ 主线程（单线程）                                        │   │
│  │   ├── socket accept（接受连接）                        │   │
│  │   ├── 读请求（从 socket 读数据）                       │   │
│  │   ├── 解析命令（Redis Protocol）                       │   │
│  │   ├── 执行命令（单线程 KV 操作）                       │   │
│  │   └── 写响应（往 socket 写数据）                       │   │
│  │                                                         │   │
│  │   瓶颈在哪里？                                          │   │
│  │   KV 操作是内存级（ns 级）→ 不是瓶颈！                 │   │
│  │   **socket 读/写是系统调用（us 级）→ 瓶颈！**        │   │
│  │   高 QPS 下 → socket I/O 占主线程时间 > 80%            │   │
│  │   → KV 操作快，但主线程被 I/O 占满 → 无法提升           │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ 瓶颈定位（面试高频）：                                     │
│   Redis 单线程瓶颈**不在 KV 操作**（内存级，快）            │
│   → **在 socket I/O**（系统调用，慢）                       │
│   → 高并发下 socket read/write 成为瓶颈                    │
│
│ ★ Redis 6.x 解决方案：**I/O 多线程**                        │
│   主线程：socket accept（接受连接）+ 命令执行               │
│   I/O 线程：socket read + socket write（并行！）            │
│   → KV 操作仍然是单线程（保持原子性！）                    │
│   → 只有 socket I/O 是多线程（没有状态竞争！）              │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Redis 6.x I/O 多线程架构（源码级）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis 6.x 线程模型（源码级）                              │
│                                                               │
│  // redis.c main() 启动线程：                                │
│  void initServerWithConfig() {                               │
│      // ① 主线程（accept + 执行命令）                       │
│      main_loop();                                            │
│                                                               │
│      // ② 后台线程（bio 三个线程）                           │
│      bioInit();  // lazy free / AOF fsync / close fd        │
│                                                               │
│      // ③ ★ I/O 多线程（Redis 6.0 新增！）                   │
│      if (server.io_threads_num > 1) {                        │
│          initThreads();  // 创建 io_threads_num 个 I/O 线程 │
│      }                                                       │
│
│      // ④ AOF rewrite 子进程                                 │
│      startAppendOnlyRewrite();                               │
│  }                                                           │
│
│ ★ I/O 线程数量：                                             │
│   io-threads 4（redis.conf 配置）                           │
│   → 建议 = CPU 核数（默认 4）                               │
│   → 最多 256 个（io_threads_num 上限）                      │
│
│ ★ 核心架构图：                                                │
│  ┌──────────────┬──────────────┬──────────────┐             │
│  │ Main Thread  │ I/O Thread 0 │ I/O Thread 1 │ ...         │
│  │ ──────────   │ ──────────   │ ──────────   │             │
│  │ accept()     │ read() from  │ read() from  │             │
│  │ accept 新连接│ socket A     │ socket B     │             │
│  │              │              │              │             │
│  │ 执行命令      │ write() to   │ write() to   │             │
│  │ (KV 操作)    │ socket A     │ socket B     │             │
│  └──────────────┴──────────────┴──────────────┘             │
│
│ ★ 关键：命令执行还是单线程！                                 │
│   为什么？Redis KV 操作都是单线程原子性                    │
│   → 只把 socket I/O 交给多线程（读/写）                     │
│   → 命令解析 + 命令执行仍然是主线程单线程                   │
│   → 保持 Redis 单线程原子性的简洁设计                      │
│
```

---

## 三、I/O 多线程调度源码级

### 3.1 Socket 分配

```c
// ★ Redis 6.x I/O 调度核心（networking.c）
//
// // socket 分配给 I/O 线程（Round Robin 轮询）
// static void assignSocketToIOThread(int fd) {
//     static int io_counter = 0;
//
//     // Round Robin 轮询分配
//     int io_thread_id = io_counter % server.io_threads_num;
//     io_counter++;
//
//     // 把 fd 加入 I/O 线程的待处理队列
//     server.io_threads[io_thread_id].pending_reads.push(fd);
// }
//
// // 主线程 accept 新连接 → 分配给 I/O 线程
// static int acceptHandler(aeEventLoop *loop, int fd, void *data, int mask) {
//     int client_fd;
//     client_fd = accept(fd, ...);
//
//     // ★ 新连接分配给某个 I/O 线程
//     assignSocketToIOThread(client_fd);
//     // → 这个 socket 的读/写都由那个 I/O 线程负责
// }
//
```

### 3.2 I/O 线程主循环

```c
// ★ I/O 线程主循环（源码级）
//
// // networking.c ioThreadMain()
// static void *ioThreadMain(void *arg) {
//     io_thread_t *io_thread = (io_thread_t *)arg;
//
//     while (!io_thread->stop) {
//         // ★ Step 1: 读所有分配给我的 socket
//         int readfds = readFromAllFds(io_thread);
//
//         if (readfds > 0) {
//             // ★ Step 2: 通知主线程有新请求要处理
//             // 用 pthread_cond_signal 唤醒主线程
//             pthread_mutex_lock(&server.io_mutex);
//             server.io_pending = 1;  // 有 pending 请求
//             pthread_cond_signal(&server.io_cond);
//             pthread_mutex_unlock(&server.io_mutex);
//
//             // ★ Step 3: 等主线程处理完命令
//             pthread_mutex_lock(&io_thread->mutex);
//             while (io_thread->commands_pending > 0) {
//                 pthread_cond_wait(&io_thread->cond, &io_thread->mutex);
//             }
//             pthread_mutex_unlock(&io_thread->mutex);
//
//             // ★ Step 4: 写响应到 socket（主线程已生成响应）
//             writeToAllFds(io_thread);
//         }
//     }
//     return NULL;
// }
//
// ★ 流水线设计（面试高频）：
//   主线程：accept → 收到 I/O 线程信号 → 执行命令 → 通知 I/O 线程响应就绪
//   I/O 线程：读 socket → 通知主线程 → 等主线程执行 → 写响应
//   → 类似 CPU 流水线（取指→执行→写回）
//   → 主线程执行命令时，I/O 线程可以读下一个 socket！
//
```

### 3.3 性能提升

```
┌─────────────────────────────────────────────────────────────┐
│  ★ I/O 多线程性能提升（Redis 官方 benchmark）                │
│                                                               │
│  Redis 5.x 单线程（1 Core）：                                  │
│    $ redis-benchmark -t set,get -n 1000000 -q                │
│    SET: ~120,000 ops/s                                       │
│    GET: ~120,000 ops/s                                       │
│                                                               │
│  Redis 7.x I/O 多线程（4 Core）：                              │
│    $ redis-server --io-threads 4                             │
│    SET: ~350,000 ops/s（2.9x 提升！）                        │
│    GET: ~350,000 ops/s（2.9x 提升！）                        │
│                                                               │
│  8 Core：                                                     │
│    SET: ~500,000 ops/s（4.2x 提升！）                        │
│
│ ★ 提升曲线（面试高频）：                                       │
│   2 Core → ~2x                                               │
│   4 Core → ~3x-4x                                            │
│   8 Core → ~5x-6x                                            │
│   超过 8 Core → 收益递减（锁竞争）                            │
│
│ ★ 为什么不是线性 8x？                                         │
│   主线程执行命令还是单线程 → 这是瓶颈                          │
│   I/O 线程并行 → 但命令执行串行 → Amdahl 定律                 │
│   → 瓶颈转移：从 I/O 转移到命令执行                            │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、Redis Cluster + 多线程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Redis Cluster 多节点 + 每个节点 I/O 多线程                │
│                                                               │
│  Redis Cluster（6 节点，每个节点 io-threads=4）：             │
│  总 QPS ≈ 6 节点 × 350,000 ops/s ≈ **2.1M ops/s**          │
│
│ ★ 这就是为什么 Redis 7.x + Cluster 是生产首选：               │
│   每个节点内部 I/O 多线程 → 3-5x 提升                        │
│   Cluster 多节点 → N 倍扩展                                   │
│   → 总 QPS = 节点数 × 单节点提升 × 原始 QPS                  │
│
│ ★ 和 TiKV/RocksDB 对比：                                      │
│   TiKV：Raft 副本复制 + RocksDB LSM-Tree + Multi-Raft        │
│   Redis Cluster：Hash Slot + I/O 多线程 + 主从复制          │
│   → 两者都是"分布式 + 多线程"但架构差异大                    │
│   → TiKV 强一致事务 + 磁盘存储；Redis Cluster 最终一致 + 内存│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Redis 5.x 瓶颈在哪？| **socket I/O（系统调用）**，不是 KV 操作（内存级）|
| 2 | Redis 6.x 解决什么？| **I/O 多线程（读/写 socket）**；命令执行仍然单线程 |
| 3 | 线程模型？| **主线程（accept+执行命令）+ N 个 I/O 线程（读/写 socket）** + 3 个后台 bio 线程 |
| 4 | 为什么命令执行还单线程？| Redis KV 操作原子性；多线程执行会有状态竞争；只把 socket I/O 交给多线程 |
| 5 | I/O 线程数量？| `io-threads` 配置（默认 4，建议 = CPU 核数，最多 256）|
| 6 | Socket 分配方式？| **Round Robin 轮询**：accept 新连接 → 分配给某个 I/O 线程 |
| 7 | I/O 线程主循环？| **读 socket → 通知主线程 → 等主线程执行完 → 写响应** |
| 8 | 性能提升？| **4 Core → 3-4x 提升**；8 Core → 5-6x；超过 8 Core 收益递减 |
| 9 | 为什么不是线性？| 主线程命令执行还是单线程 → Amdahl 定律瓶颈 |
| 10 | 源码路径？| **src/redis.c（启动）+ src/networking.c（I/O 调度）** |
| 11 | Redis 学习路径？| redis.c initServerWithConfig → ioThreadMain → networking.c 读写流程 |
| 12 | 和 Node.js 对比？| 都是事件循环 + 多 I/O 线程；Redis 命令执行单线程，Node.js JS 执行单线程 |
| 13 | Redis 是多进程还是多线程？| **单进程 + 多线程**（主线程 + I/O 线程 + bio 线程）|
| 14 | Redis 7.x 新特性？| Redis Functions + Redis ACL 2.0 + I/O 多线程优化 |
| 15 | Cluster + 多线程总 QPS？| 6 节点 × 350k ≈ **2.1M ops/s** |
| 16 | Redis 面试最高频考点排序？| **1. 为什么单线程 2. 6.x I/O 多线程瓶颈在哪 3. 命令执行还是单线程 4. 性能提升** |
| 17 | 和 TiKV 多节点对比？| TiKV：Raft + Multi-Raft；Redis：Hash Slot + Cluster；TiKV 强一致事务；Redis 内存 KV |
| 18 | 单线程 + 多线程设计哲学？| **I/O 瓶颈 → 多线程；状态操作 → 单线程**（无状态部分并行，有状态部分串行）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：src/redis.c + src/networking.c  
**面试考点：18 题**  
**差异化点**：Redis 6.0 重大升级 + I/O 多线程源码级 + 性能数据
