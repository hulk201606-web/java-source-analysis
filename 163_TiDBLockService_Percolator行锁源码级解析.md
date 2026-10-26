# TiDB Lock Service 分布式锁 + Percolator 行锁源码级深度解析

> **源码基线**：TiDB v8.0.x（Go）+ TiKV v8.0.x（Rust）  
> **核心位置**：tidb/pkg/expression + tidb/pkg/executor + tikv/src/storage/txn  
> **面试权重**：⭐⭐⭐⭐⭐（分布式锁是高频场景，TiDB 三层锁必问）

---

## 一、TiDB 三层锁全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB 锁体系三层架构（面试必背）                            │
│                                                               │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Layer 3：TiDB Server 集群级 Advisory Lock        │         │
│  │   GET_LOCK / RELEASE_LOCK / IS_USED_LOCK         │         │
│  │   MySQL 兼容的用户级锁，集群全局互斥             │         │
│  │   实现：写入 TiKV 特殊 key（类似 Distributed KV）│         │
│  └─────────────────────────────────────────────────┘         │
│                            ↑                                  │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Layer 2：TiKV Percolator 事务行锁                │         │
│  │   Prewrite 阶段写入 CF_LOCK 列族                 │         │
│  │   悲观事务：SELECT ... FOR UPDATE 即加行锁        │         │
│  │   乐观事务：Commit 时 Prewrite 检测冲突          │         │
│  │   锁等待：wait for lock → resolve lock 处理       │         │
│  └─────────────────────────────────────────────────┘         │
│                            ↑                                  │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Layer 1：协调锁（etcd + PD）                     │         │
│  │   PD Leader 选举锁（etcd CompareAndSwap）        │         │
│  │   DDL Owner 锁（TiDB 集群 DDL 协调）            │         │
│  │   分布式任务调度锁                               │         │
│  └─────────────────────────────────────────────────┘         │
│
│  注意区分：                                                    │
│   Advisory Lock = 应用层主动调用 GET_LOCK（类似 Java 的 ReentrantLock）
│   Percolator 行锁 = 事务读写自动加（数据库内部锁）
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TiKV Percolator 行锁（事务锁）源码级

### 2.1 Prewrite 加锁核心

```rust
// ★ TiKV Percolator Prewrite 阶段行锁实现（源码级）
//
// tikv/src/storage/txn/commands/prewrite.rs + planner.rs
// tikv/src/storage/engine/cf.rs （列族定义）
//
// // Percolator 三列族（面试必背）：
// const CF_DEFAULT: &str = "default";   // MVCC 数据
// const CF_WRITE:   &str = "write";     // Commit 记录（CommitTs → 可见版本）
// const CF_LOCK:    &str = "lock";      // ★ 行锁（Prewrite 写入）
//
// // Prewrite 执行核心流程：
// pub fn prewrite(txn: &Transaction, ...) -> Result<PrewriteResult> {
//     for mutation in txn.mutations {
//         let key = mutation.key;
//         let primary_is = txn.primary_key == key;
//
//         // ① 检查 key 上是否已有锁（其他事务正在 Prewrite）
//         //    读 CF_LOCK，看 lock.ts 是否属于其他事务
//         if let Some(existing_lock) = reader.get_lock(key)? {
//             if existing_lock.ts != txn.start_ts {
//                 // 已有其他事务的锁 → 冲突！
//                 //    悲观事务：wait_for_lock 等待
//                 //    乐观事务：直接返回 WriteConflict
//                 //    // tikv/src/storage/txn/resolve_lock.rs
//                 return Err(Error::WriteConflict);
//             }
//         }
//
//         // ② 检查 CF_WRITE 的已提交版本
//         //    有没有 commit_ts > txn.start_ts 的已提交写入
//         //    （比当前事务更新的数据）
//         let latest_write = reader.get_write(key, txn.start_ts)?;
//         if let Some(write) = latest_write {
//             if write.ts > txn.start_ts {
//                 return Err(Error::WriteConflict);
//                 // ★ 乐观事务冲突检测核心！
//                 // 有更新的提交版本 → 当前事务需要重试
//             }
//         }
//
//         // ③ 写入 CF_LOCK（加行锁）
//         //    lock 的内容：
//         //    - lock_type: Put/Delete/Lock（Pessimistic/Prewrite）
//         //    - primary_key：主键位置（非主键存的是 primary_key 引用）
//         //    - ts：事务 start_ts
//         //    - ttl：锁的生存时间（防止事务挂掉锁不释放）
//         let lock = Lock::new(
//             lock_type,
//             primary_key.clone(),
//             txn.start_ts,
//             DEFAULT_LOCK_TTL,   // 默认 3000ms
//             ...
//         );
//         writer.put_cf(CF_LOCK, key, lock.to_bytes())?;
//
//         // ④ 同时写 CF_DEFAULT（MVCC value，以 start_ts 为 key 后缀）
//         writer.put_cf(CF_DEFAULT, encode_key(key, txn.start_ts), value)?;
//     }
//
//     // ⑤ 成功后返回 Prewrite 完成
//     Ok(PrewriteResult { ... })
// }
//
```

### 2.2 锁等待与悲观事务

```rust
// ★ 悲观事务锁等待（源码级，面试高频）
//
// tikv/src/storage/txn/wait_for_lock.rs
// tikv/src/storage/txn/resolve_lock.rs
//
// // 悲观事务（BEGIN PESSIMISTIC）：
// // SELECT ... FOR UPDATE / FOR SHARE 语句触发
// // TiDB 生成 AcquirePessimisticLock 请求发给 TiKV
//
// pub fn acquire_pessimistic_lock(
//     reader: &MvccReader,
//     writer: &MvccWriter,
//     key: &Key,
//     start_ts: TimeStamp,
//     lock_ttl: u64,
//     ...
// ) -> Result<()> {
//     // ① 检查是否已被其他事务锁
//     if let Some(existing_lock) = reader.get_lock(key)? {
//         // 冲突！走 wait_for_lock
//         return wait_for_lock(reader, key, start_ts, lock_ttl);
//     }
//
//     // ② 无冲突 → 加悲观锁（lock_type = Pessimistic）
//     let pessimistic_lock = Lock::new(
//         LockType::Pessimistic,
//         primary_key,
//         start_ts,
//         lock_ttl,
//     );
//     writer.put_cf(CF_LOCK, key, pessimistic_lock.to_bytes())?;
//     Ok(())
// }
//
// // ★ wait_for_lock 核心逻辑：
// fn wait_for_lock(reader, key, my_ts, my_ttl) -> Result<()> {
//     loop {
//         let existing_lock = reader.get_lock(key)?;
//
//         if existing_lock.is_none() {
//             // 锁释放了！可以加了
//             break;
//         }
//
//         let lock = existing_lock.unwrap();
//
//         // 场景 A：对方锁的 TTL 过期了？
//         //   可能是对方事务挂了，需要清理 stale lock
//         if lock.ttl_expired() {
//             //   resolve_lock：回滚对方 Prewrite，清理 CF_LOCK
//             //   然后重试加锁
//             resolve_lock(key, lock)?;
//             continue;
//         }
//
//         // 场景 B：对方还活着 → 等
//         //   TiKV 通过 Raft Peer 之间的心跳感知
//         //   或者 sleep 一小段后重试
//         //   悲观事务等待时间上限：innodb_lock_wait_timeout（TiDB 参数）
//         //   默认 50s，超过则返回 LockWaitTimeout
//         thread::sleep(Duration::from_millis(50));
//     }
// }
//
// ★ 悲观 vs 乐观事务锁行为对比（面试必背）：
// ┌──────────┬──────────────────────┬─────────────────────┐
// │            │ 乐观（默认）            │ 悲观（BEGIN PESSIMISTIC）│
// ├──────────┼──────────────────────┼─────────────────────┤
// │ 锁时机     │ Commit → Prewrite 时   │ 读数据时就加 Pessimistic 锁│
// │ 冲突处理   │ 发现冲突 → 重试整个事务 │ 等待对方事务结束（wait）│
// │ 重试成本   │ 低（冲突概率低时好）   │ 高（等待期间占着连接）│
// │ 锁等待     │ 无等待（直接失败）     │ wait_for_lock 循环等待 │
// │ 典型场景   │ 读多写少、冲突率低     │ 写多冲突多、热点表     │
// └──────────┴──────────────────────┴─────────────────────┘
//
```

### 2.3 锁 TTL 与自动清理

```rust
// ★ 锁 TTL + 自动清理（源码级）
//
// TiKV 的锁有 TTL（Time To Live），防止事务崩溃锁不释放
//
// // Lock 结构体核心字段：
// pub struct Lock {
//     pub lock_type: LockType,      // Put/Delete/Pessimistic
//     pub primary_key: Vec<u8>,     // 主键位置
//     pub ts: TimeStamp,            // 持有锁的事务 start_ts
//     pub ttl: u64,                 // 毫秒，默认 3000
//     pub for_update_ts: TimeStamp, // 悲观事务专用
// }
//
// // TTL 判断：
// impl Lock {
//     pub fn ttl_expired(&self) -> bool {
//         // lock 写入时记录了写入时间
//         // 写入时间 + ttl < 当前时间 → 过期
//         let expire_time = self.creation_time + self.ttl;
//         expire_time < now()
//     }
// }
//
// // ★ 清理 stale lock 的两种机制：
// // ① 主动检测：wait_for_lock 循环里检查 TTL
// // ② GC Worker：TiKV 后台扫描清理
// //    tikv/src/storage/gc/gc_worker.rs
// //    把 start_ts < safepoint 的 Prewrite 事务锁回滚
// //    （类似 InnoDB 的 undo log purge）
//
// // ★ Lease 续期：
// // 活跃事务在执行期间会周期性续期 TTL
// // 防止长事务正常执行中锁被 GC 误清理
//
```

---

## 三、TiDB 集群级 Advisory Lock（GET_LOCK）源码级

### 3.1 函数实现框架

```go
// ★ TiDB GET_LOCK 集群级锁实现（源码级）
//
// tidb/pkg/expression/builtin_lock.go  // 函数定义
// tidb/pkg/executor/advisory_lock.go   // 执行层
//
// // GET_LOCK(lockName, timeout) → 返回 1/0/NULL
// // 同一会话持有锁，其他会话（包括其他 TiDB 节点）无法获取
//
// // 核心原理：
// //   TiDB 把"集群级锁"的状态写入 TiKV 的一个特殊 key 空间
// //   利用 TiKV 的原子 CompareAndSwap（PutIfAbsent）实现互斥
//
// const (
//     advisoryLockPrefix = []byte("_tidb/advisory_lock/")
// )
//
// // lock key 构造：
// //   lockKey = advisoryLockPrefix + lockName
// //   lock value = 持有锁的 sessionID + expire_time + owner
//
// func GetLock(ctx, lockName string, timeout int64) (int64, error) {
//     lockKey := buildLockKey(lockName)
//
//     // ① 先检查自己是否已持有（同 session 可重入）
//     if heldLock := session.GetAdvisoryLock(lockName); heldLock != nil {
//         return 1, nil  // 已持有 → 直接成功
//     }
//
//     // ② 构造锁 value
//     lockValue := &LockValue{
//         SessionID:   session.ID(),
//         TxnStartTS:  txn.StartTS(),
//         ExpireTime:  now + MAX_LOCK_DURATION,  // 最长 2^31 - 1 秒
//     }
//
//     // ③ CAS 操作：原子写入 TiKV
//     //    底层用 prewrite + commit（Percolator 事务语义）
//     //    PutIfAbsent：key 不存在 → 写入成功返回 1
//     //                 key 存在 → 写入失败返回 0/等待
//     for deadline := now + timeout; now < deadline; now = time.Now() {
//         // 尝试 CAS
//         ok, err := tikvClient.PutIfAbsent(ctx, lockKey, lockValue)
//         if ok {
//             session.AddAdvisoryLock(lockName, lockValue)
//             return 1, nil  // ★ 获取成功！
//         }
//
//         // CAS 失败 → 检查锁持有者状态
//         holder, _ := tikvClient.Get(ctx, lockKey)
//         if holder.ExpireTime < now {
//             // 锁过期了！可以尝试抢占（CompareAndSet）
//             // Compare(key, expected_old_value) → 写入新值
//             ok, _ = tikvClient.CompareAndSwap(ctx, lockKey, holder, lockValue)
//             if ok {
//                 session.AddAdvisoryLock(lockName, lockValue)
//                 return 1, nil
//             }
//         }
//
//         // 没过期 → 等一小段再试
//         time.Sleep(100 * time.Millisecond)
//     }
//
//     return 0, nil  // 超时
// }
//
// func ReleaseLock(ctx, lockName string) (int64, error) {
//     lockKey := buildLockKey(lockName)
//     lockValue := session.GetAdvisoryLock(lockName)
//
//     // 只有持有者自己能释放（sessionID 匹配）
//     ok, _ := tikvClient.CompareAndDelete(ctx, lockKey, lockValue)
//     if ok {
//         session.RemoveAdvisoryLock(lockName)
//         return 1, nil  // ★ 释放成功
//     }
//     return 0, nil  // 不是持有者或锁已过期被清理
// }
//
```

### 3.2 Advisory Lock 的特殊处理

```go
// ★ Advisory Lock 关键特性（面试高频）
//
// // 1. 锁与会话绑定：
// //    获取锁的 session 断开 → 锁自动释放（TiDB 清理 session 资源）
// //    即使没显式调用 RELEASE_LOCK
//
// // 2. 同会话可重入：
// //    一个 session 对同一 lockName 多次 GET_LOCK 只持有一次
// //    需要多次 RELEASE_LOCK 才真正释放（类似 Java ReentrantLock）
//
// // 3. 锁 TTL 兜底：
// //    即使 TiKV 层面的锁 value 没被正确删除
// //    ExpireTime 过期后其他 session 也能抢占
//
// // 4. IS_USED_LOCK / IS_FREE_LOCK：
// //    IS_USED_LOCK(lockName) → 返回持有该锁的 sessionID（或 NULL）
// //    IS_FREE_LOCK(lockName) → 返回 1（空闲）/ 0（被占用）
// //
// //    // is_used_lock 实现：
// //    // tikvClient.Get(lockKey) → 有值 → 返回 sessionID
//
// // 5. 与事务的关系：
// //    Advisory Lock 是独立于事务的
// //    事务 commit/rollback 不会自动释放锁
// //    必须显式 RELEASE_LOCK 或 session 断开
//
// // 6. 集群级锁（区别于 MySQL）：
// //    MySQL Group Replication 的 GET_LOCK 是单节点局部锁
// //    TiDB 的 GET_LOCK 是全集群统一锁（通过 TiKV 协调）
//
// // ★ 典型使用场景：
// //   - 定时任务单实例执行（分布式调度）
// //   - DDL 操作协调（虽然 TiDB 自带 DDL Owner）
// //   - 业务层互斥（如同一订单不能同时处理）
//
```

---

## 四、协调锁：etcd + PD Leader Election

```go
// ★ PD Leader 选举（etcd CompareAndSwap）源码级
//
// pd/server/server.go
// etcd/clientv3/concurrency/election.go
//
// // PD 集群启动：每个 PD 节点都跑 Campaign 循环
// func runCampaign(server *Server) {
//     for {
//         // etcd 的 Leader Election 核心：
//         // 对 /pd/leader key 执行 CompareAndSwap
//         // value = {addr, id, ttl}
//         // 如果 key 不存在 → 写入成功 → 成为 Leader
//         // 如果 key 存在 → 读 leader，等它挂了再试
//
//         election := concurrency.NewElection(session, "/pd/leader")
//         err := election.Campaign(ctx, server.Addr)
//         if err != nil {
//             // 竞选失败（其他节点抢先），等一下重试
//             continue
//         }
//
//         // ★ 成为 Leader！
//         server.IsLeader = true
//         server.runLeaderLoop(ctx)
//
//         // Leader 期间：定时续租 TTL（心跳）
//         // 如果续租失败 → 认为 etcd 出问题 → 退出竞选
//     }
// }
//
// // ★ etcd 分布式锁的原子性保证：
// //   etcd 的 compare-and-swap 是线性化的
// //   多个 PD 节点同时竞选 → 只有一个成功（etcd CP 特性）
// //
// //   session TTL 默认 5s：
// //   Leader 必须在 5s 内续租一次 etcd session
// //   如果 Leader 挂了 → session 过期 → 其他节点可以竞选
// //
// ★ etcd 锁 vs TiKV 锁 选择（面试高频）：
// ┌──────────┬───────────────────┬──────────────────┐
// │            │ etcd 锁             │ TiKV 锁            │
// ├──────────┼───────────────────┼──────────────────┤
// │ 原子操作   │ CAS（CompareAndSwap）│ PreWrite + Commit │
// │ 协调者     │ etcd（CP，轻量）    │ TiKV（Raft，重）   │
// │ 延迟       │ ★ 低（ms 级）       │ 较高（Raft 复制）   │
// │ 容量       │ 小（元数据级）      │ ★ 大（业务数据级）│
// │ 典型场景   │ PD/DDL Leader 选举  │ 业务 Advisory Lock │
// │ 锁粒度     │ key 级（通用）      │ key 级 + TTL 复杂 │
// └──────────┴───────────────────┴──────────────────┘
//
```

---

## 五、差异化对比：TiDB 锁 vs Redisson vs ZK vs etcd

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试高频对比：四套分布式锁方案                            │
│  ┌──────────┬──────────────┬──────────────┬──────────────┬────┐
│  │ 维度      │ TiDB Advisory│ Redisson      │ ZK          │etcd│
│  ├──────────┼──────────────┼──────────────┼──────────────┼────┤
│  协调者     │ TiKV Raft      │ Redis Single  │ ZK ZAB      │etcd│
│  原子性     │ Raft 复制 CP   │ Lua 脚本       │ ZAB CP      │CP  │
│  延迟       │ ~10ms          │ ★ ~1ms         │ ~5ms        │~3ms│
│  容量       │ ★ 大（与数据同）│ 大 Redis       │ 小元数据     │小元│
│  TTL/续期   │ ✅ 有（自动清理）│ ✅ Watchdog    │ Session TTL │TTL │
│  可重入     │ ✅ 同 session  │ ✅ 完整        │ ❌          │ ❌ │
│  读锁/写锁  │ ❌              │ ✅ 完整        │ ✅ 完整     │ ❌ │
│  公平锁     │ ❌              │ ✅ RedLock     │ ✅ 顺序节点 │ ❌ │
│  集群感知   │ ★ 同 TiDB 集群 │ 独立 Redis     │ 独立 ZK     │独立│
│  典型场景   │ TiDB 定时/互斥 │ Java 应用互斥  │ 通用协调     │协调│
│  面试高频   │ ★ 中           │ ★★★ 高         │ ★★ 中       │★★★│
│  └──────────┴──────────────┴──────────────┴──────────────┴────┘
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   业务在 TiDB 里 → 用 TiDB GET_LOCK（省组件、运维成本）        │
│   Java 应用通用 → Redisson（功能最全、生态好）               │
│   强 CP 协调 + Watch 监听 → ZK / etcd                        │
│   超大容量锁（百万级）→ TiKV / Redis                          │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（25 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB 锁体系三层？| **① Percolator 行锁（CF_LOCK）② Advisory Lock（GET_LOCK）③ 协调锁（etcd/PD）** |
| 2 | TiKV Percolator 三列族？| **CF_DEFAULT（数据）+ CF_WRITE（Commit 记录）+ CF_LOCK（行锁）** |
| 3 | Prewrite 加锁核心？| 遍历 mutation → 检查 CF_LOCK 和 CF_WRITE → 写 CF_LOCK + CF_DEFAULT(start_ts 后缀) |
| 4 | Prewrite 冲突检测？| CF_LOCK 有其他事务锁 → 冲突；CF_WRITE 有 commit_ts > start_ts 的提交 → 冲突 |
| 5 | 乐观 vs 悲观事务锁时机？| 乐观：Commit 时 Prewrite 才加锁；悲观：读数据时就加 Pessimistic 锁 |
| 6 | 悲观事务冲突处理？| **wait_for_lock 循环等待对方锁释放/过期清理**，超则 LockWaitTimeout |
| 7 | 乐观事务冲突处理？| 直接返回 WriteConflict，TiDB 层自动重试或应用层重试 |
| 8 | 锁 TTL 作用？| 防止事务崩溃锁不释放；默认 3000ms，过期后被清理 |
| 9 | stale lock 清理机制？| ① wait_for_lock 循环检测 TTL 过期主动清理 ② GC Worker 后台扫描 start_ts < safepoint 的锁 |
| 10 | Advisory Lock（GET_LOCK）原理？| 集群级锁，写 TiKV 特殊 key 空间，**CAS 原子写入 + TTL 兜底** |
| 11 | Advisory Lock 绑定什么？| 绑定 session，session 断开自动释放；同 session 可重入 |
| 12 | GET_LOCK vs MySQL？| MySQL 单节点局部锁；**TiDB 集群全局锁**（通过 TiKV 协调） |
| 13 | GET_LOCK 与事务的关系？| **独立于事务**，commit/rollback 不自动释放，必须显式 RELEASE 或 session 断开 |
| 14 | IS_USED_LOCK / IS_FREE_LOCK？| IS_USED_LOCK 返回持有锁的 sessionID；IS_FREE_LOCK 返回是否空闲 |
| 15 | PD Leader 选举锁？| etcd CompareAndSwap 写 /pd/leader key；session TTL 5s 心跳续租 |
| 16 | etcd vs TiKV 锁选型？| etcd 轻量、低延迟、小容量（元数据协调）；TiKV 重但大容量（业务锁）|
| 17 | TiDB vs Redisson vs ZK vs etcd 对比？| 见第五节对比表（10 个维度）|
| 18 | 面试最高频考点排序？| **1. 三层锁体系 2. Percolator Prewrite CF_LOCK 3. 乐观 vs 悲观事务锁 4. Advisory Lock 集群级 CAS 5. stale lock 清理** |
| 19 | 学习路径？| Prewrite CF_LOCK → wait_for_lock → Advisory Lock → etcd PD Leader |
| 20 | 面试常见追问？| 一个事务同时锁 100 行怎么加的？→ Prewrite 每行写 CF_LOCK，Raft 复制提交 |
| 21 | 面试常见追问？| 长事务锁什么时候续期？→ 活跃事务周期性续期 TTL，防止正常执行中被 GC |
| 22 | 面试常见追问？| 锁等太久怎么办？→ innodb_lock_wait_timeout 默认 50s，超则 LockWaitTimeout |
| 23 | 面试陷阱？| 说 TiDB Advisory Lock 是 Redis 实现的 → ❌ 是 TiKV；说乐观事务不加锁 → ❌ 是 Commit 时 Prewrite 才加，更早检测冲突 |
| 24 | RedLock 与 TiDB 锁的关系？| RedLock 是 Redis 多主算法，TiDB 锁是 TiKV Raft CP，RedLock 理论有争议（Martin Kleppmann 批评）|
| 25 | 选型建议？| 业务在 TiDB → GET_LOCK；Java 通用 → Redisson；强 CP 协调 → ZK/etcd；超大规模 → TiKV/Redis |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：tikv/src/storage/txn/commands/prewrite.rs + wait_for_lock.rs + resolve_lock.rs + tidb/pkg/expression/builtin_lock.go  
**面试考点：25 题**  
**差异化点**：三层锁体系（Percolator/Advisory/协调）、乐观 vs 悲观事务锁行为、stale lock 清理、TiDB vs Redisson vs ZK vs etcd 锁方案对比
