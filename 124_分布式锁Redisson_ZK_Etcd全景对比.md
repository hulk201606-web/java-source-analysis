# 分布式锁 Redisson vs Zookeeper vs Etcd 全景深度对比

> **面试权重**：⭐⭐⭐⭐⭐（高并发基础设施高频考点）  
> **核心参考**：Redisson（Redis）+ Zookeeper 临时节点 + Etcd Lease  
> **面试必背**：**自旋锁 vs 阻塞锁** + **Watch Dog 自动续期** + **脑裂问题**

---

## 一、分布式锁核心问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式锁 = 跨进程/跨机器的互斥锁                          │
│                                                               │
│  单机锁（synchronized/ReentrantLock）不能跨 JVM              │
│ → 需要一个外部协调者来管理锁状态                              │
│
│ ★ 分布式锁三大核心要求（面试必背！）：                        │
│  ① **互斥性**：任意时刻只有一个客户端能拿到锁                │
│  ② **可重入性**：同一个客户端可以再次获取同一个锁（可重入） │
│  ③ **自动续期**：客户端持锁期间防止锁过期（Watch Dog）       │
│  ④ **容错性**：客户端挂了锁自动释放，不会死锁                │
│  ⑤ **公平性**：按请求顺序发放（可选）                        │
│  ⑥ **可中断**：等待锁期间可以中断（可选）                    │
│
│ ★ 分布式锁三大实现方案（面试高频！）：                        │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 方案       │ 核心机制       │ CP/AP          │ 性能          │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Redisson** │ Redis SETNX │ AP（主从）    │ ⭐⭐⭐⭐⭐ 最快  │   │
│  │            │ + Watch Dog  │                │                │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **ZK**     │ 临时顺序节点    │ CP（ZAB）      │ ⭐⭐ 慢        │   │
│  │            │ + Watch       │                │                │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Etcd**   │ Lease + Key   │ CP（Raft）     │ ⭐⭐⭐ 中       │   │
│  │            │ + Watch       │                │                │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ 面试最高频：为什么需要分布式锁？                            │
│   秒杀减库存（不能超卖）+ 定时任务（不能重复执行）           │
│   + 跨服务数据一致性（支付和库存扣减不能并发）               │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Redisson 源码级（生产最常用）

### 2.1 基本可重入锁

```java
// ★ Redisson 可重入锁源码级（面试核心！）
//
// // org/redisson/RedissonReentrantLock.java
//
// public class RedissonReentrantLock implements RLock {
//     private final String name;          // 锁的 key
//     private final CommandExecutor commandExecutor;
//     private final long watchdogTimeout = 30000L;  // ★ Watch Dog 过期时间 30s
//     private final long internalLockLeaseTime = 10000L;  // 续期间隔 10s
//
//     // ★ 获取锁核心 Lua 脚本（原子执行！）
//     // KEYS[1] = 锁 key
//     // ARGV[1] = leaseTime（默认 -1 自动续期）
//     // ARGV[2] = threadId（UUID + 线程 ID）
//     public <T> RFuture<T> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId) {
//         return commandExecutor.evalWriteAsync(
//             name,
//             LongCodec.INSTANCE,
//             RedisCommands.EVAL_BOOLEAN,
//             // ★ Lua 脚本（核心！）
//             "if (redis.call('exists', KEYS[1]) == 0) then " +  // ① 锁不存在
//                 "redis.call('hset', KEYS[1], ARGV[2], 1); " + // 创建 Hash：owner→count=1
//                 "redis.call('pexpire', KEYS[1], ARGV[1]); " + // 设置过期时间
//                 "return nil; " +
//             "end; " +
//             "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +  // ② 自己已经持有（可重入）
//                 "redis.call('hincrby', KEYS[1], ARGV[2], 1); " + // count++
//                 "redis.call('pexpire', KEYS[1], ARGV[1]); " +   // 刷新过期
//                 "return nil; " +
//             "end; " +
//             "return redis.call('pttl', KEYS[1]); ",  // ③ 别人持有 → 返回剩余过期时间
//             // 参数
//             Collections.singletonList(name),
//             unit.toMillis(leaseTime),
//             threadId
//         );
//     }
//
//     // ★ 解锁 Lua 脚本
//     public <T> RFuture<T> unlockInnerAsync(long threadId) {
//         return commandExecutor.evalWriteAsync(
//             name,
//             LongCodec.INSTANCE,
//             RedisCommands.EVAL_BOOLEAN,
//             "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +  // 不是自己的锁
//                 "return nil;" +
//             "end; " +
//             "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " + // count--
//             "if (counter > 0) then " +  // 还可重入
//                 "redis.call('pexpire', KEYS[1], ARGV[2]); " +
//                 "return 0;" +
//             "else " +  // 完全释放
//                 "redis.call('del', KEYS[1]); " +
//                 "redis.call('publish', KEYS[2], ARGV[1]); " + // 发信号通知等待者
//                 "return 1;" +
//             "end; " +
//             "return nil;",
//             Collections.<Object>singletonList(name),
//             threadId
//         );
//     }
// }
//
```

### 2.2 Watch Dog 自动续期

```java
// ★ Redisson Watch Dog（看门狗自动续期，面试最高频！）
//
// 核心问题：如果客户端持锁时间超过 leaseTime → 锁自动过期 → 其他客户端可能拿到锁
//
// Watch Dog 解决：Redisson 客户端启动定时任务（默认每 10s = 30s/3）
// 不断刷新锁的过期时间 → 只要客户端活着锁就不会过期
//
// public class RedissonReentrantLock {
//     // ★ Watch Dog 续期任务
//     private Thread startWatcher() {
//         Thread watcherThread = new Thread(() -> {
//             while (!Thread.currentThread().isInterrupted()) {
//                 try {
//                     Thread.sleep(internalLockLeaseTime);  // 10s
//
//                     // ★ 检查锁是不是自己持有的
//                     if (isHeldByCurrentThread()) {
//                         // ★ 刷新过期时间到 30s
//                         asyncRemainTimeToLive();
//                     } else {
//                         break;  // 锁已释放 → Watch Dog 停止
//                     }
//                 } catch (InterruptedException e) {
//                     break;
//                 }
//             }
//         });
//         watcherThread.setName("redisson-watchdog-" + name);
//         watcherThread.setDaemon(true);  // ★ 守护线程，JVM 关闭自动停
//         watcherThread.start();
//         return watcherThread;
//     }
// }
//
// ★ 面试高频：为什么 Watch Dog 间隔是 leaseTime/3？
//   leaseTime=30s → 间隔=10s → 3 次续期内必须锁释放
//   如果 Watch Dog 也挂了 → 30s 后锁过期 → 自动释放
//   → 容错：Watch Dog 挂了锁也能自动释放
//
```

### 2.3 RedLock（多节点分布式锁）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RedLock = Redis 作者 antirez 提出的多节点方案             │
│                                                               │
│  背景：单 Redis 主从切换可能丢锁（Master 刚写锁还没同步到从   │
│        就挂了 → 从升主 → 锁没了 → 两个客户端以为自己持锁！）│
│
│  RedLock 核心：                                                │
│   ① 至少 5 个独立 Redis 集群（不要互为主从）                  │
│   ② 客户端向所有 5 个集群加锁（SETNX + NX + EX）              │
│   ③ 如果 **N/2+1 个**（比如 5 个里 3 个以上）加锁成功       │
│   ④ 并且 **总耗时 < 锁有效期**                                │
│   → 加锁成功！                                                 │
│
│ ★ RedLock 争议（Martin Kleppmann 博客）：                     │
│   Martin 认为 RedLock 还是不能解决 GC 暂停 + 网络分区问题    │
│   → 分布式锁 100% 可靠需要 CP 系统（ZK/Etcd）                │
│   → RedLock 是"足够好"的方案（生产够用但理论上有漏洞）        │
│
│ ★ 面试高频：Redisson vs RedLock？                             │
│   Redisson：单 Redis + Watch Dog（生产最常用，够用）         │
│   RedLock：多 Redis（理论更安全但实现复杂）                  │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、Zookeeper 分布式锁

### 3.1 临时顺序节点实现

```java
// ★ Zookeeper 分布式锁源码级（临时顺序节点）
//
// 核心：利用 ZK 的 临时顺序节点 + Watch 机制
//
// public class ZkDistributedLock {
//     private final String lockRoot = "/distributed_lock";
//
//     // ★ 获取锁流程
//     public void lock() {
//         // ① 创建临时顺序节点
//         String myPath = zk.create(
//             lockRoot + "/lock-",
//             data,
//             Ids.OPEN_ACL_UNSAFE,
//             CreateMode.EPHEMERAL_SEQUENTIAL  // ★ 临时顺序节点
//         );
//
//         // ② 获取所有子节点，按顺序排序
//         List<String> children = zk.getChildren(lockRoot, false);
//         Collections.sort(children);
//
//         // ③ 判断自己是不是第一个节点
//         String myNodeName = myPath.substring(myPath.lastIndexOf("/") + 1);
//         int myIndex = children.indexOf(myNodeName);
//
//         if (myIndex == 0) {
//             // ✅ 第一个 → 拿到锁
//             return;
//         } else {
//             // ④ 不是第一个 → Watch 前一个节点
//             String prevNode = lockRoot + "/" + children.get(myIndex - 1);
//             zk.getData(prevNode, new Watcher() {
//                 @Override
//                 public void process(WatchedEvent event) {
//                     // 前一个节点被删除（持锁释放）→ 唤醒
//                     if (event.getType() == Event.EventType.NodeDeleted) {
//                         synchronized (lock) {
//                             lock.notify();
//                         }
//                     }
//                 }
//             }, null);
//
//             // ⑤ 等待前一个节点释放
//             synchronized (lock) {
//                 while (!isFirst()) {
//                     lock.wait();  // 阻塞等待 Watch 通知
//                 }
//             }
//         }
//     }
//
//     // ★ 解锁：删除自己的临时节点
//     public void unlock() {
//         zk.delete(myPath, -1);
//         // ZK Watch 会通知下一个等待者
//     }
// }
//
// ★ ZK 锁的优缺点（面试高频）：
//   ✅ 优点：CP 强一致 + Watch 通知唤醒（自旋浪费 CPU）
//   ❌ 缺点：性能差（每次锁变更都要 ZK 同步）+ 临时节点 Session 过期问题
//
```

---

## 四、Etcd 分布式锁

### 4.1 Lease + Key 实现

```go
// ★ Etcd 分布式锁源码级（Lease + Key，Go）
//
// // concurrency/concurrency.go
//
// func (m *Mutex) Lock(ctx context.Context) error {
//     // ① 创建 Lease（租约，默认 10s）
//     lease, err := m.conn.Grant(ctx, ttl)
//     if err != nil {
//         return err
//     }
//     m.myKey = fmt.Sprintf("%s%x", m.pfx, lease.ID)
//
//     // ② Try PutIfNotExists（租约绑定）
//     txnResp, err := m.conn.Txn(ctx).
//         If(compare.Version(m.key).Equal(0)).  // key 不存在
//         Then(ops.OpPut(m.key, m.myKey, ops.WithLease(lease.ID))).
//         Commit()
//     if err != nil {
//         return err
//     }
//
//     if txnResp.Succeeded {
//         // ✅ 拿到锁
//         return nil
//     }
//
//     // ③ 没拿到 → Watch 锁 key + 等释放
//     for {
//         rch := m.conn.Watch(ctx, m.key)
//         for wr := range rch {
//             for _, ev := range wr.Events {
//                 if ev.Type == etcd.Delete {
//                     // 锁被删除 → 再次 Try
//                     return m.Lock(ctx)
//                 }
//             }
//         }
//     }
// }
//
// ★ Etcd vs ZK vs Redisson 锁核心差异（面试最高频！）：
//   ┌──────────┬──────────────┬──────────────┬──────────────┐   │
//   │ 维度       │ Redisson      │ Zookeeper     │ Etcd          │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ CP/AP      │ AP（主从）    │ CP（ZAB）      │ CP（Raft）    │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 锁实现      │ Hash + Watch  │ 临时顺序节点    │ Lease + Key   │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 自动续期    │ Watch Dog    │ Session 心跳   │ KeepAlive     │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 阻塞方式    │ 等待 Redis Pub│ 等待 ZK Watch  │ 等待 Etcd Watch│   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 性能        │ ⭐⭐⭐⭐⭐      │ ⭐⭐          │ ⭐⭐⭐        │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 生产场景    │ 秒杀/高并发   │ Dubbo 注册中心 │ K8s/TiDB      │   │
//   ├──────────┼──────────────┼──────────────┼──────────────┤   │
//   │ 脑裂风险    │ 有（AP）      │ 无（CP）       │ 无（CP）       │   │
//   └──────────┴──────────────┴──────────────┴──────────────┘   │
//
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 分布式锁三大方案？| **Redisson（Redis）+ Zookeeper（临时顺序节点）+ Etcd（Lease）** |
| 2 | Redisson 锁核心？| **Lua 原子脚本 + Watch Dog 自动续期（30s/10s）** |
| 3 | Redisson Lua 核心逻辑？| ① Hash 存 owner→count（可重入）② setnx + pexpire ③ unlock count-- 到 0 才 del |
| 4 | Watch Dog 续期原理？| Daemon 线程每 10s 刷新过期时间；客户端挂了线程停 → 30s 后锁自动释放 |
| 5 | Zookeeper 锁核心？| **临时顺序节点 + Watch 通知**；EPHEMERAL_SEQUENTIAL；Session 过期锁自动删除 |
| 6 | ZK 锁怎么判断拿没拿到？| 创建临时顺序节点 → 取所有子节点排序 → 自己是第一个就拿到 |
| 7 | Etcd 锁核心？| **Lease（租约）+ PutIfNotExists（key 不存在才 Put）+ Watch 等释放** |
| 8 | 三种方案 CP/AP？| **Redisson 是 AP（Redis 主从可能丢锁）；ZK 和 Etcd 是 CP（不会丢锁）** |
| 9 | RedLock 是什么？| Redis 作者提出的多节点方案；N/2+1 节点加锁成功 + 总耗时 < 有效期 = 成功 |
| 10 | Martin Kleppmann 争议？| RedLock 还是不能解决 GC 暂停 + 网络分区；100% 可靠要用 CP |
| 11 | 面试最高频考点排序？| **1. Redisson Lua 脚本 2. Watch Dog 3. ZK 临时顺序节点 4. RedLock 争议 5. 三种方案对比** |
| 12 | 学习路径？| Redisson eval Lua → Watch Dog 线程 → ZK EPHEMERAL_SEQUENTIAL → Etcd Lease → RedLock |
| 13 | 面试常见追问？| 锁过期了业务还没做完怎么办？→ Watch Dog 自动续期；为什么 AP 有脑裂？→ Redis 主从切换时可能丢锁 |
| 14 | Redisson 公平锁？| FairLock：用 List 队列记录等待者；按顺序发放 |
| 15 | Redisson 读写锁？| RReadWriteLock：读写分离；Read Lock 可以多个同时持有 |
| 16 | ZK Session 过期？| Session 超时（默认 30s）→ 所有临时节点自动删除 → 锁释放 |
| 17 | Etcd Lease KeepAlive？| 和 ZK Session 类似；自动续期；客户端挂了 Lease 过期 |
| 18 | 生产选型建议？| **高并发秒杀用 Redisson**（吞吐优先）；**一致性要求严格用 Etcd/ZK** |
| 19 | Redis 主从丢锁场景？| Master 写完锁还没同步到从 → 挂了 → 从升主 → 锁没了 → 两个客户端都以为自己持锁 |
| 20 | 2024 新方案？| **Chubby 开源了！Google Chubby 锁服务（但还在实验阶段）** |
| 21 | 面试高频场景？| 秒杀库存扣减 + 定时任务单例执行 + 跨服务互斥操作 |
| 22 | Redisson 锁 vs 自研 SETNX？| Redisson：Lua 原子 + Watch Dog + 可重入 + 公平/读写锁；自研：容易有各种 bug（过期、重入、续期）|

---

**文档版本：v1.0 · 2026-10-23**  
**面试考点：22 题**  
**差异化点**：Redisson Lua 完整脚本 + Watch Dog 续期原理 + ZK 临时顺序节点 + Etcd Lease + RedLock 争议
