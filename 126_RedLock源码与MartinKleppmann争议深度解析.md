# 分布式锁 Redis RedLock 源码级 + Martin Kleppmann 争议深度解析

> **源码基线**：Redisson 3.27.x（Java）+ Redis 7.x（C）  
> **核心来源**：antirez《Distributed Locks with Redis》(2014) + Martin Kleppmann《How to do distributed locking》(2016)  
> **面试权重**：⭐⭐⭐⭐⭐（分布式锁理论最高频，争议题必考）

---

## 一、单 Redis 分布式锁的致命问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 单 Redis 主从锁的脑裂问题（RedLock 提出背景）              │
│                                                               │
│  经典实现：SET key value NX PX 30000                         │
│                                                               │
│  故障场景（面试必背！）：                                      │
│  ① Client A 在 Master 获取锁成功                              │
│  ② Master 还没把锁同步到 Slave（异步复制）                    │
│  ③ Master 宕机                                                 │
│  ④ Sentinel 把 Slave 提升为新 Master                         │
│  ⑤ Client B 在新 Master 上 SET NX → 也获取锁成功！           │
│  → ★ 两个客户端同时持有锁 → 脑裂！                           │
│
│  后果（举例）：                                                 │
│   两个客户端同时修改同一条数据 → 数据损坏                     │
│   两个节点同时执行定时任务 → 重复扣款                         │
│
│ ★ Redis 作者 antirez 的回应：RedLock 算法                    │
│   不用主从，改用多个**互相独立**的 Redis 节点                 │
│   → 多数派加锁成功才算成功                                   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、RedLock 算法源码级

### 2.1 算法五步流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ RedLock 算法五步（antirez 原文，面试必背！）               │
│                                                               │
│  前提：5 个互相独立的 Redis Master（无主从关系）              │
│                                                               │
│  ① 客户端记录当前时间 T1                                       │
│                                                               │
│  ② 依次向 5 个 Redis 请求加锁（SET NX PX，带唯一 value）      │
│     value = UUID（客户端唯一标识，用于安全释放）              │
│     每个请求设置很短的超时（比如几十 ms），失败立即跳过       │
│                                                               │
│  ③ 客户端记录加锁完成时间 T2                                   │
│     加锁总耗时 = T2 - T1                                       │
│                                                               │
│  ④ ★ 判断加锁成功的两个条件（缺一不可）：                     │
│     a. 从 **N/2+1 = 3 个**（5 个里至少 3 个）节点获取成功    │
│     b. 总耗时 < 锁的有效期（比如总耗时 < 30s 的大部分）      │
│     → 两个都满足：加锁成功！实际有效期 = 30s - (T2-T1)       │
│     → 否则：向所有节点发解锁请求                              │
│                                                               │
│  ⑤ 释放锁：向所有 5 个节点发 Lua 解锁（不管之前有没有加成功） │
│
│ ★ 为什么要求总耗时 < 有效期？                                  │
│   防止加锁过程中锁已经过期 → 以为拿到了其实锁早失效了         │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Redisson RedLock 源码

```java
// ★ Redisson RedLock 源码级
//
// // org/redisson/RedissonRedLock.java
//
// public class RedissonRedLock extends RedissonMultiLock {
//
//     public RedissonRedLock(RLock... locks) {
//         // 传入多个独立 Redis 节点的锁
//         super(locks);  // 比如 5 个 RLock
//     }
//
//     // ★ 重写：多数派判断（N/2+1）
//     @Override
//     protected int failLocksLimit() {
//         // 5 个节点允许失败 2 个（至少要 3 个成功）
//         return locks.size() - minLocksAmount(locks);
//     }
//
//     protected int minLocksAmount(List<RLock> locks) {
//         return locks.size() / 2 + 1;  // ★ N/2 + 1
//     }
// }
//
// // 父类：RedissonMultiLock
// public class RedissonMultiLock implements RLock {
//
//     public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) {
//         long newLeaseTime = unit.toMillis(leaseTime);
//         long time = System.currentTimeMillis();      // T1
//         long remainTime = unit.toMillis(waitTime);
//
//         int lockedCount = 0;
//         List<RLock> acquiredLocks = new ArrayList<>();
//
//         // ★ ② 依次向所有节点加锁
//         for (RLock lock : locks) {
//             try {
//                 if (lock.tryLock(0, newLeaseTime, TimeUnit.MILLISECONDS)) {
//                     lockedCount++;
//                     acquiredLocks.add(lock);
//                 }
//             } catch (Exception e) {
//                 // 单个节点失败 → 跳过，继续下一个
//             }
//         }
//
//         // ★ ④ 判断：多数派 + 时间检查
//         long elapsed = System.currentTimeMillis() - time;  // T2-T1
//         if (lockedCount >= minLocksAmount(locks)
//                 && elapsed <= remainTime) {
//             // ✅ 加锁成功
//             return true;
//         }
//
//         // ❌ 失败 → 释放已获取的锁
//         for (RLock lock : acquiredLocks) {
//             lock.unlock();
//         }
//         return false;
//     }
// }
//
// // ★ 安全释放锁的 Lua（只删自己的锁）
// // if redis.call("get", KEYS[1]) == ARGV[1] then
// //     return redis.call("del", KEYS[1])
// // else
// //     return 0
// // end
//
```

---

## 三、Martin Kleppmann 的批评（面试核心！）

### 3.1 两大致命问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Martin Kleppmann《How to do distributed locking》(2016)  │
│                                                               │
│  Kleppmann 的核心观点：                                       │
│  RedLock 依赖**系统时钟**和**进程不暂停**的假设               │
│  但这两个假设在真实系统中都不成立！                           │
│
│  ─────────────────────────────────────────────────────       │
│  问题 ①：GC 暂停 / Stop-The-World（面试必背！）              │
│  ─────────────────────────────────────────────────────       │
│                                                               │
│  ① Client A 获得 RedLock（30s 租约）                          │
│  ② Client A 发生 Full GC，暂停了 35s                          │
│  ③ GC 期间 30s 锁过期                                         │
│  ④ Client B 获得同一把锁                                      │
│  ⑤ Client A GC 恢复，以为自己还持锁 → 操作共享资源           │
│  → ★ A 和 B 又同时持锁！                                     │
│                                                               │
│  RedLock 的时钟检查解决不了：                                 │
│   GC 暂停时客户端根本没在运行，无法知道时间过去了多久         │
│   （即使检查 T2-T1，GC 可能发生在加锁之后的业务执行阶段）    │
│
│  ─────────────────────────────────────────────────────       │
│  问题 ②：时钟跳跃（Clock Jump）                               │
│  ─────────────────────────────────────────────────────       │
│  Redis 的 PX 30000 依赖本地时钟                              │
│  运维误操作 ntpdate 时钟回拨 / 机器时钟漂移                  │
│  → 锁提前过期或延迟过期                                       │
│  → RedLock 的时间计算不可靠                                   │
│
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Fencing Token（击剑令牌）方案

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Kleppmann 的正确方案：Fencing Token（面试必背！）         │
│                                                               │
│  核心思想：锁服务在每次授锁时发一个**单调递增的 token**       │
│  客户端操作资源时带上 token；资源端拒绝旧 token              │
│                                                               │
│  时序：                                                        │
│  ① Client A 获取锁，得到 token=33                             │
│  ② Client A GC 暂停，锁过期                                   │
│  ③ Client B 获取锁，得到 token=34                             │
│  ④ A 恢复，写存储 {data, token=33}                           │
│  ⑤ B 写存储 {data, token=34}                                 │
│                                                               │
│  ★ 存储端检查：                                               │
│   收到 A 的写（token=33）：当前已是 34 → 33 < 34 → ★拒绝！  │
│   收到 B 的写（token=34）：34 >= 34 → 接受                   │
│   → 即使 A 以为自己持锁，它的写也会被存储拒绝                │
│
│  前提：                                                        │
│   ① 锁服务能生成单调递增 token（ZK 的 zxid / Etcd revision） │
│   ② 存储资源必须检查 token（需要资源端配合改造）             │
│   → Redis RedLock 给不出有意义的单调 token                   │
│     （Redis 的随机 value 不是单调递增的）                    │
│
│  Zookeeper 天然支持：                                         │
│   创建 znode → zxid 全局单调递增 → 可作为 fencing token      │
│   + 临时节点 Session 心跳（不依赖过期时间，GC 恢复后 Session 失效）│
│
└─────────────────────────────────────────────────────────────┘
```

### 3.3 antirez 的反驳

```
┌─────────────────────────────────────────────────────────────┐
│  ★ antirez 的反驳（了解即可）                                 │
│                                                               │
│  ① GC 问题对所有锁都存在：ZK 也有 GC 暂停问题                 │
│     （ZK 客户端 GC 时 Session 也可能超时丢锁）                │
│     → 要解决必须靠 Fencing Token，而不是换锁实现             │
│                                                               │
│  ② RedLock 定位是"高效率的近似正确锁"                         │
│     对绝大多数业务（不需要强正确）已经足够                    │
│     强正确场景本来就该用 ZK/Etcd + Fencing                   │
│                                                               │
│  ③ 可以通过随机化 TTL、延迟重启等降低时钟问题概率             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、生产选型结论（面试标准答案）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式锁选型决策树（面试必背！）                            │
│                                                               │
│  Q: 业务能容忍极端情况下的锁失效吗？（概率极低）              │
│                                                               │
│  ├─ 能容忍（99% 业务：防重复提交、定时任务单例、限流）       │
│  │   → ★ Redisson 单实例锁（Watch Dog）就够了                │
│  │   → 简单、高性能、运维方便                                │
│  │   → RedLock 实际很少用（5 个独立 Redis 成本高）           │
│  │                                                             │
│  └─ 不能容忍（金融扣款、强一致互斥、库存绝对不能超卖）        │
│      → ★ Zookeeper / Etcd（CP 系统）                         │
│      → + Fencing Token（资源端校验单调递增版本）             │
│      → + 业务幂等兜底                                        │
│
│ ★ 核心认知（面试高分点）：                                    │
│   没有任何分布式锁能 100% 正确，除非：                        │
│   ① CP 协调服务（ZK/Etcd）+ ② Fencing Token + ③ 资源端校验   │
│   + ④ 业务幂等                                                │
│   → 锁解决"效率问题"（减少并发冲突）                         │
│   → 幂等 + token 才解决"正确性问题"                          │
│
│ ★ Redis / ZK / Etcd 锁对比（复习）：                          │
│  ┌──────────┬────────────┬────────────┬────────────┐        │
│  │            │ Redis RedLock│ Zookeeper   │ Etcd         │        │
│  ├──────────┼────────────┼────────────┼────────────┤        │
│  │ 一致性     │ AP（时钟依赖）│ CP（ZAB）   │ CP（Raft）   │        │
│  │ Fencing   │ ❌ 随机 value│ ✅ zxid     │ ✅ revision  │        │
│  │ GC 安全性  │ ❌           │ ⚠️ Session   │ ⚠️ Lease     │        │
│  │ 性能       │ ⭐⭐⭐⭐⭐    │ ⭐⭐         │ ⭐⭐⭐        │        │
│  └──────────┴────────────┴────────────┴────────────┘        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 单 Redis 锁脑裂？| Master 加锁成功但未同步 Slave → Master 宕机 → Slave 升主 → 另一客户端又拿到锁 |
| 2 | RedLock 五步？| **记 T1 → 5 节点加锁 → 记 T2 → N/2+1 成功且总耗时<有效期 → 全节点解锁** |
| 3 | RedLock 成功两条件？| **① ≥3/5 节点成功 ② 加锁总耗时 < 锁有效期** |
| 4 | Redisson RedLock 源码？| RedissonRedLock（minLocksAmount=N/2+1）+ RedissonMultiLock.tryLock |
| 5 | Kleppmann 批评 ①？| **GC 暂停**：持锁期间 Full GC 35s → 锁过期 → 别人拿到锁 → 恢复后双持锁 |
| 6 | Kleppmann 批评 ②？| **时钟跳跃**：Redis PX 依赖本地时钟，ntpdate 回拨/漂移导致锁提前/延迟过期 |
| 7 | Fencing Token？| 锁服务发**单调递增 token**；客户端带 token 操作；资源端拒绝旧 token |
| 8 | 为什么 RedLock 不能做 Fencing？| Redis 返回的 value 是随机 UUID，**不单调递增** |
| 9 | 谁天然支持 Fencing？| **ZK（zxid 单调递增）+ Etcd（revision 单调递增）** |
| 10 | antirez 反驳？| GC 问题 ZK 也有；RedLock 定位是高效率近似正确锁；强正确用 ZK+Fencing |
| 11 | 生产选型结论？| **容忍极端失效 → Redisson（99% 场景）；强正确 → ZK/Etcd + Fencing + 幂等** |
| 12 | 面试最高频考点排序？| **1. GC 暂停问题 2. Fencing Token 3. RedLock 五步 4. 选型结论 5. 时钟跳跃** |
| 13 | 学习路径？| SET NX PX 单锁 → 主从脑裂 → RedLock 五步 → Kleppmann 博客 → Fencing |
| 14 | 面试常见追问？| ZK 锁就绝对安全吗？→ 不，ZK 客户端 GC 也会 Session 超时，必须配 Fencing |
| 15 | RedLock 为什么用 5 个独立节点？| 独立 = 不互为主从，避免单点故障同时影响多数派 |
| 16 | 锁过期了业务没执行完？| Redisson Watch Dog 自动续期（10s/次，30s TTL）；但解决不了 GC 暂停期间续期也停的问题 |
| 17 | 业务幂等为什么重要？| 锁是效率手段；幂等（唯一请求 ID + 去重表）才是正确性最后兜底 |
| 18 | Fencing 存储端怎么实现？| 数据库表存 version 字段；UPDATE ... WHERE version < new_version；旧 token 影响 0 行 |
| 19 | RedLock 实际用得多吗？| **很少**；多数公司直接用单 Redis Redisson；强一致用 etcd/ZK |
| 20 | 面试高频场景？| 秒杀超卖（幂等+token 兜底）+ 定时任务单例（Redisson 够用）+ 金融互斥（etcd+Fencing）|
| 21 | 安全解锁 Lua？| get 比对自己的 UUID 才 del（防止误删别人的锁）|
| 22 | 面试陷阱？| 说用了 RedLock 就绝对安全 → ❌ Kleppmann 已证明有时钟/GC 漏洞 |

---

**文档版本：v1.0 · 2026-10-23**  
**核心论文**：antirez 2014 + Martin Kleppmann 2016  
**面试考点：22 题**  
**差异化点**：RedLock 五步源码 + GC/时钟两大批评 + Fencing Token + 生产选型决策树
