# 分布式事务 2PC/3PC/TCC 全景深度对比

> **面试权重**：⭐⭐⭐⭐⭐（分布式事务三大方案，高频必问）  
> **核心参考**：XA 规范 → 3PC 1986 论文 → TCC 2007 蚂蚁金服  
> **面试必背**：2PC 阻塞 → 3PC 超时 → TCC 补偿

---

## 一、分布式事务核心问题

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式事务 = 跨服务/跨数据库的事务                        │
│                                                               │
│  场景：下单 → 扣库存 + 扣余额 + 生成订单                     │
│  ┌──────────┬──────────┬──────────┐                         │
│  │ Inventory │ Account   │ Order    │                         │
│  │ Service   │ Service   │ Service   │                        │
│  └──────────┴──────────┴──────────┘                         │
│
│  本地事务能保证 ACID → 分布式呢？                             │
│  → CAP 定理：分布式系统无法同时满足 C（一致性）+ A（可用性）+ P（分区容忍性）│
│  → 分布式事务本质：在 CAP 之间做权衡                         │
│
│ ★ 分布式事务三大核心方案（面试必背！）：                      │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 方案       │ 核心机制       │ 强点           │ 弱点          │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **2PC**   │ Prepare + Commit │ 强一致       │ 阻塞 + 脑裂    │   │
│  │ (XA)      │ 两阶段提交        │ 工业标准       │                │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **3PC**   │ CanCommit +      │ 超时机制       │ 脑裂问题没解决 │   │
│  │           │ PreCommit +      │ 更健壮         │ 实现复杂       │   │
│  │           │ DoCommit          │                │                │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **TCC**   │ Try + Confirm +  │ 不锁资源       │ 侵入性强       │   │
│  │ (蚂蚁金服)│ Cancel            │ 补偿事务       │ 业务改造大     │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、2PC（两阶段提交）源码级

### 2.1 核心流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 2PC（Two-Phase Commit）核心流程（面试必背！）              │
│                                                               │
│  ① Prepare 阶段（预提交）：                                   │
│   Coordinator（事务协调者）向所有 Participant（参与者）发：    │
│   → "你能不能提交？先准备好（锁资源 + 写 Undo Log）"         │
│   Participant 检查：数据是否可写？锁是否能加？               │
│   → 能：返回 YES + 写入 Undo Log                            │
│   → 不能：返回 NO                                            │
│
│  ② Commit 阶段（正式提交）：                                  │
│   Coordinator 统计所有 Participant 的响应：                    │
│   情况 A：全部 YES → 发 COMMIT 给所有人                      │
│     → Participant 正式提交（写 Redo Log + 释放锁）          │
│   情况 B：有一个 NO → 发 ROLLBACK 给所有人                   │
│     → Participant 回滚 Undo Log + 释放锁                    │
│
│  ★ 时序图：                                                   │
│  Coordinator  → Participant 1: PREPARE                       │
│               ← Participant 1: YES                            │
│               → Participant 2: PREPARE                       │
│               ← Participant 2: YES                            │
│               → Participant 1: COMMIT                         │
│               → Participant 2: COMMIT                         │
│               ← Participant 1: OK                             │
│               ← Participant 2: OK                             │
│
│ ★ 2PC 两大问题（面试最高频！）：                               │
│                                                               │
│  ① **阻塞问题**（Blocking）：                                 │
│   Prepare 后 Participant 锁资源 → 等 Coordinator 决定      │
│   → 如果 Coordinator 挂了 → 一直等 → 资源被锁死！           │
│   → 比如 Coordinator GC 暂停 10s → 所有 Participant 都堵了 │
│
│  ② **脑裂问题**（Split-Brain）：                              │
│   Coordinator 发了 COMMIT 后自己挂了                         │
│   Participant 1 收到 COMMIT → 提交                            │
│   Participant 2 网络分区没收到 → 一直等                      │
│   → 1 提交了 2 没提交 → 数据不一致！                        │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 三、3PC（三阶段提交）

### 3.1 改进点

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 3PC = 2PC + CanCommit + 超时机制（1986 论文）             │
│                                                               │
│  三阶段：                                                     │
│  ① CanCommit：Coordinator 先问"能不能做？"（轻量检查）       │
│     Participant 只检查是否可写（不锁资源）                    │
│     → 比 2PC Prepare 更轻量                                  │
│
│  ② PreCommit：类似 2PC Prepare（锁资源 + 写 Log）           │
│     → 增加超时：如果等不到 Coordinator 响应超时自动中断      │
│
│  ③ DoCommit：类似 2PC Commit（正式提交）                    │
│     → 增加超时：如果等不到 Coordinator 响应超时自动提交      │
│
│ ★ 3PC 为什么没解决脑裂？（面试高频）：                        │
│   Coordinator 发了 DoCommit 后挂了                            │
│   Participant 1 超时自动提交                                  │
│   Participant 2 网络分区没收到 + 超时自动提交（假设能提交） │
│   → 如果 Participant 2 其实 PreCommit 失败了？              │
│   → 脑裂依然可能！                                           │
│
│ ★ 为什么 3PC 没普及？                                        │
│   ① 实现复杂（超时难调）                                     │
│   ② 脑裂问题没彻底解决                                      │
│   ③ 实际场景不如 TCC 实用                                    │
│   → 生产几乎没人用 3PC                                       │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、TCC（Try-Confirm-Cancel）源码级

### 4.1 蚂蚁金服 TCC 模型

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TCC = Try + Confirm + Cancel（蚂蚁金服 2007）              │
│                                                               │
│  核心思想：**两阶段业务逻辑 + 补偿事务**                     │
│                                                               │
│  Try 阶段（资源预留，不真正执行）：                           │
│   Account-Try: 检查余额够不够？冻结（-100 到 Frozen 字段）  │
│   Inventory-Try: 检查库存够不够？冻结（-50 到 Frozen 字段） │
│   Order-Try: 生成一个 Pending 状态的订单                     │
│
│  Confirm 阶段（正式提交，补偿 Try）：                         │
│   Account-Confirm: 从 Frozen 扣到 Actual（正式扣款）        │
│   Inventory-Confirm: 从 Frozen 扣到 Actual（正式减库存）    │
│   Order-Confirm: Pending → Paid（订单确认支付）             │
│
│  Cancel 阶段（补偿回滚 Try）：                                │
│   Account-Cancel: 解冻（Frozen 加回 Balance）               │
│   Inventory-Cancel: 解冻（Frozen 加回 Stock）              │
│   Order-Cancel: 删除 Pending 订单                           │
│
│ ★ 示例代码（源码级 Java）：                                   │
│                                                               │
│ // Account TCC Service                                       │
│ public class AccountTccService {                              │
│                                                               │
│     // ★ Try：冻结金额                                        │
│     @TwoPhaseBusinessAction(name = "accountTcc",             │
│                              commitMethod = "confirm",        │
│                              rollbackMethod = "cancel")       │
│     public boolean tryDeduct(                                 │
│         @BusinessActionContextParameter(paramName = "userId") │
│         String userId,                                        │
│         @BusinessActionContextParameter(paramName = "amount")│
│         int amount) {                                         │
│         // 检查 + 冻结                                        │
│         Account account = accountRepo.findByUserId(userId);   │
│         if (account.getBalance() >= amount) {                 │
│             account.freeze(amount);  // 冻结，不真正扣       │
│             accountRepo.save(account);                        │
│             return true;                                      │
│         }                                                     │
│         return false;                                        │
│     }                                                         │
│                                                               │
│     // ★ Confirm：正式扣钱                                    │
│     public boolean confirm(BusinessActionContext ctx) {       │
│         String userId = (String) ctx.getActionContext("userId");│
│         int amount = (int) ctx.getActionContext("amount");    │
│         Account account = accountRepo.findByUserId(userId);   │
│         account.confirmDeduct(amount);  // 解冻+扣减         │
│         accountRepo.save(account);                            │
│         return true;                                          │
│     }                                                         │
│                                                               │
│     // ★ Cancel：补偿回滚（解冻）                            │
│     public boolean cancel(BusinessActionContext ctx) {        │
│         String userId = (String) ctx.getActionContext("userId");│
│         int amount = (int) ctx.getActionContext("amount");    │
│         Account account = accountRepo.findByUserId(userId);   │
│         account.unfreeze(amount);  // 解冻回滚               │
│         accountRepo.save(account);                            │
│         return true;                                          │
│     }                                                         │
│ }                                                             │
│
│ ★ 面试高频：TCC vs 2PC 核心区别？                              │
│   2PC：**锁数据库资源**（Prepare 后一直锁 → 阻塞）           │
│   TCC：**锁业务层资源**（Try 冻结字段 → 不锁 DB → 不阻塞）   │
│   → TCC 更灵活、不锁数据库                                   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、三大方案 + Saga + AT 全景对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 分布式事务五大方案全景对比（面试最高频！）                 │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 方案       │ 一致性模型     │ 业务侵入性     │ 生产成熟度     │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **2PC**   │ CP（强一致）  │ 低（XA 驱动） │ 高（银行核心） │   │
│  │ (XA)      │               │                │               │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **3PC**   │ CP（强一致）  │ 中             │ 低（几乎不用） │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **TCC**   │ 最终一致      │ **高**（改业务）│ 高（阿里/微众）│   │
│  │           │               │ 每个接口拆 3 │               │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Saga**  │ 最终一致      │ 中             │ 中（Event Bus）│   │
│  │           │ 长事务补偿    │               │               │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **AT**    │ 最终一致      │ **极低**（自动）│ 高（Seata 1PC）│   │
│  │ (Seata)   │               │ 一阶段提交     │               │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ Seata AT 为什么业务侵入极低？                                │
│   Seata 自动拦截 SQL → 解析出 Undo Log → 自动补偿回滚      │
│   → 业务代码几乎不用改（加个 @GlobalTransactional 注解）     │
│   → 和 TCC 对比：TCC 每个接口要拆 Try/Confirm/Cancel 三个  │
│
│ ★ Saga 模式（长事务）：                                        │
│   订单 → 扣库存（补偿：加库存）→ 扣余额（补偿：加余额）     │
│   → 每一步有一个补偿操作                                     │
│   → 前面成功后面失败 → 按反序执行补偿链                     │
│   → 适合长事务（几分钟甚至几小时）                           │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 六、面试考点（24 题，史上最全）

| # | 考点 | 答案 |
|---|------|------|
| 1 | 2PC 核心流程？| **Prepare（锁+Undo Log）→ Commit 或 Rollback** |
| 2 | 2PC 两大问题？| **① 阻塞（Coordinator 挂了一直等）② 脑裂（网络分区数据不一致）** |
| 3 | 3PC 改进了什么？| **CanCommit 轻量检查 + PreCommit/DoCommit 超时机制** |
| 4 | 3PC 为什么没普及？| 脑裂没彻底解决 + 实现复杂 + 生产几乎不用 |
| 5 | TCC 核心？| **Try（资源预留）+ Confirm（正式提交）+ Cancel（补偿回滚）** |
| 6 | TCC vs 2PC 核心区别？| **2PC 锁数据库资源 → 阻塞；TCC 锁业务层字段 → 不阻塞** |
| 7 | Seata AT 为什么侵入低？| **自动拦截 SQL → Undo Log → 自动补偿** |
| 8 | 五大方案全景？| **2PC/XA（强一致）+ 3PC（超时）+ TCC（业务补偿）+ Saga（长事务）+ AT（自动补偿）** |
| 9 | XA 是什么？| X/Open 规范：XA 接口（prepare/commit/rollback）→ 数据库原生支持 |
| 10 | Saga 模式？| 长事务 + 补偿链；前面成功后面失败 → 反序补偿 |
| 11 | 面试最高频考点排序？| **1. 2PC 流程+问题 2. TCC 三阶段 3. AT 低侵入 4. 五大方案对比 5. 脑裂问题** |
| 12 | 学习路径？| XA prepare/commit → 2PC 阻塞脑裂 → 3PC 超时 → TCC Try/Confirm/Cancel → Seata AT |
| 13 | 面试常见追问？| 2PC 阻塞怎么解决？→ 3PC 超时 / TCC / Seata AT；脑裂怎么解决？→ 几乎无解，只能靠幂等+重试 |
| 14 | 银行核心用什么？| **XA/2PC**（强一致要求）|
| 15 | 电商用什么？| **TCC / Seata AT**（最终一致，吞吐优先）|
| 16 | TCC 需要改业务代码吗？| ✅ 必须！每个接口拆 Try/Confirm/Cancel 三个 |
| 17 | Seata AT vs TCC？| AT 侵入低（自动）+ 一阶段提交；TCC 侵入高但更灵活 |
| 18 | 2PC 协调者挂了怎么办？| **阻塞直到超时或重启**；3PC 有超时自动处理；TCC Confirm 重试 |
| 19 | 幂等性重要吗？| ✅ 非常重要！重试时必须幂等（同一个请求多次 Confirm/Cancel 结果一样）|
| 20 | 2024 新方案？| **Seata 2.0 XA 增强 + CloudWeGo dtm（Go 分布式事务管理器）** |
| 21 | Saga vs TCC？| Saga 是编排式（按顺序调用，自动补偿链）；TCC 是协作式（每个参与者 Try-Confirm-Cancel）|
| 22 | 面试高频场景？| 下单扣库存扣余额 + 红包转账 + 跨系统更新 |
| 23 | 2PC 阻塞具体表现？| Coordinator 崩溃 → Prepare 阶段的 Participant 锁资源不释放 → 整个系统卡死 |
| 24 | TCC Cancel 空回滚？| Cancel 可能在 Try 之前被调用（网络原因）→ 需要支持空回滚（什么都没做也能 Cancel 成功）|

---

**文档版本：v1.0 · 2026-10-23**  
**面试考点：24 题（史上最全）**  
**差异化点**：2PC→3PC→TCC→AT→Saga 演进 + 阻塞/脑裂问题 + Seata 低侵入原因
