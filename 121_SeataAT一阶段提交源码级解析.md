# 分布式事务 Seata AT 一阶段提交源码级深度解析

> **源码基线**：Seata 2.0.0（Java）  
> **核心仓库**：apache/incubator-seata → core/ + client/ + server/  
> **面试权重**：⭐⭐⭐⭐⭐（Seata 核心优化，AT 模式一阶段提交高频必问）

---

## 一、Seata 四大模式总览

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Seata = 阿里开源分布式事务框架                          │
│                                                               │
│  Seata 四大模式（面试必背！）：                                │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 模式       │ 核心机制       │ 业务侵入性     │ 面试高频     │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **AT ★**  │ 自动 Undo Log  │ **极低**（加注解）│ ⭐⭐⭐⭐⭐      │   │
│  │           │ 一阶段提交     │                │              │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **TCC**   │ Try/Confirm/  │ **高**（改业务）│ ⭐⭐⭐⭐       │   │
│  │           │ Cancel        │                │              │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **Saga**  │ 长事务编排     │ 中             │ ⭐⭐⭐        │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ **XA**    │ 2PC 数据库驱动 │ 低（XA 协议）  │ ⭐⭐         │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ 为什么 AT 模式是生产首选？                                   │
│   业务侵入极低 → 几乎只需要加一个 @GlobalTransactional 注解   │
│   Seata 自动拦截 SQL → 生成 Undo Log → 自动补偿回滚          │
│   不需要像 TCC 那样改造业务代码                               │
│
│ ★ Seata 架构（面试高频）：                                     │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ TC（Transaction Coordinator）★ 核心！                  │   │
│  │   Seata Server：管理全局事务 + 分支事务 + 回滚日志     │   │
│  └───────────────────────────────────────────────────────┘   │
│         ↓ RPC                                                  │
│  ┌──────────┬──────────┐                                     │
│  │ TM          │ RM           │                                │
│  │ Transaction │ ResourceManager│                             │
│  │ Manager     │ 资源管理器（DB）│                              │
│  │ 开启/结束全局事务 │ 分支事务 + Undo Log │                    │
│  └──────────┴──────────┘                                     │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、AT 模式一阶段提交源码级

### 2.1 一阶段提交核心流程

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Seata AT 一阶段提交（面试核心！）                          │
│                                                               │
│  经典 2PC/XA：                                                 │
│   Prepare → 锁资源 → 等 Coordinator → Commit/Rollback        │
│   → **阻塞！Coordinator 挂了一直等**                         │
│
│  Seata AT 一阶段提交（革命性优化）：                            │
│   ① 拦截 SQL → 解析 Undo Log（before image + after image）  │
│   ② 执行 SQL + **立即提交到 DB**（不锁资源！）               │
│   ③ 把 Undo Log 写到 Seata Server                            │
│   ④ 立刻返回成功给业务层                                      │
│
│  ★ 核心创新点：                                                │
│   经典 2PC：Prepare 后不提交 → 等 Coordinator                │
│   Seata AT：一阶段就提交到 DB → 不阻塞业务                   │
│   → **性能提升 10x+！业务无感知！**                           │
│
│  ★ 示例流程：                                                   │
│                                                               │
│  业务代码（几乎不用改！）：                                    │
│   @GlobalTransactional                                        │
│   public void createOrder() {                                │
│       orderService.insert(...);   ← Seata 拦截               │
│       accountService.deduct(...); ← Seata 拦截               │
│   }                                                           │
│                                                               │
│  执行链路：                                                    │
│   ① TM 开启全局事务（XID=12345）                             │
│   ② RM1（orderService）：                                     │
│      Seata 拦截 INSERT SQL                                   │
│      → before image（空，新数据）+ after image（订单数据）     │
│      → 执行 INSERT → 立即 Commit（一阶段提交！）              │
│      → Undo Log 写入 TC                                      │
│      → Branch ID=101 返回给 TM                               │
│   ③ RM2（accountService）：同理                               │
│   ④ TM 通知 TC：全局事务 Commit（二阶段通知）                 │
│   ⑤ TC 异步删除 Undo Log（已提交，不需要回滚了）             │
│
│ ★ 面试高频：一阶段就提交怎么保证一致性？                       │
│   正常情况：一阶段提交 → 全局 Commit → Undo Log 删除         │
│   异常情况：某个 RM 失败 → 全局 Rollback → TC 根据 Undo Log 补偿│
│   → 最终一致（业务层看到的是立即提交的效果）                 │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Undo Log 源码级

```java
// ★ Seata AT 模式 Undo Log 源码级
//
// // client/rm/datasource/undo/UndoLogManager.java
//
// public class UndoLogManager {
//
//     // ★ 拦截 UPDATE/DELETE/INSERT 生成 Undo Log
//     public UndoLogResult generateUndoLog(
//         Connection connection,
//         String sql,
//         Object[] params
//     ) {
//         // ① 解析 SQL → 拿到 WHERE 条件
//         WhereCondition where = parseWhere(sql, params);
//
//         // ② 查询 Before Image（变更前的数据）
//         String selectBeforeSql = buildSelectSql(sql, where);
//         List<Row> beforeImage = executeQuery(connection, selectBeforeSql);
//         // 比如 UPDATE user SET age=30 WHERE id=1
//         // Before Image: [{id=1, name="Alice", age=25}]
//
//         // ③ 执行原始 SQL（一阶段提交，立即 Commit）
//         executeUpdate(connection, sql, params);
//
//         // ④ 查询 After Image（变更后的数据）
//         List<Row> afterImage = executeQuery(connection, selectBeforeSql);
//         // After Image: [{id=1, name="Alice", age=30}]
//
//         // ⑤ 生成 Undo Log（保存 Before + After Image）
//         return UndoLogResult.builder()
//             .beforeImage(beforeImage)
//             .afterImage(afterImage)
//             .sqlType(SqlType.UPDATE)
//             .tableName("user")
//             .build();
//     }
//
//     // ★ Undo Log 写入 TC（RPC）
//     public void writeUndoLog(String xid, long branchId, UndoLogResult undoLog) {
//         // 发送给 Seata Server（TC）
//         tcRpcService.reportUndoLog(xid, branchId, undoLog);
//     }
// }
//
```

---

## 三、Seata 全局回滚源码级

```java
// ★ Seata AT 模式全局回滚源码级
//
// // server/undo/UndoLogManager（TC 端）
//
// public class ServerUndoLogManager {
//
//     // ★ TC 收到 Rollback 请求 → 通知所有 RM 回滚分支事务
//     public void rollbackGlobalTransaction(String xid) {
//         GlobalSession globalSession = getGlobalSession(xid);
//
//         for (BranchSession branch : globalSession.getBranches()) {
//             // ★ 逐个通知 RM 回滚
//             try {
//                 rmRpcService.branchRollback(xid, branch.getBranchId());
//             } catch (Exception e) {
//                 // RM 可能挂了 → 定时重试
//                 retryLater(branch);
//             }
//         }
//     }
// }
//
// // ★ RM 端收到 Rollback → 根据 Undo Log 补偿回滚
// // client/rm/datasource/undo/UndoLogExecutor.java
//
// public class UndoLogExecutor {
//
//     public void rollbackBranch(String xid, long branchId) {
//         // ① 从 TC 拉 Undo Log（Before Image）
//         UndoLog undoLog = tcRpcService.getUndoLog(xid, branchId);
//
//         // ② 生成反向 SQL（根据 Before Image + After Image）
//         // 比如：UPDATE user SET age=25 WHERE id=1（用 Before Image 回滚）
//         String rollbackSql = generateRollbackSql(undoLog);
//
//         // ③ 执行补偿 SQL
//         try {
//             executeRollback(connection, rollbackSql);
//         } catch (LockConflictException e) {
//             // ★ 脏写！有其他事务改过了 → 必须人工干预
//             // Seata 检测到数据不一致 → 回滚失败 → 告警
//             throw new UndoLogException("脏写检测！需要人工处理", e);
//         }
//     }
// }
//
// ★ 面试高频：脏写检测（面试必背！）：
//   在补偿回滚时，如果 After Image 和当前 DB 数据不一样
//   → 说明有其他事务改过了（脏写）
//   → Seata 拒绝回滚（避免把别人的修改也回滚掉）
//   → 需要人工介入处理
//
```

---

## 四、Seata AT vs 2PC/XA vs TCC 源码级对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Seata AT vs 2PC/XA vs TCC 核心对比（面试最高频！）         │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │            │ Seata AT       │ 2PC/XA         │ TCC          │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 提交时机    │ **一阶段就提交** │ 等 Coordinator │ Try 锁定 +      │   │
│  │            │ 不阻塞         │ 再 Commit      │ Confirm 提交   │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 阻塞问题    │ ✅ 完全无阻塞  │ ❌ Prepare 后阻塞 │ ❌ Try 阶段锁 │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 一致性模型  │ 最终一致       │ 强一致          │ 最终一致        │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 业务侵入性  │ **极低**（注解）│ 低（XA 协议）   │ **高**（改业务）│   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 脏写问题    │ ⚠️ 有（检测到失败）│ 无（强一致）    │ 无（业务控制）   │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 性能对比    │ ⭐⭐⭐⭐⭐      │ ⭐⭐            │ ⭐⭐⭐          │   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ Seata AT 核心创新（面试必背！）：                            │
│   经典 2PC：Prepare（锁资源，不提交）→ Commit（释放锁）      │
│   Seata AT：一阶段就 **真正 Commit 到 DB** → 不锁资源        │
│   → 性能提升 10x+                                           │
│   → 业务无感知（只用 @GlobalTransactional）                  │
│   → 这就是 Seata AT 成为生产首选的原因！                     │
│
│ ★ Seata AT 一阶段提交流程图：                                  │
│   ┌──────────────┐                                            │
│   │ 业务代码       │                                            │
│   │ @GlobalTx     │                                            │
│   └──────┬───────┘                                            │
│          ↓                                                    │
│   ┌──────────────┐                                            │
│   │ TM（开启全局事务）│ XID=12345                               │
│   └──────┬───────┘                                            │
│          ↓                                                    │
│   ┌────────────────────────┐                                  │
│   │ RM（拦截 SQL）           │                                  │
│   │ ① 生成 Undo Log          │                                  │
│   │ ② 执行 SQL               │                                  │
│   │ ③ ★ 一阶段 Commit 到 DB ★│ ← 核心！                     │
│   │ ④ Undo Log 写 TC         │                                  │
│   └────────────────────────┘                                  │
│          ↓                                                    │
│   全部成功 → TC 通知全局 Commit → Undo Log 异步删除           │
│   有失败  → TC 通知全局 Rollback → 根据 Undo Log 补偿回滚      │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（22 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Seata 四大模式？| **AT（自动）+ TCC（业务层）+ Saga（长事务）+ XA（2PC）** |
| 2 | AT 模式核心？| **拦截 SQL → 生成 Undo Log → 一阶段就 Commit 到 DB → 不阻塞** |
| 3 | 一阶段提交创新？| 经典 2PC Prepare 后不提交；Seata AT **一阶段就真正 Commit** → 不锁资源 |
| 4 | Undo Log 生成？| Before Image（变更前）+ After Image（变更后）+ SQL 类型 + 表名 |
| 5 | 全局回滚？| TC 通知所有 RM → RM 根据 Undo Log 生成反向 SQL → 执行补偿 |
| 6 | 脏写检测？| 补偿回滚时 After Image 和当前 DB 数据不一致 → 脏写 → 回滚失败 → 人工处理 |
| 7 | TM/RM/TC 角色？| **TM 开启/结束全局事务；RM 管理分支事务；TC 管理全局/分支 + 回滚日志** |
| 8 | Seata vs 2PC/XA？| Seata：一阶段提交 + 无阻塞 + 业务侵入极低；2PC：Prepare 阻塞 + 强一致 |
| 9 | Seata vs TCC？| Seata：自动（拦截 SQL）+ 极低侵入；TCC：手动（Try/Confirm/Cancel）+ 高侵入 |
| 10 | 面试最高频考点排序？| **1. AT 一阶段提交 2. Undo Log 3. 脏写检测 4. TM/RM/TC 5. Seata vs 2PC/TCC** |
| 11 | 学习路径？| @GlobalTransactional → TM → RM → UndoLogManager → TC rollback |
| 12 | 面试常见追问？| 一阶段就提交怎么保证回滚？→ 异常时 TC 根据 Undo Log 补偿回滚；脏写怎么办？→ 需要人工 |
| 13 | Seata AT 对业务侵入？| **极低！加 @GlobalTransactional 注解即可**；SQL 自动被拦截 |
| 14 | Seata AT 性能？| 比 2PC/XA 快 10x+（因为不锁资源 + 不阻塞）|
| 15 | Seata TC 存储？| 支持 MySQL / Redis / 其他 |
| 16 | Seata 2.0 新特性？| **XID 生成优化 + AT 模式增强 + Saga 增强** |
| 17 | 面试高频场景？| 电商下单（扣库存+扣余额+生成订单）+ 红包转账 + 跨服务数据更新 |
| 18 | Seata AT 源码路径？| client/rm/datasource/undo/UndoLogManager.java + server/undo/ |
| 19 | Undo Log 何时删除？| 全局 Commit 后异步删除；全局 Rollback 后补偿完删除 |
| 20 | Seata AT 唯一缺点？| **脏写检测到后回滚失败**（需要人工处理）；某些场景可能出现 |
| 21 | Seata TCC 业务改造？| 每个接口拆 Try（预留）/Confirm（提交）/Cancel（回滚）三个 |
| 22 | 面试陷阱？| 说 Seata AT 是 2PC → ❌ Seata AT 是一阶段提交（核心创新就是不等 Coordinator）|

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：client/rm/datasource/undo/UndoLogManager.java + server/undo/  
**面试考点：22 题**  
**差异化点**：AT 一阶段提交源码 + Undo Log Before/After Image + 脏写检测 + Seata vs 2PC/TCC
