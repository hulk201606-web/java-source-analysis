# Seata TCC + SAGA 分布式事务模式源码级深度解析

> **源码基线**：Seata 2.x（Java）  
> **核心位置**：seata-core/src/main/java + seata-tcc + seata-saga  
> **面试权重**：⭐⭐⭐⭐⭐（分布式事务必问，之前 121 仅覆盖 AT）

---

## 一、Seata 四种分布式事务模式全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Seata 四种模式 = 不同一致性强度的权衡（面试必背对比）     │
│                                                               │
│  已覆盖过 AT 模式（121）—— 一阶段提交 + 全局回滚               │
│  本文件覆盖 **TCC + SAGA + XA 对比**                          │
│                                                               │
│  ★ 一致性强度排序（面试高频）：                                │
│   XA（2PC）> AT（自动 undo）> TCC（Try-Confirm-Cancel）> SAGA（最终一致） │
│                                                               │
│  ★ 适用场景排序：                                              │
│   XA：数据库原生支持，强一致但锁长                            │
│   AT：大多数 Java + MySQL 场景（自动，不用改业务代码）      │
│   TCC：跨服务/数据库、需精细控制的复杂场景                    │
│   SAGA：长流程、业务补偿已实现、最终一致可接受                 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、TCC 模式源码级

### 2.1 TCC 三阶段状态机

```java
// ★ TCC 核心概念：Try - Confirm - Cancel（源码级）
//
// seata-core/src/main/java/.../tcc/TCCResource.java
// seata-tcc/.../TccTransactionManager.java
//
// // TCC Resource 接口（被注解 @TccService 标注的 Bean 实现）：
// public interface TCCResource {
//     // ① Try：预留资源（不是真扣减）
//     //    例：订单服务 tryCreateOrder → 写 ORDER_TRY 表（预留订单 ID）
//     //       库存服务 tryDeductStock    → 写 STOCK_TRY 表（预留库存）
//     boolean try(InvocationContext context, Object params);
//
//     // ② Confirm：确认预留（真正扣减）
//     //    例：confirmCreateOrder → 把 ORDER_TRY → ORDER（状态改确认）
//     //       confirmDeductStock → 把 STOCK_TRY → 扣减真实库存
//     boolean confirm(InvocationContext context);
//
//     // ③ Cancel：取消预留（释放资源）
//     //    例：cancelCreateOrder → 删 ORDER_TRY 记录
//     //       cancelDeductStock → 删 STOCK_TRY 记录，库存释放
//     boolean cancel(InvocationContext context);
//
//     String getResourceId();  // 全局唯一 Resource ID（类似 XA XID）
// }
//
// // ★ TCC 调用流程（源码级）：
// // seata-tcc/.../TccTransactionManager.begin()
// seata.beginGlobalTransaction();
//
// // Service A（订单服务）—— Try
// orderTccResource.try(ctx, orderParams);
// // Service B（库存服务）—— Try
// stockTccResource.try(ctx, deductParams);
//
// // 全部 Try 成功 → 全局 Commit → 所有 Confirm
// seata.commitGlobalTransaction();
// //    TCC Manager 并发调 orderTccResource.confirm(ctx)
// //    TCC Manager 并发调 stockTccResource.confirm(ctx)
//
// // 任一 Try 失败 → 全局 Rollback → 所有 Cancel
// seata.rollbackGlobalTransaction();
// //    TCC Manager 并发调 orderTccResource.cancel(ctx)
// //    TCC Manager 并发调 stockTccResource.cancel(ctx)
//
// // ★ TCC Resource 注册：
// // @TccService(beanName = "orderTccResource")
// // public class OrderTccResource implements TCCResource { ... }
// // Seata 启动时自动扫描 @TccService → 注册到 TCC Manager
//
```

### 2.2 TCC 经典三异常（面试必背）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TCC 经典三异常（生产高频踩坑，面试必背答案模板）           │
│                                                               │
│  ┌──────────────┬──────────────────────┬──────────────────┐ │
│  │ 异常类型       │ 产生场景              │ 解决方案          │ │
│  ├──────────────┼──────────────────────┼──────────────────┤ │
│  │ Suspension   │ Confirm/Cancel 被调用  │ 空处理           │ │
│  │ （悬挂）       │ 时 Try 还没被调用（对 │ （直接返回 true，  │ │
│  │                │ 端 Try 超时但不知道） │  因为已经释放/扣减了）│ │
│  │                │                      │ tryIdempotentMap │ │
│  │                │                      │ 记录）          │ │
│  ├──────────────┼──────────────────────┼──────────────────┤ │
│  │ Null-Comp.   │ Cancel 时 Try 还没    │ Cancel 做幂等    │ │
│  │ （空回滚）     │ 执行（请求没到 Try）  │ （如果没 Try 就   │ │
│  │                │ → Cancel 找不到 Try  │ 直接返回 true）  │ │
│  │                │ 记录                │ tryLogTable 记录  │ │
│  ├──────────────┼──────────────────────┼──────────────────┤ │
│  │ Idempotency  │ Confirm/Cancel 被重复  │ tryConfirmLog    │ │
│  │ （幂等）       │ 调用（网络超时重试）  │ tryCancelLog     │ │
│  │                │                      │ 记录已执行过 →  │ │
│  │                │                      │ 再调直接返回     │ │
│  └──────────────┴──────────────────────┴──────────────────┘ │
│                                                               │
│  ★ Suspension 源码级处理：                                    │
// TCC Manager 调 Confirm：
public boolean confirm(String xid, String resourceId) {
//     // 如果这个 Resource 没 Try 过 → 悬挂异常
//     if (!tryLogTable.exists(xid, resourceId)) {
//         // 空处理！
//         return true;  // 不报错，因为对方 Try 可能还在
//     }
//     // 正常 Confirm
//     return resource.confirm(buildContext(xid, resourceId));
// }
//
// ★ Null-Compensation 源码级处理：
public boolean cancel(String xid, String resourceId) {
//     if (!tryLogTable.exists(xid, resourceId)) {
//         // 空回滚直接返回 true（幂等设计）
//         return true;
//     }
//     return resource.cancel(buildContext(xid, resourceId));
// }
//
// ★ Idempotency 源码级处理：
// Confirm 第一次调用后：
//     confirmLogTable.put(xid, resourceId);  // 记录已 Confirm
// Confirm 再来一次：
//     if (confirmLogTable.exists(xid, resourceId)) return true;
//     // 已执行过 → 直接返回
//
// ★ 实现这三种幂等的统一方案（面试高频答案）：
//   创建 TCC Log 表：tcc_try_log / tcc_confirm_log / tcc_cancel_log
//   每个 Resource 实现 Confirm/Cancel 时先查 Log 表
//   Log 表和 Try/Confirm 业务操作在同一 DB 事务里（保证原子性）
//
```

---

## 三、SAGA 模式源码级

### 3.1 SAGA 两种编排模式

```java
// ★ SAGA 两种编排模式（源码级，面试必背）
//
// seata-saga/.../SagaTransactionManager.java
// seata-saga/.../SagaStateMachine.java
//
// // ① 编排模式（Orchestration / Centralized）—— Seata 默认
// //    一个中心协调器定义整个 SAGA 流程
// //    每个步骤定义：Action（正向）+ Compensate（补偿）
//
// // 流程图（Seata SAGA DSL / 注解）：
// @SagaStart
// public void createOrderAndPay(OrderInput input) {
//     // Step 1: 创建订单
//     order.create(input);
//     order.addCompensate(order::cancel);  // 补偿：取消订单
//
//     // Step 2: 扣减库存
//     stock.deduct(input.productId, input.count);
//     stock.addCompensate(stock::restore);  // 补偿：恢复库存
//
//     // Step 3: 扣减金额
//     account.debit(input.accountId, input.amount);
//     account.addCompensate(account::credit);  // 补偿：返还金额
// }
//
// // 执行流程：
// //   Step1 ok → Step2 ok → Step3 ok → SAGA 完成（成功）
// //   Step1 ok → Step2 FAIL → 调 Step1 的 Compensate → 回滚
// //   Step1 ok → Step2 ok → Step3 FAIL → 调 Step2 的 Compensate → 调 Step1 的 Compensate
// //   ★ 补偿顺序：**反向补偿**（和正向顺序相反）
//
// // ② 协同模式（Choreography / Decentralized）
// //    每个服务知道自己要补偿哪些前置步骤
// //    通过事件驱动（消息队列）
// //    Service A 成功 → 发事件 → Service B 消费并执行
// //    Service B 失败 → 发补偿事件 → Service A 执行补偿
//
// // ★ Seata SAGA 状态机（源码级）：
// seata-saga/.../SagaStateMachineImpl.java
// // 每个 Saga 有一个状态机：
// //    Running → OneStepForward → Running → ... → Success
// //    Running → OneStepFail → Running → Compensating → ... → Failed
// //
// // 状态机持久化到 Seata Server（MySQL）：
// //   saga_state_machine（状态机定义）
// //   saga_instance（当前运行实例）
// //   saga_step_history（每个 Step 的执行历史）
//
```

### 3.2 SAGA 与 TCC/AT 核心区别

```
┌─────────────────────────────────────────────────────────────┐
│  ★ SAGA 核心特性（面试高频，与 TCC/AT 对比）                  │
│                                                               │
│  SAGA 优点：                                                  │
│   ✅ 长流程支持（几分钟/几小时的业务流程）                     │
│   ✅ 最终一致（不强依赖瞬时一致性）                           │
│   ✅ 跨数据库/微服务（任何可调用的服务都能参与）               │
│   ✅ 无数据库锁（比 AT/TCC 锁持有时间短得多）                 │
│                                                               │
│  SAGA 缺点：                                                  │
│   ❌ 补偿业务逻辑复杂（每个正向操作要写反向补偿）             │
│   ❌ 中间状态暴露（补偿执行前数据是"半完成"的）               │
│   ❌ 无法隔离（一个 Saga 中其他事务能看到中间状态）           │
│   ❌ 补偿失败处理（T+1 人工介入或自动重试无限循环）          │
│                                                               │
│  ★ AT vs TCC vs SAGA 核心差异：                               │
│  ┌──────────┬────────────────────┬──────────────┐             │
│  │ AT        │ TCC                 │ SAGA          │             │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 代码侵入   │ ★ 无（自动生成）   │ ★ 高（三接口）│ ★ 中（补偿）│             │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 锁时间     │ 短（一阶段）      │ 中（Try 锁到  │ ★ 无锁    │             │
│  │            │                   │  Confirm/Cancel）│           │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 隔离级别   │ ★ 数据库锁（严格） │ Try 状态隔离  │ ❌ 无隔离  │             │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 回滚方式   │ Undo Log 自动     │ ★ 手动 Cancel │ ★ 手动补偿 │             │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 实现难度   │ ★ 低（自动）      │ ★ 中（三接口）│ ★ 高（补偿）│             │
│  ├──────────┼────────────────────┼──────────────┤             │
│  │ 长流程     │ ❌ 不适合         │ ❌ Try 锁时间 │ ★ 最适合 │             │
│  │            │                   │  有限          │             │
│  └──────────┴────────────────────┴──────────────┘             │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、四种模式全维度对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 面试最高频：Seata 四种模式 + XA 全维度对比表             │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐│
│  │ 维度      │ AT         │ TCC        │ SAGA       │ XA        ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 一致性     │ ★ 最终     │ ★ 最终     │ 最终       │ ★ 强一致  ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 隔离级别   │ 数据库锁    │ Try 隔离   │ ❌ 无      │ ★ 数据库锁 ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 代码侵入   │ ★ 无      │ ★ 高       │ ★ 中       │ ★ 中       ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 回滚机制   │ Undo Log   │ Cancel     │ 补偿       │ Rollback  ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 锁时间     │ 短（一阶段）│ 中（Try锁）│ ★ 无锁     │ 长（二阶段）││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 适用场景   │ Java+MySQL │ 跨服务复杂 │ 长流程     │ 同 DB 强一致││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 长流程     │ ❌ 不适合  │ ❌ Try 锁  │ ★ 最优     │ ❌ 不适合  ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 实现难度   │ ★ 低（自动）│ ★ 高（三接口）│ ★ 高（补偿）│ ★ 中       ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 跨库支持   │ ✅        │ ✅        │ ✅        │ ❌ 单库    ││
│  ├──────────┼──────────┼──────────┼──────────┼──────────┤│
│  │ 经典踩坑   │ 脏写/脏读 │ ★ 三异常  │ 补偿失败   │ 锁太长     ││
│  └──────────┴──────────┴──────────┴──────────┴──────────┘│
│                                                               │
│  ★ 选型建议（面试追问）：                                     │
│   普通 Java+MySQL → **AT 模式**（自动、无代码侵入）          │
│   跨服务需精细控制 → **TCC 模式**（Try-Confirm-Cancel 三接口）│
│   长流程（几分钟/小时）→ **SAGA 模式**（补偿最终一致）      │
│   同 DB 强一致 → **XA 模式**（但锁太长不推荐）                │
│   三者配合：AT 处理短事务 + SAGA 处理长事务 + TCC 做跨服务协调│
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（28 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Seata TCC 是什么？| **Try - Confirm - Cancel** 三阶段接口；预留资源 → 确认扣减/取消释放 |
| 2 | TCC 接口三方法？| **try（预留）+ confirm（确认）+ cancel（取消）** |
| 3 | TCC 调用时机？| Try 阶段并发调所有 Resource；**全部 Try 成功才 Commit → 调所有 Confirm**；任一 Try 失败 → Rollback → 调所有 Cancel |
| 4 | ★ TCC 经典三异常？| **Suspension（悬挂）+ Null-Compensation（空回滚）+ Idempotency（幂等）** |
| 5 | Suspension（悬挂）是什么？| Confirm/Cancel 被调用时 Try 还没被调用；**解决：空处理 + tryLogTable 判断** |
| 6 | Null-Compensation（空回滚）是什么？| Cancel 时 Try 还没执行；**解决：Cancel 做幂等（没 Try 就直接返回 true）** |
| 7 | Idempotency（幂等）是什么？| Confirm/Cancel 被重复调用（网络超时重试）；**解决：confirmLog/tryCancelLog 记录已执行** |
| 8 | TCC 三异常统一解决方案？| **TCC Log 表（try_log / confirm_log / cancel_log）+ 查 Log 判断 + 幂等返回**；Log 和业务操作同 DB 事务保证原子 |
| 9 | Seata SAGA 两种编排模式？| **Orchestration（集中式，Seata 默认）+ Choreography（协同式，消息驱动）** |
| 10 | SAGA 补偿顺序？| **反向补偿**（正向 Step1→Step2→Step3；失败补偿 Step3→Step2→Step1）|
| 11 | SAGA 状态机核心状态？| Running → OneStepForward → Running → Success；Running → OneStepFail → Compensating → Failed |
| 12 | AT vs TCC vs SAGA vs XA 核心区别？| 见第四节对比表（10 维度）|
| 13 | 面试最高频考点排序？| **1. TCC 三异常 2. 四种模式对比表 3. SAGA 两种编排 4. Try-Confirm-Cancel 状态机 5. 选型建议** |
| 14 | 学习路径？| TCCResource 接口 → TccTransactionManager → SAGA 注解 → SagaStateMachine → 四种模式对比 |
| 15 | 面试常见追问？| TCC 和本地事务什么关系？| Try/Confirm/Cancel 各自在本地 DB 事务里执行；Seata 协调全局顺序 |
| 16 | 面试常见追问？| SAGA 补偿失败怎么办？| Seata 默认无限重试补偿；最终人工介入；可用 SagaEndTask 做 T+1 告警 |
| 17 | 面试陷阱？| 说 TCC 是"两阶段提交"→ ❌ TCC 是**三阶段**（Try-Confirm-Cancel）；XA 是两阶段（Prepare-Commit）|
| 18 | 面试陷阱？| 说 SAGA 是"强一致事务"→ ❌ SAGA 是**最终一致**，中间状态可见，无隔离 |
| 19 | SAGA 和 AT 怎么配合？| **AT 处理长事务链中的每一小段（短事务）+ SAGA 编排整个长流程（补偿）** |
| 20 | Seata Server 存什么？| 全局事务状态（root_xid / branch_xid / lock / undo_log / saga_state_machine）|
| 21 | Suspension 为什么要空处理？| 悬挂时 Try 还没到 → Cancel 不能报错（对方 Try 可能正在执行，稍后会 Confirm/Cancel）|
| 22 | AT vs TCC vs SAGA 代码侵入？| AT 无（自动生成 Undo）；TCC 高（写三接口）；SAGA 中（写补偿）|
| 23 | TCC 的 Try 操作是不是真扣减？| **不是真扣减，是预留**；写一张 TRY 表（预留记录），Confirm 时才真扣减 |
| 24 | 典型生产用法？| 电商下单：Order TCC + Stock TCC + Account AT → SAGA 编排整个支付-发货长流程 |
| 25 | 跨服务 TCC Resource 怎么注册？| @TccService 注解 + Resource 自动注册到 Seata Server；RPC 框架（Dubbo/Feign）自动带 XID |
| 26 | Seata 与 ClickHouse 集成？| ClickHouse 可用 SAGA 模式（不支持 AT 的 Undo Log）；Seata Server + ClickHouse 存 Saga 状态 |
| 27 | AT 的脏写脏读问题？| AT 一阶段提交后全局锁释放；MVCC 读不会脏；但另一事务 Commit 同一行可能覆盖（需要全局锁更细）|
| 28 | XA vs AT 选择？| 单 DB 场景 XA 锁太长（Prepare→Commit 期间锁不释放）；分布式 Java 场景 **AT 首选** |

---

**文档版本：v1.0 · 2026-10-26**  
**源码位置**：seata-core/.../tcc/TCCResource.java + seata-saga/.../SagaStateMachine.java + SagaTransactionManager.java  
**面试考点：28 题**  
**差异化点**：TCC 三异常（Suspension/Null-Compensation/Idempotency）源码级处理、SAGA 两种编排模式、**AT vs TCC vs SAGA vs XA 四模式全维度对比表**、选型建议
