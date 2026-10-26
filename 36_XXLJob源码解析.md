# XXL-Job 分布式调度源码深度解析

> **源码基线**：XXL-Job 2.4.2（最新稳定）  
> **核心仓库**：https://github.com/xuxueli/xxl-job  
> **面试权重**：⭐⭐⭐⭐⭐（国内调度高频，Quartz 替代）

---

## 一、XXL-Job 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：分布式任务调度平台（比 Quartz 强在哪？）             │
│                                                               │
│  ┌───────────┬──────────────────────────────────────┐       │
│  │   特性     │  Quartz（单机）        XXL-Job（分布式） │       │
│  ├───────────┼──────────────────────────────────────┤       │
│  │ 分布式调度  │ ❌ 单机                ✅ 调度中心 + 执行器  │       │
│  │ 动态管理   │ ❌ API 有限             ✅ Web UI 可视化     │       │
│  │ 任务失败重试 │ ❌ 需自己实现          ✅ 内置重试 + 告警    │       │
│  │ 日志追踪   │ ❌ 文件日志             ✅ 调度中心集中存储    │       │
│  │ 分片任务   │ ❌                     ✅ 动态分片（广播/分片）│       │
│  │ 动态代码   │ ❌ 改代码要重新打包      ✅ GLUE(Java) 在线编写 │       │
│  └───────────┴──────────────────────────────────────┘       │
│                                                               │
│  两大角色：                                                    │
│  Admin（调度中心，独立 Web 应用）+ Executor（执行器，嵌入业务应用）│
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  调度核心流程                                                  │
│                                                               │
│  Admin（调度中心）：                                           │
│  ① 启动时加载所有 Job 配置 → 构建 ThreadPoolTaskScheduler       │
│  ② 扫描下 5s 内要触发的 Job                                    │
│  ③ 触发时间到 → 根据 JobConfig 获取 Executor 列表               │
│  ④ RPC 调用 Executor.run 接口（HTTP）                           │
│                                                               │
│  Executor（执行器）：                                          │
│  ① 启动时自动注册到 Admin（RegistryHelper）                     │
│  ② Admin 调用 run → 按 JobHandler 名称找本地 Handler            │
│  ③ JobThread 在线程池执行 Handler.execute                        │
│  ④ 执行完成回调 Admin 存储日志                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Admin 调度触发

```java
// ★ XxlJobTrigger（调度触发核心）
//
// Admin 端维护：
//   Map<Integer, XxlJobTrigger> triggerMap → 每个 Job 一个 Trigger
//   内部 ScheduledExecutorService（每个 Trigger 一个线程）
//
// 触发流程：
//   Trigger.run()
//     → CronUtils.getNextValidTime(time, cronExpression)
//     → 计算下一次触发时间
//     → 到时间了 → XxlJobTrigger.trigger()
//       ↓
//       // 查询 Job 配置（JobInfoDao）
//       JobInfo jobInfo = xxlJobDao.load(jobId);
//
//       // 获取 Executor 列表（RegistryHelper 查询注册中心）
//       List<XxlJobRegistry> registryList = ...;
//
//       // 选一个 Executor（轮询/随机/一致性哈希）
//       Executor executor = select(registryList, jobInfo.getExecutorStrategy());
//
//       // RPC 调用 Executor（HTTP，不是 Dubbo/Netty）
//       XxlJobClient.run(executor.getAddress(), jobParam, ...);
//
// ★ 注意：XXL-Job 用 HTTP 做调度中心和执行器之间的通信！
//   不是 Netty/Dubbo RPC → 简单但性能略低
```

### 2.2 Executor 执行（★★★★）

```java
// ★ XxlJobExecutor（执行器核心）
//
// Executor 启动流程：
//   1. XxlJobExecutor.start()
//      → RegistryHelper.registryToAdmin()
//        → POST Admin/registry → 注册自己的 address + appname
//        → Admin 存到 xxl_job_registry 表（MySQL）
//
//   2. 启动 JobThread 线程池
//      → 每个 Job 一个 JobThread（不是共享线程池！）
//      → JobThread 阻塞等待 Admin 的 run 请求
//
//   3. Admin 调用 run 接口
//      → XxlJobRunController.run()
//        → XxlJobExecutor.run(jobId, param, ...)
//          → 找到对应的 JobThread
//          → JobThread 在线程池执行
//
// ★ JobThread 执行逻辑：
//   // 伪代码
//   public class JobThread extends Thread {
//       public void run() {
//           while (true) {
//               try {
//                   // 等待 Admin 的触发通知（阻塞队列）
//                   JobTriggerParam triggerParam = triggerQueue.take();
//
//                   // 找到 Handler
//                   IJobHandler handler = JobHandlerFactory.getJobHandler(triggerParam.getHandler());
//
//                   // 执行
//                   handler.execute(triggerParam);
//
//                   // 回调 Admin 存储日志
//                   logService.log(triggerParam, RETURN_SUCCESS);
//               } catch (Exception e) {
//                   logService.log(triggerParam, RETURN_FAIL);
//               }
//           }
//       }
//   }
```

### 2.3 动态分片任务（★★★★）

```java
// ★ XXL-Job 分片任务（最有特色的功能！）
//
// 问题场景：有 100w 条订单要处理 → 单机跑太慢
// 方案：动态分片 → N 个 Executor 各处理一部分
//
// Handler 实现：
//   @XxlJob("orderJob")
//   public void orderJob() {
//       // 从上下文中获取分片信息
//       int shardIndex = XxlJobHelper.getShardIndex();  // 当前执行器的分片索引
//       int shardTotal = XxlJobHelper.getShardTotal();  // 总分片数
//
//       // 只处理属于自己分片的订单
//       // 方案 1：SQL WHERE id % shardTotal = shardIndex
//       List<Order> orders = orderDao.selectAll(shardIndex, shardTotal);
//
//       // 方案 2：按业务字段分片 WHERE hash(userId) % shardTotal = shardIndex
//       // ... 处理 orders ...
//   }
//
// Admin 端怎么分配？
//   触发分片任务时 → 把 Executor 列表编号 [0, 1, 2, ... N-1]
//   → 第一个 Executor 发 shardIndex=0, shardTotal=N
//   → 第二个发 shardIndex=1, shardTotal=N
//   → ... 依此类推
//
// ★ 和 Quartz 对比：
//   Quartz 没有分片！要自己在 Handler 里写分片逻辑
//   XXL-Job 内置分片 → Executor 数量动态变化也能自适应
```

### 2.4 GLUE(Java) 在线编写代码（★★★）

```java
// ★ GLUE 模式：在 Admin Web UI 上直接写 Java 代码 → 热加载
//
// 支持模式：
//   BEAN：@XxlJob 注解的 Handler（最常用）
//   GLUE(Java)：在线编写 → 管理员发布 → Executor 热加载 Class
//   GLUE(Groovy)：在线写 Groovy 脚本
//   GLUE(Shell)：Shell 脚本
//
// GLUE(Java) 原理：
//   1. Admin UI 上写 Java 代码（实现 IJobHandler）
//   2. Admin 把代码存到 xxl_job_logglue 表
//   3. Executor 启动时拉取最新代码
//   4. GlueJavaHandler → Javassist 动态编译 + ClassLoader 加载
//   5. 调用 handler.execute()
//
// ★ 适合场景：临时改逻辑不想重新打包部署
// ★ 缺点：代码安全风险（谁都能写 GLUE 代码！）
```

### 2.5 注册中心（RegistryHelper）

```java
// ★ XXL-Job 注册中心非常简单（和 Nacos/Eureka 复杂的注册中心不同）
//
// 实现：MySQL 数据库
//   表：xxl_job_registry
//   字段：registry_group + registry_key + registry_value + update_time
//
// Executor 注册：
//   RegistryHelper.registry()
//     → POST Admin/registry
//     → INSERT/UPDATE xxl_job_registry
//     → registry_value = "http://192.168.1.100:9999/"
//     → 每 30s 心跳一次（update_time 更新）
//
// 注销：
//   Executor 优雅关闭时 → POST Admin/registryRemove
//
// Admin 发现 Executor：
//   SELECT * FROM xxl_job_registry WHERE registry_group = ? AND registry_key = ?
//   AND update_time > NOW() - 30s（超时剔除）
//
// ★ 为什么不用 ZooKeeper/Nacos？
//   XXL-Job 设计者认为注册中心不复杂 → MySQL 足够了
//   → 不需要额外依赖 Nacos/ZK
//   → 简单但缺少 Nacos 那样的主动推（Admin 定时扫描）
```

---

## 三、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | XXL-Job 两大角色？| **Admin（调度中心，Web UI + 调度触发）+ Executor（执行器，嵌入业务应用）** |
| 2 | Admin 调度核心？| XxlJobTrigger 按 Cron 表达式触发 → RPC 调用 Executor → Executor 执行 Handler |
| 3 | Executor 怎么注册？| **RegistryHelper** → HTTP POST Admin/registry → MySQL xxl_job_registry 表 → 每 30s 心跳 |
| 4 | 动态分片任务？| Admin 把 Executor 列表编号 → shardIndex + shardTotal 传给每个 Executor → Handler 只处理自己的分片（WHERE id % shardTotal = shardIndex）|
| 5 | GLUE(Java) 模式？| **Admin UI 写 Java 代码 → Javassist 动态编译 + ClassLoader 热加载** → 适合临时改逻辑不想重新打包 |
| 6 | XXL-Job vs Quartz？| XXL-Job：**分布式 + Web UI + 动态分片 + 失败重试 + 日志集中**；Quartz：单机 + API + 无分片 |
| 7 | XXL-Job vs PowerJob？| 国内两个主流分布式调度框架；PowerJob：延迟任务 + 流式处理；XXL-Job：稳定性 + 社区成熟 |
| 8 | 任务失败重试？| 配置 **misfireStrategy（错过策略）**：忽略 / 立即执行一次 / 执行一次补偿 |
| 9 | 调度中心挂了怎么办？| Admin 支持集群（MySQL 共享任务配置）→ 多个 Admin 同时启动 → 只有一个调度（分布式锁抢占）|
| 10 | Executor 挂了怎么办？| Admin 定时扫描注册中心 → update_time 超过 30s 视为离线 → 不再调度到这个 Executor |
| 11 | 调度 RPC 协议？| **HTTP**（不是 Netty/Dubbo RPC）→ 简单但高并发下性能略低 |
| 12 | JobHandler 实现三种方式？| ① @XxlJob 注解方法 → ② 继承 IJobHandler Bean → ③ GLUE(Java/Groovy/Shell) 在线代码 |
| 13 | 任务阻塞超时？| jobTimeout 配置（默认 0 = 不限）→ 超时 Admin 中断 + 标记失败 |
| 14 | Executor 线程模型？| **每个 Job 一个独立 JobThread**（不是共享线程池）→ JobThread 阻塞队列等待 Admin 触发通知 |
| 15 | 注册中心为什么用 MySQL？| XXL-Job 设计者认为注册中心需求简单 → MySQL 足够 → 不需要额外依赖 Nacos/ZK |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：XXL-Job 2.4.2  
**面试考点：15 题**
