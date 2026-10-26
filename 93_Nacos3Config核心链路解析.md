# Nacos 3.x Config 核心链路源码级解析（本地 3.2.4）

> **源码基线**：Nacos 3.2.4（本地：`d:\GC_WorkSpaces\nacos-3.2.4`）  
> **核心模块**：`config/src/main/java/.../controller/v3/ConfigControllerV3.java`  
> **面试权重**：⭐⭐⭐⭐（Config 核心链路 + JRaft 写 MySQL）

---

## 一、Nacos Config 源码结构（本地可验证）

```
d:\GC_WorkSpaces\nacos-3.2.4\config\src\main\java\com\alibaba\nacos\config\server\
├── controller\v3\
│   ├── ConfigControllerV3.java      ★ L119 核心入口
│   ├── ConfigOpenApiController.java
│   ├── ConfigOpsControllerV3.java
│   ├── ListenerControllerV3.java
│   ├── HistoryControllerV3.java
│   └── MetricsControllerV3.java
├── service\
│   ├── ConfigOperationService.java  ★ 发布/删除核心服务
│   ├── ConfigDetailService.java
│   ├── ConfigInfoPersistService.java  ★ MySQL 持久化 + JRaft
│   └── listener\
│       └── ConfigListenerManager.java ★ 推送变更给 Client
└── repository\
    └── ConfigInfoPersistService
        → JRaft + MySQL（CP 模式）
        → 或 Derby + DB（单机/AP 模式）
```

---

## 二、ConfigControllerV3 核心链路（本地锚点）

### 2.1 publishConfig 完整链路

```java
// ★ 锚点：d:\GC_WorkSpaces\nacos-3.2.4\config\...\controller\v3\ConfigControllerV3.java:L119
//
// ConfigControllerV3 核心依赖（L129-141）：
// private final ConfigOperationService configOperationService;        // L129
// private final ConfigInfoPersistService configInfoPersistService;   // L131
// private final ConfigDetailService configDetailService;             // L133
// private final ConfigListenerStateDelegate configListenerStateDelegate; // L141
//
// ★ publishConfig 完整链路（源码级）：
//
// ① HTTP/gRPC 入口
//    POST /nacos/v3/cs/config（Admin API）
//    → ConfigControllerV3.publishConfig(@RequestBody ConfigFormV3)
//    → 参数验证：dataId, groupName, content, namespaceId
//
// ② ConfigOperationService.publishConfig()
//    // L129 注入的 ConfigOperationService
//    // 内部调用：
//    boolean isNew = configInfoPersistService.saveConfigInfo(configInfo);
//    // ↓ 走 JRaft + MySQL（CP 模式）
//
// ③ ConfigInfoPersistService.saveConfigInfo()
//    // ★ 核心（面试高频）：走 JRaft 写 MySQL
//    //   如果是 CP 模式（JRaft）：
//    //   → 包装成 WriteRequest（Protobuf）
//    //   → JRaftServer.write("config", writeRequest)
//    //   → NacosStateMachine.onApply()
//    //   → ConfigRequestProcessor.onApply()
//    //   → INSERT INTO config_info (data_id, group_id, content, ...)
//    //   → Raft 过半确认 → 返回成功
//    //
//    //   如果是 AP 模式（Derby/MySQL 直写）：
//    //   → 直接 INSERT INTO config_info
//    //   → Distro Protocol 异步广播给集群其他节点
//
// ④ 发布变更事件
//    → ConfigDataChangeEvent（Spring Event）
//    → ConfigListenerManager 收到事件
//    → gRPC Server Streaming 推送给订阅 Client（3.x 真推！）
//
// ⑤ 返回 200 OK 给 Client
//    → publishConfig 返回 Result.success()
//
// ★ 关键设计：Controller → Service → Persist 三层解耦
//   每层职责清晰 → 易于扩展（比如支持多种持久化后端）
```

### 2.2 Config 读取链路

```java
// ★ ConfigControllerV3.getConfig() 读取链路（本地锚点 L175-199）
//
// @GetMapping
// public Result<ConfigDetailInfo> getConfig(ConfigFormV3 configForm) {
//     configForm.validate();
//     String namespaceId = NamespaceUtil.processNamespaceParameter(configForm.getNamespaceId());
//     String dataId = configForm.getDataId();
//     String groupName = configForm.getGroupName();
//
//     // ★ L186-187 核心：读数据库
//     ConfigAllInfo configAllInfo =
//         configInfoPersistService.findConfigAllInfo(dataId, groupName, namespaceId);
//     if (Objects.isNull(configAllInfo)) {
//         throw new NacosApiException(NacosException.NOT_FOUND, ...);
//     }
//
//     // ★ L193-196 解密（如果加密了）
//     String encryptedDataKey = configAllInfo.getEncryptedDataKey();
//     Pair<String, String> pair = EncryptionHandler.decryptHandler(dataId, encryptedDataKey,
//         configAllInfo.getContent());
//     configAllInfo.setContent(pair.getSecond());
//
//     ConfigDetailInfo result = ResponseUtil.transferToConfigDetailInfo(configAllInfo);
//     return Result.success(result);
// }
//
// ★ 读的 CP/AP 模式差异：
//   CP 模式（JRaft）：findConfigAllInfo → **从 Leader 读（强一致）**
//     → JRaft Follower 转发给 Leader
//     → Leader 读本地 MySQL（数据已同步）
//     → 返回最新版本
//
//   AP 模式（Distro）：findConfigAllInfo → **直接从本地 MySQL 读**
//     → 可能读到旧版本（Distro 异步同步）
//     → 最终一致
//
// ★ 缓存优化：
//   ConfigInfoPersistService 内部有 **ConfigCache**
//   → 内存缓存热点配置
//   → 定期从 DB 刷新（每 5s）
//   → 减少 DB 压力
//
```

---

## 三、Config 推送变更全链路（3.x 真推）

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Nacos 3.x Config 变更 gRPC 推送全链路（面试高频！）        │
│                                                               │
│  ① Admin/Console 发布配置                                    │
│     POST /nacos/v3/cs/config → ConfigControllerV3            │
│     → ConfigOperationService → ConfigInfoPersistService.save  │
│     → JRaft → MySQL INSERT                                    │
│
│  ② 发布事件                                                  │
│     ConfigDataChangeEvent（Spring NotifyCenter）              │
│     → ConfigListenerManager 订阅                              │
│
│  ③ ConfigListenerManager.sendConfigChangeNotification()      │
│     // ★ 3.x 核心变化：gRPC Server Streaming 推              │
│     for each listener (gRPC stream):                          │
│         listener.stream.send(NotifyConfigResponse)            │
│         // 直接往 Client 的 HTTP/2 Stream 写！                │
│         // 不需要 Client 发新请求（2.x 是长轮询 hold）       │
│
│  ④ Client 收到推送                                           │
│     Spring Cloud Nacos Config Starter                        │
│     → 收到 gRPC Server Streaming NotifyConfigResponse        │
│     → 刷新 @RefreshScope Bean（Spring Cloud 机制）           │
│     → 整个过程 Client 没主动发任何请求！                      │
│
│ ★ 2.x vs 3.x 推送差异：                                      │
│  2.x：长轮询（Client 发 Request → Server hold 30s → 返回）   │
│       → 假推送（还是 Client 发起请求）                       │
│  3.x：gRPC Server Streaming（Server 主动往 Client 写）       │
│       → 真推送（Server 主导）                                │
│       → 延迟从 ~30s 降到 ~100ms                              │
│
│ ★ gRPC Server Streaming 优势：                               │
│  ① Client 不需要主动发请求 → 省流量                          │
│  ② Server 直接写 → 秒级生效                                  │
│  ③ 一条 HTTP/2 Stream 可以多次推 → 效率高                   │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（15 题）

| # | 考点 | 源码锚点 | 答案 |
|---|------|---------|------|
| 1 | ConfigControllerV3 位置？| L119 | `config/src/main/java/.../controller/v3/ConfigControllerV3.java:L119` |
| 2 | 三层解耦？| L129-141 | **Controller（入口）→ ConfigOperationService（业务）→ ConfigInfoPersistService（持久化）** |
| 3 | publishConfig CP 模式？| ConfigInfoPersistService | **JRaft 写 MySQL** → WriteRequest → JRaftServer.write → NacosStateMachine.onApply → ConfigRequestProcessor.onApply → MySQL INSERT |
| 4 | publishConfig AP 模式？| 同 | Distro 异步广播给集群其他节点 → 最终一致 |
| 5 | getConfig 读链路？| L175-199 | ConfigInfoPersistService.findConfigAllInfo → EncryptionHandler.decryptHandler → 返回 |
| 6 | CP 读 vs AP 读？| 同 | CP：**从 JRaft Leader 读（强一致）**；AP：直接读本地 MySQL（最终一致）|
| 7 | Config 变更推送？| ConfigListenerManager | **ConfigDataChangeEvent → gRPC Server Streaming 直接推 Client** |
| 8 | 2.x vs 3.x 推送差异？| 同 | 2.x 长轮询（假推，Client 发起）；3.x **gRPC Server Streaming（真推，Server 主动）** |
| 9 | ConfigCache 内存缓存？| ConfigInfoPersistService | 热点配置内存缓存 → 定期刷新（5s）→ 减 DB 压力 |
| 10 | Config 加密？| L193-196 | `EncryptionHandler.decryptHandler(dataId, encryptedDataKey, content)` → 加解密 Handler SPI |
| 11 | Config 学习路径？| 本地源码 | ConfigControllerV3 → ConfigOperationService → ConfigInfoPersistService → JRaftConfigStorage → MySQL 表 |
| 12 | MySQL config_info 表结构？| MySQL 数据库 | data_id / group_id / content / type / tenant_id / gmt_create / gmt_modified / encrypted_data_key |
| 13 | 面试高频考点？| 6 点 | **publishConfig CP 模式 / Config 变更推送 3.x 真推 / 三层解耦 / getConfig 读链路** |
| 14 | Config 与 Naming 差异？| 同 | Config：JRaft CP + 写 MySQL；Naming：Distro AP + 内存数据 |
| 15 | 本地源码验证？| 全路径 | `d:\GC_WorkSpaces\nacos-3.2.4\config\src\main\java\com\alibaba\nacos\config\server\` |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Nacos 3.2.4（本地精确路径 + 行号锚点）  
**面试考点：15 题**  
**差异化点**：本地源码可精确验证（唯一一份这样做的 Config 链路）
