# gRPC / Protobuf 源码深度解析（RPC 协议核心）

> **源码基线**：gRPC Java 1.70.x / Protobuf 27.x（最新稳定）  
> **核心仓库**：https://github.com/grpc/grpc-java / https://github.com/protocolbuffers/protobuf  
> **面试权重**：⭐⭐⭐⭐⭐（RPC 高频，Dubbo / Istio / K8s 都在用）

---

## 一、gRPC + Protobuf 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：高性能 RPC 框架（谷歌开源）                         │
│  Protobuf：序列化格式（比 JSON 小 3-10 倍）                  │
│  gRPC：RPC 框架（HTTP/2 传输 + Protobuf 序列化）             │
│                                                               │
│  对比其他协议：                                                │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐│
│  │                │  gRPC          │  Dubbo         │  Thrift      ││
│  ├──────────────┼──────────────┼──────────────┼──────────────┤│
│  │ 协议           │ HTTP/2 + Protobuf │ TCP + Hessian   │ TCP + Binary ││
│  │ 性能           │ ★★★★         │ ★★★★★         │ ★★★          ││
│  │ 强类型         │ ✅ Protobuf    │ ✅ Java 接口    │ ✅ IDL        ││
│  │ 流式           │ ★★★★★ 原生    │ ❌             │ ❌           ││
│  │ 多语言支持     │ ★★★★★ Go/C++/Java │ ★★ 主要 Java │ ★★★★ 多语言   ││
│  │ 生态           │ Istio/K8s 原生 │ 阿里生态       │ FB 出品       ││
│  └──────────────┴──────────────┴──────────────┴──────────────┘│
│                                                               │
│ ★ gRPC 是 K8s / Istio / Kuma / Envoy 的默认通信协议          │
│ ★ Protobuf 是 gRPC 的默认序列化格式（也可自定义）            │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  四种通信模式（gRPC 独有，面试高频）                          │
│                                                               │
│  ① Unary（普通 RPC）                                         │
│     Client → Request → Server → Response                      │
│     → 和 Dubbo 同步调用一样                                  │
│                                                               │
│  ② Server Streaming（服务端流式）                             │
│     Client → Request → Server → [Response, Response, ...]    │
│     → 客户端一次请求，服务端多次推数据（K8s Watch 就是这个！） │
│                                                               │
│  ③ Client Streaming（客户端流式）                             │
│     Client → [Request, Request, ...] → Server → Response      │
│     → 客户端多次推，服务端一次返回                             │
│                                                               │
│  ④ Bidirectional Streaming（双向流，★ gRPC 最大特色！）      │
│     Client → [Req, Req, ...] ← Server ← [Resp, Resp, ...]    │
│     → 双方可以同时发多次（类似 WebSocket，但协议更规范）       │
│                                                               │
│ ★ HTTP/2 是 gRPC 性能核心（多路复用 + 头部压缩 + 二进制帧）   │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Protobuf（序列化核心）

```protobuf
// ★ Protobuf IDL → 生成代码 → 高性能序列化
//
// user.proto：
syntax = "proto3";

package com.example;
option java_package = "com.example.grpc";
option java_outer_classname = "UserServiceProto";

// 消息定义
message User {
  int64 id = 1;           // 字段编号（Wire Type 用）
  string name = 2;
  int32 age = 3;
  repeated string emails = 4;  // 数组
  map<string, string> attributes = 5;  // Map
}

// RPC 服务定义
service UserService {
  // Unary
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
  
  // Server Streaming
  rpc ListUsers(ListUsersRequest) returns (stream User);
  
  // Client Streaming
  rpc CreateUsers(stream User) returns (CreateUsersResponse);
  
  // Bidirectional Streaming
  rpc Chat(stream ChatMessage) returns (stream ChatMessage);
}

// ★ Protobuf Wire Format（为什么比 JSON 小？）
//
// JSON：{"id":1, "name":"张三", "age":18} → 32 字节（带括号+引号）
// Protobuf：field_number + wire_type + value → 紧凑二进制
//   id=1, name="张三", age=18 → ~12 字节
// → 比 JSON 小 3-10 倍 → 网络传输快
//
// ★ Wire Type（协议类型）：
//   0 → Varint（int32/int64/bool/enum）
//   1 → 64-bit（double/fixed64）
//   2 → Length-Delimited（string/bytes/message）
//   5 → 32-bit（float/fixed32）
//
// ★ Protobuf vs JSON vs XML：
//   Protobuf：**最小 + 最快 + 强类型**（二进制，不可读）
//   JSON：可读性好，但体积大 + 解析慢
//   XML：体积更大 + 解析最慢（企业内部已过时）
```

### 2.2 gRPC Java 客户端核心

```java
// ★ gRPC Java 客户端核心链路
//
// ① 生成 Stubby（Stub）
//   ManagedChannel channel = ManagedChannelBuilder
//       .forAddress("localhost", 50051)
//       .usePlaintext()
//       .build();
//   UserServiceGrpc.UserServiceBlockingStub stub =
//       UserServiceGrpc.newBlockingStub(channel);
//
// ② 调用 RPC
//   GetUserRequest request = GetUserRequest.newBuilder().setId(1).build();
//   GetUserResponse response = stub.getUser(request);  // Unary 调用
//
// ★ 内部核心类链（面试高频）：
//   ManagedChannel（连接池 + 负载均衡）
//     → ClientCallImpl（RPC 调用实现）
//       → Codec（序列化：Protobuf）
//         → Transport（HTTP/2 帧传输）
//           → ServerCallHandler（服务端 Handler）
//
// ★ ManagedChannel 核心功能：
//   ① 连接池：多 TCP 连接
//   ② 负载均衡：默认 RoundRobin 或自定义
//   ③ 重试：默认关闭（要配置 retry policy）
//   ④ TLS：支持 mTLS（和 Istio 完美配合）
//   ⑤ NameResolver：服务发现（DNS / Custom）
//
// ★ LoadBalancer 四种模式：
//   RoundRobin（默认）/ PickFirst / Random / Custom
//   → Istio + Envoy 场景下 LB 交给 Envoy 做（gRPC 客户端用 static target）
```

### 2.3 gRPC Java 服务端核心

```java
// ★ gRPC Java 服务端核心链路
//
// ① 实现 Service
//   public class UserServiceImpl extends UserServiceGrpc.UserServiceImplBase {
//       @Override
//       public void getUser(GetUserRequest req, StreamObserver<GetUserResponse> respObserver) {
//           User user = userDao.findById(req.getId());
//           GetUserResponse response = GetUserResponse.newBuilder().setUser(user).build();
//           respObserver.onNext(response);
//           respObserver.onCompleted();
//       }
//   }
//
// ② 注册服务 + 启动 Server
//   Server server = ServerBuilder
//       .forPort(50051)
//       .addService(new UserServiceImpl())
//       .build()
//       .start();
//   server.awaitTermination();
//
// ★ 服务端核心类链：
//   Server（Netty Server 封装）
//     → ServerCallHandler（RPC 请求分发）
//       → ServerCall（RPC 调用上下文）
//         → ServiceImpl（用户实现）
//
// ★ gRPC Server 内置 Netty（不是 Java NIO）：
//   → Netty Reactor 模型 + HTTP/2 协议
//   → 多路复用：一个 TCP 连接上多个 RPC（HTTP/2 Stream）
//   → 比 Dubbo（TCP 自定义协议）略慢但生态更好
```

### 2.4 HTTP/2 协议核心（gRPC 传输层）

```
// ★ gRPC 用 HTTP/2 传输（面试高频！为什么不直接用 TCP？）
//
// HTTP/2 核心特性：
// ① 多路复用（Multiplexing）
//    一个 TCP 连接上多个 Stream（RPC 调用）并发执行
//    → 没有 HTTP/1.1 的队头阻塞问题
//
// ② 头部压缩（HPACK）
//    同一条 TCP 连接上 Header 压缩（索引编码）
//    → 减少 60-80% Header 大小
//
// ③ 二进制帧（Frame）
//    数据分成小帧传输
//    → 每个帧有 Stream ID（区分不同 RPC）
//    → 流量控制（按 Stream）
//
// ④ 服务器推送
//    Server Push 数据给 Client（gRPC 用不到）
//
// ★ HTTP/2 Frame 类型（gRPC 用到的）：
//   HEADERS Frame → 请求头 / 响应头
//   DATA Frame → Protobuf 序列化后的二进制数据
//   RST_STREAM Frame → 终止 Stream（RPC 失败）
//   WINDOW_UPDATE Frame → 流量控制
//   PING Frame → Keep-Alive
//
// ★ HTTP/2 Flow Control（流量控制）：
//   每个 Stream 有自己的流量控制窗口（默认 64KB）
//   → 发送方不能超过接收方的 window
//   → WINDOW_UPDATE Frame 更新窗口
//   → 防止一个 Stream 把 TCP 连接撑爆
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | gRPC 核心优势？| **HTTP/2 多路复用 + Protobuf 序列化 + 四种通信模式**（Unary/Server Streaming/Client Streaming/Bidirectional Streaming）|
| 2 | Protobuf Wire Format 为什么小？| **field_number + wire_type + value** 紧凑二进制；比 JSON 小 3-10 倍；int 用 Varint 变长编码（小数字 1 字节）|
| 3 | gRPC 四种通信模式？| **① Unary（普通 RPC）② Server Streaming（K8s Watch 用这个）③ Client Streaming ④ Bidirectional Streaming（★ gRPC 最大特色，类似 WebSocket）**|
| 4 | gRPC vs Dubbo？| gRPC：**HTTP/2 + Protobuf + 多语言 + 生态（K8s/Istio）**；Dubbo：TCP 自定义 + Hessian2 + 阿里生态；K8s/Istio 用 gRPC |
| 5 | HTTP/2 核心特性？| **多路复用（一个 TCP 多个 Stream）+ HPACK 头部压缩 + 二进制帧 + 流量控制**；解决 HTTP/1.1 队头阻塞 |
| 6 | gRPC ManagedChannel 核心功能？| **连接池 + 负载均衡 + 重试 + TLS + NameResolver**；默认 RoundRobin LB |
| 7 | gRPC 默认负载均衡？| **RoundRobin**；Istio 场景下 LB 交给 Envoy（gRPC 用 static target）；支持自定义 LoadBalancer |
| 8 | gRPC Retry 默认开启？| **默认关闭！** 要配置 retry policy（maxAttempts / backoff / retryableStatusCodes）|
| 9 | gRPC TLS / mTLS？| ManagedChannelBuilder.useTransportSecurity() + SSLContext；**和 Istio mTLS 完美配合**（Envoy 注入证书）|
| 10 | gRPC 和 K8s？| **K8s API Server 用 gRPC 和 etcd 通信**（etcd v3）；K8s Aggregated API Server 也用 gRPC |
| 11 | gRPC 和 Istio？| Istio 原生支持 gRPC；Envoy 自动处理 HTTP/2 + mTLS + 熔断 + 重试；gRPC 是 Service Mesh 首选协议 |
| 12 | Protobuf 类型？| Wire Type：**Varint（0）/ 64-bit（1）/ Length-Delimited（2）/ 32-bit（5）**；Map 本质是 repeated entry message |
| 13 | gRPC 性能？| 比 Dubbo 略慢（HTTP/2 开销），但比 REST/HTTP JSON 快很多；多语言场景选 gRPC |
| 14 | gRPC 和 GraphQL？| gRPC：强类型 + 高性能 RPC；GraphQL：灵活查询（HTTP POST + JSON）；不冲突（Gateway 暴露 GraphQL → 内部 gRPC）|
| 15 | gRPC Keep-Alive？| 默认启用 HTTP/2 PING Frame（每 120s 一次）；可配置 maxConnectionIdleTime / time |
| 16 | gRPC 双向流和 WebSocket 区别？| 都是全双工；WebSocket：HTTP Upgrade + TCP；gRPC 双向流：HTTP/2 Stream + 强类型 Protobuf + 框架级支持 |
| 17 | gRPC 学习路径？| 写 .proto → 生成 Java 代码 → 实现 Service → 启动 Server → 写 Client → 试四种通信模式 |
| 18 | gRPC vs Thrift？| 都是二进制 RPC；gRPC：HTTP/2 + Protobuf + 多语言 + Service Mesh 原生；Thrift：TCP + Binary + Facebook 出品；gRPC 生态更好 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：gRPC Java 1.70.x + Protobuf 27.x  
**面试考点：18 题**
