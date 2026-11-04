# Netty 源码深度解析（异步网络框架）

> **源码基线**：Netty 4.1.139（最新稳定，Netty 5.x 已废弃）  
> **核心仓库**：https://github.com/netty/netty  
> **面试权重**：⭐⭐⭐⭐⭐（RPC/网关/IM 底层核心）

---

## 一、Netty 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：NIO 网络编程框架（对 Java NIO 的封装 + 增强）      │
│  底层：Java NIO（Selector + SocketChannel）                  │
│  上层：Dubbo / RocketMQ / Spring WebFlux / gRPC / 自研 RPC    │
│                                                               │
│  三大核心组件：                                                │
│  Bootstrap / ServerBootstrap（启动器）→ Channel（连接）       │
│  → EventLoopGroup（线程池 + Selector）→ Pipeline（责任链）   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Netty vs Java NIO                                           │
│                                                               │
│  ┌──────────┬──────────────────────┬────────────────────┐   │
│  │   特性    │     Java NIO          │     Netty          │   │
│  ├──────────┼──────────────────────┼────────────────────┤   │
│  │ Buffer    │ ByteBuffer（复杂）     │ ByteBuf（读写指针分离）│   │
│  │ 线程模型  │ 一个 Selector 一个线程  │ EventLoop（线程池 + Selector）│   │
│  │ 事件分发  │ 手动写 switch case     │ Pipeline + HandlerChain│   │
│  │ 编解码    │ 手动解析 Byte          │ Codec 自动编解码     │   │
│  │ 零拷贝    │ 支持（FileChannel.transferTo）│ ByteBuf 也支持（slice + reference count）│   │
│  │ 线程泄漏  │ 容易（手动管理生命周期）│ EventLoop 自动调度   │   │
│  └──────────┴──────────────────────┴────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、核心源码链路

### 2.1 Reactor 线程模型（EventLoop + Selector）

```java
// ★ Reactor 模式（Netty 核心！面试必问）
//
// BossGroup（Accept 组）：
//   EventLoopGroup bossGroup = new NioEventLoopGroup(1);
//   → 只有 1 个线程（Accept 连接任务少）
//   → NioEventLoop 内部：Selector + while(true) { select(); accept(); }
//   → 收到新连接 → 创建 NioSocketChannel → 注册到 WorkerGroup
//
// WorkerGroup（IO 组）：
//   EventLoopGroup workerGroup = new NioEventLoopGroup(NIO_THREADS);
//   → N 个线程（默认 CPU * 2）
//   → 每个 NioEventLoop 有独立 Selector
//   → Channel 注册后固定绑定一个 EventLoop（不会跨线程！保证无锁）
//   → while(true) { select(); read(); write(); }
//
// ★ 关键设计：一个 Channel 生命周期内只属于一个 EventLoop
//   → 无锁！（不需要 synchronized）
//   → 高性能！（避免多线程竞争）
//
// ★ EventLoop 执行流程：
//   // NioEventLoop.run() 核心循环（简化）
//   protected void run() {
//       for (;;) {
//           // 1. select（最多 1s 超时）
//           selector.select(1000);
//
//           // 2. 处理 IO 事件
//           Set<SelectionKey> selectedKeys = selector.selectedKeys();
//           for (SelectionKey key : selectedKeys) {
//               handleSelectedKey(key);
//               // → read/write event → Pipeline.fireChannelRead / fireChannelWrite
//           }
//
//           // 3. 处理任务队列
//           runAllTasks();
//           // → 用户 submit 的任务 → 在 EventLoop 线程执行
//           // → 如果任务执行超过 1s → 继续处理下一批
//       }
//   }
//
// ★ NioEventLoop 双队列：
//   taskQueue（普通任务）+ delayedTaskQueue（延时任务）
//   → 用户 channel.eventLoop().submit(() -> { ... }) → 进 taskQueue
```

### 2.2 ChannelPipeline（★ 责任链模型核心）

```java
// ★ ChannelPipeline 是 Netty 最精妙的设计之一！
//   责任链模型（类似 Sentinel Slot 链 / Gateway Filter 链）
//
// Pipeline 双向流动：
//   Inbound（入站，从网络进来→业务）：
//     SocketChannelRead → Handler1 → Handler2 → Handler3 → ...
//
//   Outbound（出站，业务→网络出去）：
//     Handler3.write → Handler2.write → Handler1.write → SocketChannelWrite
//
// ★ Pipeline 内部是双向链表（AbstractChannelHandlerContext）
//
// AbstractChannelHandlerContext 核心字段：
//   ChannelHandler handler;       // 持有的 Handler
//   AbstractChannelHandlerContext prev;  // 前一个 Context
//   AbstractChannelHandlerContext next;  // 后一个 Context
//
// ★ Inbound 事件流动（fireChannelRead）：
//   channelRead() 调用 → ctx.fireChannelRead(msg)
//     → 遍历 next 直到最后一个 InboundHandler
//     → 如果某 handler 不调用 fire → 事件中断（不会传给下一个）
//
// ★ Outbound 事件流动（write）：
//   write() 调用 → ctx.write(msg)
//     → 遍历 prev 直到第一个 OutboundHandler
//     → OutboundHandler 是反向流动的！
//
// ★ 默认编解码链（典型配置）：
//   SocketChannel →
//   [Inbound] ByteToMessageDecoder（解码：ByteBuf → POJO）
//   [Inbound] Handler（业务处理）
//   [Outbound] MessageToByteEncoder（编码：POJO → ByteBuf）
//   [Outbound] 其他 OutboundHandler
//   → SocketChannel
```

### 2.3 ByteBuf（★★★★★ 核心设计）

```java
// ★ ByteBuf vs Java NIO ByteBuffer（为什么 Netty 要自己实现 Buffer？）
//
// ByteBuffer 问题：
//   ① read/write 共用 position（读写切换要 flip + compact）
//   ② 长度固定（扩容麻烦）
//   ③ 无法同时读又写
//
// ByteBuf 解决：
//   ① readerIndex + writerIndex 分离（读指针和写指针独立！不用 flip）
//      ┌────────────────────────────────┐
//      │  discardable  │ readable │ writable │
//      │  ← readerIndex │ ← writerIndex  │ ← capacity │
//      └────────────────────────────────┘
//      readableBytes = writerIndex - readerIndex
//      writableBytes = capacity - writerIndex
//
//   ② 自动扩容（writeByte 超过 writableBytes → expand）
//
//   ③ 三种类型：
//     HeapByteBuf（JVM 堆内存，GC 管理）
//     DirectByteBuf（堆外内存，零拷贝需要，但不受 GC 管理！）
//     CompositeByteBuf（组合多个 ByteBuf 成一个视图，不复制数据）
//
// ★ 零拷贝场景：
//   DirectByteBuf → 数据直接在堆外 → SocketChannel.write(directBuf) 不需要 JVM 堆到内核态拷贝
//   传统 ByteBuffer 是 Heap → SocketChannel.write(heapBuf) 要先复制到堆外 Buffer
//   → DirectByteBuf 比 HeapByteBuf 快 30%+
//
// ★ PooledByteBufAllocator（★ 对象池，高性能！）：
//   Netty 4.x 默认用 Pooled 分配器
//   → 预分配一组 ByteBuf → 用完还回池（不是 new + GC）
//   → 减少 GC 压力 + 减少内存碎片
//   → 但 Pooled 有内存泄漏风险（要配合 leakDetectionThreshold）
```

### 2.4 Bootstrap 启动流程

```java
// ★ ServerBootstrap（服务端启动）完整流程
//
// ServerBootstrap bootstrap = new ServerBootstrap();
// bootstrap
//     .group(bossGroup, workerGroup)  // Boss + Worker 组
//     .channel(NioServerSocketChannel.class)  // 指定 Channel 类型
//     .option(ChannelOption.SO_BACKLOG, 128)  // TCP 积压队列
//     .childOption(ChannelOption.SO_KEEPALIVE, true)
//     .childHandler(new ChannelInitializer<SocketChannel>() {
//         @Override
//         protected void initChannel(SocketChannel ch) {
//             ch.pipeline().addLast(new Decoder());  // Inbound
//             ch.pipeline().addLast(new MyHandler()); // Inbound
//             ch.pipeline().addLast(new Encoder());  // Outbound
//         }
//     });
//
// ChannelFuture future = bootstrap.bind(8080).sync();
// future.channel().closeFuture().sync();  // 阻塞直到 Channel 关闭
//
// ★ 内部流程：
//   bind() → doBind() → initAndRegister() → ChannelFutureListener
//     → 等待 Channel 注册到 BossEventLoop
//     → NioServerSocketChannel.doBind → serverSocketChannel.bind()
//     → 成功 → Pipeline.fireChannelActive()
//
// ★ NioServerSocketChannel vs NioSocketChannel：
//   Server：accept 新连接 → 创建 SocketChannel → 注册到 WorkerGroup
//   Client：直接 connect → 注册到 EventLoopGroup
```

---

## 三、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Netty Reactor 线程模型？| **BossGroup（Accept 新连接，1 线程）+ WorkerGroup（处理 IO，CPU*2 线程）**；一个 Channel 生命周期内只属于一个 EventLoop → 无锁 |
| 2 | EventLoop 核心循环？| **while(true) { select(); handleSelectedKey(); runAllTasks(); }**；select 最多 1s 超时 → 及时处理任务队列 |
| 3 | Pipeline 双向流动？| **Inbound（从网络→业务）正向流动；Outbound（业务→网络）反向流动**；双向链表 + prev/next Context |
| 4 | ByteBuf 相比 ByteBuffer 的优势？| **readerIndex/writerIndex 分离（不用 flip/compact）+ 自动扩容 + 三种类型（Heap/Direct/Composite）** |
| 5 | 零拷贝原理？| **DirectByteBuf 堆外内存 → SocketChannel.write(directBuf) 直接写网卡**；HeapByteBuffer 要先复制到堆外 → Direct 快 30%+ |
| 6 | Pooled vs Unpooled？| PooledByteBufAllocator（默认）**对象池预分配 + 用完还回** → 减少 GC + 减少内存碎片；但有内存泄漏风险 |
| 7 | Netty 默认编解码？| 自己实现 **ByteToMessageDecoder / MessageToByteEncoder**；或用内置 ObjectDecoder / StringDecoder |
| 8 | ChannelHandler 两种接口？| ChannelInboundHandler（Inbound 处理）+ ChannelOutboundHandler（Outbound 处理）；SimpleChannelInboundHandler（只处理某类型，自动 release）|
| 9 | write vs flush？| **write 只是把消息放入 ChannelOutboundBuffer（出站缓冲区）；flush 才把 Buffer 里的数据真正写到 SocketChannel** |
| 10 | 连接泄漏检测？| **ResourceLeakDetector** → 设置 leakDetectionThreshold=512（采样频率）→ ByteBuf 用完没 release 会打印堆栈 |
| 11 | Netty vs NIO？| Netty：**封装好 EventLoop/Pipeline/ByteBuf** + 无锁设计；NIO：Selector + ByteBuffer 手动管理，容易出错 |
| 12 | EventLoop 执行任务超时？| runAllTasks(maxTime=0.1s) → 如果任务队列处理超过 100ms → 下一轮 select |
| 13 | NioEventLoopGroup 默认线程数？| **CPU 核心数 * 2**；但如果任务都是 IO 型（等待网络），*2 是合理的 |
| 14 | Netty 自动重连？| **Bootstrap.reconnect(true)** + ChannelFutureListener → 连接断开 → EventLoop 里定时重连 |
| 15 | Pipeline addLast vs addFirst？| addLast 加到链表尾部；addFirst 加到链表头部 → **Inbound 先处理 addFirst 的，Outbound 先处理 addLast 的** |
| 16 | ChannelFuture / Promise？| ChannelFuture（异步结果，只能读）+ ChannelPromise（可写，EventLoop 内部用来 setSuccess）|
| 17 | EventLoop 能 submit 任务吗？| 可以！channel.eventLoop().submit(() -> { ... }) → 进 taskQueue → 在 EventLoop 线程执行（保证无锁）|
| 18 | Netty 线程切换？| 用户代码在 EventLoop 线程执行 → 不要在 Handler 里做耗时操作 → 如果要，用 **EventExecutorGroup**（另开线程池）|

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Netty 4.1.139  
**面试考点：18 题**
