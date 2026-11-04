# 220 · Netty 高频面试题库（50 题 · 详解）

> **方向**：Netty 4.1.x 线程模型、Pipeline、ByteBuf、编解码与工程实践  
> **面试权重**：⭐⭐⭐⭐⭐（网络框架必问）  
> **配套源码文档**：[039_Netty源码解析.md](039_Netty源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问；不写未经验证的行号。

---

## 一、线程模型与 EventLoop（Q1–Q8）

### Q1. Netty 为什么比原生 NIO 快、为什么更易用？

**结论**：Netty 快在“无锁串行化 + 池化内存 + 批量系统调用”，易用在“把 Selector 的复杂度封装成 EventLoop/Pipeline”。

**机制**：
- 一个 Channel 从注册到销毁只绑定一个 EventLoop，同一 Channel 的所有 IO 事件都在同一线程处理，天然免锁。
- ByteBuf 池化 + 堆外内存 + 引用计数，减少 GC 与拷贝。
- 批量处理 selectedKeys、批量 flush，减少 epoll 系统调用次数。
- Pipeline 用责任链把编解码、业务、异常处理解耦。

**追问**：Netty 并不是“所有场景都比 NIO 快”，小并发短连接下优势不明显；它的核心价值是稳定性与可维护性。

### Q2. 解释 Netty 的 Reactor 主从模型。

**结论**：BossGroup 只负责 Accept，WorkerGroup 负责已连接 Channel 的读写，形成“主 Reactor + 从 Reactor 池”。

**机制**：
- BossGroup 中的 EventLoop 注册 ServerSocketChannel，只监听 OP_ACCEPT。
- Accept 到新连接后，把 SocketChannel 注册到 WorkerGroup 中某个 EventLoop。
- 每个 EventLoop 内部仍是“单线程 + 一个 Selector + 一个任务队列”。
- 多个 Worker EventLoop 之间通过 Channel 绑定关系天然分片，避免共享状态。

**追问**：为什么不让一个线程既 Accept 又读写？Accept 与读事件会相互争抢，连接建立抖动会拖慢已有连接。

### Q3. BossGroup 与 WorkerGroup 的线程数如何设置？

**结论**：BossGroup 通常 1 个线程即可；WorkerGroup 默认 `CPU 核数 × 2`。

**机制**：
- `NioEventLoopGroup` 不指定线程数时，默认 `DEFAULT_EVENT_LOOP_THREADS = max(1, CPU 核数 × 2)`。
- Boss 只处理 Accept，连接建立频率远低于 IO 事件，1 个线程足够；多网卡/超高连接建立速率才需要增加。
- Worker 线程数不是越多越好：线程过多会增加上下文切换与 Selector 空转。

**追问**：IO 密集型与 CPU 密集型如何区分？Netty 线程主要等待网络，`×2` 的经验值针对 IO 型；若 Handler 内做重计算，应另设业务线程池。

### Q4. 一个 Channel 会绑定几个 EventLoop？运行中能切换吗？

**结论**：生命周期内只绑定一个 EventLoop，不能切换。

**机制**：
- `Channel` 持有 `EventLoop` 引用，注册时确定，之后所有读写、定时任务、Promise 完成都在该 EventLoop 线程执行。
- 这保证了 Channel 内部状态（Pipeline、ByteBuf 引用计数）的单线程访问语义。
- `channel.eventLoop()` 永远返回同一个实例。

**追问**：如果某个 EventLoop 负载过高怎么办？只能在上线前合理分片，或把耗时业务移出 EventLoop，不能运行时迁移 Channel。

### Q5. NioEventLoop 的主循环做了什么？

**结论**：循环执行“选择就绪事件 → 处理 IO → 处理任务队列”，并按 ioRatio 控制 IO 与任务的时间配比。

**机制**：
- `selectStrategy.calculateStrategy()` 决定本次是 select、继续处理任务还是跳过。
- `selector.select(timeout)` 阻塞等待就绪事件，带超时以便及时回到任务队列。
- `processSelectedKeys()` 处理 OP_ACCEPT / OP_READ / OP_WRITE。
- `runAllTasks()` 执行用户提交的普通任务与定时任务；`ioRatio` 默认 50，表示 IO 与任务大致各占一半时间。

**追问**：为什么任务队列必须由 EventLoop 自己执行？为了保证同一 Channel 的状态变更仍在单线程内完成。

### Q6. 如何向 EventLoop 提交任务？有什么注意点？

**结论**：用 `channel.eventLoop().execute/submit` 或 `channel.pipeline().context(...).executor()`，任务是串行执行的。

**机制**：
- 外部线程调用 `execute` 会把任务放入 `taskQueue`，并唤醒 Selector。
- EventLoop 线程内提交任务会直接入队，在当前轮次的 `runAllTasks` 执行。
- 任务异常会被捕获并交给 `exceptionCaught`，不会打断 EventLoop。

**追问**：任务里再阻塞会怎样？会阻塞整个 EventLoop 上的所有 Channel，必须避免。

### Q7. 在 ChannelHandler 中做耗时操作会发生什么？怎么解决？

**结论**：会阻塞该 EventLoop 绑定的所有 Channel，导致读事件积压、心跳超时甚至连接被拖死。

**机制**：
- EventLoop 是单线程，Handler 的 `channelRead` 在 EventLoop 线程执行。
- 耗时操作（数据库、远程调用、大文件计算）会让 `select` 无法及时返回。
- 解决方案：业务线程池（`DefaultEventExecutorGroup`）、异步编排、或把阻塞逻辑下沉到独立服务。

**追问**：加了 `DefaultEventExecutorGroup` 后还是同一个线程吗？不是，Handler 会被移到业务 Executor 执行，但 IO 事件仍要切回 EventLoop。

### Q8. Netty 的线程模型和“每连接一线程”相比优势在哪？

**结论**：用少量 EventLoop 线程承载大量连接，避免线程数量随连接数线性增长。

**机制**：
- 每连接一线程在万级连接时会产生大量线程栈与上下文切换。
- Netty 采用 IO 多路复用，一个线程管理多个 Channel。
- 单 Channel 串行化又避免了共享状态加锁。

**追问**：适合 CPU 密集业务吗？不适合，IO 模型解决的是等待问题，计算仍需业务线程池。

---

## 二、Pipeline 与 Handler（Q9–Q15）

### Q9. Pipeline 的数据结构是什么？事件如何传播？

**结论**：`ChannelPipeline` 是 `ChannelHandlerContext` 组成的双向链表，Inbound 从 head 到 tail，Outbound 从 tail 到 head。

**机制**：
- 每个 Handler 被包装成 `ChannelHandlerContext`，持有 prev/next 指针。
- Inbound 事件（channelRead、channelActive）沿 next 方向传播。
- Outbound 事件（write、flush、close）沿 prev 方向传播。
- head 与 tail 是哨兵 Context，保证边界事件有统一入口。

**追问**：为什么 Outbound 要反向？因为写操作从业务 Handler 向网络方向回流，越靠近 tail 的 Outbound Handler 越先被调用。

### Q10. addFirst 与 addLast 对执行顺序有什么影响？

**结论**：addFirst 更靠近 head，addLast 更靠近 tail；Inbound 先执行 addFirst 的，Outbound 先执行 addLast 的。

**机制**：
- Inbound 顺序 = head → addFirst → … → addLast → tail。
- Outbound 顺序 = tail → addLast → … → addFirst → head。
- 编解码器通常 addLast 在入站链尾部，确保业务 Handler 看到的是解码后的对象。

**追问**：如果编码器 addFirst 会怎样？Outbound 时它会最后执行，可能拿不到尚未编码的消息，顺序容易出错。

### Q11. `ctx.writeAndFlush` 与 `channel.writeAndFlush` 有什么区别？

**结论**：`ctx.write` 从“当前 Handler 的前一个 Outbound Handler”开始传播；`channel.write` 从 Pipeline 尾部开始传播。

**机制**：
- `ctx.writeAndFlush(msg)` 只经过当前 Context 之前的 Outbound Handler，不会经过当前 Handler 之后的。
- `channel.writeAndFlush(msg)` 等价于从 tail 开始，经过完整 Outbound 链。
- 在自定义 Handler 内通常用 `ctx.writeAndFlush`，避免重复经过自己或下游 Handler。

**追问**：`ctx.channel().write` 和 `channel.write` 一样吗？一样，都是全链路。

### Q12. ChannelHandler 有哪些生命周期回调？

**结论**：`handlerAdded`、`channelRegistered`、`channelActive`、`channelRead`、`channelReadComplete`、`channelInactive`、`channelUnregistered`、`handlerRemoved`、`exceptionCaught`。

**机制**：
- Handler 被加入 Pipeline 时触发 `handlerAdded`，移除时触发 `handlerRemoved`。
- Channel 注册到 EventLoop、连接建立、连接关闭分别触发对应事件。
- `exceptionCaught` 默认会关闭 Channel，业务通常要覆写做日志与降级。

**追问**：`channelReadComplete` 能保证一次读完吗？不能，它表示本轮 select 的读事件处理结束，可能仍有半包。

### Q13. `@ChannelHandler.Sharable` 的使用条件是什么？

**结论**：只有无状态、线程安全的 Handler 才能加 `@Sharable`，否则每个 Channel 必须创建独立实例。

**机制**：
- Sharable Handler 会被多个 Channel 共享，成员变量可能被多个 EventLoop 并发访问。
- 无状态 Handler（只做转换、不保存每连接数据）可以安全共享。
- 有状态 Handler（保存连接上下文、累计计数）必须 `new` 一份。

**追问**：给非 Sharable Handler 复用实例会怎样？Netty 会抛出 `IllegalStateException`，提示已被其他 Pipeline 使用。

### Q14. `SimpleChannelInboundHandler` 为什么能自动释放消息？

**结论**：它在 `channelRead` 中调用 `channelRead0` 后自动 `ReferenceCountUtil.release(msg)`。

**机制**：
- 泛型匹配到指定消息类型才交给 `channelRead0`，否则继续向后传播。
- 处理完自动释放，避免 ByteBuf 泄漏。
- 如果要把消息保留到异步任务中，必须先 `retain()`，否则会被提前释放。

**追问**：什么时候不能用它？消息需要跨 Handler 传递或异步持有时，应使用 `ChannelInboundHandlerAdapter` 手动管理引用计数。

### Q15. 如何实现心跳与空闲检测？

**结论**：用 `IdleStateHandler` 检测读/写空闲，再在自定义 Handler 的 `userEventTriggered` 中发送心跳或关闭连接。

**机制**：
- `IdleStateHandler(readerIdle, writerIdle, allIdle)` 添加读、写、全空闲三个定时任务。
- 触发时产生 `IdleStateEvent`，由 `userEventTriggered` 捕获。
- 常见策略：读空闲发送 ping，多次未响应则关闭；写空闲可触发保活。

**追问**：为什么依赖 TCP KeepAlive 不够？默认 2 小时才探测，无法满足业务级故障发现；应用层心跳更可控。

---

## 三、ByteBuf 与内存管理（Q16–Q24）

### Q16. ByteBuf 相比 ByteBuffer 有哪些改进？

**结论**：读写索引分离、自动扩容、池化、引用计数、复合缓冲区，解决了 ByteBuffer 的 flip/容量固定/无池化问题。

**机制**：
- `readerIndex` 与 `writerIndex` 分离，读写互不干扰，不需要每次 `flip()`。
- 容量不足时自动扩容，避免手动 `compact` 或重新分配。
- 支持堆内、堆外、复合三种实现。
- 通过 `ReferenceCounted` 管理生命周期，池化复用减少 GC。

**追问**：为什么 ByteBuffer 需要 flip？它只有一个 position，读写方向切换要重置位置。

### Q17. `readerIndex`、`writerIndex`、`capacity` 的关系是什么？`discardReadBytes` 做什么？

**结论**：不变量是 `0 ≤ readerIndex ≤ writerIndex ≤ capacity`；`discardReadBytes` 丢弃已读区间并压缩可写空间。

**机制**：
- `readableBytes = writerIndex - readerIndex`。
- `writableBytes = capacity - writerIndex`。
- `discardReadBytes` 把 `[readerIndex, writerIndex)` 搬到 0 起始位置，释放前部空间，但会触发内存移动。
- 频繁调用会影响性能，通常只在需要复用大缓冲区时使用。

**追问**：`clear()` 与 `discardReadBytes` 区别？`clear` 只把两个索引归零，不移动数据。

### Q18. Netty 的三种 ByteBuf 分别是什么？

**结论**：`HeapByteBuf`、`DirectByteBuf`、`CompositeByteBuf`。

**机制**：
- Heap：JVM 堆内数组，分配快，适合短生命周期与堆内处理。
- Direct：堆外内存，避免 JVM 堆与内核之间的一次拷贝，适合网络 IO。
- Composite：把多个 ByteBuf 逻辑组合，不实际拷贝，适合协议拼接。

**追问**：Direct 一定更快吗？不一定，分配/释放成本更高，小对象或频繁创建场景可能 Heap 更合适。

### Q19. PooledByteBufAllocator 的池化是怎么工作的？

**结论**：按 Arena、Chunk、Page 分层管理内存，分配时从线程本地缓存或共享池取，释放时归还而不是交给 GC。

**机制**：
- 每个 EventLoop 绑定一个 Arena，减少竞争。
- Chunk 通常按 16MB 管理，内部切分为 Page（默认 8KB）。
- 小对象走 `PoolThreadCache` 的 Tiny/Small 缓存，大对象走 Normal 分配。
- Netty 4.1 默认使用 `PooledByteBufAllocator.DEFAULT`。

**追问**：池化会带来什么问题？内存占用更“黏”，释放不及时会表现为堆外内存持续上涨，需要泄漏检测。

### Q20. ByteBuf 的引用计数规则是什么？什么时候必须 release？

**结论**：创建时计数为 1；每次 `retain()` 加 1，每次 `release()` 减 1；归零时回收内存。

**机制**：
- 谁最后持有谁负责释放；Pipeline 中若消息继续向后传递，不能提前释放。
- `SimpleChannelInboundHandler` 自动释放；`ChannelInboundHandlerAdapter` 需要手动释放。
- 写出到 Channel 后，Netty 会在写完成后释放（`ChannelOutboundBuffer` 负责）。

**追问**：重复 release 会怎样？引用计数变负，可能触发 `IllegalReferenceCountException` 或内存错误。

### Q21. 如何排查 Netty 内存泄漏？

**结论**：开启 `ResourceLeakDetector` 的检测级别，结合堆外内存监控与泄漏报告定位未 release 的 ByteBuf。

**机制**：
- `-Dio.netty.leakDetection.level=paranoid` 做高采样检测。
- 泄漏时打印分配堆栈，能看到创建位置。
- 常见原因：异常分支未释放、异步任务持有消息未 retain、自定义 Handler 忘记 release。
- 生产环境用 `simple` 或 `advanced`，避免 paranoid 的性能开销。

**追问**：泄漏一定在堆内吗？ByteBuf 泄漏常表现为 Direct Memory 上涨，堆 dump 看不到，需要 NMT 或 JVM 堆外内存指标。

### Q22. Netty 中的“零拷贝”体现在哪些地方？

**结论**：CompositeByteBuf 逻辑拼接、slice/duplicate 共享底层内存、DirectByteBuf 直接写 Socket、FileRegion 走 sendfile。

**机制**：
- 复合缓冲区避免把多个消息复制成一个大缓冲。
- `slice()` 共享同一底层数组，只改索引。
- DirectBuffer 交给 SocketChannel 时减少堆内到堆外的复制。
- `FileRegion` 利用操作系统 sendfile，避免用户态与内核态来回拷贝。

**追问**：零拷贝等于完全不拷贝吗？不等于，它指减少不必要的用户态拷贝，内核态仍可能有 DMA 拷贝。

### Q23. CompositeByteBuf 适合什么场景？

**结论**：需要把多个缓冲区逻辑上当成一个连续缓冲区时使用，避免实际内存复制。

**机制**：
- 典型场景：HTTP 响应头 + 响应体、协议分段拼接、多个小包合并写出。
- 内部维护组件列表与偏移，读取时按组件定位。
- 修改组件会触发索引重算，频繁增删组件仍有成本。

**追问**：Composite 的组件需要单独 release 吗？通常由 Composite 统一管理，但要遵循引用计数规则，避免重复释放。

### Q24. 堆内 ByteBuf 与堆外 ByteBuf 如何选择？

**结论**：网络读写优先 Direct，短生命周期与频繁创建优先 Heap，最终以压测与内存监控为准。

**机制**：
- Direct 减少一次堆内到堆外拷贝，适合大流量网络 IO。
- Direct 分配慢、回收依赖 Cleaner/池化，滥用会导致堆外内存压力。
- Heap 受 GC 管理，分配快，但写出时可能需要复制到直接缓冲区。

**追问**：为什么 Netty 默认用 Direct？默认分配器是 PooledDirect，配合池化后分配成本被摊薄，网络场景收益更稳定。

---

## 四、编解码与粘包拆包（Q25–Q30）

### Q25. TCP 为什么会出现粘包和拆包？

**结论**：TCP 是字节流协议，不保证“一次 write 对应一次 read”，报文边界需要应用层自己定义。

**机制**：
- 发送端可能合并多个小包（Nagle、缓冲区）。
- 接收端一次 read 可能拿到多个报文，也可能只拿到半个报文。
- 网络分片、延迟、拥塞都会改变到达粒度。

**追问**：UDP 有粘包吗？UDP 保留消息边界，通常没有粘包问题，但会丢包、乱序。

### Q26. 常见的粘包解决方案有哪些？

**结论**：固定长度、分隔符、长度字段、协议头携带长度，四类方案各有适用场景。

**机制**：
- 固定长度：实现简单，但浪费空间、不灵活。
- 分隔符：适合文本协议，需处理转义与分隔符出现在内容中的情况。
- 长度字段：最通用，先读长度再读内容，支持变长报文。
- 自定义协议头：长度 + 类型 + 版本 + 序列号，扩展性最好。

**追问**：为什么生产环境更常用长度字段？边界明确、解析成本低、可与二进制序列化配合。

### Q27. `LengthFieldBasedFrameDecoder` 的关键参数怎么理解？

**结论**：五个参数分别描述“帧最大长度、长度字段偏移、长度字段自身长度、长度修正值、解析后跳过多少字节”。

**机制**：
- `maxFrameLength`：防止恶意超长帧打爆内存。
- `lengthFieldOffset`：长度字段在报文中的起始位置。
- `lengthFieldLength`：长度字段占几个字节（1/2/3/4/8）。
- `lengthAdjustment`：长度字段值是否包含长度字段本身或协议头。
- `initialBytesToStrip`：解析后是否把协议头剥掉再交给业务。

**追问**：长度字段表示“消息体长度”还是“整包长度”？取决于协议，必须用 `lengthAdjustment` 对齐，否则会错位。

### Q28. `ByteToMessageDecoder` 如何解决半包问题？

**结论**：它把未消费的字节累积在 `cumulation` 中，只有满足解码条件时才调用 `decode`，否则等待下次读事件。

**机制**：
- 每次 `channelRead` 把新字节加入累积缓冲区。
- `decode` 返回后，已消费部分被丢弃，剩余部分保留。
- 若一次读到多个完整报文，`decode` 会被循环调用直到不足一帧。
- 解码出的消息通过 `out.add()` 交给下一个 Handler。

**追问**：累积缓冲区会不会无限增长？会，必须设置最大帧长度并在超限时关闭连接，防止内存攻击。

### Q29. 编码器 `MessageToByteEncoder` 的执行逻辑是什么？

**结论**：只处理匹配类型的消息，调用 `encode` 把对象写成 ByteBuf，再交给下一个 Outbound Handler。

**机制**：
- 泛型匹配 `acceptOutboundMessage`，不匹配则直接透传。
- `encode` 中通常先 `allocate` 一个缓冲区，再写入字段。
- 若消息本身是 ByteBuf，编码器通常不处理，避免二次编码。

**追问**：编码后需要 flush 吗？`writeAndFlush` 会 flush；只 `write` 则进入写缓冲，等待 flush 或水位触发。

### Q30. 为什么推荐“长度字段 + 协议头”而不是只用分隔符？

**结论**：长度字段解析是 O(1) 定位，分隔符需要扫描内容，且二进制数据中分隔符可能合法出现。

**机制**：
- 分隔符方案要处理转义，文本协议尚可，二进制协议容易歧义。
- 长度字段可直接判断是否半包，不需要逐字节扫描。
- 协议头可携带版本、类型、序列化方式，便于演进与路由。

**追问**：长度字段本身被篡改怎么办？校验最大长度、校验和、签名，并在超限时断开连接。

---

## 五、可靠性与性能（Q31–Q35）

### Q31. ChannelOutboundBuffer 的高低压水位线有什么用？

**结论**：用 `isWritable()` 反馈写入压力，防止无界堆积导致 OOM。

**机制**：
- 默认高水位 64KB、低水位 32KB，可配置。
- 超过高水位后 `channel.isWritable()` 返回 false，业务应暂停写入。
- 低于低水位后恢复可写，触发 `channelWritabilityChanged`。
- 写缓冲堆积本质是下游消费慢，不能靠无限扩容解决。

**追问**：`isWritable=false` 时继续 write 会怎样？消息继续入缓冲，最终可能 OOM，必须做限流或丢包策略。

### Q32. ChannelFuture 与 ChannelPromise 的区别是什么？

**结论**：Future 是只读异步结果，Promise 是可写的 Future，供 EventLoop 内部设置成功/失败。

**机制**：
- `ChannelFuture` 提供 `isDone`、`isSuccess`、`cause`、`addListener`。
- `ChannelPromise` 增加 `setSuccess`、`setFailure`、`trySuccess`。
- 业务侧通常只拿 Future 注册监听，Promise 由 Netty 内部创建与完成。

**追问**：为什么监听器在 EventLoop 线程执行？保证回调与 Channel 状态变更在同一线程，避免并发问题。

### Q33. Netty 客户端如何正确实现断线重连？

**结论**：监听 `channelInactive` 或 `closeFuture`，用 EventLoop 的定时任务做退避重连，并避免在旧 Channel 上复用状态。

**机制**：
- `bootstrap.connect()` 返回 Future，失败与断开都要处理。
- 重连间隔采用指数退避 + 抖动，避免雪崩。
- 重连前清理旧 Pipeline 状态与引用计数。
- 用 `EventLoop.schedule` 而不是 `Thread.sleep`。

**追问**：为什么不能在 Handler 里直接 `while(true)` 重连？会阻塞 EventLoop，且旧连接资源无法释放。

### Q34. 如何优雅关闭 Netty 服务？

**结论**：先关闭 ServerChannel 停止接收新连接，再 `shutdownGracefully` 等待存量任务与连接处理完。

**机制**：
- `serverChannel.close()` 停止 Accept。
- `bossGroup.shutdownGracefully(quietPeriod, timeout, unit)` 与 worker 同理。
- 等待 `terminationFuture`，确认线程池退出。
- 关闭前可主动通知客户端、停止心跳、刷新写缓冲。

**追问**：直接 `shutdownNow` 有什么风险？会中断正在处理的任务，消息可能丢失，引用计数可能未释放。

### Q35. Netty 与原生 NIO 的差异？Netty 5 为什么没有成为主流？

**结论**：Netty 在 NIO 之上补了线程模型、内存管理、Pipeline、编解码与容错；Netty 5 因 API 激进与维护成本回退到 4.x 主线。

**机制**：
- 原生 NIO 需要自己处理 Selector 空转、半包、释放、异常、重连。
- Netty 提供成熟的 EventLoop、ByteBuf、Codec、Handler 体系。
- Netty 5 尝试 ForkJoinPool 与新的 API，但兼容性与收益不足，最终 4.1 持续演进。
- 现在新项目通常选 4.1.x，关注安全公告与版本升级。

**追问**：Netty 4.2 有什么变化？引入新的自适应分配器与部分 API 调整，但核心模型仍是 EventLoop + Pipeline + ByteBuf。

---

## 六、进阶补充（Q36–Q40）

### Q36. Native Epoll Transport 与 NIO 有什么区别？

**结论**：Linux 上 epoll 原生传输减少空轮询与系统调用，适合高并发长连接；NIO 跨平台但有一层 JDK Selector 封装。

**机制**：
- `EpollEventLoopGroup` 使用 epoll 边缘触发，减少无效唤醒。
- 规避 JDK Selector 空轮询问题。
- 需要引入 `netty-transport-native-epoll` 与对应 classifier。
- 非 Linux 平台回退 NIO。

**追问**：什么时候选 epoll？Linux 高并发网络服务；容器环境需确认内核与依赖。

### Q37. Netty 如何支持 HTTP/2？

**结论**：通过 `Http2FrameCodec`/`Http2MultiplexHandler` 支持多路复用、头部压缩与流式传输，h2/h2c 均可。

**机制**：
- HTTP/2 用 Stream ID 复用一条连接。
- Header 压缩（HPACK）减少开销。
- TLS 场景需 ALPN 协商 h2。
- 业务 Handler 按 stream 隔离。

**追问**：HTTP/2 一定比 HTTP/1.1 快吗？高并发多请求下优势明显，小请求受协议开销影响。

### Q38. Netty 的 SslHandler 如何工作？

**结论**：作为 Pipeline 的首个入站 Handler 处理 TLS 握手与加解密，业务 Handler 看到明文。

**机制**：
- `SslContextBuilder` 配置证书与信任链。
- `SslHandler` 负责握手、ALPN、会话复用。
- 握手完成前写操作会排队。
- 需要正确配置 TLS 版本与套件。

**追问**：为什么 SslHandler 要放最前面？保证所有入站数据先解密、出站数据最后加密。

### Q39. Netty 的内存分配器如何调优？

**结论**：优先 PooledByteBufAllocator + Direct，结合水位、Arena/Chunk 参数与泄漏检测，按压测调整。

**机制**：
- `PooledByteBufAllocator` 默认，减少分配与 GC。
- `-Dio.netty.allocator.numDirectArenas` 控制 Arena 数。
- 监控堆外内存与 `ResourceLeakDetector`。
- 避免频繁创建大 ByteBuf。

**追问**：堆外内存持续上涨怎么查？开启泄漏检测，结合 NMT/系统内存与分配堆栈。

### Q40. Netty 在 RPC/网关中的典型设计是什么？

**结论**：长连接 + 自定义协议编解码 + 心跳 + 业务线程池 + 背压，Netty 负责 IO，业务层负责编排。

**机制**：
- 协议头携带长度、类型、序列化 ID、请求 ID。
- 心跳与空闲检测维持连接。
- 请求 ID 关联异步响应。
- 业务线程池隔离耗时逻辑，避免阻塞 EventLoop。
- 写缓冲水位线做背压。

**追问**：Netty 与 Spring WebFlux 如何选？需要自定义二进制协议/极致控制选 Netty；HTTP 响应式服务可用 WebFlux。

---

## 七、进阶补充（Q41–Q45）

### Q41. Netty 的 FastThreadLocal 与 ThreadLocal 有什么区别？

**结论**：FastThreadLocal 用数组下标代替哈希表，减少冲突与清理开销，配合 FastThreadLocalThread 效果最佳。

**机制**：
- 每个 FastThreadLocal 分配稳定 index。
- 线程内部用数组直接定位。
- 需调用 `remove()` 避免残留。
- 普通 Thread 上仍可用，但无法完全发挥优势。

**追问**：FastThreadLocal 会内存泄漏吗？会，线程池复用时不 remove 会残留；Java ThreadLocal 基础见 [228 Q20](228_Java核心与并发面试题库.md)。

### Q42. Netty 的 HashedWheelTimer 是什么？

**结论**：基于时间轮的高效定时器，适合大量短周期任务，精度为 tick 级别。

**机制**：
- 任务按槽位分布，指针轮转触发。
- 插入/删除近似 O(1)。
- 不适合超长延时或高精度场景。
- Netty 用于空闲检测、重连等。

**追问**：时间轮精度如何保证？通过 tickDuration 与 wheelSize 平衡精度与内存。

### Q43. Pipeline 中的异常如何传播？

**结论**：入站异常沿 `exceptionCaught` 向后传播，默认最终关闭 Channel；出站异常通常体现在写 Future。

**机制**：
- Handler 抛异常触发当前 Context 的 exceptionCaught。
- 未处理则继续向后传播。
- 默认实现记录日志并关闭连接。
- 写失败通过 ChannelFuture 通知。

**追问**：为什么异常会静默？Handler 吞掉异常或未覆写 exceptionCaught，需统一兜底。

### Q44. 常用的 ChannelOption 有哪些？

**结论**：SO_BACKLOG、TCP_NODELAY、SO_KEEPALIVE、SO_REUSEADDR、ALLOCATOR、写水位线等。

**机制**：
- `SO_BACKLOG` 控制 Accept 队列。
- `TCP_NODELAY` 禁用 Nagle，降低小包延迟。
- `SO_KEEPALIVE` 依赖系统级保活。
- `ALLOCATOR` 选择池化/堆外分配器。
- 水位线控制写缓冲。

**追问**：TCP_NODELAY 一定开启吗？低延迟交互建议开启，大吞吐批量场景可权衡。

### Q45. EventLoop 的任务队列会无限增长吗？

**结论**：NioEventLoop 使用无界任务队列，任务提交不会拒绝，但堆积会占用内存并延迟 IO。

**机制**：
- `execute` 入队并唤醒 Selector。
- 无界队列没有容量保护。
- 大量任务会拖慢 IO 处理。
- 关闭时任务可能被拒绝/丢弃。

**追问**：如何防任务堆积？业务侧限流、监控队列长度、避免在 EventLoop 提交大量任务；任务提交方式见 [Q6](220_Netty面试题库.md)。

---

## 八、进阶补充（Q46–Q50）

### Q46. Netty 如何支持 WebSocket？

**结论**：用 `WebSocketServerProtocolHandler` 处理握手与帧编解码，业务 Handler 处理文本/二进制帧。

**机制**：
- HTTP 升级握手由 Handler 完成。
- 支持 TextWebSocketFrame/BinaryWebSocketFrame。
- 内置 Ping/Pong 与关闭帧处理。
- 可配置路径与子协议。

**追问**：WebSocket 需要心跳吗？需要，长连接仍需应用层保活与断线重连。

### Q47. Netty 的编解码器如何选型？

**结论**：按协议与性能选择 Protobuf、JSON、自定义二进制；长度字段 + 类型标识是通用做法。

**机制**：
- Protobuf 体积小、跨语言，需 IDL。
- JSON 可读性好但体积大。
- 自定义二进制需版本与兼容设计。
- 编解码器要与协议头、序列化 ID 对齐。

**追问**：编解码器能复用吗？无状态可 Sharable，有状态需每连接实例；基础见 [Q29–Q30](220_Netty面试题库.md)。

### Q48. Pipeline 运行中能动态增删 Handler 吗？

**结论**：可以，但应在 EventLoop 线程操作，避免并发修改导致事件顺序错乱。

**机制**：
- `pipeline.addLast/remove` 线程安全由 EventLoop 保证。
- 动态变更会影响后续事件传播。
- 读事件处理中增删需谨慎。
- 可用 Context 定位插入位置。

**追问**：动态增删会导致内存泄漏吗？移除的 Handler 若有资源需手动释放；基础见 [Q12](220_Netty面试题库.md)。

### Q49. Netty 服务应监控哪些指标？

**结论**：连接数、读写字节、EventLoop 任务积压、写缓冲水位、ByteBuf 内存与泄漏、异常率。

**机制**：
- Channel 活跃数与新建/关闭速率。
- EventLoop pending tasks 与 select 时间。
- `ChannelOutboundBuffer` 水位。
- 堆外内存与泄漏检测。
- 业务 Handler 异常统计。

**追问**：如何发现 EventLoop 被阻塞？看任务队列、IO 延迟与线程栈；水位见 [Q31](220_Netty面试题库.md)。

### Q50. Netty 版本升级要注意什么？

**结论**：核对 API 变更、依赖与 Native Transport 兼容，先灰度并压测，关注安全公告。

**机制**：
- 4.1→4.2 有 API 与行为调整。
- Native epoll 需匹配 classifier。
- 编解码与内存分配器默认值可能变化。
- 升级后回归连接、内存与性能。

**追问**：能长期停留在旧版本吗？安全漏洞与兼容性风险会增加，需定期评估。

---

## 九、速记清单

| 主题 | 必背一句话 |
|---|---|
| 线程模型 | 一个 Channel 绑定一个 EventLoop，单线程串行化免锁 |
| Pipeline | Inbound 头到尾，Outbound 尾到头 |
| ByteBuf | 读写索引分离 + 池化 + 引用计数 |
| 零拷贝 | Composite、slice、Direct、FileRegion |
| 粘包 | 长度字段 + 协议头最通用 |
| 可靠性 | 高低水位线 + 引用计数 + 优雅关闭 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：50 题 · 详解**
