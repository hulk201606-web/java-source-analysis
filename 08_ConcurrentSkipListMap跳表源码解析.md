# 08 · ConcurrentSkipListMap 跳表源码解析

> 源码位置：`src/java/util/concurrent/ConcurrentSkipListMap.java`  
> 面试权重：⭐⭐⭐⭐⭐（TreeMap 并发版 + 无锁跳表 + 异步删除 + 多线程协作）  
> 前置：建议先理解 TreeMap 红黑树 + CAS 自旋

---

## 一、跳表 vs 红黑树

```
红黑树（TreeMap）：
  每层都是完整 BST → O(log n) 查找
  旋转 + 变色 → 实现复杂
  需要锁保护（ConcurrentHashMap 的树化需要 synchronized）

跳表（ConcurrentSkipListMap）：
  多层链表 + skip 指针 → O(log n) 查找
  按概率建树（p=0.5）→ 实现简单
  ★ 用 CAS 自旋 → 无锁并发！
  → 平均每节点 2 个指针（next + down），内存开销比红黑树多
```

**跳表层级概率**：
```
JDK 源码硬编码：p = 0.5（k=1, p=0.5）
  层数 1 概率：50%
  层数 2 概率：25%
  层数 3 概率：12.5%
  层数 k 概率：p^(k-1) = 0.5^(k-1)

每层平均节点数：1/p = 2
查找/插入/删除复杂度：O(log n)（期望值）
```

---

## 二、核心数据结构

### 2.1 Node · 底层链表节点

**源码锚点**：L417-508

```java
static final class Node<K,V> {
    final K key;
    volatile Object value;           // ★ volatile！可见性
    volatile Node<K,V> next;         // ★ volatile！可见性 + CAS

    // 正常节点构造
    Node(K key, Object value, Node<K,V> next) { ... }

    // marker 节点构造（删除标记）
    Node(Node<K,V> next) {
        this.key = null;
        this.value = this;           // ★ value == this → marker！
        this.next = next;
    }

    boolean casValue(Object cmp, Object val) { ... }   // CAS value
    boolean casNext(Node<K,V> cmp, Node<K,V> val) { ... }   // CAS next

    boolean isMarker() { return value == this; }       // 自身标记
    boolean isBaseHeader() { return value == BASE_HEADER; }   // 哨兵标记

    boolean appendMarker(Node<K,V> f) {                // ★ 两步删除第一步
        return casNext(f, new Node<K,V>(f));           // 在 n 和 n.next 之间插 marker
    }

    void helpDelete(Node<K,V> b, Node<K,V> f) {        // ★ 多线程协作删除
        if (f == next && this == b.next) {
            if (f == null || f.value != f)              // 还没插 marker → 帮忙插
                casNext(f, new Node<K,V>(f));
            else
                b.casNext(this, f.next);               // marker 已有 → 帮忙 unlink
        }
    }
}
```

**Node 三种身份**：
```
1. 正常节点：key!=null, value!=null, value!=this, value!=BASE_HEADER
   → 存放 KV 数据

2. marker 节点（删除标记）：key=null, value==this
   → 插在"要删的节点 n"和"n.next"之间
   → 告诉其他线程：n 正在被删除

3. base-level header（哨兵）：key=null, value==BASE_HEADER
   → 每层链表的头（HeadIndex.node 指向它）
```

### 2.2 Index · 跳表索引节点

**源码锚点**：L565-632

```java
static class Index<K,V> {
    final Node<K,V> node;      // ★ 指向底层 Node
    final Index<K,V> down;      // 下层 Index（volatile? 不 volatile → 构造后不变）
    volatile Index<K,V> right; // ★ volatile！CAS 竞争

    boolean indexesDeletedNode() { return node.value == null; }  // 指向的 Node 被删了？

    final boolean link(Index<K,V> succ, Index<K,V> newSucc) {
        Node<K,V> n = node;
        newSucc.right = succ;
        return n.value != null && casRight(succ, newSucc);  // node 没被删才 link
    }

    final boolean unlink(Index<K,V> succ) {
        return node.value != null && casRight(succ, succ.right);  // CAS unlink succ
    }
}
```

### 2.3 HeadIndex · 层头

**源码锚点**：L639-644

```java
static final class HeadIndex<K,V> extends Index<K,V> {
    final int level;   // ★ 当前这一层的 level 值
    HeadIndex(Node<K,V> node, Index<K,V> down, Index<K,V> right, int level) {
        super(node, down, right);
        this.level = level;
    }
}
```

### 2.4 完整结构图

```
head (HeadIndex level=3)
  │ node → BASE_HEADER
  │
  │ down
  │
HeadIndex(3) ──right──▶ Index(n7) ──right──▶ Index(n12) ──right──▶ null
  │                       │                     │
  │ down                  │ down                │ down
  ▼                       ▼                     ▼
HeadIndex(2) ──right──▶ Index(n3) ──right──▶ Index(n7) ──right──▶ Index(n12) ──right──▶ null
  │                       │                     │                    │
  │ down                  │ down                │ down               │ down
  ▼                       ▼                     ▼                    ▼
HeadIndex(1) ──right──▶ Index(n1) ──right──▶ Index(n3) ──right──▶ Index(n7) ──right──▶ Index(n12) ──right──▶ null
  │                       │                     │                    │                    │
  │ down                  │ down                │ down               │ down               │ down
  ▼                       ▼                     ▼                    ▼                    ▼
BASE_HEADER ──next──▶ n1 ──next──▶ n3 ──next──▶ n7 ──next──▶ n12 ──next──▶ null
  │                                                       │
  └─ node 指向同一底层链表（base-level）─────────────────────┘

每层是一个有序链表，down 指向下一层同 key 的 Index
查找从最高层开始，每层往右走直到 key > target → 降层
→ O(log n) 复杂度（平均每层 2 个节点，跳一半跳过）
```

---

## 三、findPredecessor · 查找前驱（核心中的核心）

**源码锚点**：L668-694

```java
private Node<K,V> findPredecessor(Object key, Comparator<? super K> cmp) {
    if (key == null) throw new NullPointerException();
    for (;;) {
        for (Index<K,V> q = head, r = q.right, d;;) {
            if (r != null) {
                Node<K,V> n = r.node;
                K k = n.key;
                if (n.value == null) {          // ★ r 指向的 Node 已被删！
                    if (!q.unlink(r)) break;     // CAS 从 Index 层移除 r → 失败则 restart
                    r = q.right;                 // 重新读 r（CAS 后 right 可能变了）
                    continue;
                }
                if (cpr(cmp, key, k) > 0) {     // key > r.node.key → 往右走
                    q = r;
                    r = r.right;
                    continue;
                }
            }
            // key <= r.node.key 或 r==null → 降层
            if ((d = q.down) == null)            // 没下一层了 → q.node 就是前驱
                return q.node;
            q = d;
            r = d.right;
        }
    }
}
```

**findPredecessor 路径图解**：
```
目标 key=7，从 HeadIndex(3) 开始：

  HeadIndex(3) ──r──▶ Index(n12)    // n12.key=12 > 7 → 不往右 → 降层
       │ down
  HeadIndex(2) ──r──▶ Index(n7) ──r──▶ Index(n12)   // n7.key=7 == 7 → 再往右？
                                                     // n12.key=12 > 7 → 降层
       │ down
  HeadIndex(1) ──r──▶ Index(n3) ──r──▶ Index(n7) ──r──▶ Index(n12)
                     n3.key=3 < 7 → q=r 继续
                                                     // 降到 base-level！
       │ down
  BASE_HEADER ──▶ n1 ──▶ n3 ──▶ n7 ──▶ n12
                  n1<7  n3<7  n7==7
  
  q = Index(n3), d = q.down → Index(n3) 的 down = 指向 base-level n3 的 Index
  但这层是 base-level → q.down==null → return q.node
  
  findPredecessor(7) 返回 n3 ✓ 正确前驱！
```

**findPredecessor 的关键行为**：
```
1. 从最高层 HeadIndex.right 开始向右走
2. r.node 已删除（value==null）→ q.unlink(r) 从 Index 层移除（CAS）
   → 多线程同时 findPredecessor 都在帮着清理！
3. key > r.node.key → q=r, r=r.right（继续向右）
4. key <= r.node.key 或 r==null → 降层 q=q.down
5. down==null → q.node 就是前驱（base-level 的 Node）

★ 为什么 unlink 失败要 break 外层 for(;;)?
   → CAS 冲突说明并发修改了 → 需要完全 restart
```

---

## 四、doPut · 插入核心

**源码锚点**：L817-932

### 4.1 Base-level 插入

```java
outer: for (;;) {
    for (Node<K,V> b = findPredecessor(key, cmp), n = b.next;;) {
        if (n != null) {
            Object v; int c;
            Node<K,V> f = n.next;
            if (n != b.next)               // 三检查 1：b.next 不是 n → 被别人改了
                break;                     // → break 内层，外层 for(;;) 重新 findPredecessor
            if ((v = n.value) == null) {   // 三检查 2：n 被删了
                n.helpDelete(b, f);        // 帮忙删
                break;
            }
            if (b.value == null || v == n) // 三检查 3：b 被删了 或 b 是 marker
                break;
            if ((c = cpr(cmp, key, n.key)) > 0) {
                b = n; n = f; continue;
            }
            if (c == 0) {
                // key 已存在 → CAS 替换 value
                if (onlyIfAbsent || n.casValue(v, value)) return vv;
                break;  // CAS 失败 → restart
            }
        }

        z = new Node<K,V>(key, value, n);   // 新建 Node（next = n）
        if (!b.casNext(n, z))               // ★ CAS 把 b.next 从 n 改成 z
            break;                           // CAS 失败 → restart
        break outer;                         // CAS 成功！→ 跳出 outer，去建索引
    }
}
```

### 4.2 三检查机制

```
每次进入 base-level 遍历都做三次"快照有效性"检查：
1. n != b.next？→ 说明 b 的 next 被并发修改了（别人插/删了节点）
2. n.value == null？→ n 被删了
3. b.value == null 或 v == n？→ b 被删了 或 b 是 marker

任何检查失败 → break 内层 → 外层 for(;;) 重新 findPredecessor
→ 这是经典的"CAS 自旋 + 一致性快照"无锁设计
```

### 4.3 随机层级 + 索引插入

```java
// 随机层级（p=0.5）
int rnd = ThreadLocalRandom.nextSecondarySeed();
if ((rnd & 0x80000001) == 0) {    // 最高位+最低位 都为 0 才能建树
    int level = 1, max;
    while (((rnd >>>= 1) & 1) != 0)   // 连续最低位为 1 → 层数 +1
        ++level;                       // 平均 level = 2（p=0.5）
    
    // 如果 level > head.level → 还需要新建 HeadIndex 层！
    if (level <= (max = h.level)) {
        // 在已有层级建 Index 链：bottom-up 建
        for (int i = 1; i <= level; ++i)
            idx = new Index<K,V>(z, idx, null);   // down 指向上一层
    } else {
        // level > max → CAS 更新 head.level + 新建 HeadIndex 层
        // 用 CAS 避免多线程同时加层
        ...
    }
    
    // ★★★ splice：从高到低每层插入 Index（类似 findPredecessor 反过来）
    splice: for (int insertionLevel = level;;) {
        int j = h.level;
        for (Index<K,V> q = h, r = q.right, t = idx;;) {
            if (j == insertionLevel) {
                if (!q.link(r, t)) break;   // CAS 插入 Index
                if (t.node.value == null) {
                    findNode(key);           // node 被删了 → 清理
                    break splice;
                }
                if (--insertionLevel == 0) break splice;   // 所有层插完！
            }
            if (--j >= insertionLevel && j < level) t = t.down;
            q = q.down; r = q.right;
        }
    }
}
```

---

## 五、doRemove · 无锁两步删除

**源码锚点**：L955-996

### 5.1 删除三步流程

```java
// ① 找到节点 n（三检查 + findPredecessor + base-level 遍历）
// ② CAS n.value → null（逻辑删除）
if (!n.casValue(v, null))
    break;   // CAS 失败 → restart

// ③ appendMarker + CAS unlink（物理删除）
if (!n.appendMarker(f) || !b.casNext(n, f))
    findNode(key);     // 并发冲突 → 通过 findNode 清理 + retry
else {
    findPredecessor(key, cmp);   // ★ 清理 Index 层（unlink 指向 n 的 Index）
    if (head.right == null)
        tryReduceLevel();        // 顶层空了 → 降层
}
return vv;
```

### 5.2 两步删除图解

```
初始：b → n → f → ...
                    n.value = 5

步骤 ①：CAS n.value: 5 → null（逻辑删除）
  b → n(value=null) → f → ...

步骤 ②：appendMarker(f) → n.next 插入 marker
  b → n(value=null) → marker(value=this) → f → ...
  marker 节点：key=null, value=this, next=f

步骤 ③：CAS b.next: n → f（物理 unlink）
  b → f → ...    （n 和 marker 脱离链表 → GC）

为什么要两步？
  → 一步直接 CAS b.next: n → f
    如果另一个线程正在 findPredecessor，可能刚读完 b.next=n
    → 它会认为 n 还在链上 → 可能基于过期快照做错误操作
  → 两步：先 CAS value=null 标记，再 appendMarker，最后 unlink
    所有遍历代码都检查 value==null → 标记后所有线程都能看到"正在被删"
    appendMarker 让其他线程知道"有人正在删，帮忙完成"
```

### 5.3 多线程协作删除（helpDelete）

```
场景：
  T1 正在 doRemove(key=7)：
    CAS n.value = null ✅（步骤①完成）
    CAS b.next = f ❌（被其他线程 T2 插了新节点到 b 和 n 之间 → b.next != n 了）
    → 调用 findNode(key=7) 清理 → return null
  
  T2 正在 findPredecessor(key=7)：
    遍历到 n.value==null → 看到被删除
    → 调用 n.helpDelete(b, f)
    → f 不是 marker（f.value != f）→ 帮忙 casNext(f, new Node(f)) 插 marker
    → 或 f 已经是 marker → 帮忙 b.casNext(this, f.next) 物理删除

→ 这就是 JDK 的"乐观并发控制 + 多线程协作"：
  一个线程删失败了，其他遍历线程会帮忙完成！
  没人"拥有"删除锁，但所有线程都能"帮助"删除
```

### 5.4 tryReduceLevel · 避免层级抖动

**源码锚点**：L1018-1045

```java
private void tryReduceLevel() {
    HeadIndex<K,V> h = head;
    HeadIndex<K,V> d; HeadIndex<K,V> e;
    // 只有最顶层三层都空才降层
    // （避免频繁 insert/delete 导致 level 来回抖动）
    if (h.level > 3 &&
        (d = (HeadIndex<K,V>)h.down) != null && d.right == null &&
        (e = (HeadIndex<K,V>)d.down) != null && e.right == null &&
        e.down != null && e.down.down != null) {
        HeadIndex<K,V> newh = new HeadIndex<>(h.node, h.down.down, null, h.level-1);
        if (casHead(h, newh)) return;
    }
}
```

**为什么顶层三层都空才降？**
```
场景：频繁 insert/delete 某个 key
  → delete 后顶层只有这个 key 的 Index
  → delete 完成 → 顶层空了 → 降层
  → 立刻又 insert → 升层
  → 再 delete → 再降层
  → 反复 CAS 升降层 → 性能抖动

JDK 解法：只有顶层三层全空才降一层
  → 避免单次 delete 就降层
  → 至少 3 次 delete 顶层变空 → 才降层
  → "宁可层级多一点，也不要频繁抖动"
```

---

## 六、get · doGet

**源码锚点**：L776-805

```java
private V doGet(Object key) {
    Comparator<? super K> cmp = comparator;
    outer: for (;;) {
        for (Node<K,V> b = findPredecessor(key, cmp), n = b.next;;) {
            Object v; int c;
            if (n == null) break outer;
            Node<K,V> f = n.next;
            // ★★★ 和 doPut/doRemove 完全一样的三检查！
            if (n != b.next) break;              // 检查 1
            if ((v = n.value) == null) {         // 检查 2：n 被删
                n.helpDelete(b, f);
                break;
            }
            if (b.value == null || v == n) break;  // 检查 3：b 被删
            if ((c = cpr(cmp, key, n.key)) == 0) {
                return (V)v;   // ★ 找到！返回 value
            }
            if (c < 0) break outer;
            b = n; n = f;
        }
    }
    return null;
}
```

**doGet 的精妙之处**：
```
1. get 是只读 → 不修改任何结构
2. 但遍历时遇到被删节点 → 调用 helpDelete 帮忙清理！
3. findPredecessor 也会 CAS unlink 被删 Index → 帮着清理索引层
→ 所有"读"操作都在帮着"写"操作做垃圾回收！
→ 这就是为什么 ConcurrentSkipListMap 不需要专门的 GC 线程
```

---

## 七、并发模型总结

| 操作 | 锁 | 关键机制 |
|------|-----|---------|
| **put** | 无 | CAS b.next(n→z) + randomLevel + CAS Index.right 从高到低插 |
| **get** | 无 | findPredecessor（帮清被删 Index）+ base-level 三检查 + helpDelete |
| **remove** | 无 | CAS value→null + appendMarker + CAS unlink + findPredecessor 清 Index + tryReduceLevel |
| **降层** | 无 | CAS casHead(h, newh)，只有顶层三层空才降 |

**多线程协作机制**：
```
1. 乐观并发控制：所有操作都是 CAS 自旋 + 三检查重试
2. 多线程帮助删除：helpDelete() 任何线程看到被删节点都能帮忙
3. findPredecessor 副作用：自动清理路径上的被删 Index
4. 无"所有权"：没有线程"拥有"任何锁，任何线程都能做任何事
5. 随机化：ThreadLocalRandom 避免所有线程同时 CAS 同一位置
```

---

## 八、ConcurrentSkipListMap vs TreeMap vs ConcurrentHashMap

| 维度 | **ConcurrentSkipListMap** | **TreeMap** | **ConcurrentHashMap** |
|------|--------------------------|------------|----------------------|
| 底层结构 | **无锁跳表** | 红黑树（单线程）| 分段锁 + 数组+链表+红黑树 |
| 有序？| ✅ key 有序 | ✅ key 有序 | ❌ |
| 线程安全？| ✅ **完全无锁**（CAS 自旋）| ❌ | ✅ 分段锁（JDK8 synchronized 树）|
| 查找 | O(log n) | O(log n) | O(1)（平均）/ O(log n)（退化）|
| 并发性能 | ⭐⭐⭐⭐⭐ | ❌ 不同步 | ⭐⭐⭐⭐ |
| 内存开销 | 每层 2 指针（平均 2 层）| 每个 Entry parent+color | 桶位数组（2×N）|
| 适用场景 | **并发 + 有序** | 单线程 + 有序 | 通用并发 KV |

---

## 九、面试考点补全

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **核心结构** | | |
| 1 | ConcurrentSkipListMap 用什么数据结构？| **跳表**（Skip List）+ 无锁 CAS + 多层 Index L565-632 |
| 2 | Node 的 value 有哪几种身份？| 正常值 / **null（逻辑删除）** / **this（marker 标记）** / **BASE_HEADER（哨兵）** L417-486 |
| 3 | Index 为什么只有 right volatile？| down 指针构造后不变 → 不需要 volatile；right 会被 CAS 修改 → 必须 volatile L568 |
| **findPredecessor** | | |
| 4 | findPredecessor 清理被删 Index 是 CAS 吗？| 是的！**q.unlink(r) → casRight(succ, succ.right)** L615-617 |
| 5 | findPredecessor 失败为什么要 restart？| CAS 冲突说明并发修改 → 外层 for(;;) 重新找起点 L668-694 |
| **doPut** | | |
| 6 | doPut 三检查是什么？| **① n != b.next（b.next 被改了）② n.value == null（n 被删）③ b.value == null 或 v==n（b 被删/是 marker）** L827-833 |
| 7 | 随机层级的概率？| **p=0.5**，`rnd & 0x80000001 == 0` 才建树 → 连续最低位为 1 层数 +1 L857-861 |
| 8 | 新建 Node 后 CAS 什么？| **b.casNext(n, z)** → 把 b.next 从 n 改成新 Node z L851 |
| 9 | Index 层怎么插入？| splice 循环：**从最高层往下**，每层 q.link(r, t) CAS 插入 L891-929 |
| **doRemove** | | |
| 10 | 无锁删除是一步还是两步？| **两步**：① CAS value→null（逻辑删除）② appendMarker + CAS unlink（物理删除）L982-984 |
| 11 | appendMarker 做什么？| **在 n 和 n.next 之间插 marker 节点**（marker.value=this 标记）L484-486 |
| 12 | 多线程怎么协作删除？| **helpDelete(b, f)**：任何线程看到 value==null 都能帮忙插 marker 或 unlink L495-507 |
| 13 | findNode 调 findPredecessor 清理 Index？| 是的！**findPredecessor 遍历路径上自动 CAS unlink 被删 Index** → 所有读操作都在帮清理 |
| 14 | tryReduceLevel 为什么顶层三层空才降？| **避免 insert/delete 频繁抖动层级** → 宁可多保留一层 L1018-1045 |
| **并发模型** | | |
| 15 | ConcurrentSkipListMap 有锁吗？| **没有！完全基于 CAS 自旋 + 多线程协作** |
| 16 | 为什么 get 不修改结构也调 helpDelete？| **读操作副作用清理垃圾** → 不需要专门 GC 线程 |
| 17 | ThreadLocalRandom 在 insert 中做什么？| **随机层级 + 随机化 CAS 竞争位置** → 避免所有线程同时 CAS 同一位置 L857 |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线：JDK 1.8.0_451**  
**覆盖：跳表结构 + Node/Index/HeadIndex + findPredecessor CAS 清理 + doPut 三检查 + doRemove 两步删除 + helpDelete 多线程协作 + tryReduceLevel 防抖 + 17 个面试考点**
