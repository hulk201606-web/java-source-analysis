# TiDB SQL Parser 源码级深度解析

> **源码基线**：TiDB v8.0.x（Go）  
> **核心仓库**：pingcap/tidb → parser/  
> **面试权重**：⭐⭐⭐⭐（TiDB 核心模块，SQL 解析链路）

---

## 一、TiDB SQL Parser 核心地位

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB SQL Parser = TiDB 核心入口                          │
│                                                               │
│  SQL 完整处理链路：                                           │
│  ┌───────────────────────────────────────────────────────┐   │
│  │ 客户端连接 TiDB                                       │   │
│  │   ↓ 接收 SQL 字符串                                    │   │
│  │                                                         │   │
│  │ ★ Step 1: Lexer（词法分析）                            │   │
│  │   SELECT * FROM users WHERE age > 25;                  │   │
│  │   → 拆成 Token 流：                                    │   │
│  │   [SELECT] [*] [FROM] [users] [WHERE] [age] [>] [25]  │   │
│  │                                                         │   │
│  │ ★ Step 2: Parser（语法分析 ★ 本文重点！）             │   │
│  │   Token 流 → AST（抽象语法树）                         │   │
│  │   → SelectStmt{From:[users], Where: age>25, ...}      │   │
│  │                                                         │   │
│  │ Step 3: Preprocessor（预处理）                         │   │
│  │   → Name Resolution（表/列名解析）                     │   │
│  │   → Type Inference（类型推导）                        │   │
│  │                                                         │   │
│  │ Step 4: Optimizer（CBO 优化）                          │   │
│  │   → 逻辑计划 → 物理计划（选最优 Join/Index）           │   │
│  │                                                         │   │
│  │ Step 5: Executor（执行）                                │   │
│  │   → 编译好的计划 → 发 RPC 给 TiKV Region               │   │
│  └───────────────────────────────────────────────────────┘   │
│
│ ★ TiDB Parser 技术选型（面试高频）：                          │
│   Antlr4：最成熟的 LL(*) 文法工具                           │
│   → 把 MySQL 官方 .y 语法（Yacc）翻译成 Antlr4 语法        │
│   → 生成 Go 代码（lexer.go + parser.go）                  │
│   → TiDB 自己写了 AST 节点 + Visitor 模式                  │
│
│ ★ TiDB Parser vs MySQL Parser vs Calcite：                   │
│   TiDB Parser：Antlr4 + 自研 AST → 完全兼容 MySQL 8.0      │
│   MySQL Parser：自研递归下降 + Bison                       │
│   Calcite Parser：Apache Calcite 通用引擎 → Doris/Flink/Spark 用 │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Lexer 词法分析源码级

### 2.1 核心入口

```go
// ★ TiDB Lexer 入口（源码级）
//
// // parser/lexer.go
//
// type Lexer struct {
//     src       []byte     // SQL 源码字节数组
//     pos       int        // 当前位置
//     bufPos    int        // 下一个 Token 位置
//     lastPos   int        // 上一个 Token 结束位置
//     // ...
// }
//
// // ★ Lex 核心方法：逐个 Token 解析
// func (l *Lexer) Lex() (tok token.TokenType, pos int, literal string) {
//     l.skipWhitespaceAndComment()  // 跳过空格和注释
//
//     // 关键字匹配（SELECT/FROM/WHERE/...）
//     if tok := l.matchKeyword(); tok != token.Unknown {
//         return tok, l.pos, l.literal()
//     }
//
//     // 标识符（表名/列名）
//     if l.isIdentStart() {
//         return l.readIdent()
//     }
//
//     // 数字（整数/浮点）
//     if l.isDigitStart() {
//         return l.readNumber()
//     }
//
//     // 字符串常量
//     if l.isStringStart() {
//         return l.readString()
//     }
//
//     // 运算符（+/-/*///>/</=等）
//     return l.matchOperator()
// }
//
// // 例子："SELECT * FROM users WHERE age > 25"
// // Lexer.Lex() 调用 N 次：
// //   1: Lex() → token.SELECT, literal="SELECT"
// //   2: Lex() → token.Star, literal="*"
// //   3: Lex() → token.FROM, literal="FROM"
// //   4: Lex() → token.Ident, literal="users"
// //   5: Lex() → token.WHERE, literal="WHERE"
// //   6: Lex() → token.Ident, literal="age"
// //   7: Lex() → token.GT, literal=">"
// //   8: Lex() → token.IntLit, literal="25"
// //   9: Lex() → token.EOF, literal=""
//
```

---

## 三、Parser 语法分析源码级

### 3.1 Antlr4 文法文件

```
// ★ TiDB Antlr4 文法文件（parser/parser.y → antlr4 转换）
//
// // parser/parser.y（MySQL 官方语法，TiDB 适配）
//
// %token <strval> SELECT FROM WHERE GROUP ORDER BY HAVING LIMIT
// %token <strval> Ident IntLit StringLit
// %token <strval> GT LT EQ NE GE LE
//
// %%
//
// // ★ SelectStmt 语法规则（简化）
// select_stmt:
//     SELECT select_expr_list
//     FROM table_refs
//     where_clause?
//     group_by_clause?
//     having_clause?
//     order_by_clause?
//     limit_clause?
//     ;
//
// select_expr_list:
//     select_expr
//     | select_expr_list ',' select_expr
//     ;
//
// select_expr:
//     '*' | table_name '.' '*'
//     | expr [AS] Identifier
//     ;
//
// where_clause: WHERE expr ;
//
// %%
//
// ★ Antlr4 自动生成：
//   ① Lexer.go（词法分析器）
//   ② Parser.go（语法分析器）
//   ③ 每个规则对应一个 Parse 函数
//
```

### 3.2 AST 节点源码级

```go
// ★ TiDB AST 节点（源码级）
//
// // parser/ast/dml.go
//
// // SelectStmt = SELECT 语句的 AST 根节点
// type SelectStmt struct {
//     // Select 子句
//     Fields     *FieldList        // SELECT 后的字段列表
//
//     // From 子句
//     From       *TableRefsClause  // FROM 后的表引用
//
//     // Where 子句
//     Where      *WhereExpr        // WHERE 后的条件表达式
//
//     // Group By
//     GroupBy    *GroupByClause
//     Having     *Having
//
//     // Order By
//     OrderBy    *OrderByClause
//
//     // Limit
//     Limit      *Limit
//
//     // Lock（SELECT ... FOR UPDATE）
//     Lock       *LockInfo
// }
//
// // WhereExpr = WHERE 条件表达式
// type WhereExpr struct {
//     Expr Expr  // 表达式树（可以嵌套 AND/OR/比较）
// }
//
// // BinaryOperationExpr = 二元运算（a > b, a AND b）
// type BinaryOperationExpr struct {
//     Op  opcode.OpType
//     L   Expr   // 左操作数
//     R   Expr   // 右操作数
// }
//
// ★ 完整例子：SELECT * FROM users WHERE age > 25 AND name = 'Alice'
// AST 结构：
// SelectStmt {
//   Fields: [Star],
//   From:   [TableRef{Name: "users"}],
//   Where:  WhereExpr{
//     Expr: BinaryOperationExpr{
//       Op: OpAnd,
//       L:  BinaryOperationExpr{Op: OpGT, L: Column{Name: "age"}, R: Const{Val: 25}},
//       R:  BinaryOperationExpr{Op: OpEQ, L: Column{Name: "name"}, R: Const{Val: "Alice"}}
//     }
//   }
// }
//
```

### 3.3 Visitor 模式遍历 AST

```go
// ★ TiDB AST Visitor 模式（源码级）
//
// // 遍历 AST 的核心设计（Visitor 模式）
//
// type Visitor interface {
//     Enter(in Node) (out Node, skipChildren bool)
//     Leave(in Node) (out Node, err error)
// }
//
// // 例子：Preprocessor 用 Visitor 做 Name Resolution
// func (p *preprocessor) Enter(in ast.Node) (out ast.Node, skip bool) {
//     switch node := in.(type) {
//     case *ast.ColumnName:
//         // 把列名解析成 Table.Column
//         // 查 TableSchema 拿到列类型
//         return p.resolveColumn(node), false
//     case *ast.TableName:
//         // 把表名解析成 Schema.Table
//         return p.resolveTable(node), false
//     // ... 其他 AST 节点
//     }
// }
//
// // 例子：Optimizer 用 Visitor 做常量折叠
// // SELECT * FROM t WHERE 1 + 1 = 2 → SELECT * FROM t WHERE TRUE
// func (o *optimizer) constantFold(node ast.Node) ast.Node {
//     if binary, ok := node.(*ast.BinaryOperationExpr); ok {
//         left := constantFold(binary.L)
//         right := constantFold(binary.R)
//         if isConst(left) && isConst(right) {
//             // 折叠：直接算出来
//             return compute(binary.Op, left, right)
//         }
//     }
//     return node
// }
//
```

---

## 四、TiDB vs Calcite Parser 对比

```
┌─────────────────────────────────────────────────────────────┐
│  ★ TiDB Parser vs Calcite Parser（面试高频！）               │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┐                 │
│  │            │ TiDB Parser   │ Calcite Parser │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 语法       │ Antlr4（MySQL│ Antlr4（通用）│              │
│  │            │  官方.y 改编）│  CalciteParser│             │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ AST       │ 自研 AST 节点  │ RelNode（关系代数）│         │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ Visitor    │ Enter+Leave  │ Visitor/Shuttle│              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 优化器     │ 自研 CBO      │ Calcite CBO    │              │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 典型应用   │ TiDB          │ Doris/Flink/Spark/Presto │    │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ MySQL 兼容  │ 100% 兼容    │ 不兼容（通用 SQL）│           │
│  └──────────┴──────────────┴──────────────┘                 │
│
│ ★ TiDB Parser 优势（面试高频）：                              │
│   ① 完全兼容 MySQL 8.0 语法（对用户透明）                    │
│   ② Antlr4 生成代码 + 自研 AST → 深度定制                    │
│   ③ 编译 + 优化一体化（Preprocessor + Optimizer）             │
│   ④ 和 TiKV RPC 无缝集成                                     │
│
│ ★ Calcite Parser 优势：                                      │
│   ① 通用 SQL 引擎（不只 MySQL）                              │
│   ② RelNode 关系代数 AST → 优化空间大                       │
│   ③ 开源社区大（Doris/Flink/Spark 都用）                    │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 五、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | TiDB Parser 技术选型？| **Antlr4 + MySQL 官方 .y 语法改编** → 生成 Go 代码 |
| 2 | SQL 处理完整链路？| **Lexer → Parser → Preprocessor → Optimizer → Executor** |
| 3 | Lexer 做什么？| **词法分析：SQL 字符串 → Token 流**（SELECT/*/FROM/Ident/IntLit...）|
| 4 | Parser 做什么？| **语法分析：Token 流 → AST（抽象语法树）** |
| 5 | AST 核心节点？| SelectStmt / BinaryOperationExpr / ColumnName / TableName / Const |
| 6 | Visitor 模式？| **Enter(in) → skipChildren / Leave(in)**；Preprocessor 用它做 Name Resolution |
| 7 | Preprocessor 做什么？| **Name Resolution（表/列名解析）+ Type Inference（类型推导）** |
| 8 | 常量折叠？| Optimizer 里做：1+1=2 → TRUE 简化 |
| 9 | TiDB Parser vs MySQL Parser？| TiDB：Antlr4 + 自研 AST（完全兼容 MySQL 8.0）；MySQL：自研递归下降 + Bison |
| 10 | TiDB vs Calcite Parser？| TiDB：MySQL 专用；Calcite：通用（Doris/Flink/Spark 用）|
| 11 | Antlr4 优势？| **LL(*) 文法 + 自动生成 Lexer/Parser 代码 + 错误报告友好** |
| 12 | TiDB Parser 源码路径？| **parser/lexer.go + parser/parser.go + parser/ast/** |
| 13 | Visitor 源码入口？| `ast.Node.Accept(v Visitor) (Node, error)` |
| 14 | AST 怎么序列化？| 用 JSON 序列化 AST → 方便日志/调试 |
| 15 | 语法错误处理？| Antlr4 内置错误恢复 → 报告错误但继续解析 |
| 16 | 面试最高频考点排序？| **1. 完整 SQL 处理链路 2. Lexer/Parser 区别 3. AST 结构 4. Visitor 模式 5. TiDB vs Calcite** |
| 17 | 学习路径？| parser/lexer.go → Antlr4 文法 → AST 节点 → Visitor Preprocessor → Optimizer |
| 18 | TiDB Parser 为什么重要？| SQL 是 TiDB 入口 → Parser 是入口的入口；AST 是所有后续处理的核心数据结构 |

---

**文档版本：v1.0 · 2026-10-23**  
**源码路径**：pingcap/tidb → parser/  
**面试考点：18 题**  
**差异化点**：Antlr4 + 自研 AST + Visitor 模式 + TiDB vs Calcite
