# Envoy WASM Filter + Go 插件扩展深度源码解析

> **源码基线**：Envoy 1.32.x + Istio 1.22.x  
> **核心仓库**：envoyproxy/envoy + tetratelabs/proxy-wasm-go-sdk  
> **面试权重**：⭐⭐⭐⭐（Service Mesh 定制化扩展最高频）

---

## 一、Envoy 插件扩展全景

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Envoy 四种插件扩展方式（面试最高频！）                    │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 方式       │ 语言/模型     │ 动态加载     │ 性能/场景    │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 原生 C++   │ C++ Filter  │ ❌ 重编译    │ 最高（C++） │   │
│  │ WASM      │ Rust/C/C++   │ ✅ 热加载    │ 高（接近 C++）│   │
│  │ Go 插件   │ Go Filter     │ ✅ 热加载    │ 中（Go 运行时）│   │
│  │ LUA Filter│ OpenResty 风格│ ❌ 重编译   │ 中（OpenResty）│   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ 为什么 WASM 是主流？                                        │
│  ① 跨语言：Rust/C/C++/AssemblyScript 都能编译 WASM          │
│  ② 沙箱安全：WASM 运行在 Envoy 进程内但受限访问              │
│  ③ 热加载：不需要重编译 Envoy → 运维方便                     │
│  ④ 性能高：JIT 编译 → 接近原生 C++ 性能                     │
│  → 业界首选：Envoy WASM + Tetrate proxy-wasm-go-sdk         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 二、WASM Filter 核心架构

### 2.1 WASM Runtime in Envoy

```
┌─────────────────────────────────────────────────────────────┐
│  ★ Envoy WASM Filter 运行架构（面试高频！）                 │
│                                                               │
│  Envoy C++ 进程：                                             │
│  ┌─────────────────────────────────────────────────────┐     │
│  │ Main Envoy Process（C++）                            │     │
│  │  ┌─────────────────────────────────────────────────┐ │     │
│  │  │ WASM VM Runtime（V8 Wasmtime / WAMR）            │ │     │
│  │  │  ┌──────────┬──────────┬──────────┐              │ │     │
│  │  │  │WASM Filter1│WASM Filter2│WASM Filter3│         │ │     │
│  │  │  │（Rust）    │（AssemblyScript）│（Go）  │       │ │     │
│  │  │  └──────────┴──────────┴──────────┘              │ │     │
│  │  └─────────────────────────────────────────────────┘ │     │
│  └─────────────────────────────────────────────────────┘     │
│                                                               │
│  WASM Filter 通过 **ABI（Application Binary Interface）**    │
│  和 Envoy 交互（面试高频）：                                  │
│  ① Envoy → WASM：HTTP 请求/响应事件（onHttpRequestHeaders...）│
│  ② WASM → Envoy：修改 Header/Dispatch/Reject/Log          │
│                                                               │
│ ★ Tetrate proxy-wasm-go-sdk（主流 Go WASM 开发 SDK）：       │
│   把 ABI 封装成 Go 接口 → 开发者写 Go 代码 → 编译成 WASM     │
│   → 类似 WebAssembly 的"Java → WASM"编译器                 │
│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 WASM Filter 开发示例

```go
// ★ Go WASM Filter 开发示例（proxy-wasm-go-sdk）
//
// 需求：给所有出站请求添加自定义 Header "x-custom-filter: v1"
//
// // 用 TinyGo 编译（Go → WASM）
// // tinygo build -o custom_filter.wasm -target=wasi main.go
//
// package main
//
// import (
//     "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
//     "github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
// )
//
// // ★ Context 定义 Filter 生命周期（面试高频！）
// type myHttpFilter struct {
//     types.DefaultHttpContext
// }
//
// type myPluginContext struct {
//     types.DefaultPluginContext
// }
//
// // ★ Main 注册 Filter
// func main() {
//     proxywasm.SetVMContext(&vmContext{})
// }
//
// func (vm *vmContext) OnPluginStart(pluginConfigurationSize int) types.OnPluginStartStatus {
//     return types.OnPluginStartStatusOK
// }
//
// type vmContext struct {
//     types.DefaultVMContext
// }
//
// // ★ 核心回调：onHttpRequestHeaders
// func (ctx *myHttpFilter) OnHttpRequestHeaders(numHeaders int, endOfStream bool) types.Action {
//     // ★ 添加自定义 Header
//     err := proxywasm.AddHttpRequestHeader("x-custom-filter", "v1")
//     if err != nil {
//         proxywasm.LogErrorf("failed to add header: %v", err)
//     }
//     return types.ActionContinue
// }
//
// func (ctx *myPluginContext) OnHttpContextCreate(contextID uint32, rootContextID uint32,
//     configSize int) types.HttpContext {
//     return &myHttpFilter{}
// }
//
// ★ 编译命令：
//   go install tinygo.org/x/tinygo@latest
//   tinygo build -o custom_filter.wasm -target=wasi main.go
//
```

### 2.3 Istio WasmPlugin CRD 配置

```yaml
# ★ Istio 1.14+ WasmPlugin CRD（面试高频！）
#
apiVersion: extensions.istio.io/v1alpha1
kind: WasmPlugin
metadata:
  name: custom-header-filter
spec:
  # ① WASM 文件地址（OCI Registry 或本地文件）
  url: oci://docker.io/istio/custom-filter:v1
  # ② 选择器（哪些 Pod 应用）
  selector:
    matchLabels:
      app: order-service
  # ③ 插入位置（插入到哪个 Filter 之前/之后）
  phase: AUTHZ # PRE_AUTHZ / AUTHZ / POST_AUTHZ
  # ④ 插件配置（ConfigMap 引用）
  pluginConfig:
    name: custom-filter-config
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: custom-filter-config
data:
  config.yaml: |
    header-name: x-custom-filter
    header-value: v1
```

---

## 三、Envoy 原生 C++ Filter vs WASM vs Go 插件

```
┌─────────────────────────────────────────────────────────────┐
│  ★ 三种插件扩展方式深度对比（面试必背！）                    │
│                                                               │
│  ┌──────────┬──────────────┬──────────────┬──────────────┐   │
│  │ 对比维度   │ 原生 C++       │ WASM Filter    │ Go 插件       │   │
│  ├──────────┼──────────────┼──────────────┼──────────────┤   │
│  │ 开发语言   │ C++           │ Rust/C/Go 编译 │ Go           │   │
│  │ 编译方式   │ 重编译 Envoy │ 独立编译 WASM  │ 独立编译.so │   │
│  │ 热加载     │ ❌            │ ✅ 秒级        │ ✅ 秒级       │   │
│  │ 性能       │ ★★★★★      │ ★★★★（接近）   │ ★★★（一般）  │   │
│  │ 沙箱隔离   │ ❌（和 Envoy 共享地址空间）│ ✅ 完全隔离  │ ❌ 共享内存   │   │
│  │ 维护成本   │ 高（改 Envoy 源码）│ 低（独立 WASM 文件）│ 中（Go 运行时）│
│  │ 生产流行   │ 低（只有大厂改）│ ★★★★★ 极高    │ 中           │   │
│  │ 安全风险   │ 低（C++ 内存安全）│ 极低（沙箱）  │ 中（可能崩溃）│   │
│  └──────────┴──────────────┴──────────────┴──────────────┘   │
│
│ ★ 主流选择：WASM Filter（Tetrate proxy-wasm-go-sdk）       │
│   → Go 开发者友好（不需要学 Rust/C++）                      │
│   → 独立编译 WASM 文件 → 热加载 → 运维简单                │
│   → Istio WasmPlugin CRD 原生支持                         │
│
└─────────────────────────────────────────────────────────────┘
```

---

## 四、面试考点（15 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Envoy 四种插件扩展方式？| **原生 C++ / WASM / Go / LUA**；主流：WASM + proxy-wasm-go-sdk |
| 2 | WASM 为什么主流？| ①跨语言 ②沙箱安全 ③热加载 ④性能接近 C++ ⑤Istio 原生支持 |
| 3 | proxy-wasm-go-sdk？| Tetrate 维护；把 WASM ABI 封装成 Go 接口 → Go 写 → TinyGo 编译成 WASM |
| 4 | Istio WasmPlugin CRD？| 指定 url（WASM 文件）+ selector（应用哪些 Pod）+ phase（插入位置）+ pluginConfig（配置）|
| 5 | WASM Filter 生命周期？| OnPluginStart → OnHttpContextCreate → OnHttpRequestHeaders → OnHttpResponseHeaders |
| 6 | Go WASM Filter 编译？| `tinygo build -o filter.wasm -target=wasi main.go` |
| 7 | WASM 性能？| JIT 编译 → 接近原生 C++（约 80-90% 性能）；远高于 Go 插件（2-3x）|
| 8 | WASM vs 原生 C++？| 原生 C++ 性能最高但要改 Envoy 源码；WASM 性能接近但独立编译热加载 |
| 9 | WASM 沙箱安全？| WASM 运行在受限 VM 内 → 不能访问 Envoy 内存 → 不能崩溃 Envoy 进程 |
| 10 | WasmPlugin phase？| PRE_AUTHN / AUTHN / POST_AUTHN / PRE_AUTHZ / **AUTHZ** / POST_AUTHZ |
| 11 | WASM Filter 学习路径？| Tetrate proxy-wasm-go-sdk → Go 写 Filter → TinyGo 编译 → Istio WasmPlugin 部署 |
| 12 | Envoy Filter vs Gateway Filter？| Envoy Filter：C++/WASM（高性能）；Gateway Filter：Java/Groovy（灵活但性能低）|
| 13 | WASM 内存限制？| Envoy 默认给 WASM VM 64MB 内存 → 可配置 max_memory_size |
| 14 | WASM 标准？| **proxy-wasm ABI 标准**（Envoy 团队维护）→ 所有 WASM Filter 必须遵循 |
| 15 | 面试最高频考点？| **WASM Filter 开发流程 + Istio WasmPlugin CRD + WASM vs C++ 原生 Filter 对比** |

---

**文档版本：v1.0 · 2026-10-22**  
**面试考点：15 题**  
**差异化点**：Service Mesh 定制化扩展（WASM + Go SDK + Istio CRD 完整链路）
