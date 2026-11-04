# Nginx / Envoy 网关配置深度解析

> **Nginx 版本**：1.27.x（最新稳定）  
> **Envoy 版本**：1.32.x（Istio 默认）  
> **面试权重**：⭐⭐⭐⭐（网关高频，Istio 数据平面是 Envoy）

---

## 一、Nginx 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│  核心定位：HTTP/TCP 反向代理 + 负载均衡 + Web 服务器         │
│                                                               │
│  Nginx vs Apache：                                           │
│  ┌──────────┬──────────────┬──────────────┐                 │
│  │            │   Nginx        │   Apache       │             │
│  ├──────────┼──────────────┼──────────────┤                 │
│  │ 模型       │ Master + Worker多进程 │ Prefork/Event │         │
│  │ 连接数     │ ★★★★★ 百万级 │ ★★ 几千级    │                 │
│  │ 性能       │ ★★★★★ 高     │ ★★ 低        │                 │
│  │ 动态模块   │ ❌ 编译时静态  │ ✅ .so 动态  │                 │
│  │ 配置复杂度 │ ★★★ 中等      │ ★★ 简单      │                 │
│  └──────────┴──────────────┴──────────────┘                 │
│                                                               │
│ ★ Nginx 是全球最流行的 Web Server + 反向代理                  │
│ ★ Istio/Envoy 和 Nginx 定位类似，但 Envoy 更现代（xDS 动态）  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Nginx 进程模型（★ 面试高频）                                │
│                                                               │
│   ┌───────────────┐                                          │
│   │  Master 进程   │                                          │
│   │  - 读取配置     │                                          │
│   │  - 启动 Worker  │                                          │
│   │  - 信号处理     │                                          │
│   │  - 热加载配置   │                                          │
│   └──────┬────────┘                                          │
│          │ fork()                                             │
│   ┌──────┴────────────────────────────────────┐              │
│   │         Worker 进程（N 个，CPU 核数）         │              │
│   │  ┌──────────────────────────────────────┐ │              │
│   │  │  Reactor 模型（epoll/kqueue）          │ │              │
│   │  │  - 监听端口                            │ │              │
│   │  │  - 接收新连接                          │ │              │
│   │  │  - 处理请求（解析/转发/压缩/缓存）     │ │              │
│   │  │  - 每个 Worker 是独立进程（无锁！）     │ │              │
│   │  └──────────────────────────────────────┘ │              │
│   └────────────────────────────────────────────┘              │
│                                                               │
│ ★ 为什么 Nginx 性能高？                                     │
│   ① 单 Worker 进程内 Reactor 模型（epoll 异步）             │
│   ② Worker 间无锁竞争（每个 Worker 独立处理连接）            │
│   ③ Master/Worker 职责分离（Master 管理，Worker 干活）      │
└─────────────────────────────────────────────────────────────┘
```

---

## 二、Nginx 核心配置

### 2.1 反向代理 + 负载均衡（★ 面试必问）

```nginx
# ★ Nginx 反向代理配置（面试高频）
upstream backend {
    # 五种负载均衡策略
    # 1. 默认：轮询
    server 192.168.1.10:8080;
    server 192.168.1.11:8080;
    
    # 2. 加权轮询
    # server 192.168.1.10:8080 weight=3;
    # server 192.168.1.11:8080 weight=1;
    
    # 3. IP Hash（同一个客户端 IP 永远到同一台）
    # ip_hash;
    # server 192.168.1.10:8080;
    # server 192.168.1.11:8080;
    
    # 4. Least Conn（最少连接数）
    # least_conn;
    # server 192.168.1.10:8080;
    # server 192.168.1.11:8080;
    
    # 5. 最短响应时间（商业版才有）
    
    # 健康检查
    max_fails=3 fail_timeout=30s;  # 3 次失败标记不健康，30s 后再试
    keepalive 32;                   # 连接池大小
}

server {
    listen 80;
    server_name api.example.com;
    
    # 反向代理
    location /api/ {
        proxy_pass http://backend/;  # 注意结尾 / 的区别！
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # 超时设置
        proxy_connect_timeout 5s;   # 连接超时
        proxy_send_timeout 60s;      # 发送超时
        proxy_read_timeout 60s;      # 读取超时
    }
}
```

### 2.2 动静分离 + 缓存

```nginx
# ★ 动静分离配置（面试高频）
server {
    listen 80;
    server_name www.example.com;
    
    # 静态资源（本地文件）
    location ~* \.(jpg|jpeg|png|gif|ico|css|js)$ {
        root /var/www/static;
        expires 30d;                    # 缓存 30 天
        add_header Cache-Control "public, immutable";
        access_log off;                  # 不记录日志（性能优化）
    }
    
    # 动态请求（后端转发）
    location / {
        proxy_pass http://backend;
    }
}

# ★ Proxy Cache 配置（Nginx 缓存后端响应）
http {
    proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=my_cache:10m max_size=10g inactive=60m;
    
    server {
        location /api/ {
            proxy_pass http://backend;
            proxy_cache my_cache;
            proxy_cache_valid 200 302 10m;    # 200/302 缓存 10min
            proxy_cache_valid 404 1m;          # 404 缓存 1min
            proxy_cache_key "$scheme$request_method$host$request_uri";
            add_header X-Cache-Status $upstream_cache_status;  # HIT/MISS
        }
    }
}
```

### 2.3 SSL/TLS + HTTP2

```nginx
# ★ HTTPS + HTTP2 配置（面试高频）
server {
    listen 443 ssl http2;
    server_name api.example.com;
    
    # 证书配置
    ssl_certificate /etc/nginx/cert/fullchain.pem;
    ssl_certificate_key /etc/nginx/cert/privkey.pem;
    
    # TLS 版本（禁用旧版本）
    ssl_protocols TLSv1.2 TLSv1.3;
    
    # 加密套件（安全优先）
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers on;
    
    # Session 复用（性能优化）
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    ssl_session_tickets off;    # 用 session cache 代替 tickets
    
    # OCSP Stapling（证书在线验证）
    ssl_stapling on;
    ssl_stapling_verify on;
    resolver 8.8.8.8 valid=300s;
    
    location / {
        proxy_pass http://backend;
    }
}

# HTTP → HTTPS 跳转
server {
    listen 80;
    server_name api.example.com;
    return 301 https://$host$request_uri;
}
```

---

## 三、Envoy 核心（Istio 数据平面）

### 3.1 Envoy vs Nginx

```
// ★ Envoy vs Nginx（面试高频）
//   Nginx：静态配置 + C + 高并发
//   Envoy：动态配置（xDS）+ C++ + Service Mesh 原生
//
// ┌──────────────┬──────────────┬──────────────┐
// │                │   Nginx       │   Envoy       │
// ├──────────────┼──────────────┼──────────────┤
// │ 配置方式       │ 静态（reload）│ **动态（xDS）**│
// │ 更新延迟       │ 秒级（reload）│ **毫秒级**    │
// │ mTLS          │ 手动配置      │ **自动（SDS）**│
// │ 熔断/限流      │ 插件（ngx_http_limit）│ **原生支持**│
// │ 可观测性       │ Access Log   │ **原生 Prometheus** │
// │ 协议支持       │ HTTP/HTTPS/TCP│ **HTTP/gRPC/TCP** │
// │ 集成 K8s       │ Ingress Controller │ **Istio Sidecar** │
// │ 适用场景       │ 入口网关     │ **服务网格代理**│
// └──────────────┴──────────────┴──────────────┘
//
// ★ 生产组合：Nginx Ingress（入口）+ Istio Envoy Sidecar（服务网格）
```

### 3.2 Envoy xDS 配置

```yaml
# ★ Envoy 通过 xDS 动态获取配置（和 Istiod 通信）
#   五种 Service Discovery：
#   LDS（Listener）/ CDS（Cluster）/ EDS（Endpoints）/ RDS（Routes）/ SDS（Secrets）
#
# 静态配置示例（简化）：
static_resources:
  listeners:
    - name: listener_0
      address:
        socket_address: { address: 0.0.0.0, port_value: 80 }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: ingress_http
                route_config:
                  name: local_route
                  virtual_hosts:
                    - name: backend
                      domains: ["*"]
                      routes:
                        - match: { prefix: "/" }
                          route: { cluster: backend_service }
                http_filters:
                  - name: envoy.filters.http.rbac
                  - name: envoy.filters.http.router
  
  clusters:
    - name: backend_service
      connect_timeout: 0.25s
      type: EDS    # 动态从 EDS 获取 Endpoints
      eds_cluster_config:
        eds_config:
          service_name: backend_service
      lb_policy: ROUND_ROBIN
      health_checks:
        - timeout: 1s
          interval: 10s
          http_health_check: { path: "/health" }
```

---

## 四、面试考点（18 题）

| # | 考点 | 答案 |
|---|------|------|
| 1 | Nginx 进程模型？| **Master（读配置/管理）+ N 个 Worker（Reactor/epoll 处理连接）**；Worker 间无锁竞争 → 百万级连接 |
| 2 | Nginx 五种负载均衡？| **轮询（默认）/ 加权轮询 / IP Hash / Least Conn / 最短响应时间（商业版）**；`upstream server` 配置 |
| 3 | Nginx 动静分离？| **静态资源（图片/css/js）本地返回 + expires 长缓存**；动态请求 proxy_pass 到后端；减少后端压力 |
| 4 | Nginx Proxy Cache？| `proxy_cache_path` + `proxy_cache` + `proxy_cache_valid`；缓存后端响应 → X-Cache-Status HIT/MISS |
| 5 | Nginx 热加载？| `nginx -s reload` → Master 读新配置 → Worker 完成当前请求后重启；不断服务 |
| 6 | Nginx keepalive？| HTTP/1.1 默认开启；`upstream keepalive 32` 连接池；减少 TCP 握手开销 |
| 7 | Nginx 压缩？| `gzip on` + `gzip_comp_level 6` + `gzip_types text/plain application/json`；减少响应体积 |
| 8 | Nginx vs Envoy？| Nginx：**入口网关 + 静态配置 + C**；Envoy：**服务网格代理 + 动态 xDS + C++ + mTLS 原生**；生产组合用 |
| 9 | Nginx vs Apache？| Nginx：**Reactor 模型 + Worker 进程 + 百万连接 + 快**；Apache：Prefork + Event + 几千连接 + 慢但配置简单 |
| 10 | Envoy xDS 五种？| **LDS（Listener）/ CDS（Cluster）/ EDS（Endpoints）/ RDS（Routes）/ SDS（Secrets）**；Istio 核心！|
| 11 | Envoy Listener + Filter Chain？| Listener（监听端口）→ Filter Chain（HTTP Connection Manager → RBAC → Router）；和 Netty Pipeline 类似 |
| 12 | Envoy Cluster 类型？| **STATIC（静态 Endpoints）/ EDS（动态 Service Discovery）/ LOGICAL_DNS / STRICT_DNS / ORIGINAL_DST**；K8s/Istio 用 EDS |
| 13 | Envoy mTLS？| SDS API 动态获取证书（**不落盘**）；Istio 自动管理证书生命周期 |
| 14 | Nginx location 匹配顺序？| ① 精确 `=` ② `^~` 前缀（不再查正则）③ `~ / ~*` 正则（先匹配到哪个用哪个）④ 普通前缀（最长匹配）|
| 15 | Nginx proxy_pass 结尾 / 的区别？| `proxy_pass http://backend/;` → 去掉 location 匹配部分；`proxy_pass http://backend;` → 完整转发 |
| 16 | Nginx 限流？| `limit_req_zone`（请求速率）+ `limit_conn_zone`（连接数）+ 黑名单；比 Envoy 功能弱 |
| 17 | Nginx Ingress Controller？| K8s 入口：Ingress CRD → Nginx Controller → 生成 Nginx 配置 → 重载；Istio Gateway 类似 |
| 18 | Nginx/Envoy 学习路径？| Nginx 基础配置 → upstream/load_balance → proxy_pass/cache/ssl → Envoy LDS/CDS/RDS → Istio 场景 |

---

**文档版本：v1.0 · 2026-10-22**  
**源码基线**：Nginx 1.27.x + Envoy 1.32.x  
**面试考点：18 题**
