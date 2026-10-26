# 17 · Spring MVC 核心源码深度解析

> **源码基线**：Spring Framework 5.3.35（Java 8 兼容版）  
> **源码位置**：`d:\GC_WorkSpaces\spring-framework-5.3.35\spring-webmvc\src\main\java\`  
> **面试权重**：⭐⭐⭐⭐⭐（Web 开发必考）

---

## 一、Spring MVC 总体架构

### 1.1 核心组件

```
浏览器 → DispatcherServlet（前端控制器） → Spring MVC 核心链路

DispatcherServlet
  │
  ├─ ① HandlerMapping：找到哪个 Handler（Controller 方法）来处理请求
  │    → RequestMappingHandlerMapping（@RequestMapping 解析）
  │    → BeanNameUrlHandlerMapping（XML 配置的 Bean name）
  │    → 遍历所有 HandlerMapping，第一个返回非 null 的 HandlerExecutionChain
  │
  ├─ ② HandlerExecutionChain：Handler + 拦截器链
  │    → mappedHandler.getHandler() 拿到 Handler（HandlerMethod）
  │    → mappedHandler.applyPreHandle() 执行拦截器 preHandle
  │
  ├─ ③ HandlerAdapter：执行 Handler（不同类型的 Handler 用不同 Adapter）
  │    → RequestMappingHandlerAdapter（@RequestMapping 最常用）
  │    → HttpRequestHandlerAdapter（实现 HttpRequestHandler 接口）
  │    → SimpleControllerHandlerAdapter（实现 Controller 接口）
  │    → adapter.supports(handler) 判断 → adapter.handle() 执行
  │
  ├─ ④ HandlerMethodArgumentResolver：参数解析（@RequestParam/@RequestBody/@PathVariable 等）
  │    → RequestMappingHandlerAdapter 内部 composite 列表
  │    → 遍历 Resolver，supportsParameter 返回 true 的来解析
  │
  ├─ ⑤ HandlerMethodReturnValueHandler：返回值处理
  │    → RequestResponseBodyMethodProcessor（@ResponseBody → Jackson 序列化）
  │    → ViewNameMethodReturnValueHandler（返回值是视图名 → 走 ViewResolver）
  │    → ModelAndViewMethodReturnValueHandler（返回 ModelAndView）
  │
  ├─ ⑥ ViewResolver：视图解析（如果不是 @ResponseBody）
  │    → InternalResourceViewResolver（JSP）
  │    → ThymeleafViewResolver（Thymeleaf）
  │    → FreeMarkerViewResolver
  │
  ├─ ⑦ HandlerExceptionResolver：异常处理
  │    → @ControllerAdvice + @ExceptionHandler
  │    → DefaultHandlerExceptionResolver（404/405/406 等）
  │    → ResponseStatusExceptionResolver（@ResponseStatus）
  │
  └─ ⑧ HandlerInterceptor：拦截器（类似 AOP 但只拦 MVC）
       → preHandle（Controller 前）
       → postHandle（Controller 后，View 渲染前）
       → afterCompletion（View 渲染后，finally 语义）
```

### 1.2 完整请求处理链路图

```
HTTP Request
  │
  ▼
┌──────────────────────────────────────────────────────────┐
│  DispatcherServlet.doDispatch()  ★ L1032                  │
│  │                                                          │
│  ├─ ① checkMultipart() → 上传文件请求处理                  │
│  │                                                          │
│  ├─ ② getHandler(request)                                   │
│  │    → 遍历所有 HandlerMapping                             │
│  │    → RequestMappingHandlerMapping.getHandler()          │
│  │    → ★ 返回 HandlerExecutionChain(handler + interceptors)│
│  │    └─ 如果 null → noHandlerFound → 404 或抛异常          │
│  │                                                          │
│  ├─ ③ getHandlerAdapter(handler)                           │
│  │    → 遍历所有 HandlerAdapter                             │
│  │    → ★ RequestMappingHandlerAdapter（supports HandlerMethod）│
│  │                                                          │
│  ├─ ④ mappedHandler.applyPreHandle()  ← 拦截器 preHandle   │
│  │    → 如果返回 false → 中断执行 + applyAfterCompletion    │
│  │                                                          │
│  ├─ ⑤ ★ ha.handle() → 执行 Controller 方法                 │
│  │    │                                                    │
│  │    ├─ invokeHandlerMethod()                             │
│  │    │    ├─ 参数解析（HandlerMethodArgumentResolver）    │
│  │    │    │    → @RequestParam / @RequestBody / ...       │
│  │    │    ├─ 反射调用 Controller 方法                      │
│  │    │    └─ 返回值处理（HandlerMethodReturnValueHandler） │
│  │    │         → @ResponseBody → Jackson 序列化到 response │
│  │    │         → 返回视图名 → ModelAndView                │
│  │    │                                                    │
│  │    └─ 返回 ModelAndView（可能为 null，如果 @ResponseBody）│
│  │                                                          │
│  ├─ ⑥ applyDefaultViewName() → 默认视图名                  │
│  │                                                          │
│  ├─ ⑦ mappedHandler.applyPostHandle()  ← 拦截器 postHandle│
│  │                                                          │
│  ├─ ⑧ processDispatchResult()                              │
│  │    │                                                    │
│  │    ├─ 有异常 → HandlerExceptionResolver 链处理          │
│  │    │    → @ControllerAdvice + @ExceptionHandler          │
│  │    │    → DefaultHandlerExceptionResolver（404/405）     │
│  │    │                                                    │
│  │    ├─ 有 ModelAndView → ViewResolver.resolveViewName()  │
│  │    │    → 找到 View 对象                                 │
│  │    │    → view.render() → JSP 渲染 / Thymeleaf 渲染      │
│  │    │                                                    │
│  │    └─ mappedHandler.triggerAfterCompletion() ← 拦截器    │
│  │        afterCompletion（finally 语义，总是执行）          │
│  │                                                          │
│  └─ 异常 catch → HandlerExceptionResolver → 渲染错误页面   │
│                                                          │
└──────────────────────────────────────────────────────────┘
  │
  ▼
HTTP Response
```

---

## 二、DispatcherServlet.doDispatch 核心源码

**源码锚点**：DispatcherServlet.java L1032-1111

```java
protected void doDispatch(HttpServletRequest request, HttpServletResponse response) throws Exception {
    HttpServletRequest processedRequest = request;
    HandlerExecutionChain mappedHandler = null;
    boolean multipartRequestParsed = false;

    try {
        ModelAndView mv = null;
        Exception dispatchException = null;

        try {
            // ① multipart 请求处理（上传文件）
            processedRequest = checkMultipart(request);
            multipartRequestParsed = (processedRequest != request);

            // ★★ ② 找到 Handler（Controller 方法）
            //    遍历所有 HandlerMapping → RequestMappingHandlerMapping
            //    → 返回 HandlerExecutionChain(handler + interceptors)
            mappedHandler = getHandler(processedRequest);   // L1048
            if (mappedHandler == null) {
                noHandlerFound(processedRequest, response);   // 404
                return;
            }

            // ★★ ③ 找到 HandlerAdapter
            //    RequestMappingHandlerAdapter（支持 HandlerMethod）
            HandlerAdapter ha = getHandlerAdapter(mappedHandler.getHandler());   // L1055

            // ④ GET/HEAD last-modified 缓存检查
            String method = request.getMethod();
            boolean isGet = HttpMethod.GET.matches(method);
            if (isGet || HttpMethod.HEAD.matches(method)) {
                long lastModified = ha.getLastModified(request, mappedHandler.getHandler());
                if (new ServletWebRequest(request, response).checkNotModified(lastModified) && isGet) {
                    return;   // 304 Not Modified → 浏览器缓存可用
                }
            }

            // ★ ⑤ 拦截器链 preHandle（按注册顺序）
            //    有一个返回 false → 中断执行
            if (!mappedHandler.applyPreHandle(processedRequest, response)) {   // L1067
                return;
            }

            // ★★★ ⑥ 核心！执行 Controller 方法
            //    RequestMappingHandlerAdapter.handle()
            //    → 参数解析 → 反射调用 → 返回值处理 → 生成 ModelAndView
            mv = ha.handle(processedRequest, response, mappedHandler.getHandler());   // L1072

            // ⑦ 异步请求处理（WebAsyncTask / DeferredResult）
            if (asyncManager.isConcurrentHandlingStarted()) {
                return;
            }

            // ⑧ 默认视图名
            applyDefaultViewName(processedRequest, mv);
            
            // ★ ⑨ 拦截器链 postHandle（按注册逆序！）
            mappedHandler.applyPostHandle(processedRequest, response, mv);   // L1079
        }
        catch (Exception ex) {
            dispatchException = ex;   // 捕获异常 → processDispatchResult 统一处理
        }
        catch (Throwable err) {
            dispatchException = new NestedServletException("Handler dispatch failed", err);
        }
        
        // ★★ ⑩ 统一结果处理（正常 + 异常）
        processDispatchResult(processedRequest, response, mappedHandler, mv, dispatchException);   // L1089
    }
    catch (Exception ex) {
        triggerAfterCompletion(processedRequest, response, mappedHandler, ex);
    }
    catch (Throwable err) {
        triggerAfterCompletion(processedRequest, response, mappedHandler, new NestedServletException("...", err));
    }
    finally {
        // ⑪ multipart 资源清理
        if (multipartRequestParsed) {
            cleanupMultipart(processedRequest);
        }
    }
}
```

---

## 三、HandlerMapping · RequestMappingHandlerMapping

### 3.1 RequestMapping 注册流程

**源码锚点**：RequestMappingHandlerMapping.java L189-205

```java
public void afterPropertiesSet() {
    this.config = new RequestMappingInfo.BuilderConfiguration();
    // ★ 初始化配置（路径匹配 / 后缀模式 / TrailingSlash 等）
    super.afterPropertiesSet();
}

// super.afterPropertiesSet() → AbstractDetectingUrlHandlerMapping → detectHandlers()
// → BeanFactory.getBeansOfType(Object.class) → 遍历所有 Bean
// → 检查 @Controller / @RequestMapping 注解
// → 类级别 @RequestMapping + 方法级别 @RequestMapping → 合并
// → 生成 RequestMappingInfo（pattern + method + param + header + consumes + produces）
// → registerHandlerMethod() 注册到 mapping registry
```

### 3.2 RequestMappingHandlerMapping.getHandler · 匹配逻辑

```java
// RequestMappingHandlerMapping.getHandler(request) 简化逻辑：
//
// ① 从 request 提取 lookup path（去掉 context path + servlet path）
// ② 遍历所有已注册的 RequestMappingInfo
// ③ 匹配规则（所有条件 AND）：
//    → pattern 匹配（AntPathMatcher / PathPatternParser）
//    → HTTP method 匹配
//    → params 匹配
//    → headers 匹配
//    → consumes（Content-Type）匹配
//    → produces（Accept）匹配
// ④ 找到最精确匹配（有多个时 → best match）
// ⑤ 返回 HandlerExecutionChain(handler + interceptors)
//
// 匹配优先级：
//   精确路径 > 变量路径（/user/{id}）> 通配符（/user/*）> **（/user/**）
//   → getBestMatchingPattern 算法
```

---

## 四、RequestMappingHandlerAdapter · 执行 Controller 方法

### 4.1 supports + handle

**源码锚点**：RequestMappingHandlerAdapter.java

```java
// supports：判断 Handler 类型
public boolean supports(Object handler) {
    return handler instanceof HandlerMethod;   // ★ 只支持 HandlerMethod（@RequestMapping 解析出来的）
}

// handle：执行 Controller 方法
public ModelAndView handle(HttpServletRequest request, HttpServletResponse response, Object handler) {
    return handleInternal(request, response, (HandlerMethod) handler);
}

// handleInternal：实际执行
protected ModelAndView handleInternal(request, response, handlerMethod) {
    // ① 检查 session / 锁
    // ② invokeHandlerMethod()  ← ★ 核心！
    // ③ 处理 ModelAndView / @ResponseBody 已直接写入 response
}
```

### 4.2 invokeHandlerMethod · 参数解析 + 反射调用 + 返回值处理

```java
// invokeHandlerMethod 简化版：
protected ModelAndView invokeHandlerMethod(request, response, handlerMethod) {
    
    // ① 包装成 ServletInvocableHandlerMethod
    ServletInvocableHandlerMethod invocableMethod = createInvocableHandlerMethod(handlerMethod);
    
    // ★ ② 设置参数解析器 + 返回值处理器
    invocableMethod.setArgumentResolvers(this.argumentResolvers);
    invocableMethod.setReturnValueHandlers(this.returnValueHandlers);
    
    // ③ 参数解析 + 反射调用
    //    invocableMethod.invokeAndHandle(webRequest, mavContainer, providedArgs)
    //    → 遍历参数 → HandlerMethodArgumentResolverComposite.supportsParameter()
    //    → 匹配的 Resolver.resolveArgument()
    //    → 最终 mavContainer.getModel() 里有 ModelAndView
    
    // ④ 返回值处理
    //    → HandlerMethodReturnValueHandlerComposite.handleReturnValue()
    //    → @ResponseBody → RequestResponseBodyMethodProcessor → Jackson 序列化到 response.getWriter()
    //    → 返回 String（视图名）→ ViewNameMethodReturnValueHandler → 设置 mavContainer.viewName
    //    → 返回 ModelAndView → ModelAndViewMethodReturnValueHandler
    
    // ⑤ 处理 Model / View
    return mavContainer.getModelAndView();
}
```

### 4.3 HandlerMethodArgumentResolver · 参数解析器

```java
// ★ 核心 Resolver 清单（注册顺序很重要！）：
//
// ① RequestParamMethodArgumentResolver（默认）
//    → 解析 @RequestParam（也能解析没有注解但基本类型的参数）
//
// ② RequestResponseBodyMethodProcessor
//    → 解析 @RequestBody → 读取 request body → Jackson / JSON 反序列化成对象
//
// ③ PathVariableMethodArgumentResolver
//    → 解析 @PathVariable（从 URI 模板变量里取值）
//
// ④ RequestHeaderMethodArgumentResolver
//    → 解析 @RequestHeader（从 HTTP header 取值）
//
// ⑤ MatrixVariableMethodArgumentResolver
//    → 解析 @MatrixVariable（URL 分号分隔变量）
//
// ⑥ RequestPartMethodArgumentResolver
//    → 解析 @RequestPart（multipart 请求的文件/JSON 部分）
//
// ⑦ ServletRequestMethodArgumentResolver
//    → 直接注入 HttpServletRequest / HttpServletResponse / HttpSession
//
// ⑧ ModelMethodProcessor
//    → 注入 Model / Map（Controller 方法直接写 Model model 参数）
//
// ⑨ MapMethodProcessor
//    → 注入 @RequestHeader Map / @RequestParam Map（把所有 header/param 放 Map 里）
//
// ★ 匹配逻辑：
//   supportsParameter(MethodParameter parameter) → 遍历所有 Resolver
//   第一个返回 true 的 Resolver 来 resolveArgument()
//   → 所以 Resolver 注册顺序很重要！排在前面的优先匹配
```

### 4.4 HandlerMethodReturnValueHandler · 返回值处理器

```java
// ★ 核心 ReturnValueHandler 清单：
//
// ① RequestResponseBodyMethodProcessor
//    → @ResponseBody → Jackson 序列化 → response.getWriter().write(json)
//    → 有两个位置：RequestMappingHandlerAdapter 和 ExceptionHandlerExceptionResolver 都有
//
// ② ModelAndViewMethodReturnValueHandler
//    → 返回值是 ModelAndView → 直接用
//
// ③ ViewNameMethodReturnValueHandler
//    → 返回值是 String → 当作视图名
//
// ④ ModelMethodProcessor
//    → 返回值是 Model / Map → 当作 Model
//
// ⑤ HttpEntityMethodProcessor
//    → 返回值是 HttpEntity / ResponseEntity
//
// ★ 匹配逻辑同 Resolver：supportsReturnType() → 第一个支持的来 handleReturnValue()
```

---

## 五、HandlerInterceptor · 拦截器链

### 5.1 HandlerExecutionChain.applyPreHandle 源码

**源码锚点**：DispatcherServlet.java L1067 + HandlerExecutionChain.java

```java
// HandlerExecutionChain 持有 handler + interceptors
public class HandlerExecutionChain {
    private final Object handler;
    private HandlerInterceptor[] interceptors;
    
    // ★ 拦截器链执行顺序：
    // preHandle → 按注册顺序 1→2→3
    // postHandle → 按注册逆序 3→2→1
    // afterCompletion → 按注册逆序 3→2→1
    // （类似 AOP 洋葱模型！）
    
    boolean applyPreHandle(request, response) {
        HandlerInterceptor[] interceptors = getInterceptors();
        if (interceptors != null) {
            for (int i = 0; i < interceptors.length; i++) {
                HandlerInterceptor interceptor = interceptors[i];
                // ★ 如果返回 false → 中断执行
                if (!interceptor.preHandle(request, response, this.handler)) {
                    triggerAfterCompletion(request, response, null);   // afterCompletion 从 i 开始逆序
                    return false;
                }
                this.interceptorIndex = i;
            }
        }
        return true;
    }
    
    void applyPostHandle(request, response, mv) {
        HandlerInterceptor[] interceptors = getInterceptors();
        if (interceptors != null) {
            for (int i = interceptors.length - 1; i >= 0; i--) {   // ★ 逆序！
                interceptors[i].postHandle(request, response, this.handler, mv);
            }
        }
    }
    
    void triggerAfterCompletion(request, response, ex) {
        HandlerInterceptor[] interceptors = getInterceptors();
        if (interceptors != null) {
            for (int i = this.interceptorIndex; i >= 0; i--) {   // ★ 从失败的那个开始逆序
                interceptors[i].afterCompletion(request, response, this.handler, ex);
            }
        }
    }
}

// ★ 洋葱模型对比：
//   AOP 洋葱模型：@Before（外层先）→ target → @After（外层后）
//   MVC 拦截器：preHandle（外层先）→ Controller → postHandle（外层后）→ afterCompletion（外层最后）
//   → 完全一致！都是逆序 post 阶段
```

### 5.2 自定义拦截器

```java
// 示例：登录检查 + 日志
@Component
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        // Controller 方法执行前
        User user = (User) request.getSession().getAttribute("user");
        if (user == null) {
            response.sendRedirect("/login");
            return false;   // ★ 返回 false 中断执行
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response,
            Object handler, ModelAndView modelAndView) {
        // Controller 执行完，View 渲染前
        if (modelAndView != null) {
            modelAndView.addObject("currentUser", request.getSession().getAttribute("user"));
        }
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
            Object handler, Exception ex) {
        // View 渲染后，finally 语义（异常也会执行）
        log.info("Request completed: {} {} → {}", request.getMethod(), request.getRequestURI(), 
                ex != null ? ex.getMessage() : "OK");
    }
}

// 注册到 Spring MVC
@Configuration
public class WebConfig implements WebMvcConfigurer {
    @Autowired
    private LoginInterceptor loginInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(loginInterceptor)
                .addPathPatterns("/**")          // 拦截所有
                .excludePathPatterns("/login", "/css/**", "/js/**");  // 排除登录页和静态资源
    }
}
```

---

## 六、ViewResolver · 视图解析

### 6.1 ViewResolver 完整流程

```java
// Controller 返回 String viewName → 视图解析流程：
//
// ① RequestMappingHandlerAdapter.handle() 返回 ModelAndView(viewName + model)
// ② DispatcherServlet.processDispatchResult() 里 → render()
//
// protected void render(ModelAndView mv, request, response) {
//     View view;
//     if (mv.isReference()) {
//         // ★ 是视图名（String）→ 用 ViewResolver 解析
//         for (ViewResolver viewResolver : this.viewResolvers) {
//             view = viewResolver.resolveViewName(mv.getView(), locale);
//             if (view != null) break;
//         }
//     } else {
//         // 直接返回 View 对象 → 不需要 ViewResolver
//         view = mv.getView();
//     }
//     view.render(mv.getModel().asMap(), request, response);
// }
//
// 常见 ViewResolver：
//   InternalResourceViewResolver → JSP（prefix + suffix 拼接）
//   ThymeleafViewResolver → Thymeleaf
//   FreeMarkerViewResolver → FreeMarker
//   ContentNegotiatingViewResolver → 根据 Accept header 选择
//   BeanNameViewResolver → 直接按 Bean name 找 View
//
// ★ @ResponseBody 会跳过 ViewResolver！
//   → RequestResponseBodyMethodProcessor 在 handleReturnValue 里已经把 JSON 写到 response 了
//   → ModelAndView 为 null → render() 直接跳过
```

---

## 七、@ExceptionHandler · 全局异常处理

```java
// ★ 推荐方式：@ControllerAdvice + @ExceptionHandler
//
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ErrorResult> handleBusiness(BusinessException ex) {
        ErrorResult result = ErrorResult.fail(ex.getCode(), ex.getMessage());
        return ResponseEntity.status(ex.getHttpStatus()).body(result);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResult> handleValidation(MethodArgumentNotValidException ex) {
        String msg = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest().body(ErrorResult.fail(400, msg));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResult> handleAll(Exception ex) {
        log.error("未知异常", ex);
        return ResponseEntity.internalServerError().body(ErrorResult.fail(500, "系统繁忙"));
    }
}

// HandlerExceptionResolver 链（DispatcherServlet 里的异常处理顺序）：
//
// ① ExceptionHandlerExceptionResolver（@ControllerAdvice + @ExceptionHandler）← 自定义的
// ② ResponseStatusExceptionResolver（@ResponseStatus 注解 + ResponseStatusException）
// ③ DefaultHandlerExceptionResolver（404/405/406/409/415/500 等标准 HTTP 异常）
// ④ SimpleMappingExceptionResolver（旧 XML 配置方式，用得少了）
//
// ★ 有多个匹配的 ExceptionHandler → 选最精确的（子类优先于父类）
```

---

## 八、面试考点（Spring MVC 20 题）

| # | 考点 | 答案 + 源码锚点 |
|---|------|----------------|
| **核心链路** | | |
| 1 | DispatcherServlet.doDispatch 11 步？| ① checkMultipart → ② getHandler → ③ getHandlerAdapter → ④ lastModified 缓存 → ⑤ applyPreHandle → ⑥ ha.handle → ⑦ async → ⑧ applyDefaultViewName → ⑨ applyPostHandle → ⑩ processDispatchResult → ⑪ cleanupMultipart |
| 2 | getHandler 返回什么？| **HandlerExecutionChain(handler + interceptors)** → handler 是 HandlerMethod（封装 Controller 类 + 方法 + Bean）|
| 3 | RequestMappingHandlerMapping 注册时机？| afterPropertiesSet() → detectHandlers() → 遍历所有 Bean → 检查 @Controller/@RequestMapping → 生成 RequestMappingInfo → registerHandlerMethod |
| 4 | RequestMappingHandlerAdapter supports？| **只支持 HandlerMethod**（instanceof HandlerMethod）|
| **参数 / 返回值** | | |
| 5 | 参数解析器匹配顺序？| 遍历所有 HandlerMethodArgumentResolver → **第一个 supportsParameter=true 的来解析** → 注册顺序很重要！|
| 6 | @RequestBody 用哪个 Resolver？| **RequestResponseBodyMethodProcessor** → 读 request body → Jackson 反序列化 |
| 7 | @PathVariable 用哪个 Resolver？| **PathVariableMethodArgumentResolver** → 从 URI 模板变量取值 |
| 8 | @ResponseBody 怎么实现的？| **RequestResponseBodyMethodProcessor.handleReturnValue()** → Jackson 序列化 → response.getWriter().write(json) → 跳过 ViewResolver |
| 9 | HandlerMethodArgumentResolver 和 HandlerMethodReturnValueHandler 顺序重要吗？| **非常重要！** 遍历时第一个匹配的来处理 → 排在前面的优先级高 |
| **拦截器** | | |
| 10 | 拦截器链执行顺序？| **preHandle 正序（1→2→3）→ postHandle 逆序（3→2→1）→ afterCompletion 逆序（3→2→1）**（洋葱模型）|
| 11 | preHandle 返回 false 会怎样？| **中断执行 → triggerAfterCompletion 从当前拦截器位置开始逆序执行 → Controller 不会执行** |
| 12 | afterCompletion 一定会执行吗？| **是的！** 即使 Controller 抛异常也会执行（finally 语义）→ 资源清理放在这里 |
| 13 | 拦截器 vs AOP 区别？| 拦截器只在 MVC 层（Controller 调用）；AOP 在 Spring 容器的 Bean 方法调用（更广）；拦截器能拿到 Handler / ModelAndView；AOP 能拿到 JoinPoint |
| **异常处理** | | |
| 14 | DispatcherServlet 异常处理顺序？| **① ExceptionHandlerExceptionResolver（@ControllerAdvice）→ ② ResponseStatusExceptionResolver（@ResponseStatus）→ ③ DefaultHandlerExceptionResolver（404/405）** |
| 15 | @ExceptionHandler 多个匹配？| **选最精确的**（异常子类优先于父类）|
| **高级** | | |
| 16 | 为什么要有 HandlerAdapter？| **适配器模式！** DispatcherServlet 不关心 Handler 类型 → 不同 Handler（@RequestMapping / HttpRequestHandler / Controller 接口）用不同 Adapter |
| 17 | HandlerMapping 有多个怎么选择？| **遍历所有 HandlerMapping → 第一个返回非 null 的 HandlerExecutionChain** → 按 Order 排序 |
| 18 | GET 请求的 last-modified 检查？| **ha.getLastModified() → checkNotModified() → 304 Not Modified → 浏览器用缓存，服务器不返回内容** |
| 19 | 异步请求怎么处理？| **DeferredResult / WebAsyncTask / Callable** → asyncManager.isConcurrentHandlingStarted() → 直接 return，等异步完成再处理 |
| 20 | multipart 请求怎么处理？| **checkMultipart() → MultipartHttpServletRequest** → @RequestPart 或 MultipartFile 注入 → finally cleanupMultipart() 清理临时文件 |

---

**文档版本：v1.0 · 2026-10-20**  
**源码基线**：Spring Framework 5.3.35（Java 8 兼容版）  
**覆盖**：Spring MVC 完整请求处理链路（DispatcherServlet.doDispatch 11 步 + HandlerMapping + HandlerAdapter + HandlerMethodArgumentResolver + HandlerMethodReturnValueHandler + HandlerInterceptor 洋葱模型 + ViewResolver + @ExceptionHandler 全局异常）+ 20 个面试考点  
**源码锚点行号全部来自 Spring Framework 5.3.35 源码副本**
