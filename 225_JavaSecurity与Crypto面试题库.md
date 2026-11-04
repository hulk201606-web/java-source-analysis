# 225 · Java Security 与 Crypto 高频面试题库（55 题 · 详解）

> **方向**：JCA/JCE、对称/非对称加密、摘要与签名、KeyStore、TLS、Java 安全模型  
> **面试权重**：⭐⭐⭐⭐（安全与工程实践高频）  
> **配套源码文档**：[016_Spring核心源码解析.md](016_Spring核心源码解析.md)（Spring Security 相关）、[013_网络IO与类加载源码解析.md](013_网络IO与类加载源码解析.md)  
> **答案组织**：结论 → 关键机制 → 常见追问；算法参数以 JDK 8/11/17 常用实现为准。

---

## 一、JCA/JCE 架构（Q1–Q5）

### Q1. JCA 与 JCE 有什么区别？

**结论**：JCA 提供摘要、签名、证书、KeyStore 等基础密码学框架；JCE 提供加密、密钥生成、MAC 等扩展能力。

**机制**：
- JCA：`MessageDigest`、`Signature`、`KeyStore`、`Certificate`、`SecureRandom`。
- JCE：`Cipher`、`KeyGenerator`、`KeyAgreement`、`Mac`、`SecretKey`。
- 两者都基于 Provider 架构，算法实现可替换。
- 历史上 JCE 受出口管制，现在 JDK 已内置常用强度。

**追问**：为什么分两套 API？历史与出口管制原因，现代使用时可统一看作 Java 密码学框架。

### Q2. Provider 与 Service 机制是如何工作的？

**结论**：Provider 是算法实现集合，JCA 通过“算法名 → Service”查找实现，支持多 Provider 优先级。

**机制**：
- `java.security.Provider` 注册算法实现。
- `Security.getProvider("SunJCE")` 获取 Provider。
- `Cipher.getInstance("AES/GCM/NoPadding")` 按名称查找 Service。
- 多个 Provider 按注册顺序或指定 Provider 选择。
- 可通过 `Security.addProvider` 动态注册。

**追问**：找不到算法会怎样？抛 `NoSuchAlgorithmException`，需检查 Provider 与算法名。

### Q3. 常见的 JDK Provider 有哪些？

**结论**：Sun、SunJCE、SunRsaSign、SunEC、SunJSSE、SunPKCS11 等，分别覆盖基础、加密、签名、椭圆曲线、TLS 与硬件密钥。

**机制**：
- Sun：基础摘要与密钥。
- SunJCE：AES/DES/RSA 加密、Mac。
- SunRsaSign：RSA 签名。
- SunEC：ECC/ECDSA/ECDH。
- SunJSSE：SSL/TLS。
- SunPKCS11：HSM/智能卡。

**追问**：BouncyCastle 算什么？第三方 Provider，补充国密、更多曲线与算法。

### Q4. `Cipher.getInstance("AES/GCM/NoPadding")` 中的三段分别是什么？

**结论**：算法 / 工作模式 / 填充方式；省略时使用 Provider 默认值，生产应显式指定。

**机制**：
- 算法：AES、DES、RSA、ChaCha20 等。
- 模式：ECB、CBC、CTR、GCM、CFB、OFB。
- 填充：NoPadding、PKCS5Padding、OAEPPadding。
- 不同 Provider 对默认值不同，显式指定避免歧义。

**追问**：为什么 GCM 用 NoPadding？GCM 是流式认证加密，不需要块填充。

### Q5. SecureRandom 与 Random 有什么区别？

**结论**：Random 是伪随机、可预测；SecureRandom 使用密码学安全熵源，适合密钥、IV、盐值。

**机制**：
- `new Random()` 基于线性同余，种子可预测。
- `SecureRandom` 使用系统熵源与安全算法。
- 常见算法：NativePRNG、DRBG。
- 不要在多线程共享一个 SecureRandom 造成争用，可用 ThreadLocal 或适当复用。

**追问**：`SecureRandom.setSeed` 能提高安全性吗？不应依赖用户种子，系统熵源才是关键。

---

## 二、对称加密（Q6–Q11）

### Q6. AES 与 DES 有什么区别？如何选择？

**结论**：AES 更安全、更快，DES 已不安全；生产使用 AES-128/256，禁用 DES/3DES。

**机制**：
- AES 分组 128 位，密钥 128/192/256 位。
- DES 分组 64 位、密钥 56 位，易被暴力破解。
- 3DES 兼容旧系统但性能差、安全性不足。
- AES 有硬件指令加速（AES-NI）。

**追问**：AES-256 一定比 AES-128 好吗？更强但更慢，AES-128 在多数场景已足够安全。

### Q7. ECB、CBC、CTR、GCM 各自的优缺点是什么？

**结论**：ECB 不安全；CBC 需要随机 IV 且易受填充攻击；CTR 并行但无认证；GCM 认证加密最推荐。

**机制**：
- ECB：相同明文块产生相同密文，泄露模式。
- CBC：链式异或，需随机 IV，不能并行加密。
- CTR：计数器流式加密，可并行，但必须保证 nonce 唯一。
- GCM：CTR + GHASH，提供机密性与完整性。

**追问**：GCM 的 IV 能复用吗？绝对不能，同一密钥下 IV 复用会 catastrophic failure。

### Q8. 为什么 CBC 需要填充？PKCS5Padding 是什么？

**结论**：CBC 要求明文长度为分组大小整数倍，PKCS5/7 用填充字节补齐并记录填充长度。

**机制**：
- AES 分组 16 字节，不足则填充。
- PKCS5Padding 在 Java 中实际按 16 字节处理，等价 PKCS#7。
- 解密后校验填充，错误可触发 Padding Oracle。
- GCM/CTR 是流模式，通常不需要填充。

**追问**：NoPadding 直接加密会怎样？长度不合法会报错，或产生错误结果。

### Q9. IV（初始化向量）应该怎么管理？

**结论**：IV 不是密钥，必须随机、唯一；CBC 可公开传输，GCM 绝不能在同一密钥下复用。

**机制**：
- CBC 每次加密生成随机 IV，随密文一起保存。
- GCM 推荐 12 字节 IV，并保证全局唯一。
- IV 不需要保密，但不能可预测或复用。
- 不要把固定 IV 硬编码。

**追问**：IV 可以用常量吗？不可以，会破坏语义安全，GCM 复用还会泄露认证密钥。

### Q10. GCM 模式为什么能在加密同时校验完整性？

**结论**：GCM 在 CTR 加密基础上用 GHASH 计算认证标签，验证密文与附加数据未被篡改。

**机制**：
- 输入密钥、IV、明文、AAD。
- 输出密文与 128 位认证标签。
- 解密时先校验标签，失败则拒绝。
- 支持 AAD（如 HTTP 头）参与认证。

**追问**：GCM 能防重放吗？不能，重放需要 nonce/时间戳/序列号等业务机制。

### Q11. Java 中 AES-256 需要额外配置吗？

**结论**：现代 JDK 默认支持 AES-256；老版本受策略文件限制，需要安装 unlimited strength policy。

**机制**：
- JDK 8u161+ 与 JDK 9+ 默认启用无限强度。
- 老版本需替换 `local_policy.jar`/`US_export_policy.jar`。
- 可通过 `Cipher.getMaxAllowedKeyLength("AES")` 检查。
- 生产应确认 JDK 版本与合规要求。

**追问**：密钥长度受出口限制怎么办？升级 JDK 或使用合规 Provider，不要自己实现弱化算法。

---

## 三、非对称加密、签名与证书（Q12–Q17）

### Q12. RSA 加密与签名有什么区别？

**结论**：加密用公钥、私钥解密；签名用私钥签、公钥验，目的与密钥方向相反。

**机制**：
- 加密：`Cipher.ENCRYPT_MODE` 公钥，`DECRYPT_MODE` 私钥。
- 签名：`Signature.initSign` 私钥，`initVerify` 公钥。
- RSA 加密有长度限制，通常只加密对称密钥。
- 签名先对摘要签名，支持大消息。

**追问**：RSA 能直接加密大文件吗？不能，受模长限制，应使用混合加密（RSA + AES）。

### Q13. RSA 的填充方式为什么重要？

**结论**：PKCS#1 v1.5 存在填充攻击风险，推荐 OAEP；签名推荐 PSS。

**机制**：
- 加密填充：`RSA/ECB/OAEPWithSHA-256AndMGF1Padding`。
- 签名填充：`SHA256withRSA/PSS`。
- 错误填充可被 Padding Oracle 利用。
- 密钥长度建议 2048 位以上。

**追问**：`RSA/ECB/...` 里的 ECB 是什么意思？RSA 不是分组模式，这里的 ECB 表示不使用对称分组链接，是历史命名。

### Q14. ECC 与 RSA 相比有什么优势？

**结论**：相同安全强度下 ECC 密钥更短、运算更快、带宽更省，适合移动与 TLS。

**机制**：
- 常用曲线 P-256、P-384、secp256k1。
- ECDSA 用于签名，ECDH 用于密钥交换。
- 256 位 ECC 约等于 3072 位 RSA 的安全强度。
- 曲线选择需符合合规与互操作要求。

**追问**：ECC 有什么风险？曲线参数与实现侧信道攻击，需使用成熟 Provider。

### Q15. 数字签名的完整流程是什么？

**结论**：对消息做摘要，用私钥签名摘要，接收方用公钥验签并比对摘要。

**机制**：
- 发送方：`MessageDigest` → `Signature.sign`。
- 接收方：`Signature.verify`。
- 签名保证完整性、身份认证与不可否认。
- 需要可信公钥分发，通常由证书链解决。

**追问**：签名能加密吗？签名不提供机密性，只保证完整性与身份。

### Q16. X.509 证书包含哪些信息？

**结论**：版本、序列号、签名算法、颁发者、有效期、主体、公钥、扩展、CA 签名。

**机制**：
- 主体包含 CN/O/OU/C 等。
- 扩展包含 SAN、KeyUsage、BasicConstraints。
- 证书由 CA 私钥签名，形成信任链。
- Java 用 `X509Certificate` 解析与校验。

**追问**：SAN 与 CN 哪个用于主机名校验？现代 TLS 优先 SAN，CN 已逐步弃用。

### Q17. PKI 与 CA 的信任链是如何建立的？

**结论**：终端证书由中间 CA 签发，中间 CA 由根 CA 签发，客户端用信任库中的根证书验证整条链。

**机制**：
- 根证书预置在操作系统/JDK 信任库。
- 验证签名、有效期、用途、吊销状态。
- CRL/OCSP 检查吊销。
- 主机名校验防止中间人攻击。

**追问**：自签名证书能用于生产吗？内部系统可配合自定义信任库，但公网服务应使用受信 CA。

---

## 四、摘要、MAC 与密码存储（Q18–Q21）

### Q18. MessageDigest 有什么特性？MD5/SHA-1 还能用吗？

**结论**：摘要不可逆、定长、抗碰撞；MD5/SHA-1 已不具备碰撞安全，不能用于签名与安全场景。

**机制**：
- 输出定长，输入微小变化导致雪崩。
- 用于完整性校验、指纹、派生。
- SHA-256/384/512 是当前推荐。
- MD5 仅可用于非安全去重/缓存键。

**追问**：摘要能当加密用吗？不能，摘要不可逆，无法还原原文。

### Q19. HMAC 与普通摘要有什么区别？

**结论**：HMAC 用密钥与消息一起计算，提供完整性 + 身份认证，能防篡改与伪造。

**机制**：
- `Mac.getInstance("HmacSHA256")`。
- 双方共享密钥。
- 比“hash(secret + message)”更安全，避免长度扩展攻击。
- 常用于 API 签名、Webhook 校验。

**追问**：HMAC 与数字签名区别？HMAC 对称共享密钥，签名非对称、支持不可否认。

### Q20. 用户密码应该如何存储？

**结论**：使用慢哈希/密码哈希（bcrypt、scrypt、Argon2、PBKDF2），加盐、足够迭代，绝不存明文或裸 MD5。

**机制**：
- 每个用户随机盐，防彩虹表。
- 迭代次数提高暴力破解成本。
- 不要用可逆加密存密码。
- 登录比对用常量时间比较，防时序攻击。

**追问**：Spring Security 的 `PasswordEncoder` 怎么选？优先 `BCryptPasswordEncoder` 或 `Argon2PasswordEncoder`。

### Q21. Base64 是加密吗？为什么不能用于保护敏感数据？

**结论**：Base64 只是编码，可逆且无密钥，任何人都能解码，不能提供机密性。

**机制**：
- 把字节映射为字符，便于传输。
- 不改变信息量，不提供完整性。
- 常见于 JWT、图片、配置传输。
- 敏感数据必须加密或使用受控通道。

**追问**：JWT 的 payload 是加密的吗？默认只是 Base64URL 编码，不能放敏感明文。

---

## 五、密钥管理与 TLS（Q22–Q27）

### Q22. KeyStore 是什么？JKS 与 PKCS12 有什么区别？

**结论**：KeyStore 管理密钥与证书；JKS 是 Java 私有格式，PKCS12 是标准格式、跨平台更通用。

**机制**：
- `KeyStore.getInstance("PKCS12")`。
- 存储私钥、公钥证书、信任证书。
- 私钥条目需要密码保护。
- 现代应用优先 PKCS12，JKS 逐步淘汰。

**追问**：KeyStore 密码和私钥密码一样吗？可以不同，私钥条目可单独设置密码。

### Q23. KeyGenerator、KeyPairGenerator、KeyFactory 有什么区别？

**结论**：KeyGenerator 生成对称密钥；KeyPairGenerator 生成非对称密钥对；KeyFactory 在密钥规范与 Key 对象之间转换。

**机制**：
- `KeyGenerator.getInstance("AES")` + `init(256)`。
- `KeyPairGenerator.getInstance("RSA")` + `initialize(2048)`。
- `KeyFactory` 用于 `X509EncodedKeySpec`/`PKCS8EncodedKeySpec` 转换。
- 密钥生成应使用 SecureRandom。

**追问**：密钥能硬编码在代码里吗？不能，应使用 KMS/HSM/配置中心，并做轮换。

### Q24. SSLContext 与 SSLEngine 的关系是什么？

**结论**：SSLContext 是 TLS 配置与工厂，SSLEngine 是单连接的非阻塞 TLS 状态机。

**机制**：
- `SSLContext.getInstance("TLS")` 初始化 KeyManager/TrustManager。
- `SSLSocketFactory` 创建阻塞式 TLS Socket。
- `SSLEngine` 用于 NIO/Netty 的非阻塞握手与加解密。
- `SSLContext` 可复用，Engine 每连接一个。

**追问**：Netty 的 SslContext 与 JDK 关系？Netty 封装 JDK/OpenSSL 实现，提供 Pipeline Handler。

### Q25. HTTPS 的 TLS 握手过程是怎样的？

**结论**：客户端与服务端协商版本与套件、验证证书、协商会话密钥，之后用对称加密传输。

**机制**：
- ClientHello → ServerHello → Certificate → 密钥交换。
- TLS 1.2 使用 RSA/ECDHE 协商预主密钥。
- TLS 1.3 简化握手，默认前向 secrecy。
- 证书校验包含链、主机名、有效期、吊销。

**追问**：TLS 1.3 为什么更快？减少往返、移除老旧算法、支持 0-RTT（有重放风险）。

### Q26. 为什么仅配置 HTTPS 还不够？

**结论**：还需要正确的主机名校验、证书链校验、协议/套件限制、HSTS 与安全配置，否则仍可能被中间人攻击。

**机制**：
- 禁用 SSLv3/TLS1.0/1.1 与弱套件。
- 校验证书链与主机名。
- 开启 HSTS，防止降级。
- 保护私钥、定期轮换。
- 避免自定义 TrustManager 无条件信任。

**追问**：`TrustAllManager` 有什么风险？完全失去身份校验，任何中间人都能冒充服务端。

### Q27. 常见 TLS 漏洞有哪些？

**结论**：降级攻击、BEAST、POODLE、Heartbleed、证书校验缺失、弱随机与弱套件。

**机制**：
- POODLE 利用 SSLv3 填充。
- Heartbleed 是 OpenSSL 越界读。
- 降级攻击强制弱协议。
- 客户端不校验证书导致中间人。
- 修复靠升级、禁用旧协议、正确校验。

**追问**：如何检查服务端 TLS 配置？用 `openssl s_client`、testssl.sh、SSLLabs 扫描。

---

## 六、Java 安全模型与实战（Q28–Q35）

### Q28. Java 的权限模型（Policy/AccessController）如何工作？

**结论**：通过 ProtectionDomain、Permission、Policy 决定代码能做什么，`AccessController.doPrivileged` 控制权限边界。

**机制**：
- 类加载时关联 CodeSource 与 ProtectionDomain。
- Policy 定义权限集合。
- 安全检查遍历调用栈，任一域缺少权限则拒绝。
- `doPrivileged` 表示“以当前域权限执行，不再检查调用者”。

**追问**：为什么 `doPrivileged` 有风险？它会缩短检查链，滥用会提权。

### Q29. SecurityManager 为什么被废弃？

**结论**：它难以正确使用、性能开销大、与现代模块化/容器安全模型冲突，JDK 17 起标记废弃。

**机制**：
- JEP 411 弃用 SecurityManager。
- 很多库依赖它做沙箱，但配置复杂且易绕过。
- 现代替代是进程隔离、容器、最小权限与模块系统。
- 未来版本可能移除。

**追问**：存量系统怎么办？逐步移除依赖，使用容器/OS 权限与代码审查替代。

### Q30. JAAS 是什么？适合什么场景？

**结论**：Java 认证与授权服务，提供可插拔的 LoginModule 与 Subject/Principal 模型。

**机制**：
- `LoginContext` 加载 LoginModule。
- 认证成功后填充 Subject。
- 授权可结合 Policy 或业务 RBAC。
- 适合企业级认证集成（LDAP、Kerberos）。

**追问**：Spring Security 与 JAAS 关系？Spring Security 有自己的认证授权链，通常不直接依赖 JAAS。

### Q31. Java 反序列化为什么危险？如何防护？

**结论**：反序列化会按字节流构造对象并触发 readObject/反射链，可被利用执行任意代码。

**机制**：
- 攻击者控制序列化数据，构造 gadget chain。
- `ObjectInputStream` 默认无类型白名单。
- 防护：不使用原生反序列化、白名单 `ObjectInputFilter`、升级依赖。
- JEP 290 提供过滤器。

**追问**：JSON 反序列化安全吗？相对安全，但仍有 autoType、多态与 SSRF 等风险，需白名单。

### Q32. 常见加密误用有哪些？

**结论**：ECB、固定 IV、硬编码密钥、弱随机、Base64 当加密、无认证、错误填充处理。

**机制**：
- ECB 泄露模式。
- GCM IV 复用灾难性。
- 密钥进代码仓库。
- `Random` 生成密钥。
- 只加密不认证，易被篡改。
- 填充错误直接返回细节，触发 Padding Oracle。

**追问**：怎么快速自查？统一封装 CryptoUtils、禁用弱算法、代码扫描密钥、强制 GCM/OAEP。

### Q33. Java 如何支持国密算法？

**结论**：JDK 原生不完整支持 SM2/SM3/SM4，通常通过 BouncyCastle 或国密 Provider 实现。

**机制**：
- BouncyCastle 提供 SM2/SM3/SM4。
- 需要注册 Provider 并使用对应算法名。
- 合规场景需使用通过认证的密码模块。
- 注意证书、签名格式与互操作。

**追问**：国密 TLS 怎么做？需要支持 GMSSL/TLCP 的 Provider 与双证书配置。

### Q34. 代码签名与签名 JAR 的作用是什么？

**结论**：用私钥对 JAR 摘要签名，运行时验证签名与证书，确保代码来源与完整性。

**机制**：
- `jarsigner` 生成签名块。
- JAR 包含 MANIFEST 摘要与签名文件。
- 运行时校验签名与证书链。
- 防止篡改与恶意替换。

**追问**：签名能防反编译吗？不能，只保证完整性与来源，混淆/加密另做。

### Q35. 生产环境密码学实践清单有哪些？

**结论**：算法现代化、密钥托管、随机源安全、协议加固、最小权限、审计与轮换。

**机制**：
- 对称 AES-GCM，非对称 RSA-OAEP/ECDSA，摘要 SHA-256+。
- 密钥放 KMS/HSM，禁止硬编码，定期轮换。
- 使用 SecureRandom，IV/nonce 唯一。
- TLS 1.2+，禁用弱套件，校验证书。
- 日志脱敏，密钥与密文分级管理。
- 依赖漏洞扫描与安全测试。

**追问**：历史数据怎么迁移？双写/双读过渡，先新算法写入，再批量重加密旧数据。

---

## 七、进阶补充（Q36–Q40）

### Q36. JWT 的结构是什么？有哪些安全风险？

**结论**：JWT 由 Header.Payload.Signature 三段组成，默认只签名不加密；风险在算法混淆、密钥泄露、过期与重放。

**机制**：
- Header 声明算法，Payload 存声明，Signature 防篡改。
- Base64URL 编码，任何人都能解码 Payload。
- 禁止 `alg=none`，校验算法白名单。
- 校验 iss/aud/exp/nbf，防止重放与越权。
- 敏感信息不要放 Payload。

**追问**：JWT 能主动失效吗？无状态 JWT 需短 TTL + 黑名单/版本号机制。

### Q37. OAuth2 与 OIDC 的区别是什么？

**结论**：OAuth2 是授权框架，OIDC 在其上增加身份认证层，提供 ID Token 与用户信息。

**机制**：
- OAuth2 关注“能否访问资源”，核心是 access token。
- OIDC 关注“你是谁”，返回 id_token（JWT）。
- 常见授权码模式适合 Web，PKCE 适合移动/SPA。
- 不要用 implicit 流程处理敏感令牌。

**追问**：access token 与 refresh token 区别？前者短期访问，后者换取新令牌，需严格保护。

### Q38. CSRF 与 SSRF 如何防护？

**结论**：CSRF 利用用户已登录会话发起请求；SSRF 让服务端访问内网/任意地址；分别用令牌校验与出站白名单。

**机制**：
- CSRF：SameSite Cookie、CSRF Token、校验 Origin/Referer。
- SSRF：校验 URL 协议/域名/IP，禁止内网地址与重定向。
- 最小权限与网络分段。
- 记录与告警异常出站请求。

**追问**：JWT 放 Header 能防 CSRF 吗？能降低风险，但仍需 SameSite 与 CORS 配置。

### Q39. TLS 1.3 有哪些改进？

**结论**：更短握手、强制前向保密、移除弱算法，默认使用 AEAD 与现代密钥交换。

**机制**：
- 减少握手往返，支持 0-RTT（有重放风险）。
- 密钥交换以 ECDHE 为主，提供前向保密。
- 移除 RSA 密钥传输、RC4、SHA-1 等。
- 需要客户端/服务端与中间设备兼容。

**追问**：0-RTT 有什么风险？重放攻击，敏感操作应禁用或加防重放。

### Q40. 密钥轮换与 Secret 管理怎么做？

**结论**：密钥集中托管（KMS/HSM/密钥管理服务），定期轮换、版本化、最小权限，禁止硬编码与明文入库。

**机制**：
- 密钥分级与环境隔离。
- 轮换采用双密钥过渡，先解密兼容再切换。
- 记录密钥版本与使用审计。
- 应用通过短期凭证获取密钥，减少长期暴露。

**追问**：历史数据怎么轮换？批量重加密或保留旧密钥只解密，逐步迁移。

---

## 八、进阶补充（Q41–Q45）

### Q41. 国密 SM2/SM3/SM4 的工程落地要注意什么？

**结论**：通过合规 Provider 提供 SM2/SM3/SM4，注意证书格式、签名编码、互操作与合规认证。

**机制**：
- SM2 非对称签名/加密，SM3 摘要，SM4 对称加密。
- JDK 原生不完整支持，常用 BouncyCastle/国密 Provider。
- 证书与签名格式需与对方系统对齐。
- 合规场景需使用认证密码模块。

**追问**：SM2 与 RSA 能直接替换吗？算法、密钥与签名格式不同，接口与协议需适配；基础见 [Q33](225_JavaSecurity与Crypto面试题库.md)。

### Q42. HSM/KMS 如何做到密钥不落地？

**结论**：密钥生成与运算在 HSM/KMS 内完成，应用只持有密钥标识与短期凭证，避免明文密钥进入应用内存。

**机制**：
- KMS 提供加密/解密/签名 API。
- HSM 通过 PKCS#11 等接口调用。
- 应用使用短期凭证与最小权限。
- 审计每次密钥使用。

**追问**：KMS 调用延迟会影响性能吗？会，可缓存数据密钥或本地加解密分层。

### Q43. FIPS 140 合规对 Java 应用有什么要求？

**结论**：使用经过认证的密码模块、限定算法与密钥长度、启用 FIPS 模式并保留审计证据。

**机制**：
- 选择 FIPS 认证的 Provider/库。
- 禁用非批准算法（如 MD5、RC4）。
- 配置 FIPS 模式与自检。
- 记录配置、版本与测试结果。

**追问**：普通业务需要 FIPS 吗？金融、政企等合规场景才强制，普通业务按安全基线即可。

### Q44. SecureRandom 为什么会阻塞？如何避免？

**结论**：部分熵源在熵池不足时会阻塞；可选用非阻塞算法或预热，但不能降低安全强度。

**机制**：
- `/dev/random` 可能阻塞，`/dev/urandom` 通常不阻塞。
- NativePRNG/DRBG 行为依赖实现与配置。
- 启动时预热熵源。
- 避免在高并发路径频繁新建 SecureRandom。

**追问**：能用普通 Random 替代吗？不能用于密钥/IV/盐，必须使用密码学安全随机；基础见 [Q5](225_JavaSecurity与Crypto面试题库.md)。

### Q45. 密钥泄露后如何应急与演练？

**结论**：立即吊销/轮换密钥、评估影响范围、阻断旧密钥、审计异常访问，并定期演练。

**机制**：
- 启用新密钥并双密钥过渡。
- 吊销证书/令牌，通知依赖方。
- 排查日志与异常调用。
- 重加密敏感数据或重置凭证。
- 复盘并更新流程。

**追问**：轮换后旧数据怎么办？保留旧密钥仅解密，逐步重加密；流程见 [Q40](225_JavaSecurity与Crypto面试题库.md)。

---

## 九、进阶补充（Q46–Q50）

### Q46. 密钥派生函数（KDF）如何选择？

**结论**：密码派生用 PBKDF2/Argon2/scrypt/bcrypt，密钥派生用 HKDF；按场景选择慢哈希与盐。

**机制**：
- PBKDF2 兼容性好，迭代次数可调。
- Argon2 抗 GPU/ASIC，内存参数可调。
- HKDF 用于从主密钥派生用途密钥。
- 必须使用随机盐与足够强度。

**追问**：能直接用 SHA-256 当 KDF 吗？不能，缺少慢化与盐，易被暴力破解；密码存储见 [Q20](225_JavaSecurity与Crypto面试题库.md)。

### Q47. 信封加密是什么？

**结论**：用数据密钥加密数据，再用主密钥加密数据密钥，主密钥留在 KMS/HSM，实现密钥分层与轮换。

**机制**：
- 每条数据/批次生成数据密钥。
- 密文包含加密的数据密钥。
- 主密钥只用于加解密数据密钥。
- 轮换主密钥不影响历史数据。

**追问**：数据密钥需要保存吗？以加密形式随密文保存，使用时由 KMS 解密；密钥管理见 [Q40](225_JavaSecurity与Crypto面试题库.md)。

### Q48. 证书吊销如何检查？

**结论**：用 CRL 或 OCSP 检查吊销状态，OCSP Stapling 减少延迟；失败时按安全策略处理。

**机制**：
- CRL 定期下载列表。
- OCSP 实时查询状态。
- Stapling 由服务端附带响应。
- 查询失败可软失败/硬失败。

**追问**：软失败有什么风险？可能接受已吊销证书，需按业务安全等级决定。

### Q49. 安全审计日志应记录什么？

**结论**：记录认证、授权、密钥使用、敏感数据访问与配置变更，保证完整、防篡改与可追溯。

**机制**：
- 用户/服务身份与操作对象。
- 时间、结果与来源 IP。
- 密钥/证书使用记录。
- 日志防篡改与保留周期。

**追问**：审计日志能包含敏感数据吗？不能，需脱敏并控制访问。

### Q50. 供应链安全如何治理？

**结论**：依赖漏洞扫描、版本锁定、制品签名与来源校验，减少恶意依赖与供应链攻击。

**机制**：
- SCA 扫描依赖漏洞与许可证。
- 锁定版本与校验和。
- 制品签名/ SBOM。
- 私有仓库与可信源。

**追问**：扫描无漏洞就安全吗？不够，还需防投毒、权限与构建链安全。

---

## 十、进阶补充（Q51–Q55）

### Q51. WebAuthn/Passkey 是什么？

**结论**：基于公钥的无密码认证标准，用户用设备生物识别/安全密钥完成认证，服务端只存公钥。

**机制**：
- 注册时生成密钥对，私钥留在设备。
- 认证时服务端发 challenge，设备签名。
- 防钓鱼，绑定域名。
- 可替代部分密码/短信验证码。

**追问**：Passkey 能完全替代密码吗？可作为主认证或二次认证，存量系统需兼容方案。

### Q52. SAML 与 OAuth2/OIDC 有什么区别？

**结论**：SAML 偏企业 SSO 的 XML 断言，OAuth2/OIDC 偏授权与 API 访问，协议与使用场景不同。

**机制**：
- SAML 用 IdP/SP 交换 XML Assertion。
- OIDC 用 id_token（JWT）表达身份。
- SAML 配置复杂但企业集成成熟。
- OIDC 更适合移动/Web API。

**追问**：新系统选哪个？对外 API/移动端优先 OIDC，企业内 SSO 视现有 IdP 选择。

### Q53. OAuth2 PKCE 的作用是什么？

**结论**：为授权码流程增加动态 code_verifier/challenge，防止授权码被截获后滥用，适合公共客户端。

**机制**：
- 客户端生成随机 verifier。
- 发送其哈希 challenge。
- 换令牌时提交 verifier。
- 移动/SPA 不应使用隐式流程。

**追问**：PKCE 能替代客户端密钥吗？公共客户端可，机密客户端仍建议密钥 + PKCE。

### Q54. 证书/公钥固定（Pinning）有什么利弊？

**结论**：固定证书或公钥可防中间人，但证书轮换会导致客户端失效，需备份 pin 与更新机制。

**机制**：
- 固定证书哈希或公钥。
- 防伪造 CA 证书。
- 轮换需提前下发备份 pin。
- 移动端风险更高。

**追问**：Pinning 适合所有应用吗？不是，轮换与运维成本高，按安全等级使用。

### Q55. 密码敏捷性（Crypto Agility）是什么？

**结论**：算法与密钥可配置、可平滑轮换，避免硬编码导致升级或漏洞修复困难。

**机制**：
- 算法/密钥版本化。
- 密文携带算法标识。
- 支持双算法过渡。
- 淘汰弱算法有迁移路径。

**追问**：为什么不能硬编码 AES？漏洞或合规变化时无法快速切换；密钥管理见 [Q40](225_JavaSecurity与Crypto面试题库.md)。

---

## 十一、速记清单

| 主题 | 必背一句话 |
|---|---|
| JCA/JCE | Provider 提供算法，Cipher 三段式指定算法/模式/填充 |
| 对称 | AES-GCM 首选，IV 唯一，禁用 ECB |
| 非对称 | RSA-OAEP/ECDSA，私钥签名公钥验签 |
| 摘要 | SHA-256+，MD5/SHA-1 不再安全 |
| 密码 | bcrypt/Argon2 + 盐 + 迭代 |
| 密钥 | KeyStore/KMS/HSM，禁止硬编码 |
| TLS | 证书链 + 主机名 + 现代协议套件 |
| 安全模型 | SecurityManager 已废弃，最小权限与隔离 |

---

**文档版本：v1.0 · 2026-11-05**  
**题目数量：55 题 · 详解**
