# 本地进程内消息队列（local-message-broker-java）

一个只依赖内存与本地能力、不依赖任何外部中间件的进程内消息队列核心，通过 HTTP 对外提供
**发布 / 订阅（消费组）/ 拉取 / 确认 / 失败重试 / 死信 / 背压 / 去重**能力。
基于 Spring Boot 3.4 + Java 21，启动即用，数据不持久化（进程退出即清空）。

## 1. 快速开始

```bash
mvn spring-boot:run            # 默认随机端口（server.port=0）；可在 application.properties 固定端口
mvn test                       # 运行全部 22 个测试（单元 + 并发 + HTTP 端到端）
```

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/topics/{topic}` | 创建主题（幂等） |
| POST | `/api/topics/{topic}/groups` | 注册消费组（幂等；body `{"group":"g1"}`） |
| POST | `/api/topics/{topic}/messages` | 发布消息；body `{"payload":"...", "dedupKey":"可选"}` |
| POST | `/api/topics/{topic}/pull` | 拉取消息；body `{"group":"g1","consumer":"c1"}`，无消息返回 `204` |
| POST | `/api/topics/{topic}/messages/{offset}/ack` | 确认；body `{"group":"g1","leaseId":123}` |
| POST | `/api/topics/{topic}/messages/{offset}/nack` | 否定确认（消费失败）；body 额外带 `"reason"` |
| POST | `/api/topics/{topic}/seek` | 移动消费位置，**只允许向前**；body `{"group":"g1","offset":5}` |
| POST | `/api/admin/reinitialize` | 组件重新初始化：所有在途（未确认）消息退回重投 |
| GET | `/api/topics/{topic}/groups/{group}/status` | 查询消费位点 / 在途 / 死信偏移量 |
| GET | `/api/topics/{topic}/groups/{group}/dead-letters` | 查询死信明细 |

拉取响应示例：

```json
{ "offset": 0, "payload": "hello", "dedupKey": "", "group": "g1",
  "consumer": "c1", "deliveryCount": 1, "leaseId": 42 }
```

错误响应统一为 `{"code": "错误码", "reason": "具体原因"}`：

| 错误码 | HTTP 状态码 | 含义 |
| --- | --- | --- |
| `TOPIC_NOT_FOUND` / `GROUP_NOT_FOUND` | 404 | 主题/消费组不存在 |
| `INVALID_MESSAGE` | 400 | 消息本身非法（空内容、超长） |
| `INVALID_REQUEST` / `INVALID_OFFSET` | 400 | 请求参数非法 / 偏移量越界 |
| `CAPACITY_FULL` | **503** | 队列瞬时写满（背压，稍后可重试） |
| `LEASE_NOT_FOUND` | 409 | 在途租约不存在或已失效（旧/错 leaseId） |
| `SEEK_BACKWARD_REJECTED` | 409 | 回退消费位置被明确拒绝，reason 中说明当前位点与请求位点 |

### curl 速览

```bash
curl -X POST localhost:8080/api/topics/orders
curl -X POST localhost:8080/api/topics/orders/groups -H 'Content-Type: application/json' -d '{"group":"billing"}'
curl -X POST localhost:8080/api/topics/orders/messages -H 'Content-Type: application/json' -d '{"payload":"{\"id\":1}","dedupKey":"order-1"}'
curl -X POST localhost:8080/api/topics/orders/pull     -H 'Content-Type: application/json' -d '{"group":"billing","consumer":"node-A"}'
curl -X POST localhost:8080/api/topics/orders/messages/0/ack  -H 'Content-Type: application/json' -d '{"group":"billing","leaseId":1}'
```

## 3. 投递语义：至少一次（at-least-once）

- 消息被 `pull` 后进入该消费组的**在途表（inflight）**并获得一个单调递增的 `leaseId`；
  在确认/否定确认/重新初始化之前，同组任何消费者都拉不到这条消息，因此
  **同一条消息在同一个消费组里同一时刻只会被一个消费者处理**。
- 不同消费组各自维护独立的消费位点，因此**每个消费组都能独立消费到完整消息流**。
  消息只有被**所有已注册消费组**连续越过后才会被物理回收。组建议在发布前通过
  `/groups` 注册（`pull` 时也会惰性自动建组，但自动建组只能看到当时仍保留在内存中的消息）。
- 消费组的已确认位置 `committed`（语义为“下一个待处理偏移量”）**只能向前推进**：
  内部以“连续位点 + 乱序完成集合”实现，乱序确认时先记录、等空洞补齐再连续折叠；
  并发确认、重复确认都不会让位点回退，也不会让一次确认重复生效。
- **重复确认幂等**：已终态（已确认/已死信）的消息再确认直接忽略；
  消息仍在途但 `leaseId` 不匹配时返回 `LEASE_NOT_FOUND`，防止旧持有者迟到的确认误生效。
- 消费者失败时调用 `nack`（带可区分的 `reason`），消息立即重新可投递；
  若应用内组件崩溃/重新初始化，调用 `POST /api/admin/reinitialize`
  （或直接重新调用 `MessageBroker.reinitializeInflight()`）会把**所有未确认的在途消息
  解除租约退回重投，已确认位点与死信保持不变**，消息不会被静默丢掉。
  退回重投的消息 `deliveryCount` 递增并分配新的 `leaseId`。
- `seek` 只允许向前：目标位点小于当前 `committed` 一律返回
  `SEEK_BACKWARD_REJECTED`（409）并在 reason 中写明“拒绝回退”、当前位点与请求位点。

## 4. 重试与死信规则

- 每次实际投递都会把该消息在该组的投递次数 `deliveryCount` 加 1（首次为 1）。
- `nack` 时若 `deliveryCount < broker.max-delivery-attempts`（默认 **3**），消息进入重试：
  下一次 `pull` 会以更大的投递次数、新的租约重新投出，日志记录
  `消息消费失败，将重试 ... 已投递=N次 ... 失败原因=...`。
- `nack` 时若已达到上限，消息进入**死信**：
  - 记录 `topic/group/offset/payload/attempts/lastReason`，可通过
    `GET .../dead-letters` 查询，最后一次失败原因原样保留；
  - 死信视为该消息的终态，消费位点可以连续越过它继续向前；
  - 死信消息不会再被投递；
  - 日志以 ERROR 打印 `消息进入死信 ... 累计投递=N次, 最后失败原因=...`。

## 5. 背压策略

- 每个主题有容量上限 `broker.topic-capacity`（默认 1000 条）。
- 发布时先尝试物理回收“所有消费组都已连续越过”的消息前缀；回收后仍然写满，
  发布立即失败并返回 **503 `CAPACITY_FULL`**（瞬时背压，调用方稍后重试即可成功），
  与主题不存在（404）、消息非法（400）三类失败原因彼此可区分。
- 单条消息大小受 `broker.max-payload-bytes`（默认 64 KiB，按 UTF-8 字节计）限制，
  空内容或超限返回 400 `INVALID_MESSAGE`。
- 位点之前的未确认消息（在途）会继续占用容量，因此慢消费者会对发布端形成背压，
  消费确认后容量自然释放。

## 6. 去重策略

- 发布时可携带 `dedupKey`（主题级去重，跨消费组生效）。
- 在有效窗口 `broker.dedup-window-millis`（默认 5 分钟）内，相同去重键的重复发布
  **不会产生新消息、不会被重复投递**：返回第一条消息的 `offset` 且 `deduped=true`。
- 窗口过期后（惰性清理，发生在下一次携带相同键的发布时），相同键视为新消息。
- 不带去重键（空串或 null）的消息不参与去重。

## 7. 可配置项（`application.properties`）

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `broker.topic-capacity` | 1000 | 每主题保留消息条数上限，写满返回 CAPACITY_FULL |
| `broker.max-payload-bytes` | 65536 | 单条 payload 最大字节数 |
| `broker.max-delivery-attempts` | 3 | 达到此次数后再 nack 即进入死信 |
| `broker.dedup-window-millis` | 300000 | 去重键有效窗口（毫秒） |

## 8. 可观测性与日志

关键路径均有结构化中文日志，至少包含：
消费位置 `committed`、投递次数（第 N 次投递/累计投递）、拒绝与失败原因
（`CAPACITY_FULL`、`LEASE_NOT_FOUND`、`SEEK_BACKWARD_REJECTED`、业务 `reason` 等）。
`GET .../groups/{group}/status` 返回 `committedOffset / inflightOffsets /
deadLetterOffsets`，方便断言与排障。

## 9. 本地验证（测试场景对照）

```bash
mvn test
```

| 场景 | 测试类 / 用例 |
| --- | --- |
| 多消费组独立消费、组内排他、乱序/重复确认位点只进不退、旧租约拒绝、nack 重投 | `GroupConsumptionSemanticsTest` |
| 重试到上限进死信、失败原因保留、位点越过死信继续消费 | `RetryDeadLetterTest` |
| 回退位点被明确拒绝、向前 seek、重新初始化后未确认消息重投且次数递增 | `SeekAndReinitializeTest` |
| 三类发布拒绝原因可区分、写满为瞬时背压、确认后释放容量、去重窗口内外行为 | `BackpressureDedupTest` |
| 8 线程并发发布（偏移连续唯一/拒绝计数精确）、6 消费者并发消费（每条恰好处理一次）、并发重复确认 | `ConcurrencyTest` |
| HTTP 端到端状态码/错误码、重试死信链路、重新初始化、去重 | `BrokerHttpIntegrationTest` |

关键不变量断言：成功偏移量连续唯一；同组每条消息恰好被一个消费者处理一次；
任何交错下 `committedOffset` 最终恰好等于消息总数且中途不回退；
超容量发布拒绝数精确等于“尝试数 - 容量”；死信消息不再投递。

## 10. 范围与限制

- 纯内存、无持久化：进程重启数据清空；`reinitializeInflight()` 模拟的是
  “应用内组件重新初始化（如连接断开、消费者重建）”这一场景，而非进程重启。
- 拉取为非长轮询：无消息立即返回 `204`，由调用方决定重试节奏。
- 单实例、单进程：通过每主题一把锁保证线程安全，不涉及网络分区与多副本。
