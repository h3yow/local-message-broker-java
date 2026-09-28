# 本地进程内消息队列（Local In-Process Message Broker）

一个纯内存、零外部中间件依赖的消息队列核心，通过 HTTP 对外提供
**发布 / 订阅 / 拉取 / 确认 / 否定确认 / 重初始化 / 位点调整**能力。
基于 Spring Boot 3 + JDK 21，所有状态保存在进程内存中。

> 适用范围：本地开发、集成测试、教学演示、单机工具。重启进程后数据不保留。

---

## 1. 核心模型

- **主题（Topic）**：一条 append-only 日志，消息按到达顺序获得从 0 开始连续递增的 `offset`。
  每个主题有独立的容量上限（背压）。
- **消费组（Consumer Group）**：
  - 同一条消息在**同一个消费组内同一时刻只会投递给一个消费者**（在途互斥）。
  - **不同消费组各自独立消费完整一份消息**，互不影响确认位点。
- **已提交消费位置 `committedPosition`**：该组已连续处理完成（已确认或已死信）的位置，
  语义为“下一个待提交的 offset”。该值**只增不减**。

## 2. 投递语义：至少一次（At-Least-Once）

- 消息被 `pull` 后进入该组的**在途集合（in-flight）**，在被 `ack` / `nack` 前不会再次分发。
- `ack` 后消息进入已确认终态；位点在连续前缀完整后才向前推进，因此：
  - **乱序确认**（先确认大 offset 再确认小 offset）不会丢消息，位点会在小 offset 补齐后连贯前进；
  - **重复确认**是幂等的：已确认或位点之前的 `ack` 返回 `duplicateAck=true`，位点不回退；
  - 从未投递过、或正在等待重试退避的 offset 被确认会被拒绝（`INVALID_DELIVERY`）。
- **组件重新初始化后不丢消息**：调用 `reinitialize`（模拟消费者实例重启/重平衡）会把
  所有“在途 + 等待重试”的消息退回可投递状态，下一次 `pull` 会重新拉到它们，
  投递次数计数重置为 1，且位点不会前移。
- 位点只能前进：任何 `seek` 到当前位点之前的请求都返回 `409 SEEK_REJECTED`，
  错误信息明确说明“committed position can never move backwards”。
  向前 seek 也会在存在未决消息（在途/重试中且位于目标之前）时被拒绝，避免静默跳过未确认消息。

## 3. 失败、重试与死信

- `nack` 必须携带 `reason`（可区分的失败原因，空值会被记为 `unspecified`）。
- 消息 nack 后进入**重试等待**，经过固定退避窗口后可被再次 `pull`；
  投递视图中的 `attempt` 从 1 递增，可观测当前是第几次投递。
- 当投递次数达到上限 `broker.max-delivery-attempts`（默认 3）后再次 nack，
  消息进入**死信队列（DLQ）**：
  - 永不再投递；位点可越过它继续前进；
  - 死信记录保留 `offset / payload / attempts / lastReason / deadLetteredAt`；
  - 对已死信消息再 `ack` 返回 `409 MESSAGE_DEAD_LETTERED`。
- 死信可通过 `GET .../dead-letters` 查询。

## 4. 背压（发布容量上限）

- 每个主题有容量上限：创建时可指定 `capacity`，否则取全局默认 `broker.topic-capacity`（默认 1000）。
- 队列写满时发布**立即失败**，错误码 `QUEUE_FULL`（HTTP 503），
  表示**瞬时**背压——消费者推进后可以重试发布。
- 发布失败原因通过 `errorCode` 明确区分：

| 场景 | errorCode | HTTP |
| --- | --- | --- |
| 队列瞬时写满 | `QUEUE_FULL` | 503 |
| 主题不存在 | `TOPIC_NOT_FOUND` | 404 |
| 消息非法（payload 为空、名称空白、JSON 畸形等） | `INVALID_MESSAGE` | 422 |
| 去重窗口内重复（非异常，响应体 `duplicate=true`） | — | 200 |
| 消费组不存在（未订阅先拉取） | `GROUP_NOT_FOUND` | 404 |
| 重复创建主题 / 重复订阅 | `TOPIC_ALREADY_EXISTS` / `GROUP_ALREADY_SUBSCRIBED` | 409 |
| 回退位点 / 跳过未决消息 | `SEEK_REJECTED` | 409 |
| 确认已死信消息 | `MESSAGE_DEAD_LETTERED` | 409 |
| 确认未在途 offset | `INVALID_DELIVERY` | 422 |

## 5. 去重

- 发布时可携带 `dedupKey`。在有效窗口 `broker.dedup-window-millis`（默认 60000ms）内，
  同一个去重键**只有第一次发布会真正入队**；后续发布返回
  `{"offset": <首次偏移量>, "duplicate": true}`，不产生重复投递。
- 窗口按发布时间惰性判定：超过窗口后同键可再次发布。过期键采用惰性清理，无后台线程。
- 不带 `dedupKey` 的消息不参与去重。

## 6. 并发与一致性

- 每个主题的发布在主题追加锁内串行，保证 offset 连续、容量判定与入队原子。
- 每个消费组有独立组锁，`pull / ack / nack / seek / reinitialize` 在组内串行：
  同一条消息不可能同时出现在两个消费者手中，位点更新与在途集合变更原子一致。
- 组之间完全并行。关键不变量：
  1. 同一组内每个 offset 任一时刻至多一个在途持有者；
  2. 每条消息最终要么被确认、要么进死信，不会静默丢失；
  3. `committedPosition` 单调不减；
  4. 超出容量的发布数与接纳数之和恰等于总发布尝试数，接纳 offset 连续无空洞。

## 7. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/topics` | 创建主题，body：`{"name":"t","capacity":100}`（capacity 可省略） |
| POST | `/api/topics/{t}/messages` | 发布，body：`{"payload":"...","dedupKey":"..."}` |
| POST | `/api/topics/{t}/subscriptions` | 消费组订阅（从最新位置之后开始），body：`{"group":"g"}` |
| POST | `/api/topics/{t}/groups/{g}/pull` | 拉取一条；无消息返回 **204** |
| POST | `/api/topics/{t}/groups/{g}/ack` | 确认，body：`{"offset":0}` |
| POST | `/api/topics/{t}/groups/{g}/nack` | 否定确认（失败重试/死信），body：`{"offset":0,"reason":"..."}` |
| POST | `/api/topics/{t}/groups/{g}/reinitialize` | 重初始化，退回所有未决消息 |
| POST | `/api/topics/{t}/groups/{g}/seek` | 调整位点，body：`{"offset":5}` |
| GET  | `/api/topics/{t}/groups/{g}/status` | 位点 / 在途数 / 死信数 |
| GET  | `/api/topics/{t}/groups/{g}/dead-letters` | 死信列表 |

## 8. 配置项（application.properties 可覆盖）

```properties
broker.topic-capacity=1000      # 每主题默认容量
broker.max-delivery-attempts=3  # 最大投递次数，超过后进死信
broker.retry-backoff-millis=100 # 重试固定退避
broker.dedup-window-millis=60000# 去重键有效窗口
server.port=8080                # 当前默认为 0（随机端口），本地验证可固定
```

## 9. 本地验证

### 9.1 运行测试

```bash
mvn test
```

覆盖的场景：

- 组内并发消费，每条消息恰好处理一次、无重复分发；
- 混合成功/失败的并发消费，重试后全部确认、无死信；
- 并发发布：offset 连续，超出容量全部得到 `QUEUE_FULL`；
- 8 线程对同一批 offset 重复/乱序确认，位点单调且精确到达终点；
- nack 重试到上限进入死信、死信原因与次数可查、死信后 ack 被拒；
- 重试退避窗口内不重复投递；
- `reinitialize` 后未确认消息重新投递且 attempt 重置；
- seek 回退被明确拒绝、带未决消息的前跳被拒绝；
- 去重窗口内重复发布只投递一次；
- 全流程 HTTP 集成测试（错误码、204、201 等状态码）。

### 9.2 curl 手工演练

```bash
# 0. 启动（固定端口）
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# 1. 建主题
curl -s -XPOST localhost:8080/api/topics -H 'Content-Type: application/json' \
  -d '{"name":"orders","capacity":100}'

# 2. 订阅消费组（建议先订阅再发布）
curl -s -XPOST localhost:8080/api/topics/orders/subscriptions \
  -H 'Content-Type: application/json' -d '{"group":"billing"}'

# 3. 发布（带去重键）
curl -s -XPOST localhost:8080/api/topics/orders/messages \
  -H 'Content-Type: application/json' -d '{"payload":"hello","dedupKey":"k1"}'
# -> {"offset":0,"duplicate":false}
curl -s -XPOST localhost:8080/api/topics/orders/messages \
  -H 'Content-Type: application/json' -d '{"payload":"hello","dedupKey":"k1"}'
# -> {"offset":0,"duplicate":true}

# 4. 拉取
curl -s -XPOST localhost:8080/api/topics/orders/groups/billing/pull
# -> {"topic":"orders","group":"billing","offset":0,"payload":"hello","attempt":1,"deliveryId":...}

# 5. 确认 / 失败
curl -s -XPOST localhost:8080/api/topics/orders/groups/billing/ack \
  -H 'Content-Type: application/json' -d '{"offset":0}'
# 失败重试：
curl -s -XPOST localhost:8080/api/topics/orders/groups/billing/nack \
  -H 'Content-Type: application/json' -d '{"offset":0,"reason":"downstream timeout"}'

# 6. 状态 / 死信 / 重初始化 / 位点
curl -s localhost:8080/api/topics/orders/groups/billing/status
curl -s localhost:8080/api/topics/orders/groups/billing/dead-letters
curl -s -XPOST localhost:8080/api/topics/orders/groups/billing/reinitialize
curl -s -XPOST localhost:8080/api/topics/orders/groups/billing/seek \
  -H 'Content-Type: application/json' -d '{"offset":0}'
```

服务端日志会打印消费位置（`committed=...`）、投递次数（`attempt=...`）以及
拒绝/失败原因（`queue full`、`scheduled for retry`、`moved to dead letter`、
`seek rejected ... backwards` 等）。

## 10. 局限

- 数据仅存内存，进程退出即丢失（无 WAL / 无持久化）；
- 日志不做截断（容量限制新增，不回收旧消息），长期运行需自行重建主题；
- 去重窗口基于墙钟时间；重试退避为固定值。
