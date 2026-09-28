package com.github.h3yow.localmessagebrokerjava.web;

import com.github.h3yow.localmessagebrokerjava.broker.MessageBroker;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetter;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.Message;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Broker HTTP 接口：
 * <ul>
 *   <li>POST   /api/topics/{topic}                       创建主题</li>
 *   <li>POST   /api/topics/{topic}/messages              发布消息</li>
 *   <li>POST   /api/topics/{topic}/pull                  拉取消息</li>
 *   <li>POST   /api/topics/{topic}/messages/{offset}/ack    确认</li>
 *   <li>POST   /api/topics/{topic}/messages/{offset}/nack   否定确认（失败重试/死信）</li>
 *   <li>POST   /api/topics/{topic}/seek                  移动消费位置（只许向前）</li>
 *   <li>POST   /api/admin/reinitialize                   组件重新初始化：在途消息退回重投</li>
 *   <li>GET    /api/topics/{topic}/groups/{group}/dead-letters  查询死信</li>
 *   <li>GET    /api/topics/{topic}/groups/{group}/status       查询消费组状态</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class BrokerController {

    private final MessageBroker broker;

    public BrokerController(MessageBroker broker) {
        this.broker = broker;
    }

    @PostMapping("/topics/{topic}")
    public ResponseEntity<Void> createTopic(@PathVariable String topic) {
        broker.createTopic(topic);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PostMapping("/topics/{topic}/groups")
    public ResponseEntity<Void> createGroup(@PathVariable String topic,
                                            @RequestBody(required = false) GroupRequest request) {
        if (request == null || request.group() == null || request.group().isBlank()) {
            throw invalid("group 不能为空");
        }
        broker.createGroup(topic, request.group());
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PostMapping("/topics/{topic}/messages")
    public Map<String, Object> publish(@PathVariable String topic, @RequestBody(required = false) PublishRequest request) {
        if (request == null) {
            throw invalid("请求体缺失，需要 payload");
        }
        PublishResult result = broker.publish(topic, request.payload(),
                request.dedupKey() == null || request.dedupKey().isBlank() ? null : request.dedupKey());
        return Map.of("offset", result.offset(), "deduped", result.deduped());
    }

    @PostMapping("/topics/{topic}/pull")
    public ResponseEntity<Map<String, Object>> pull(@PathVariable String topic,
                                                    @RequestBody(required = false) PullRequest request) {
        if (request == null) {
            throw invalid("请求体缺失，需要 group 与 consumer");
        }
        var delivery = broker.pull(topic, request.group(), request.consumer());
        if (delivery.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        Delivery d = delivery.get();
        Message m = d.message();
        return ResponseEntity.ok(Map.of(
                "offset", m.offset(),
                "payload", m.payload(),
                "dedupKey", m.dedupKey() == null ? "" : m.dedupKey(),
                "group", d.group(),
                "consumer", d.consumer(),
                "deliveryCount", d.deliveryCount(),
                "leaseId", d.leaseId()));
    }

    @PostMapping("/topics/{topic}/messages/{offset}/ack")
    public Map<String, Object> ack(@PathVariable String topic, @PathVariable long offset,
                                   @RequestBody(required = false) AckRequest request) {
        if (request == null || request.group() == null || request.group().isBlank()) {
            throw invalid("group 不能为空");
        }
        if (request.leaseId() == null) {
            throw invalid("leaseId 不能为空");
        }
        broker.ack(topic, request.group(), offset, request.leaseId());
        return Map.of("offset", offset, "status", "acked");
    }

    @PostMapping("/topics/{topic}/messages/{offset}/nack")
    public Map<String, Object> nack(@PathVariable String topic, @PathVariable long offset,
                                    @RequestBody(required = false) AckRequest request) {
        if (request == null || request.group() == null || request.group().isBlank()) {
            throw invalid("group 不能为空");
        }
        if (request.leaseId() == null) {
            throw invalid("leaseId 不能为空");
        }
        broker.nack(topic, request.group(), offset, request.leaseId(),
                request.reason() == null || request.reason().isBlank() ? "UNSPECIFIED" : request.reason());
        return Map.of("offset", offset, "status", "nacked");
    }

    @PostMapping("/topics/{topic}/seek")
    public Map<String, Object> seek(@PathVariable String topic,
                                    @RequestBody(required = false) SeekRequest request) {
        if (request == null || request.group() == null || request.group().isBlank()) {
            throw invalid("group 不能为空");
        }
        if (request.offset() == null) {
            throw invalid("offset 不能为空");
        }
        broker.seek(topic, request.group(), request.offset());
        return Map.of("offset", request.offset(), "status", "seeked");
    }

    @PostMapping("/admin/reinitialize")
    public Map<String, Object> reinitialize() {
        int returned = broker.reinitializeInflight();
        return Map.of("returnedInflight", returned);
    }

    @GetMapping("/topics/{topic}/groups/{group}/dead-letters")
    public List<DeadLetter> deadLetters(@PathVariable String topic, @PathVariable String group) {
        return broker.deadLetters(topic, group);
    }

    @GetMapping("/topics/{topic}/groups/{group}/status")
    public GroupStatus status(@PathVariable String topic, @PathVariable String group) {
        return broker.groupStatus(topic, group);
    }

    private static BrokerException invalid(String reason) {
        return new BrokerException(BrokerErrorCode.INVALID_REQUEST, reason);
    }
}
