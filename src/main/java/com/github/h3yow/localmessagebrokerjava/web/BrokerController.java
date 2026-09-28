package com.github.h3yow.localmessagebrokerjava.web;

import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.exception.ErrorCode;
import com.github.h3yow.localmessagebrokerjava.model.AckResult;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetterEntry;
import com.github.h3yow.localmessagebrokerjava.model.DeliveryView;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import com.github.h3yow.localmessagebrokerjava.service.MessageBroker;
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
 * 消息队列 HTTP 接口：发布 / 订阅 / 拉取 / 确认 / 否定确认 / 重初始化 / 位点调整 / 状态查询。
 */
@RestController
@RequestMapping("/api")
public class BrokerController {

    public record CreateTopicRequest(String name, Integer capacity) {
    }

    public record PublishRequest(String payload, String dedupKey) {
    }

    public record SubscribeRequest(String group) {
    }

    public record AckRequest(Long offset) {
    }

    public record NackRequest(Long offset, String reason) {
    }

    public record SeekRequest(Long offset) {
    }

    private final MessageBroker broker;

    public BrokerController(MessageBroker broker) {
        this.broker = broker;
    }

    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@RequestBody CreateTopicRequest request) {
        requireRequest(request, "request body required");
        String name = requireText(request.name(), "topic name");
        broker.createTopic(name, request.capacity() == null ? 0 : request.capacity());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(Map.of("topic", name, "created", true));
    }

    @PostMapping("/topics/{topic}/messages")
    public PublishResult publish(@PathVariable String topic, @RequestBody PublishRequest request) {
        requireRequest(request, "request body required");
        return broker.publish(topic, request.payload(), request.dedupKey());
    }

    @PostMapping("/topics/{topic}/subscriptions")
    public ResponseEntity<Void> subscribe(@PathVariable String topic,
                                          @RequestBody SubscribeRequest request) {
        requireRequest(request, "request body required");
        broker.subscribe(topic, requireText(request.group(), "group"));
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @PostMapping("/topics/{topic}/groups/{group}/pull")
    public ResponseEntity<DeliveryView> pull(@PathVariable String topic,
                                             @PathVariable String group) {
        DeliveryView delivery = broker.pull(topic, group);
        // 204：当前没有可投递消息（不是错误，区别于主题不存在等 4xx）。
        return delivery == null
                ? ResponseEntity.noContent().build()
                : ResponseEntity.ok(delivery);
    }

    @PostMapping("/topics/{topic}/groups/{group}/ack")
    public AckResult ack(@PathVariable String topic, @PathVariable String group,
                         @RequestBody AckRequest request) {
        requireRequest(request, "request body required");
        if (request.offset() == null) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE, "offset is required");
        }
        return broker.acknowledge(topic, group, request.offset());
    }

    @PostMapping("/topics/{topic}/groups/{group}/nack")
    public ResponseEntity<Void> nack(@PathVariable String topic, @PathVariable String group,
                                     @RequestBody NackRequest request) {
        requireRequest(request, "request body required");
        if (request.offset() == null) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE, "offset is required");
        }
        broker.negativeAcknowledge(topic, group, request.offset(), request.reason());
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/topics/{topic}/groups/{group}/reinitialize")
    public ResponseEntity<Map<String, Object>> reinitialize(@PathVariable String topic,
                                                            @PathVariable String group) {
        int returned = broker.reinitialize(topic, group);
        return ResponseEntity.ok(Map.of("returnedToQueue", returned));
    }

    @PostMapping("/topics/{topic}/groups/{group}/seek")
    public Map<String, Object> seek(@PathVariable String topic, @PathVariable String group,
                                    @RequestBody SeekRequest request) {
        requireRequest(request, "request body required");
        if (request.offset() == null) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE, "offset is required");
        }
        long position = broker.seek(topic, group, request.offset());
        return Map.of("committedPosition", position);
    }

    @GetMapping("/topics/{topic}/groups/{group}/status")
    public GroupStatus status(@PathVariable String topic, @PathVariable String group) {
        return broker.status(topic, group);
    }

    @GetMapping("/topics/{topic}/groups/{group}/dead-letters")
    public List<DeadLetterEntry> deadLetters(@PathVariable String topic,
                                             @PathVariable String group) {
        return broker.deadLetters(topic, group);
    }

    private static void requireRequest(Object request, String message) {
        if (request == null) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE, message);
        }
    }

    private static String requireText(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE, what + " must not be blank");
        }
        return value;
    }
}
