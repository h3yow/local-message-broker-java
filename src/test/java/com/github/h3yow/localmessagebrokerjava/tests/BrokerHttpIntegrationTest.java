package com.github.h3yow.localmessagebrokerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端 HTTP 语义：发布/订阅/拉取/确认/重试死信/背压/去重/重初始化/seek 拒绝，
 * 以及失败原因通过 errorCode 区分。
 */
@SpringBootTest(properties = {
        "broker.topic-capacity=3",
        "broker.max-delivery-attempts=2",
        "broker.retry-backoff-millis=0",
        "broker.dedup-window-millis=60000"
})
@AutoConfigureMockMvc
class BrokerHttpIntegrationTest {

    @Autowired
    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();

    private MvcResult createTopic(String name, Integer capacity) throws Exception {
        String body = capacity == null
                ? "{\"name\":\"" + name + "\"}"
                : "{\"name\":\"" + name + "\",\"capacity\":" + capacity + "}";
        return mvc.perform(post("/api/topics")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn();
    }

    private void subscribe(String topic, String group) throws Exception {
        mvc.perform(post("/api/topics/" + topic + "/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"group\":\"" + group + "\"}"))
                .andExpect(status().isCreated());
    }

    private long publish(String topic, String payload, String dedupKey) throws Exception {
        String keyPart = dedupKey == null ? "" : ",\"dedupKey\":\"" + dedupKey + "\"";
        MvcResult result = mvc.perform(post("/api/topics/" + topic + "/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"" + payload + "\"" + keyPart + "}"))
                .andExpect(status().isOk()).andReturn();
        return json.readTree(result.getResponse().getContentAsString()).get("offset").asLong();
    }

    private JsonNode pull(String topic, String group) throws Exception {
        MvcResult result = mvc.perform(post("/api/topics/" + topic + "/groups/" + group + "/pull"))
                .andReturn();
        if (result.getResponse().getStatus() == 204) {
            return null;
        }
        return json.readTree(result.getResponse().getContentAsString());
    }

    @Test
    void happyPathPublishSubscribePullAckAndStatus() throws Exception {
        createTopic("orders", null);
        subscribe("orders", "billing");
        publish("orders", "hello", null);

        mvc.perform(post("/api/topics/orders/groups/billing/pull"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.offset").value(0))
                .andExpect(jsonPath("$.payload").value("hello"))
                .andExpect(jsonPath("$.attempt").value(1));

        mvc.perform(post("/api/topics/orders/groups/billing/ack")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"offset\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedPosition").value(1))
                .andExpect(jsonPath("$.duplicateAck").value(false));

        // 重复确认：幂等成功
        mvc.perform(post("/api/topics/orders/groups/billing/ack")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"offset\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicateAck").value(true));

        mvc.perform(get("/api/topics/orders/groups/billing/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedPosition").value(1))
                .andExpect(jsonPath("$.inFlightCount").value(0))
                .andExpect(jsonPath("$.deadLetterCount").value(0));

        // 没有更多消息 -> 204
        mvc.perform(post("/api/topics/orders/groups/billing/pull"))
                .andExpect(status().isNoContent());
    }

    @Test
    void failuresHaveDistinguishableErrorCodes() throws Exception {
        // 主题不存在 -> 404 TOPIC_NOT_FOUND
        mvc.perform(post("/api/topics/nope/messages")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"payload\":\"x\"}"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("TOPIC_NOT_FOUND"));

        createTopic("small", 1);
        subscribe("small", "g");
        publish("small", "a", null);

        // 队列写满 -> 503 QUEUE_FULL（瞬时背压）
        mvc.perform(post("/api/topics/small/messages")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"payload\":\"b\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.errorCode").value("QUEUE_FULL"));

        // 非法消息 -> 422 INVALID_MESSAGE
        mvc.perform(post("/api/topics/small/messages")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"payload\":\"\"}"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errorCode").value("INVALID_MESSAGE"));

        // 未订阅的组拉取 -> 404 GROUP_NOT_FOUND
        mvc.perform(post("/api/topics/small/groups/ghost/pull"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.errorCode").value("GROUP_NOT_FOUND"));
    }

    @Test
    void dedupRejectsDuplicateWithinWindow() throws Exception {
        createTopic("dedup", null);
        subscribe("dedup", "g");
        publish("dedup", "first", "key-X");
        mvc.perform(post("/api/topics/dedup/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\":\"second\",\"dedupKey\":\"key-X\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.duplicate").value(true))
                .andExpect(jsonPath("$.offset").value(0));

        // 消费组只能收到一次
        mvc.perform(post("/api/topics/dedup/groups/g/pull"))
                .andExpect(jsonPath("$.offset").value(0));
        mvc.perform(post("/api/topics/dedup/groups/g/pull"))
                .andExpect(status().isNoContent());
    }

    @Test
    void retriesToLimitDeadLettersAndReinitRedelivers() throws Exception {
        createTopic("dlq", null);
        subscribe("dlq", "g");
        publish("dlq", "poison", null);

        // 第 1 次投递 -> nack -> 重试；第 2 次再 nack -> 死信（上限 2）
        JsonNode first = pull("dlq", "g");
        org.junit.jupiter.api.Assertions.assertEquals(1, first.get("attempt").asInt());
        mvc.perform(post("/api/topics/dlq/groups/g/nack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"offset\":0,\"reason\":\"first-blow-up\"}"))
                .andExpect(status().isAccepted());
        JsonNode second = pull("dlq", "g");
        org.junit.jupiter.api.Assertions.assertEquals(2, second.get("attempt").asInt());
        mvc.perform(post("/api/topics/dlq/groups/g/nack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"offset\":0,\"reason\":\"second-blow-up\"}"))
                .andExpect(status().isAccepted());

        mvc.perform(get("/api/topics/dlq/groups/g/status"))
                .andExpect(jsonPath("$.committedPosition").value(1))
                .andExpect(jsonPath("$.deadLetterCount").value(1));
        mvc.perform(get("/api/topics/dlq/groups/g/dead-letters"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].offset").value(0))
                .andExpect(jsonPath("$[0].attempts").value(2))
                .andExpect(jsonPath("$[0].lastReason").value("second-blow-up"));
        // 已死信不能 ack
        mvc.perform(post("/api/topics/dlq/groups/g/ack")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"offset\":0}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("MESSAGE_DEAD_LETTERED"));
    }

    @Test
    void reinitializeRedeliversUnackedAndSeekBackwardsIsRejected() throws Exception {
        createTopic("reinit", null);
        subscribe("reinit", "g");
        publish("reinit", "a", null);
        publish("reinit", "b", null);
        JsonNode d0 = pull("reinit", "g");
        JsonNode d1 = pull("reinit", "g");
        org.junit.jupiter.api.Assertions.assertEquals(0L, d0.get("offset").asLong());
        org.junit.jupiter.api.Assertions.assertEquals(1L, d1.get("offset").asLong());

        mvc.perform(post("/api/topics/reinit/groups/g/reinitialize"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.returnedToQueue").value(2));

        // 未确认的两条重新投递
        mvc.perform(post("/api/topics/reinit/groups/g/pull"))
                .andExpect(jsonPath("$.offset").value(0))
                .andExpect(jsonPath("$.attempt").value(1));

        // 回退位点被明确拒绝
        mvc.perform(post("/api/topics/reinit/groups/g/ack")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"offset\":0}"))
                .andExpect(status().isOk());
        mvc.perform(post("/api/topics/reinit/groups/g/seek")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"offset\":0}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.errorCode").value("SEEK_REJECTED"))
                .andExpect(jsonPath("$.message").value(
                        org.hamcrest.Matchers.containsString("backwards")));
    }
}
