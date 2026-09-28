package com.github.h3yow.localmessagebrokerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 端到端：HTTP 发布/订阅/拉取/确认/重试死信/位点保护/重新初始化/去重，
 * 并断言对外暴露的状态码与可区分错误码。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "broker.topic-capacity=3",
        "broker.max-delivery-attempts=3"
})
class BrokerHttpIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate rest;

    private final ObjectMapper json = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    private ResponseEntity<String> post(String path, Object body) {
        return rest.exchange(url(path), HttpMethod.POST,
                new HttpEntity<>(body == null ? null : json.valueToTree(body), jsonHeaders()), String.class);
    }

    private JsonNode tree(ResponseEntity<String> resp) throws Exception {
        return json.readTree(resp.getBody());
    }

    @Test
    @DisplayName("完整链路：创建->发布->拉取->nack重试->死信；非法/背压/回退错误码正确")
    void endToEndLifecycle() throws Exception {
        // 主题与消费组
        assertEquals(HttpStatus.CREATED, post("/api/topics/http-t", Map.of()).getStatusCode());
        assertEquals(HttpStatus.CREATED, post("/api/topics/http-t/groups", Map.of("group", "g")).getStatusCode());

        // 正常发布
        ResponseEntity<String> pub = post("/api/topics/http-t/messages", Map.of("payload", "hello"));
        assertEquals(HttpStatus.OK, pub.getStatusCode());
        assertEquals(0L, tree(pub).get("offset").asLong());

        // 主题不存在 -> 404
        ResponseEntity<String> noTopic = post("/api/topics/nope/messages", Map.of("payload", "x"));
        assertEquals(HttpStatus.NOT_FOUND, noTopic.getStatusCode());
        assertEquals("TOPIC_NOT_FOUND", tree(noTopic).get("code").asText());

        // 空消息 -> 400 INVALID_MESSAGE
        ResponseEntity<String> bad = post("/api/topics/http-t/messages", Map.of("payload", ""));
        assertEquals(HttpStatus.BAD_REQUEST, bad.getStatusCode());
        assertEquals("INVALID_MESSAGE", tree(bad).get("code").asText());

        // 容量=3，再发 2 条填满
        assertEquals(HttpStatus.OK, post("/api/topics/http-t/messages", Map.of("payload", "b")).getStatusCode());
        assertEquals(HttpStatus.OK, post("/api/topics/http-t/messages", Map.of("payload", "c")).getStatusCode());
        ResponseEntity<String> full = post("/api/topics/http-t/messages", Map.of("payload", "d"));
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, full.getStatusCode(), "写满应为 503（瞬时可重试）");
        assertEquals("CAPACITY_FULL", tree(full).get("code").asText());

        // 拉取第一条
        ResponseEntity<String> pullResp = post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w"));
        assertEquals(HttpStatus.OK, pullResp.getStatusCode());
        JsonNode delivery = tree(pullResp);
        assertEquals(0L, delivery.get("offset").asLong());
        assertEquals("hello", delivery.get("payload").asText());
        assertEquals(1, delivery.get("deliveryCount").asInt());
        long leaseId = delivery.get("leaseId").asLong();

        // offset 0 在途时，其他消费者可以继续拿到下一条 offset 1（同组排他是针对同一条消息）
        JsonNode other = tree(post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w2")));
        assertEquals(1L, other.get("offset").asLong());
        // offset 2 也还没被处理，继续被 w3 拿到
        JsonNode third = tree(post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w3")));
        assertEquals(2L, third.get("offset").asLong());
        // 0/1/2 全部在途后，再拉取 -> 204
        assertEquals(HttpStatus.NO_CONTENT,
                post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w4")).getStatusCode());
        // 把 offset 1、2 确认掉（offset 0 保持在途以便演示重试/死信）
        post("/api/topics/http-t/messages/1/ack",
                Map.of("group", "g", "leaseId", other.get("leaseId").asLong()));
        post("/api/topics/http-t/messages/2/ack",
                Map.of("group", "g", "leaseId", third.get("leaseId").asLong()));

        // 错误租约 ack -> 409
        ResponseEntity<String> staleAck = post("/api/topics/http-t/messages/0/ack",
                Map.of("group", "g", "leaseId", leaseId + 1));
        assertEquals(HttpStatus.CONFLICT, staleAck.getStatusCode());
        assertEquals("LEASE_NOT_FOUND", tree(staleAck).get("code").asText());

        // 连续 3 次投递失败 -> 第三次 nack 后进死信
        post("/api/topics/http-t/messages/0/nack",
                Map.of("group", "g", "leaseId", leaseId, "reason", "TIMEOUT"));
        JsonNode d2 = tree(post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w")));
        assertEquals(2, d2.get("deliveryCount").asInt());
        post("/api/topics/http-t/messages/0/nack",
                Map.of("group", "g", "leaseId", d2.get("leaseId").asLong(), "reason", "BIZ_ERROR"));
        JsonNode d3 = tree(post("/api/topics/http-t/pull", Map.of("group", "g", "consumer", "w")));
        assertEquals(3, d3.get("deliveryCount").asInt());
        ResponseEntity<String> lastNack = post("/api/topics/http-t/messages/0/nack",
                Map.of("group", "g", "leaseId", d3.get("leaseId").asLong(), "reason", "FATAL"));
        assertEquals(HttpStatus.OK, lastNack.getStatusCode());

        // 死信可查询，原因可区分
        JsonNode dead = tree(rest.getForEntity(url("/api/topics/http-t/groups/g/dead-letters"), String.class));
        assertTrue(dead.isArray());
        assertEquals(1, dead.size());
        assertEquals(0L, dead.get(0).get("offset").asLong());
        assertEquals(3, dead.get(0).get("attempts").asInt());
        assertEquals("FATAL", dead.get(0).get("lastReason").asText());

        // 位点已越过死信（1、2 此前已确认，0 死信后连续折叠到 3）
        JsonNode status = tree(rest.getForEntity(url("/api/topics/http-t/groups/g/status"), String.class));
        assertEquals(3, status.get("committedOffset").asInt());

        // 回退位点 -> 409 且原因明确
        ResponseEntity<String> seekBack = post("/api/topics/http-t/seek", Map.of("group", "g", "offset", 0));
        assertEquals(HttpStatus.CONFLICT, seekBack.getStatusCode());
        assertEquals("SEEK_BACKWARD_REJECTED", tree(seekBack).get("code").asText());
        assertTrue(tree(seekBack).get("reason").asText().contains("拒绝回退"));
    }

    @Test
    @DisplayName("重新初始化后未确认消息通过 HTTP 可被重新拉取，且去重键只产生一条消息")
    void reinitializeAndDedupOverHttp() throws Exception {
        post("/api/topics/re", Map.of());
        post("/api/topics/re/groups", Map.of("group", "g"));

        // 去重：同窗口同键只生效一次
        ResponseEntity<String> p1 = post("/api/topics/re/messages",
                Map.of("payload", "x", "dedupKey", "dk-1"));
        ResponseEntity<String> p2 = post("/api/topics/re/messages",
                Map.of("payload", "x-dup", "dedupKey", "dk-1"));
        assertEquals(tree(p1).get("offset").asLong(), tree(p2).get("offset").asLong());
        assertTrue(tree(p2).get("deduped").asBoolean());
        assertFalse(tree(p1).get("deduped").asBoolean());

        // 拉取后不确认，直接重新初始化
        JsonNode d1 = tree(post("/api/topics/re/pull", Map.of("group", "g", "consumer", "w")));
        assertEquals(1, d1.get("deliveryCount").asInt());
        ResponseEntity<String> rein = post("/api/admin/reinitialize", Map.of());
        assertEquals(HttpStatus.OK, rein.getStatusCode());
        assertEquals(1, tree(rein).get("returnedInflight").asInt());

        // 可以重新拉到，投递次数变 2，已确认位点仍为 0
        JsonNode d2 = tree(post("/api/topics/re/pull", Map.of("group", "g", "consumer", "w2")));
        assertEquals(d1.get("offset").asLong(), d2.get("offset").asLong());
        assertEquals(2, d2.get("deliveryCount").asInt());
        JsonNode status = tree(rest.getForEntity(url("/api/topics/re/groups/g/status"), String.class));
        assertEquals(0, status.get("committedOffset").asInt());

        // 请求体缺失 -> 400
        ResponseEntity<String> noBody = post("/api/topics/re/messages", null);
        assertEquals(HttpStatus.BAD_REQUEST, noBody.getStatusCode());
    }
}
