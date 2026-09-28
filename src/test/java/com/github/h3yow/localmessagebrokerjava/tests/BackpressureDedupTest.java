package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.broker.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.broker.InMemoryMessageBroker;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 发布端背压与去重键去重。
 */
class BackpressureDedupTest {

    private InMemoryMessageBroker broker;

    @BeforeEach
    void setUp() {
        BrokerProperties props = new BrokerProperties();
        props.setTopicCapacity(5);
        props.setMaxPayloadBytes(10);
        broker = new InMemoryMessageBroker(props);
        broker.createTopic("t");
    }

    @Test
    @DisplayName("拒绝原因可区分：主题不存在 / 消息非法 / 队列瞬时写满")
    void publishRejectionReasonsAreDistinguishable() {
        // 1) 主题不存在
        BrokerException noTopic = assertThrows(BrokerException.class,
                () -> broker.publish("missing", "x", null));
        assertEquals(BrokerErrorCode.TOPIC_NOT_FOUND, noTopic.getCode());

        // 2) 消息非法：空内容
        BrokerException empty = assertThrows(BrokerException.class,
                () -> broker.publish("t", "", null));
        assertEquals(BrokerErrorCode.INVALID_MESSAGE, empty.getCode());
        BrokerException nullPayload = assertThrows(BrokerException.class,
                () -> broker.publish("t", null, null));
        assertEquals(BrokerErrorCode.INVALID_MESSAGE, nullPayload.getCode());

        // 3) 消息非法：超长
        BrokerException tooLarge = assertThrows(BrokerException.class,
                () -> broker.publish("t", "01234567890", null)); // 11 字节 > 10
        assertEquals(BrokerErrorCode.INVALID_MESSAGE, tooLarge.getCode());

        // 4) 队列瞬时写满（没有消费组时不会回收，5 条即满）
        for (int i = 0; i < 5; i++) {
            broker.publish("t", "m" + i, null);
        }
        BrokerException full = assertThrows(BrokerException.class,
                () -> broker.publish("t", "overflow", null));
        assertEquals(BrokerErrorCode.CAPACITY_FULL, full.getCode());
        assertTrue(full.getMessage().contains("瞬时写满"), "失败原因需要可区分: " + full.getMessage());
    }

    @Test
    @DisplayName("队列写满是瞬时背压：消费并确认释放容量后可继续发布")
    void capacityReleasedAfterAckAllowsNewPublishes() {
        broker.createGroup("t", "g");
        for (int i = 0; i < 5; i++) {
            broker.publish("t", "m" + i, null);
        }
        assertEquals(BrokerErrorCode.CAPACITY_FULL,
                assertThrows(BrokerException.class, () -> broker.publish("t", "x", null)).getCode());

        // 消费确认 3 条 -> 唯一消费组越过 3，前缀被回收，腾出 3 个位置
        List<Delivery> consumed = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            consumed.add(broker.pull("t", "g", "c").orElseThrow());
        }
        for (Delivery d : consumed) {
            broker.ack("t", "g", d.message().offset(), d.leaseId());
        }
        for (int i = 0; i < 3; i++) {
            PublishResult r = broker.publish("t", "n" + i, null);
            assertFalse(r.deduped());
        }
        // 仍然有容量上限：再发又会满
        int acceptedAfter = 0;
        try {
            for (int i = 0; i < 100; i++) {
                broker.publish("t", "y" + i, null);
                acceptedAfter++;
            }
        } catch (BrokerException ex) {
            assertEquals(BrokerErrorCode.CAPACITY_FULL, ex.getCode());
        }
        assertEquals(0, acceptedAfter, "offset 3/4 仍在途未确认，不能回收；发布 5/6/7 后已满 5 条，下一次发布立即被拒");
    }

    @Test
    @DisplayName("去重窗口内相同去重键只投递一次，返回相同偏移量")
    void sameDedupKeyWithinWindowFolded() {
        PublishResult r1 = broker.publish("t", "first", "key-1");
        PublishResult r2 = broker.publish("t", "dup", "key-1");
        PublishResult r3 = broker.publish("t", "third", "key-1");
        assertEquals(r1.offset(), r2.offset());
        assertEquals(r1.offset(), r3.offset());
        assertTrue(r2.deduped() && r3.deduped());
        assertFalse(r1.deduped());

        // 组里只能消费到一条物理消息
        broker.createGroup("t", "g");
        Delivery only = broker.pull("t", "g", "c").orElseThrow();
        assertEquals("first", only.message().payload());
        assertEquals(r1.offset(), only.message().offset());
        broker.ack("t", "g", only.message().offset(), only.leaseId());
        assertTrue(broker.pull("t", "g", "c").isEmpty(), "重复发布不能重复投递");

        // 不同去重键互不影响
        PublishResult other = broker.publish("t", "second-key", "key-2");
        assertEquals(r1.offset() + 1, other.offset());
    }

    @Test
    @DisplayName("去重窗口过期后相同去重键视为新消息")
    void dedupKeyAfterWindowExpires() throws InterruptedException {
        BrokerProperties props = new BrokerProperties();
        props.setTopicCapacity(5);
        props.setDedupWindowMillis(30);
        InMemoryMessageBroker shortWindowBroker = new InMemoryMessageBroker(props);
        shortWindowBroker.createTopic("sw");
        PublishResult first = shortWindowBroker.publish("sw", "a", "k");
        Thread.sleep(45);
        // 过期惰性清理发生在“带相同去重键的下一次发布”时
        PublishResult second = shortWindowBroker.publish("sw", "b", "k");
        assertFalse(second.deduped());
        assertEquals(first.offset() + 1, second.offset());
    }
}
