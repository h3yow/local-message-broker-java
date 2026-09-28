package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.config.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.exception.ErrorCode;
import com.github.h3yow.localmessagebrokerjava.model.AckResult;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetterEntry;
import com.github.h3yow.localmessagebrokerjava.model.DeliveryView;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import com.github.h3yow.localmessagebrokerjava.service.InMemoryMessageBroker;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Broker 核心语义：组内独占、组间独立、至少一次、乱序确认、重试到死信、背压、去重、重初始化、seek 拒绝。
 */
class InMemoryMessageBrokerTest {

    private InMemoryMessageBroker broker(int capacity, int maxAttempts, long backoffMs,
                                         long dedupWindowMs) {
        BrokerProperties props = new BrokerProperties();
        props.setTopicCapacity(capacity);
        props.setMaxDeliveryAttempts(maxAttempts);
        props.setRetryBackoffMillis(backoffMs);
        props.setDedupWindowMillis(dedupWindowMs);
        return new InMemoryMessageBroker(props);
    }

    private InMemoryMessageBroker defaultBroker() {
        return broker(1000, 3, 0L, 60_000L);
    }

    private void seed(InMemoryMessageBroker b, String topic, String group, int messages) {
        b.createTopic(topic, 0);
        b.subscribe(topic, group);
        for (int i = 0; i < messages; i++) {
            assertEquals(i, b.publish(topic, "m" + i, null).offset());
        }
    }

    // ---- 基础投递 ----

    @Test
    void deliversInOffsetOrderAndAckAdvancesPosition() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 3);

        DeliveryView d0 = b.pull("t", "g");
        assertEquals(0L, d0.offset());
        assertEquals(1, d0.attempt());
        DeliveryView d1 = b.pull("t", "g");
        assertEquals(1L, d1.offset());

        // 乱序确认：先确认 1，位点只能停在 0，不能越过未确认的 0
        AckResult ack1 = b.acknowledge("t", "g", 1L);
        assertFalse(ack1.duplicateAck());
        assertEquals(0L, ack1.committedPosition());

        // 确认 0 后，位点连贯推进到 2
        AckResult ack0 = b.acknowledge("t", "g", 0L);
        assertEquals(2L, ack0.committedPosition());
        assertEquals(2L, b.status("t", "g").committedPosition());
    }

    @Test
    void duplicateAckIsIdempotentAndNeverMovesPositionBackwards() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 2);
        b.pull("t", "g");
        b.pull("t", "g");
        b.acknowledge("t", "g", 0L);
        b.acknowledge("t", "g", 1L);
        assertEquals(2L, b.status("t", "g").committedPosition());

        AckResult dup = b.acknowledge("t", "g", 0L);
        assertTrue(dup.duplicateAck());
        assertEquals(2L, dup.committedPosition());
        // 再重复确认仍然幂等
        assertEquals(2L, b.acknowledge("t", "g", 0L).committedPosition());
    }

    @Test
    void differentGroupsEachConsumeFullCopy() {
        InMemoryMessageBroker b = defaultBroker();
        b.createTopic("t", 0);
        b.subscribe("t", "g1");
        b.subscribe("t", "g2");
        b.publish("t", "m0", null);
        b.publish("t", "m1", null);

        DeliveryView a = b.pull("t", "g1");
        DeliveryView c = b.pull("t", "g2");
        assertEquals(0L, a.offset());
        assertEquals(0L, c.offset());
        b.acknowledge("t", "g1", 0L);
        // g1 的确认不影响 g2：g2 仍可继续消费到 offset 1
        assertEquals(0L, b.status("t", "g2").committedPosition());
        assertEquals(1L, b.pull("t", "g2").offset());
    }

    @Test
    void inFlightMessageIsNotGivenToAnotherPullInSameGroup() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 2);
        DeliveryView d = b.pull("t", "g");
        assertNotNull(d);
        assertEquals(0L, d.offset());
        // 未确认的 0 不会重复给出，下一次拉取只能拿到 1
        DeliveryView next = b.pull("t", "g");
        assertNotNull(next);
        assertEquals(1L, next.offset());
        // 两条都在途，没有别的消息时返回 null
        assertNull(b.pull("t", "g"));
        b.acknowledge("t", "g", 0L);
        b.acknowledge("t", "g", 1L);
        assertNull(b.pull("t", "g"));
    }

    // ---- 重试与死信 ----

    @Test
    void nackRetriesUpToLimitThenDeadLettersWithReason() {
        InMemoryMessageBroker b = broker(100, 3, 0L, 60_000L);
        seed(b, "t", "g", 1);

        DeliveryView first = b.pull("t", "g");
        assertEquals(1, first.attempt());
        b.negativeAcknowledge("t", "g", 0L, "boom-1");

        DeliveryView second = b.pull("t", "g");
        assertEquals(0L, second.offset());
        assertEquals(2, second.attempt());
        b.negativeAcknowledge("t", "g", 0L, "boom-2");

        DeliveryView third = b.pull("t", "g");
        assertEquals(3, third.attempt());
        b.negativeAcknowledge("t", "g", 0L, "boom-3");

        // 达到投递上限后进入死信，位点推进，不再投递
        GroupStatus status = b.status("t", "g");
        assertEquals(1L, status.committedPosition());
        assertEquals(1, status.deadLetterCount());
        assertNull(b.pull("t", "g"));

        List<DeadLetterEntry> dlq = b.deadLetters("t", "g");
        assertEquals(1, dlq.size());
        assertEquals(0L, dlq.get(0).offset());
        assertEquals(3, dlq.get(0).attempts());
        assertEquals("boom-3", dlq.get(0).lastReason());

        // 对已死信的消息再确认必须被拒绝
        BrokerException ex = assertThrows(BrokerException.class,
                () -> b.acknowledge("t", "g", 0L));
        assertEquals(ErrorCode.MESSAGE_DEAD_LETTERED, ex.getErrorCode());
    }

    @Test
    void retryRespectsBackoffWindow() throws InterruptedException {
        InMemoryMessageBroker b = broker(100, 3, 80L, 60_000L);
        seed(b, "t", "g", 1);
        b.pull("t", "g");
        b.negativeAcknowledge("t", "g", 0L, "transient");
        assertNull(b.pull("t", "g"), "must not redeliver within backoff window");
        Thread.sleep(120L);
        DeliveryView retry = b.pull("t", "g");
        assertNotNull(retry);
        assertEquals(2, retry.attempt());
    }

    // ---- 背压、去重、非法请求 ----

    @Test
    void publishFailsWithDistinguishableReasons() {
        InMemoryMessageBroker b = broker(2, 3, 0L, 60_000L);
        b.createTopic("cap", 2);
        b.publish("cap", "a", null);
        b.publish("cap", "b", null);

        BrokerException full = assertThrows(BrokerException.class,
                () -> b.publish("cap", "c", null));
        assertEquals(ErrorCode.QUEUE_FULL, full.getErrorCode());

        BrokerException noTopic = assertThrows(BrokerException.class,
                () -> b.publish("missing", "x", null));
        assertEquals(ErrorCode.TOPIC_NOT_FOUND, noTopic.getErrorCode());

        BrokerException invalid = assertThrows(BrokerException.class,
                () -> b.publish("cap", "", null));
        assertEquals(ErrorCode.INVALID_MESSAGE, invalid.getErrorCode());
    }

    @Test
    void dedupKeySuppressesDuplicatesWithinWindow() {
        InMemoryMessageBroker b = defaultBroker();
        b.createTopic("t", 100);
        PublishResult first = b.publish("t", "hello", "key-1");
        assertFalse(first.duplicate());
        assertEquals(0L, first.offset());

        PublishResult dup = b.publish("t", "hello-again", "key-1");
        assertTrue(dup.duplicate());
        assertEquals(0L, dup.offset());

        // 不带去重键的消息不受影响
        assertFalse(b.publish("t", "other", null).duplicate());
    }

    @Test
    void duplicateSubscriptionRejected() {
        InMemoryMessageBroker b = defaultBroker();
        b.createTopic("t", 10);
        b.subscribe("t", "g");
        BrokerException ex = assertThrows(BrokerException.class, () -> b.subscribe("t", "g"));
        assertEquals(ErrorCode.GROUP_ALREADY_SUBSCRIBED, ex.getErrorCode());
    }

    // ---- 重初始化与 seek ----

    @Test
    void reinitializeRedeliversUnacknowledgedMessagesWithoutLoss() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 3);
        b.pull("t", "g"); // 0 在途
        b.pull("t", "g"); // 1 在途
        b.pull("t", "g"); // 2 在途

        int returned = b.reinitialize("t", "g");
        assertEquals(3, returned);
        assertEquals(0L, b.status("t", "g").committedPosition(),
                "committed position must not jump forward on reinit");

        // 全部三条必须重新投递，且重新计数 attempt
        for (long expected = 0; expected < 3; expected++) {
            DeliveryView d = b.pull("t", "g");
            assertNotNull(d, "unacked message must be redelivered after reinit");
            assertEquals(expected, d.offset());
            assertEquals(1, d.attempt(), "attempt counter resets on reinitialize");
        }
    }

    @Test
    void seekBackwardsIsRejectedWithExplicitReason() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 3);
        b.pull("t", "g");
        b.acknowledge("t", "g", 0L);
        assertEquals(1L, b.status("t", "g").committedPosition());

        BrokerException ex = assertThrows(BrokerException.class,
                () -> b.seek("t", "g", 0L));
        assertEquals(ErrorCode.SEEK_REJECTED, ex.getErrorCode());
        assertTrue(ex.getMessage().contains("backwards"));
        // 位置保持不变
        assertEquals(1L, b.status("t", "g").committedPosition());
    }

    @Test
    void seekForwardIsRejectedWhileUnacknowledgedWouldBeSkipped() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 3);
        b.pull("t", "g"); // offset 0 在途
        BrokerException ex = assertThrows(BrokerException.class,
                () -> b.seek("t", "g", 2L));
        assertEquals(ErrorCode.SEEK_REJECTED, ex.getErrorCode());
        assertEquals(0L, b.status("t", "g").committedPosition());
    }

    @Test
    void seekForwardSucceedsWhenNothingInFlight() {
        InMemoryMessageBroker b = defaultBroker();
        seed(b, "t", "g", 3);
        // 没有任何在途消息时允许向前定位到已存在的 offset
        assertEquals(2L, b.seek("t", "g", 2L));
        assertEquals(2L, b.status("t", "g").committedPosition());
        // 位点定位后只能消费到 2，更早的消息被显式跳过
        assertEquals(2L, b.pull("t", "g").offset());
        b.acknowledge("t", "g", 2L);
        assertNull(b.pull("t", "g"));
    }

    @Test
    void pullBeforeSubscribeFailsClearly() {
        InMemoryMessageBroker b = defaultBroker();
        b.createTopic("t", 10);
        BrokerException ex = assertThrows(BrokerException.class,
                () -> b.pull("t", "ghost"));
        assertEquals(ErrorCode.GROUP_NOT_FOUND, ex.getErrorCode());
    }
}
