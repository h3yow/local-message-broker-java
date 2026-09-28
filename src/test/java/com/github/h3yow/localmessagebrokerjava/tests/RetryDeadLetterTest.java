package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.broker.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.broker.InMemoryMessageBroker;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetter;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 至少一次投递：失败重试，达到投递上限后进入死信；失败原因可区分、可查询。
 */
class RetryDeadLetterTest {

    private InMemoryMessageBroker broker;
    private BrokerProperties props;

    @BeforeEach
    void setUp() {
        props = new BrokerProperties();
        props.setMaxDeliveryAttempts(3);
        broker = new InMemoryMessageBroker(props);
        broker.createTopic("t");
        broker.createGroup("t", "g");
    }

    @Test
    @DisplayName("连续失败达到最大投递次数后进入死信，位点继续向前，消息不再被投递")
    void retryUntilLimitThenDeadLetter() {
        broker.publish("t", "poison", null);
        broker.publish("t", "ok", null);

        // 对 offset 0 重复 投递->nack 三次
        String[] reasons = {"HTTP_TIMEOUT", "DESERIALIZE_ERROR", "BUSINESS_EXCEPTION"};
        for (int attempt = 1; attempt <= props.getMaxDeliveryAttempts(); attempt++) {
            Delivery d = broker.pull("t", "g", "worker").orElseThrow();
            assertEquals(0, d.message().offset());
            assertEquals(attempt, d.deliveryCount(), "第 " + attempt + " 次投递次数应递增");
            broker.nack("t", "g", 0, d.leaseId(), reasons[attempt - 1]);
        }

        List<DeadLetter> dead = broker.deadLetters("t", "g");
        assertEquals(1, dead.size());
        DeadLetter dl = dead.get(0);
        assertEquals(0, dl.offset());
        assertEquals("poison", dl.payload());
        assertEquals(3, dl.attempts(), "死信应记录累计投递次数");
        assertEquals("BUSINESS_EXCEPTION", dl.lastReason(), "死信应保留最后一次可区分的失败原因");

        // 进入死信即终态：位点越过它，后续消息 offset 1 正常可消费
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
        assertTrue(broker.groupStatus("t", "g").deadLetterOffsets().contains(0L));

        Delivery next = broker.pull("t", "g", "worker").orElseThrow();
        assertEquals(1, next.message().offset());
        assertEquals(1, next.deliveryCount(), "新消息投递次数应从 1 开始");
        broker.ack("t", "g", 1, next.leaseId());
        assertEquals(2, broker.groupStatus("t", "g").committedOffset());
        assertTrue(broker.pull("t", "g", "worker").isEmpty(), "死信消息不得再次投递");
    }

    @Test
    @DisplayName("失败两次后成功：不进死信，最终位点正确")
    void recoversBeforeLimit() {
        broker.publish("t", "flaky", null);
        Delivery d1 = broker.pull("t", "g", "w").orElseThrow();
        broker.nack("t", "g", 0, d1.leaseId(), "TRANSIENT");
        Delivery d2 = broker.pull("t", "g", "w").orElseThrow();
        broker.nack("t", "g", 0, d2.leaseId(), "TRANSIENT");
        assertEquals(2, d2.deliveryCount());

        Delivery d3 = broker.pull("t", "g", "w").orElseThrow();
        assertEquals(3, d3.deliveryCount());
        broker.ack("t", "g", 0, d3.leaseId());

        assertTrue(broker.deadLetters("t", "g").isEmpty());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
    }
}
