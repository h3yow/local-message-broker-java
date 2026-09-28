package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.broker.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.broker.InMemoryMessageBroker;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 主题/消费组基本语义与确认位点不变量。
 */
class GroupConsumptionSemanticsTest {

    private InMemoryMessageBroker broker;

    @BeforeEach
    void setUp() {
        BrokerProperties props = new BrokerProperties();
        broker = new InMemoryMessageBroker(props);
        broker.createTopic("t");
    }

    private long publish(String payload) {
        return broker.publish("t", payload, null).offset();
    }

    @Test
    @DisplayName("不同消费组各自独立消费完整消息")
    void groupsConsumeIndependently() {
        for (String group : List.of("g1", "g2", "g3")) {
            broker.createGroup("t", group);
        }
        for (int i = 0; i < 5; i++) {
            publish("m" + i);
        }
        for (String group : List.of("g1", "g2", "g3")) {
            List<Long> offsets = new ArrayList<>();
            Optional<Delivery> d;
            while ((d = broker.pull("t", group, "c")).isPresent()) {
                offsets.add(d.get().message().offset());
                broker.ack("t", group, d.get().message().offset(), d.get().leaseId());
            }
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L), offsets, "组 " + group + " 应独立消费全部消息");
        }
    }

    @Test
    @DisplayName("同组一条消息在途期间只能被一个消费者拿到，ack 后位点推进")
    void sameGroupExclusiveInflight() {
        publish("a");
        Delivery d1 = broker.pull("t", "g", "consumer-A").orElseThrow();
        assertEquals(0, d1.message().offset());
        assertEquals(1, d1.deliveryCount(), "首次投递次数应为 1");

        assertTrue(broker.pull("t", "g", "consumer-B").isEmpty(),
                "同组其他消费者不能拿到在途消息");
        assertTrue(broker.pull("t", "g", "consumer-A").isEmpty(),
                "同一消费者也不能重复拿到同一条在途消息");

        broker.ack("t", "g", 0, d1.leaseId());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
        assertTrue(broker.pull("t", "g", "consumer-B").isEmpty(), "全部确认完后应无消息可拉");
    }

    @Test
    @DisplayName("乱序确认：位点只能随连续前缀推进，空洞补齐后一次性越过")
    void outOfOrderAckAdvancesCommittedOnlyOverContiguousPrefix() {
        for (int i = 0; i < 3; i++) {
            publish("m" + i);
        }
        Delivery d0 = broker.pull("t", "g", "c").orElseThrow();
        Delivery d1 = broker.pull("t", "g", "c").orElseThrow();
        Delivery d2 = broker.pull("t", "g", "c").orElseThrow();

        broker.ack("t", "g", 1, d1.leaseId());
        assertEquals(0, broker.groupStatus("t", "g").committedOffset(), "offset 0 仍是空洞，位点不得前进");
        broker.ack("t", "g", 2, d2.leaseId());
        assertEquals(0, broker.groupStatus("t", "g").committedOffset(), "offset 0 未确认，位点仍不得前进");
        broker.ack("t", "g", 0, d0.leaseId());
        assertEquals(3, broker.groupStatus("t", "g").committedOffset(), "空洞补齐后应连续推进到 3");
        assertTrue(broker.groupStatus("t", "g").inflightOffsets().isEmpty());
    }

    @Test
    @DisplayName("重复确认幂等：不报错、不重复生效、位点不回退")
    void duplicateAckIsIdempotent() {
        publish("a");
        publish("b");
        Delivery d0 = broker.pull("t", "g", "c").orElseThrow();
        Delivery d1 = broker.pull("t", "g", "c").orElseThrow();
        broker.ack("t", "g", 0, d0.leaseId());
        // 同一租约再确认两次
        broker.ack("t", "g", 0, d0.leaseId());
        broker.ack("t", "g", 0, d0.leaseId());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
        // 乱序确认 1 后，再拿旧请求确认 0 也不能产生副作用
        broker.ack("t", "g", 1, d1.leaseId());
        broker.ack("t", "g", 0, d0.leaseId());
        assertEquals(2, broker.groupStatus("t", "g").committedOffset());
    }

    @Test
    @DisplayName("旧/错误租约确认必须被拒绝，避免并发确认重复生效")
    void staleLeaseAckRejected() {
        publish("a");
        Delivery d1 = broker.pull("t", "g", "c").orElseThrow();
        long wrongLease = d1.leaseId() + 999;
        BrokerException ex = assertThrows(BrokerException.class,
                () -> broker.ack("t", "g", 0, wrongLease));
        assertEquals(BrokerErrorCode.LEASE_NOT_FOUND, ex.getCode());
        // 消息仍在途：位点没动，且消息仍不可被重新拉取
        assertEquals(0, broker.groupStatus("t", "g").committedOffset());
        assertEquals(List.of(0L), broker.groupStatus("t", "g").inflightOffsets());
        // 用正确租约确认仍然成功
        broker.ack("t", "g", 0, d1.leaseId());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
    }

    @Test
    @DisplayName("nack 后消息重新可投递，投递次数单调递增")
    void nackMakesMessageRedeliverableWithGrowingAttempts() {
        publish("a");
        Delivery d1 = broker.pull("t", "g", "c").orElseThrow();
        assertEquals(1, d1.deliveryCount());
        broker.nack("t", "g", 0, d1.leaseId(), "PROCESSING_ERROR");

        Delivery d2 = broker.pull("t", "g", "c2").orElseThrow();
        assertEquals(0, d2.message().offset());
        assertEquals(2, d2.deliveryCount(), "重试投递次数应为 2");
        assertNotEquals(d1.leaseId(), d2.leaseId(), "重新投递应分配新租约");
        // nack 期间位点不得前进
        assertEquals(0, broker.groupStatus("t", "g").committedOffset());
        broker.ack("t", "g", 0, d2.leaseId());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
    }
}
