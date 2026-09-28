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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 位点保护与组件重新初始化语义。
 */
class SeekAndReinitializeTest {

    private InMemoryMessageBroker broker;

    @BeforeEach
    void setUp() {
        broker = new InMemoryMessageBroker(new BrokerProperties());
        broker.createTopic("t");
        broker.createGroup("t", "g");
    }

    @Test
    @DisplayName("回退消费位置必须被明确拒绝并说明原因，位点保持不变")
    void seekBackwardRejected() {
        for (int i = 0; i < 4; i++) {
            broker.publish("t", "m" + i, null);
        }
        Delivery d0 = broker.pull("t", "g", "c").orElseThrow();
        Delivery d1 = broker.pull("t", "g", "c").orElseThrow();
        broker.ack("t", "g", 0, d0.leaseId());
        broker.ack("t", "g", 1, d1.leaseId());
        assertEquals(2, broker.groupStatus("t", "g").committedOffset());

        BrokerException ex = assertThrows(BrokerException.class, () -> broker.seek("t", "g", 1));
        assertEquals(BrokerErrorCode.SEEK_BACKWARD_REJECTED, ex.getCode());
        assertTrue(ex.getMessage().contains("拒绝回退"), "拒绝原因要明确: " + ex.getMessage());
        assertEquals(2, broker.groupStatus("t", "g").committedOffset(), "位点不得被回退");

        // 回退到 0 同样拒绝
        assertThrows(BrokerException.class, () -> broker.seek("t", "g", 0));
        assertEquals(2, broker.groupStatus("t", "g").committedOffset());
    }

    @Test
    @DisplayName("seek 只能向前：越过未处理消息后这些消息不再投递，位点推进")
    void seekForwardSkipsMessages() {
        for (int i = 0; i < 3; i++) {
            broker.publish("t", "m" + i, null);
        }
        Delivery inflight = broker.pull("t", "g", "c").orElseThrow();
        // 向前跳到 3：在途的旧租约应被作废
        broker.seek("t", "g", 3);
        assertEquals(3, broker.groupStatus("t", "g").committedOffset());
        assertTrue(broker.groupStatus("t", "g").inflightOffsets().isEmpty());
        // 旧租约再确认：消息已随 seek 进入终态，幂等成功，但位点不会回退也不会复活消息
        broker.ack("t", "g", inflight.message().offset(), inflight.leaseId());
        assertEquals(3, broker.groupStatus("t", "g").committedOffset());
        // 旧租约 nack 同样不能让已跳过的消息复活
        broker.nack("t", "g", inflight.message().offset(), inflight.leaseId(), "LATE_NACK");
        assertEquals(3, broker.groupStatus("t", "g").committedOffset());
        assertTrue(broker.pull("t", "g", "c").isEmpty());

        // 越界 offset 也要拒绝
        assertThrows(BrokerException.class, () -> broker.seek("t", "g", 100));
    }

    @Test
    @DisplayName("组件重新初始化后，所有未确认消息必须能重新投递，已确认位置不回退")
    void reinitializeReturnsInflightForRedelivery() {
        for (int i = 0; i < 5; i++) {
            broker.publish("t", "m" + i, null);
        }
        List<Delivery> held = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            held.add(broker.pull("t", "g", "worker-1").orElseThrow());
        }
        // 确认 offset 0，offset 1、2 仍在途（模拟消费者处理中组件被重建）
        broker.ack("t", "g", 0, held.get(0).leaseId());
        assertEquals(1, broker.groupStatus("t", "g").committedOffset());
        assertEquals(List.of(1L, 2L), broker.groupStatus("t", "g").inflightOffsets());

        int returned = broker.reinitializeInflight();
        assertEquals(2, returned, "应退回两条未确认消息");
        assertEquals(1, broker.groupStatus("t", "g").committedOffset(), "已确认位置不得回退");
        assertTrue(broker.groupStatus("t", "g").inflightOffsets().isEmpty(), "旧租约全部解除");

        // 重新投递：offset 1、2 能再次被（任意）消费者拿到，且投递次数递增
        Delivery again1 = broker.pull("t", "g", "worker-2").orElseThrow();
        Delivery again2 = broker.pull("t", "g", "worker-2").orElseThrow();
        assertEquals(1, again1.message().offset());
        assertEquals(2, again2.message().offset());
        assertEquals(2, again1.deliveryCount(), "退回重投应记录为第 2 次投递");
        assertEquals(2, again2.deliveryCount());
        assertNotEquals(held.get(1).leaseId(), again1.leaseId(), "应分配新租约");

        broker.ack("t", "g", 1, again1.leaseId());
        broker.ack("t", "g", 2, again2.leaseId());
        assertEquals(3, broker.groupStatus("t", "g").committedOffset());

        // offset 3、4 从未投递过，仍正常可拉
        Delivery d3 = broker.pull("t", "g", "worker-2").orElseThrow();
        assertEquals(3, d3.message().offset());
        assertEquals(1, d3.deliveryCount());
    }

    @Test
    @DisplayName("多次重新初始化幂等：没有在途消息时返回 0，不丢消息")
    void reinitializeTwiceIsSafe() {
        broker.publish("t", "m", null);
        assertEquals(0, broker.reinitializeInflight(), "没有在途消息时应返回 0");
        assertEquals(0, broker.reinitializeInflight(), "重复调用仍然为 0");
        Delivery d = broker.pull("t", "g", "c").orElseThrow();
        assertEquals(0, d.message().offset(), "未确认消息仍在，没有被静默丢掉");
        broker.ack("t", "g", 0, d.leaseId());
        assertEquals(0, broker.reinitializeInflight(), "全部确认后再重新初始化仍为 0");
    }
}
