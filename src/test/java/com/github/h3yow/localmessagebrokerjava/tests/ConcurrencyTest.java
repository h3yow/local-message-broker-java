package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.broker.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.broker.InMemoryMessageBroker;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发场景下的核心不变量：
 * 1. 并发发布：成功写入的偏移量连续且唯一，背压拒绝被明确统计；
 * 2. 并发消费：同组内每条消息恰好被一个消费者成功处理一次（在途排他 + 至少一次）；
 * 3. 并发/乱序确认：committed 只进不退，最终等于消息总数。
 */
class ConcurrencyTest {

    @Test
    @DisplayName("并发发布：偏移量唯一连续，写满拒绝次数精确")
    void concurrentPublishing() throws Exception {
        BrokerProperties props = new BrokerProperties();
        props.setTopicCapacity(100);
        InMemoryMessageBroker broker = new InMemoryMessageBroker(props);
        broker.createTopic("t");
        // 没有消费组 -> 不会回收，最多成功 100 条
        int threads = 8;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Long, Boolean> offsets = new ConcurrentHashMap<>();
        AtomicInteger rejectedFull = new AtomicInteger();
        AtomicInteger accepted = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    try {
                        long off = broker.publish("t", "p", null).offset();
                        if (offsets.put(off, Boolean.TRUE) != null) {
                            throw new AssertionError("偏移量重复: " + off);
                        }
                        accepted.incrementAndGet();
                    } catch (BrokerException ex) {
                        if (ex.getCode() == BrokerErrorCode.CAPACITY_FULL) {
                            rejectedFull.incrementAndGet();
                        } else {
                            throw ex;
                        }
                    }
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(100, accepted.get(), "容量内的消息全部成功");
        assertEquals(threads * perThread - 100, rejectedFull.get(), "其余发布必须全部以 CAPACITY_FULL 被拒");
        assertEquals(100, offsets.size());
        for (long off = 0; off < 100; off++) {
            assertTrue(offsets.containsKey(off), "成功偏移量必须连续 0..99，缺失: " + off);
        }
    }

    @Test
    @DisplayName("并发拉取消费 + 随机乱序确认：同组每条消息恰好被一个消费者处理，最终位点等于总数")
    void concurrentConsumersExclusiveAndNoLoss() throws Exception {
        BrokerProperties props = new BrokerProperties();
        InMemoryMessageBroker broker = new InMemoryMessageBroker(props);
        broker.createTopic("t");
        broker.createGroup("t", "g");
        int total = 500;
        for (int i = 0; i < total; i++) {
            broker.publish("t", "m" + i, null);
        }

        int consumers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Long, String> handledBy = new ConcurrentHashMap<>();
        AtomicInteger processed = new AtomicInteger();

        for (int c = 0; c < consumers; c++) {
            String consumer = "consumer-" + c;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (true) {
                    // pull/ack 自身线程安全：同组在途排他保证一条消息只会被一个消费者拿到
                    Delivery d = broker.pull("t", "g", consumer).orElse(null);
                    if (d == null) {
                        if (processed.get() >= total) {
                            return;
                        }
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        continue;
                    }
                    long off = d.message().offset();
                    String prev = handledBy.putIfAbsent(off, consumer);
                    if (prev != null) {
                        throw new AssertionError("同组消息被两个消费者拿到: offset=" + off
                                + ", " + prev + " 与 " + consumer);
                    }
                    processed.incrementAndGet();
                    broker.ack("t", "g", off, d.leaseId());
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "消费者线程应在超时前结束");

        assertEquals(total, processed.get());
        assertEquals(total, handledBy.size(), "每条消息恰好被处理一次");
        assertEquals(total, broker.groupStatus("t", "g").committedOffset(),
                "并发乱序确认后，消费位点必须推进到末尾且没有回退/空洞");
        assertTrue(broker.groupStatus("t", "g").inflightOffsets().isEmpty());
    }

    @Test
    @DisplayName("并发重复确认：在途时错误租约被拒；终态后重复确认幂等；位点只进不退")
    void concurrentDuplicateAcks() throws Exception {
        InMemoryMessageBroker broker = new InMemoryMessageBroker(new BrokerProperties());
        broker.createTopic("t");
        broker.createGroup("t", "g");
        int total = 100;
        for (int i = 0; i < total; i++) {
            broker.publish("t", "m", null);
        }
        // 单消费者先把全部消息拉成在途
        Delivery[] deliveries = new Delivery[total];
        for (int i = 0; i < total; i++) {
            deliveries[i] = broker.pull("t", "g", "c").orElseThrow();
        }

        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch invalidPhaseDone = new CountDownLatch(2);
        AtomicInteger leaseRejected = new AtomicInteger();

        // 阶段一：2 个线程用错误租约猛打——此时消息确定仍在途，全部必须被拒绝
        for (int w = 0; w < 2; w++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (Delivery d : deliveries) {
                    try {
                        broker.ack("t", "g", d.message().offset(), d.leaseId() + 4242);
                        throw new AssertionError("在途消息用错误租约确认不应成功");
                    } catch (BrokerException ex) {
                        assertEquals(BrokerErrorCode.LEASE_NOT_FOUND, ex.getCode());
                        leaseRejected.incrementAndGet();
                    }
                }
                invalidPhaseDone.countDown();
            });
        }
        start.countDown();
        assertTrue(invalidPhaseDone.await(30, TimeUnit.SECONDS));
        assertEquals(total * 2L, leaseRejected.get(), "两批错误租约、每批 100 次，在途期间都必须被拒绝");

        // 阶段二：2 个线程用正确租约并发乱序重复确认（每条会被确认两遍，第二遍幂等）
        CountDownLatch ackDone = new CountDownLatch(2);
        for (int w = 0; w < 2; w++) {
            pool.submit(() -> {
                try {
                    for (Delivery d : deliveries) {
                        broker.ack("t", "g", d.message().offset(), d.leaseId());
                    }
                } finally {
                    ackDone.countDown();
                }
            });
        }
        assertTrue(ackDone.await(30, TimeUnit.SECONDS));
        pool.shutdown();

        assertEquals(total, broker.groupStatus("t", "g").committedOffset(), "位点必须恰好推进到 total");

        // 全部终态后，再用任意旧/错租约重复确认都必须幂等成功，位点保持 total 不回退
        for (Delivery d : deliveries) {
            broker.ack("t", "g", d.message().offset(), d.leaseId());
            broker.ack("t", "g", d.message().offset(), d.leaseId() + 9999);
        }
        assertEquals(total, broker.groupStatus("t", "g").committedOffset());
    }
}
