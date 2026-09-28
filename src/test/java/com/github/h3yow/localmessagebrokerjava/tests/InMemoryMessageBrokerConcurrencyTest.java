package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.config.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.exception.ErrorCode;
import com.github.h3yow.localmessagebrokerjava.model.DeliveryView;
import com.github.h3yow.localmessagebrokerjava.service.InMemoryMessageBroker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发不变量：
 * 1) 同一消费组里，每条消息同一时刻最多在一个消费者手中（同组并发拉取不会重复分发），
 *    且最终每条消息到达终态、位点精确推进；
 * 2) 并发发布的 offset 连续不重不漏，超出容量的发布全部得到可区分的 QUEUE_FULL；
 * 3) 乱序/重复并发确认下，已提交位置只前进不回退。
 */
class InMemoryMessageBrokerConcurrencyTest {

    private InMemoryMessageBroker broker(int capacity, int maxAttempts, long backoffMs) {
        BrokerProperties props = new BrokerProperties();
        props.setTopicCapacity(capacity);
        props.setMaxDeliveryAttempts(maxAttempts);
        props.setRetryBackoffMillis(backoffMs);
        props.setDedupWindowMillis(60_000L);
        return new InMemoryMessageBroker(props);
    }

    @Test
    void concurrentConsumersInSameGroupEachMessageHandledExactlyOnce() throws Exception {
        InMemoryMessageBroker b = broker(10_000, 3, 0L);
        b.createTopic("t", 0);
        b.subscribe("t", "g");
        int total = 2000;
        for (int i = 0; i < total; i++) {
            b.publish("t", "m" + i, null);
        }

        int consumers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        // 每个 offset 首次投递记录的 deliveryId；若同一 offset 被同时投递给两个消费者，
        // 两个 deliveryId 会在 ack 竞争中暴露出来（失败者收到 INVALID_DELIVERY）。
        Set<Long> processedOffsets = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger lostAcks = new AtomicInteger();

        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (true) {
                        DeliveryView d = b.pull("t", "g");
                        if (d == null) {
                            if (processed.get() >= total && b.status("t", "g").inFlightCount() == 0) {
                                return;
                            }
                            Thread.sleep(1);
                            continue;
                        }
                        try {
                            var ackResult = b.acknowledge("t", "g", d.offset());
                            if (!ackResult.duplicateAck()) {
                                processedOffsets.add(d.offset());
                                processed.incrementAndGet();
                            }
                        } catch (BrokerException ex) {
                            // 只有一种可能：这条在途记录已不属于本次投递（重复分发），
                            // 在正确实现里不应发生。
                            if (ex.getErrorCode() == ErrorCode.INVALID_DELIVERY
                                    || ex.getErrorCode() == ErrorCode.MESSAGE_DEAD_LETTERED) {
                                lostAcks.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "consumers finish in time");

        assertEquals(0, lostAcks.get(), "no delivery is ever handed to two consumers");
        assertEquals(total, processed.get());
        assertEquals(total, processedOffsets.size(), "every offset handled once");
        assertEquals(total, b.status("t", "g").committedPosition());
        assertEquals(0, b.status("t", "g").inFlightCount());
    }

    @Test
    void mixedSuccessAndFailureRetriesEventuallyAcknowledgeAll() throws Exception {
        InMemoryMessageBroker b = broker(10_000, 5, 0L);
        b.createTopic("t", 0);
        b.subscribe("t", "g");
        int total = 1000;
        for (int i = 0; i < total; i++) {
            b.publish("t", "m" + i, null);
        }

        int consumers = 6;
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> completedOffsets = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicLong deliveries = new AtomicLong();
        AtomicInteger emptyPulls = new AtomicInteger();

        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (completedOffsets.size() < total
                            || b.status("t", "g").inFlightCount() > 0) {
                        DeliveryView d = b.pull("t", "g");
                        if (d == null) {
                            if (completedOffsets.size() >= total
                                    && b.status("t", "g").inFlightCount() == 0) {
                                return;
                            }
                            if (emptyPulls.incrementAndGet() > 200_000) {
                                return;
                            }
                            Thread.onSpinWait();
                            continue;
                        }
                        deliveries.incrementAndGet();
                        // 20% 的消息在前两次投递失败后重试，最终成功
                        if (d.offset() % 5 == 0 && d.attempt() <= 2) {
                            b.negativeAcknowledge("t", "g", d.offset(), "synthetic-failure");
                        } else {
                            completedOffsets.add(d.offset());
                            b.acknowledge("t", "g", d.offset());
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(total, completedOffsets.size(), "all messages eventually acknowledged");
        assertEquals(total, b.status("t", "g").committedPosition());
        assertEquals(0, b.status("t", "g").inFlightCount());
        assertEquals(0, b.status("t", "g").deadLetterCount(),
                "retried messages succeed before attempt limit");
        assertTrue(deliveries.get() > total, "retries cause extra delivery attempts");
    }

    @Test
    void concurrentPublishingProducesContiguousOffsetsOrExplicitQueueFull() throws Exception {
        int capacity = 500;
        InMemoryMessageBroker b = broker(capacity, 3, 0L);
        b.createTopic("t", capacity);

        int publishers = 16;
        int perPublisher = 100;
        ExecutorService pool = Executors.newFixedThreadPool(publishers);
        CountDownLatch start = new CountDownLatch(1);
        Set<Long> acceptedOffsets = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicInteger rejectedFull = new AtomicInteger();

        for (int p = 0; p < publishers; p++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perPublisher; i++) {
                        try {
                            long offset = b.publish("t", "payload", null).offset();
                            acceptedOffsets.add(offset);
                        } catch (BrokerException ex) {
                            if (ex.getErrorCode() == ErrorCode.QUEUE_FULL) {
                                rejectedFull.incrementAndGet();
                            } else {
                                throw ex;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(capacity, acceptedOffsets.size(), "exactly capacity messages accepted");
        assertEquals((long) publishers * perPublisher - capacity,
                rejectedFull.get(), "every other publish explicitly rejected as QUEUE_FULL");
        assertEquals(new HashSet<>(range(0, capacity)), new HashSet<>(acceptedOffsets),
                "accepted offsets are contiguous with no gap or duplicate");
    }

    @Test
    void concurrentDuplicateAndOutOfOrderAcksKeepPositionMonotonic() throws Exception {
        InMemoryMessageBroker b = broker(10_000, 3, 0L);
        b.createTopic("t", 0);
        b.subscribe("t", "g");
        int total = 500;
        List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            offsets.add(b.publish("t", "m" + i, null).offset());
        }
        for (int i = 0; i < total; i++) {
            assertEquals(i, b.pull("t", "g").offset());
        }

        int ackers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(ackers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong maxObservedPosition = new AtomicLong(0);
        AtomicInteger duplicateAcks = new AtomicInteger();

        for (int a = 0; a < ackers; a++) {
            pool.submit(() -> {
                try {
                    start.await();
                    // 每个线程都尝试确认所有 offset，制造大量重复/乱序确认
                    for (long offset : offsets) {
                        var result = b.acknowledge("t", "g", offset);
                        maxObservedPosition.updateAndGet(prev -> Math.max(prev,
                                result.committedPosition()));
                        if (result.duplicateAck()) {
                            duplicateAcks.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        // 最终位点恰好推进到 total；重复确认幂等，没有造成回退、重复生效或丢消息
        assertEquals(total, b.status("t", "g").committedPosition());
        assertEquals(total, maxObservedPosition.get(), "position never overshoots final value");
        assertTrue(duplicateAcks.get() > 0,
                "redundant acks are accepted idempotently rather than applied twice");
    }

    private static List<Long> range(int from, int until) {
        List<Long> list = new ArrayList<>();
        for (long i = from; i < until; i++) {
            list.add(i);
        }
        return list;
    }
}
