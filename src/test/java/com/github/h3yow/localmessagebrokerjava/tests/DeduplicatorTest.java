package com.github.h3yow.localmessagebrokerjava.tests;

import com.github.h3yow.localmessagebrokerjava.service.Deduplicator;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 去重窗口边界与并发不变量：窗口内同键只有一次生效，窗口外可重新登记。
 */
class DeduplicatorTest {

    @Test
    void firstSeenReturnsMinusOneAndDuplicatesReturnFirstOffset() {
        Deduplicator dedup = new Deduplicator(1000L);

        assertEquals(-1L, dedup.registerOrLookup("k", 10L, 1000L));
        // 窗口内重复：返回首次偏移量，不产生新投递
        assertEquals(10L, dedup.registerOrLookup("k", 11L, 1500L));
        assertEquals(10L, dedup.registerOrLookup("k", 12L, 1999L));

        // 不同键互不影响
        assertEquals(-1L, dedup.registerOrLookup("other", 13L, 1500L));
    }

    @Test
    void entryExpiresOutsideWindowAndCanBeRegisteredAgain() {
        Deduplicator dedup = new Deduplicator(1000L);

        assertEquals(-1L, dedup.registerOrLookup("k", 10L, 0L));
        // 到达/超过窗口边界后视为过期，重新生效
        assertEquals(-1L, dedup.registerOrLookup("k", 20L, 1000L));
        assertEquals(20L, dedup.registerOrLookup("k", 21L, 1500L));
    }

    @Test
    void concurrentPublishesWithSameKeyOnlyOneWins() throws Exception {
        Deduplicator dedup = new Deduplicator(60_000L);
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ConcurrentMap<Long, AtomicInteger> results = new ConcurrentHashMap<>();

        for (int i = 0; i < threads; i++) {
            long claimedOffset = 100L + i;
            pool.submit(() -> {
                try {
                    start.await();
                    long firstOffset = dedup.registerOrLookup("hot-key", claimedOffset,
                            System.currentTimeMillis());
                    results.computeIfAbsent(firstOffset, k -> new AtomicInteger()).incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        // 不变量：恰有一个线程登记成功（-1），其余线程全部看到同一个首次偏移量
        assertEquals(1, results.get(-1L).get(), "exactly one first registration");
        long distinctDuplicateTargets = results.keySet().stream()
                .filter(k -> k >= 0)
                .distinct()
                .count();
        assertEquals(1, distinctDuplicateTargets, "all duplicates resolve to same first offset");
        long duplicateTarget = results.keySet().stream()
                .filter(k -> k >= 0)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected duplicate lookups"));
        assertEquals(threads - 1, results.get(duplicateTarget).get(),
                "all other threads observe the winning offset");
        assertTrue(duplicateTarget >= 100L && duplicateTarget < 100L + threads,
                "winning offset belongs to one of the publishers");
    }
}
