package com.github.h3yow.localmessagebrokerjava.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于本地内存的去重键判定：在有效窗口内相同去重键只允许生效一次。
 * <p>
 * 语义：
 * <ul>
 *     <li>窗口内首次见到某个键：登记 (offset, 时间)，返回 -1，调用方应正常投递。</li>
 *     <li>窗口内再次见到：返回首次登记的偏移量，调用方不得重复投递。</li>
 *     <li>已过窗口的旧记录视为不存在，本次发布会重新登记，原消息是否还在队列不影响判定。</li>
 * </ul>
 * 过期条目采用惰性清理（登记新键时顺带清理一小批），避免引入后台线程。
 */
public class Deduplicator {

    private static final Logger log = LoggerFactory.getLogger(Deduplicator.class);

    private record Entry(long firstOffset, long registeredAtMillis) {
    }

    private final long windowMillis;
    private final ConcurrentHashMap<String, Entry> keys = new ConcurrentHashMap<>();
    private final AtomicBoolean cleaning = new AtomicBoolean(false);

    public Deduplicator(long windowMillis) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("dedup window millis must be positive");
        }
        this.windowMillis = windowMillis;
    }

    /**
     * 在窗口内首次见到该键时登记并返回 -1；窗口内重复见到时返回首次登记的偏移量。
     *
     * @param dedupKey  去重键（调用方保证非空）
     * @param offset    本次发布的偏移量
     * @param nowMillis 当前时间（便于测试注入）
     * @return -1 表示首次（允许投递）；否则返回首次发布的偏移量
     */
    public long registerOrLookup(String dedupKey, long offset, long nowMillis) {
        long[] answer = new long[1];
        keys.compute(dedupKey, (key, existing) -> {
            if (existing != null && nowMillis - existing.registeredAtMillis < windowMillis) {
                answer[0] = existing.firstOffset;
                log.debug("dedup hit key={} firstOffset={} ageMs={}",
                        key, existing.firstOffset, nowMillis - existing.registeredAtMillis);
                return existing;
            }
            answer[0] = -1L;
            return new Entry(offset, nowMillis);
        });
        if (keys.size() > 64) {
            evictExpired(nowMillis);
        }
        return answer[0];
    }

    private void evictExpired(long nowMillis) {
        if (!cleaning.compareAndSet(false, true)) {
            return;
        }
        try {
            int removed = 0;
            for (Map.Entry<String, Entry> e : keys.entrySet()) {
                if (nowMillis - e.getValue().registeredAtMillis >= windowMillis) {
                    if (keys.remove(e.getKey(), e.getValue())) {
                        removed++;
                    }
                }
            }
            if (removed > 0) {
                log.debug("dedup evicted expired keys count={}", removed);
            }
        } finally {
            cleaning.set(false);
        }
    }
}
