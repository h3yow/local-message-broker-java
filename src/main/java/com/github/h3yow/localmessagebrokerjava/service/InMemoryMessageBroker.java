package com.github.h3yow.localmessagebrokerjava.service;

import com.github.h3yow.localmessagebrokerjava.config.BrokerProperties;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.exception.ErrorCode;
import com.github.h3yow.localmessagebrokerjava.model.AckResult;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetterEntry;
import com.github.h3yow.localmessagebrokerjava.model.DeliveryView;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import com.github.h3yow.localmessagebrokerjava.model.StoredMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 纯内存实现的本地进程内消息队列。
 * <p>
 * 每个主题是一条 append-only 日志（带容量上限）；每个消费组在组锁内维护：
 * <ul>
 *     <li>{@code committedPosition}：已连续处理完成的位置（下一个待提交偏移量），只增不减。</li>
 *     <li>{@code inFlight}：已投递未确认的 offset 到投递状态。组锁保证同一 offset
 *     不会同时投递给组内两个消费者。</li>
 *     <li>{@code attempts}：每个 offset 的已投递次数；{@code retryReadyAt}：重试退避到期时间。</li>
 *     <li>{@code deadOffsets}：已进死信的 offset，永不再投递。</li>
 * </ul>
 * 加锁约定：组操作只持有组锁，读取日志时短暂获取主题 appendLock 做快照，
 * 不存在 appendLock 内嵌等待组锁的路径，因此不会死锁。
 */
@Service
public class InMemoryMessageBroker implements MessageBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMessageBroker.class);

    /** 一次在途投递的状态。 */
    private static final class InFlight {
        final int attempt;
        final long deliveryId;

        InFlight(int attempt, long deliveryId) {
            this.attempt = attempt;
            this.deliveryId = deliveryId;
        }
    }

    private static final class TopicState {
        final String name;
        final int capacity;
        /** append-only 日志；offset 从 0 开始连续且不截断，故 list.get(offset) 即对应消息。 */
        final List<StoredMessage> log = new ArrayList<>();
        final Object appendLock = new Object();
        final Deduplicator deduplicator;
        final Map<String, GroupState> groups = new ConcurrentHashMap<>();
        long nextOffset = 0;

        TopicState(String name, int capacity, long dedupWindowMillis) {
            this.name = name;
            this.capacity = capacity;
            this.deduplicator = new Deduplicator(dedupWindowMillis);
        }
    }

    private static final class GroupState {
        final String name;
        /** 组级锁：pull/ack/nack/seek/reinit 全部串行，保证位点与在途集合一致。 */
        final Object lock = new Object();
        long committedPosition;
        /** 已投递给消费者、尚未确认/否定确认的消息。 */
        final TreeMap<Long, InFlight> inFlight = new TreeMap<>();
        /** nack 后等待重试（退避到期后可重新投递）的消息。位点不得越过这些 offset。 */
        final Set<Long> pendingRedelivery = new HashSet<>();
        /** 已被消费者确认过的 offset，终态。 */
        final Set<Long> ackedOffsets = new HashSet<>();
        /** 每个 offset 的累计投递次数（在途、重试中均保留；确认或死信后清理）。 */
        final Map<Long, Integer> attempts = new HashMap<>();
        /** 重试退避到期时间戳，只对 pendingRedelivery 中的 offset 有意义。 */
        final Map<Long, Long> retryReadyAt = new HashMap<>();
        final Set<Long> deadOffsets = new HashSet<>();
        final List<DeadLetterEntry> deadLetters = new ArrayList<>();

        GroupState(String name, long startPosition) {
            this.name = name;
            this.committedPosition = startPosition;
        }
    }

    private final BrokerProperties properties;
    private final Map<String, TopicState> topics = new ConcurrentHashMap<>();
    private final AtomicLong deliveryIdSeq = new AtomicLong(1);

    public InMemoryMessageBroker(BrokerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void createTopic(String topic, int requestedCapacity) {
        String name = requireNonBlank(topic, "topic");
        int capacity = requestedCapacity > 0 ? requestedCapacity : properties.getTopicCapacity();
        if (topics.putIfAbsent(name, new TopicState(name, capacity,
                properties.getDedupWindowMillis())) != null) {
            throw new BrokerException(ErrorCode.TOPIC_ALREADY_EXISTS,
                    "topic already exists: " + name);
        }
        log.info("topic created name={} capacity={}", name, capacity);
    }

    @Override
    public PublishResult publish(String topic, String payload, String dedupKey) {
        TopicState ts = requireTopic(topic);
        if (payload == null || payload.isEmpty()) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE,
                    "payload must not be null or empty, topic=" + topic);
        }
        long now = System.currentTimeMillis();
        synchronized (ts.appendLock) {
            if (dedupKey != null && !dedupKey.isBlank()) {
                long firstOffset = ts.deduplicator.registerOrLookup(dedupKey, ts.nextOffset, now);
                if (firstOffset >= 0) {
                    log.info("publish rejected by dedup topic={} dedupKey={} firstOffset={}",
                            topic, dedupKey, firstOffset);
                    return new PublishResult(firstOffset, true);
                }
            }
            if (ts.log.size() >= ts.capacity) {
                log.warn("publish rejected queue full topic={} size={} capacity={}",
                        topic, ts.log.size(), ts.capacity);
                throw new BrokerException(ErrorCode.QUEUE_FULL,
                        "topic queue is full, topic=" + topic + ", capacity=" + ts.capacity
                                + " (transient: retry after consumers advance)");
            }
            long offset = ts.nextOffset++;
            ts.log.add(new StoredMessage(offset, payload, dedupKey, now));
            log.info("message published topic={} offset={} dedupKey={}", topic, offset, dedupKey);
            return new PublishResult(offset, false);
        }
    }

    @Override
    public void subscribe(String topic, String group) {
        TopicState ts = requireTopic(topic);
        String groupName = requireNonBlank(group, "group");
        synchronized (ts) {
            if (ts.groups.containsKey(groupName)) {
                throw new BrokerException(ErrorCode.GROUP_ALREADY_SUBSCRIBED,
                        "group already subscribed: topic=" + topic + ", group=" + groupName);
            }
            long startPosition;
            synchronized (ts.appendLock) {
                startPosition = ts.nextOffset;
            }
            ts.groups.put(groupName, new GroupState(groupName, startPosition));
            log.info("group subscribed topic={} group={} startPosition={}",
                    topic, groupName, startPosition);
        }
    }

    @Override
    public DeliveryView pull(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        List<StoredMessage> snapshot;
        synchronized (ts.appendLock) {
            snapshot = new ArrayList<>(ts.log);
        }
        synchronized (gs.lock) {
            long now = System.currentTimeMillis();
            for (StoredMessage msg : snapshot) {
                long offset = msg.offset();
                if (offset < gs.committedPosition) {
                    continue;
                }
                if (gs.deadOffsets.contains(offset) || gs.ackedOffsets.contains(offset)
                        || gs.inFlight.containsKey(offset)) {
                    continue;
                }
                if (gs.pendingRedelivery.contains(offset)) {
                    long readyAt = gs.retryReadyAt.getOrDefault(offset, 0L);
                    if (now < readyAt) {
                        log.debug("pull skipped offset={} in retry backoff readyAt={}",
                                offset, readyAt);
                        continue;
                    }
                    gs.pendingRedelivery.remove(offset);
                    gs.retryReadyAt.remove(offset);
                }
                int attempt = gs.attempts.getOrDefault(offset, 0) + 1;
                gs.attempts.put(offset, attempt);
                long deliveryId = deliveryIdSeq.incrementAndGet();
                gs.inFlight.put(offset, new InFlight(attempt, deliveryId));
                log.info("delivered topic={} group={} offset={} attempt={} deliveryId={} committed={}",
                        ts.name, gs.name, offset, attempt, deliveryId, gs.committedPosition);
                return new DeliveryView(ts.name, gs.name, offset, msg.payload(), attempt, deliveryId);
            }
            log.debug("pull returned nothing topic={} group={} committed={}",
                    ts.name, gs.name, gs.committedPosition);
            return null;
        }
    }

    @Override
    public AckResult acknowledge(String topic, String group, long offset) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        synchronized (gs.lock) {
            if (gs.deadOffsets.contains(offset)) {
                log.warn("ack rejected offset={} already dead-lettered topic={} group={}",
                        offset, topic, group);
                throw new BrokerException(ErrorCode.MESSAGE_DEAD_LETTERED,
                        "offset " + offset + " is in dead letter queue and cannot be acknowledged");
            }
            InFlight inflight = gs.inFlight.remove(offset);
            if (inflight == null) {
                if (gs.ackedOffsets.contains(offset)) {
                    // 已确认过（可能因为前面的消息还没确认，位点尚未越过它）：幂等返回。
                    log.info("duplicate/out-of-order ack ignored topic={} group={} offset={} committed={}",
                            topic, group, offset, gs.committedPosition);
                    return new AckResult(offset, true, gs.committedPosition);
                }
                if (offset < gs.committedPosition) {
                    log.info("duplicate/out-of-order ack ignored topic={} group={} offset={} committed={}",
                            topic, group, offset, gs.committedPosition);
                    return new AckResult(offset, true, gs.committedPosition);
                }
                throw new BrokerException(ErrorCode.INVALID_DELIVERY,
                        "offset " + offset + " is not currently in-flight for group " + group
                                + " (never delivered or awaiting retry backoff); committed="
                                + gs.committedPosition);
            }
            gs.attempts.remove(offset);
            gs.pendingRedelivery.remove(offset);
            gs.retryReadyAt.remove(offset);
            gs.ackedOffsets.add(offset);
            advanceCommitted(ts, gs);
            log.info("acked topic={} group={} offset={} attempt={} committed={}",
                    topic, group, offset, inflight.attempt, gs.committedPosition);
            return new AckResult(offset, false, gs.committedPosition);
        }
    }

    @Override
    public void negativeAcknowledge(String topic, String group, long offset, String reason) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        String failReason = reason == null || reason.isBlank() ? "unspecified" : reason;
        synchronized (gs.lock) {
            InFlight inflight = gs.inFlight.remove(offset);
            if (inflight == null) {
                if (gs.deadOffsets.contains(offset)) {
                    throw new BrokerException(ErrorCode.MESSAGE_DEAD_LETTERED,
                            "offset " + offset + " is already dead-lettered");
                }
                throw new BrokerException(ErrorCode.INVALID_DELIVERY,
                        "nack for offset " + offset + " that is not in-flight; committed="
                                + gs.committedPosition);
            }
            int attempt = inflight.attempt;
            if (attempt >= properties.getMaxDeliveryAttempts()) {
                moveToDeadLetter(ts, gs, offset, attempt, failReason);
                return;
            }
            gs.pendingRedelivery.add(offset);
            gs.retryReadyAt.put(offset,
                    System.currentTimeMillis() + properties.getRetryBackoffMillis());
            log.warn("message scheduled for retry topic={} group={} offset={} attempt={} reason='{}' maxAttempts={} committed={}",
                    topic, group, offset, attempt, failReason,
                    properties.getMaxDeliveryAttempts(), gs.committedPosition);
        }
    }

    private void moveToDeadLetter(TopicState ts, GroupState gs, long offset, int attempt,
                                  String reason) {
        StoredMessage msg = findMessage(ts, offset);
        DeadLetterEntry entry = new DeadLetterEntry(ts.name, gs.name, offset,
                msg == null ? null : msg.payload(), attempt, reason, System.currentTimeMillis());
        gs.deadOffsets.add(offset);
        gs.deadLetters.add(entry);
        gs.attempts.remove(offset);
        gs.pendingRedelivery.remove(offset);
        gs.retryReadyAt.remove(offset);
        advanceCommitted(ts, gs);
        log.error("message moved to dead letter topic={} group={} offset={} attempts={} reason='{}' committed={}",
                ts.name, gs.name, offset, attempt, reason, gs.committedPosition);
    }

    /**
     * 在组锁内推进已提交位置：从当前位置开始，只要该 offset 已处于终态就连贯向前推进。
     * 终态 = 已确认 或 已进死信（二者均是显式集合），且不能越过日志末端。位置只增不减。
     * 从未投递的消息既不在在途也不在任何终态集合，因此绝不可能被误跳。
     */
    private void advanceCommitted(TopicState ts, GroupState gs) {
        long logEnd = logEnd(ts);
        long cursor = gs.committedPosition;
        while (isResolved(gs, cursor, logEnd)) {
            cursor++;
        }
        if (cursor < gs.committedPosition) {
            throw new IllegalStateException("committed position attempted to move backwards");
        }
        gs.committedPosition = cursor;
    }

    /** 该 offset 是否已有终态：已确认或已死信，且该消息确实已发布。 */
    private boolean isResolved(GroupState gs, long offset, long logEnd) {
        if (offset >= logEnd) {
            return false;
        }
        return gs.ackedOffsets.contains(offset) || gs.deadOffsets.contains(offset);
    }

    private boolean hasUnresolvedBefore(GroupState gs, long targetOffset) {
        if (!gs.inFlight.isEmpty() && gs.inFlight.firstKey() < targetOffset) {
            return true;
        }
        for (long pending : gs.pendingRedelivery) {
            if (pending < targetOffset) {
                return true;
            }
        }
        return false;
    }

    private long logEnd(TopicState ts) {
        synchronized (ts.appendLock) {
            return ts.nextOffset;
        }
    }

    @Override
    public int reinitialize(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        synchronized (gs.lock) {
            int returned = gs.inFlight.size() + gs.pendingRedelivery.size();
            gs.inFlight.clear();
            gs.pendingRedelivery.clear();
            gs.attempts.clear();
            gs.retryReadyAt.clear();
            log.warn("group reinitialized topic={} group={} unackedReturned={} committed={} "
                            + "(all unacknowledged/retry-pending messages will be redelivered, attempt counters reset)",
                    topic, group, returned, gs.committedPosition);
            return returned;
        }
    }

    @Override
    public long seek(String topic, String group, long targetOffset) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        synchronized (gs.lock) {
            if (targetOffset < gs.committedPosition) {
                log.warn("seek rejected: would move position backwards topic={} group={} target={} committed={}",
                        topic, group, targetOffset, gs.committedPosition);
                throw new BrokerException(ErrorCode.SEEK_REJECTED,
                        "seek to " + targetOffset + " rejected: committed position "
                                + gs.committedPosition
                                + " can never move backwards (at-least-once guarantee)");
            }
            if (targetOffset > ts.nextOffset) {
                throw new BrokerException(ErrorCode.SEEK_REJECTED,
                        "seek target offset " + targetOffset + " does not exist yet; logEnd="
                                + ts.nextOffset);
            }
            if (hasUnresolvedBefore(gs, targetOffset)) {
                log.warn("seek rejected: unacknowledged messages would be skipped "
                                + "topic={} group={} target={} inFlight={} pending={}",
                        topic, group, targetOffset, gs.inFlight.size(),
                        gs.pendingRedelivery.size());
                throw new BrokerException(ErrorCode.SEEK_REJECTED,
                        "seek rejected: there are in-flight or retry-pending message(s) before "
                                + "target; acknowledge, nack or reinitialize the group first");
            }
            gs.committedPosition = targetOffset;
            log.info("seek applied topic={} group={} committed={}", topic, group, targetOffset);
            return targetOffset;
        }
    }

    @Override
    public GroupStatus status(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        synchronized (gs.lock) {
            return new GroupStatus(gs.committedPosition, gs.inFlight.size(), gs.deadLetters.size());
        }
    }

    @Override
    public List<DeadLetterEntry> deadLetters(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        synchronized (gs.lock) {
            return List.copyOf(gs.deadLetters);
        }
    }

    // ---- helpers ----

    private StoredMessage findMessage(TopicState ts, long offset) {
        synchronized (ts.appendLock) {
            if (offset < 0 || offset >= ts.log.size()) {
                return null;
            }
            StoredMessage msg = ts.log.get((int) offset);
            return msg.offset() == offset ? msg : null;
        }
    }

    private TopicState requireTopic(String topic) {
        TopicState ts = topics.get(requireNonBlank(topic, "topic"));
        if (ts == null) {
            throw new BrokerException(ErrorCode.TOPIC_NOT_FOUND, "topic not found: " + topic);
        }
        return ts;
    }

    private GroupState requireGroup(TopicState ts, String group) {
        GroupState gs = ts.groups.get(requireNonBlank(group, "group"));
        if (gs == null) {
            throw new BrokerException(ErrorCode.GROUP_NOT_FOUND,
                    "group not found: " + group + ", topic=" + ts.name + " (subscribe first)");
        }
        return gs;
    }

    private String requireNonBlank(String value, String what) {
        if (value == null || value.isBlank()) {
            throw new BrokerException(ErrorCode.INVALID_MESSAGE,
                    what + " must not be blank");
        }
        return value;
    }
}
