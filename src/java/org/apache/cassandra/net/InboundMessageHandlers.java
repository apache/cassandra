/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net;

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

import io.netty.channel.Channel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeInboundMetrics;
import org.apache.cassandra.net.Message.Header;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

/**
 * An aggregation of {@link InboundMessageHandler}s for all connections from a peer.
 *
 * Manages metrics and shared resource limits. Can have multiple connections of a single
 * type open simultaneousely (legacy in particular).
 */
public final class InboundMessageHandlers
{
    private final InetAddressAndPort self;
    private final InetAddressAndPort peer;

    private final int queueCapacity;
    private final ResourceLimits.Limit endpointReserveCapacity;
    private final ResourceLimits.Limit globalReserveCapacity;

    private final InboundMessageHandler.WaitQueue endpointWaitQueue;
    private final InboundMessageHandler.WaitQueue globalWaitQueue;

    private final InboundCounters urgentCounters = new InboundCounters();
    private final InboundCounters smallCounters  = new InboundCounters();
    private final InboundCounters largeCounters  = new InboundCounters();
    private final InboundCounters legacyCounters = new InboundCounters();

    private final InboundMessageCallbacks urgentCallbacks;
    private final InboundMessageCallbacks smallCallbacks;
    private final InboundMessageCallbacks largeCallbacks;
    private final InboundMessageCallbacks legacyCallbacks;

    private final InternodeInboundMetrics metrics;
    private final MessageConsumer messageConsumer;

    private final HandlerProvider handlerProvider;
    private final Collection<InboundMessageHandler> handlers = new CopyOnWriteArrayList<>();

    static class GlobalResourceLimits
    {
        final ResourceLimits.Limit reserveCapacity;
        final InboundMessageHandler.WaitQueue waitQueue;

        GlobalResourceLimits(ResourceLimits.Limit reserveCapacity)
        {
            this.reserveCapacity = reserveCapacity;
            this.waitQueue = InboundMessageHandler.WaitQueue.global(reserveCapacity);
        }
    }

    public interface MessageConsumer extends Consumer<Message<?>>
    {
        void fail(Message.Header header, Throwable failure);
    }

    public interface GlobalMetricCallbacks
    {
        LatencyConsumer internodeLatencyRecorder(InetAddressAndPort to);
        void recordInternalLatency(Verb verb, long timeElapsed, TimeUnit timeUnit);
        void recordInternodeDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit);
    }

    public InboundMessageHandlers(InetAddressAndPort self,
                                  InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  GlobalResourceLimits globalResourceLimits,
                                  GlobalMetricCallbacks globalMetricCallbacks,
                                  MessageConsumer messageConsumer)
    {
        this(self, peer, queueCapacity, endpointReserveCapacity, globalResourceLimits, globalMetricCallbacks, messageConsumer, InboundMessageHandler::new);
    }

    public InboundMessageHandlers(InetAddressAndPort self,
                                  InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  GlobalResourceLimits globalResourceLimits,
                                  GlobalMetricCallbacks globalMetricCallbacks,
                                  MessageConsumer messageConsumer,
                                  HandlerProvider handlerProvider)
    {
        this.self = self;
        this.peer = peer;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = new ResourceLimits.Concurrent(endpointReserveCapacity);
        this.globalReserveCapacity = globalResourceLimits.reserveCapacity;
        this.endpointWaitQueue = InboundMessageHandler.WaitQueue.endpoint(this.endpointReserveCapacity);
        this.globalWaitQueue = globalResourceLimits.waitQueue;
        this.messageConsumer = messageConsumer;

        this.handlerProvider = handlerProvider;

        urgentCallbacks = makeMessageCallbacks(peer, urgentCounters, globalMetricCallbacks, messageConsumer);
        smallCallbacks  = makeMessageCallbacks(peer, smallCounters,  globalMetricCallbacks, messageConsumer);
        largeCallbacks  = makeMessageCallbacks(peer, largeCounters,  globalMetricCallbacks, messageConsumer);
        legacyCallbacks = makeMessageCallbacks(peer, legacyCounters, globalMetricCallbacks, messageConsumer);

        metrics = new InternodeInboundMetrics(peer, this);
    }

    InboundMessageHandler createHandler(FrameDecoder frameDecoder, ConnectionType type, Channel channel, int version)
    {
        InboundMessageHandler handler =
            handlerProvider.provide(frameDecoder,

                                    type,
                                    channel,
                                    self,
                                    peer,
                                    version,
                                    OutboundConnections.LARGE_MESSAGE_THRESHOLD,

                                    queueCapacity,
                                    endpointReserveCapacity,
                                    globalReserveCapacity,
                                    endpointWaitQueue,
                                    globalWaitQueue,

                                    this::onHandlerClosed,
                                    callbacksFor(type),
                                    messageConsumer);
        handlers.add(handler);
        return handler;
    }

    void releaseMetrics()
    {
        metrics.release();
    }

    private void onHandlerClosed(AbstractMessageHandler handler)
    {
        assert handler instanceof InboundMessageHandler;
        handlers.remove(handler);
        absorbCounters((InboundMessageHandler)handler);
    }

    @VisibleForTesting
    public int count()
    {
        return handlers.size();
    }

    /*
     * Message callbacks
     */

    private InboundMessageCallbacks callbacksFor(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return urgentCallbacks;
            case  SMALL_MESSAGES: return smallCallbacks;
            case  LARGE_MESSAGES: return largeCallbacks;
            case LEGACY_MESSAGES: return legacyCallbacks;
        }

        throw new IllegalArgumentException();
    }

    private static InboundMessageCallbacks makeMessageCallbacks(InetAddressAndPort peer, InboundCounters counters, GlobalMetricCallbacks globalMetrics, MessageConsumer messageConsumer)
    {
        LatencyConsumer internodeLatency = globalMetrics.internodeLatencyRecorder(peer);

        return new InboundMessageCallbacks()
        {
            @Override
            public void onHeaderArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                // do not log latency if we are within error bars of zero
                if (timeElapsed > unit.convert(approxTime.error(), NANOSECONDS))
                    internodeLatency.accept(timeElapsed, unit);
            }

            @Override
            public void onArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
            }

            @Override
            public void onArrivedExpired(int messageSize, Header header, boolean wasCorrupt, long timeElapsed, TimeUnit unit)
            {
                counters.addExpired(messageSize);

                globalMetrics.recordInternodeDroppedMessage(header.verb, timeElapsed, unit);
            }

            @Override
            public void onArrivedCorrupt(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                counters.addError(messageSize);

                messageConsumer.fail(header, new Crc.InvalidCrc(0, 0)); // could use one of the original exceptions?
            }

            @Override
            public void onClosedBeforeArrival(int messageSize, Header header, int bytesReceived, boolean wasCorrupt, boolean wasExpired)
            {
                counters.addError(messageSize);

                messageConsumer.fail(header, new InvalidSerializedSizeException(header.verb, messageSize, bytesReceived));
            }

            @Override
            public void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                counters.addExpired(messageSize);

                globalMetrics.recordInternodeDroppedMessage(header.verb, timeElapsed, unit);
            }

            @Override
            public void onFailedDeserialize(int messageSize, Header header, Throwable t)
            {
                counters.addError(messageSize);

                /*
                 * If an exception is caught during deser, return a failure response immediately
                 * instead of waiting for the callback on the other end to expire.
                 */
                messageConsumer.fail(header, t);
            }

            @Override
            public void onDispatched(int messageSize, Header header)
            {
                counters.addPending(messageSize);
            }

            @Override
            public void onExecuting(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                globalMetrics.recordInternalLatency(header.verb, timeElapsed, unit);
            }

            @Override
            public void onExecuted(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                counters.removePending(messageSize);
            }

            @Override
            public void onProcessed(int messageSize, Header header)
            {
                counters.addProcessed(messageSize);
            }
        };
    }

    /*
     * Aggregated counters
     */

    InboundCounters countersFor(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return urgentCounters;
            case  SMALL_MESSAGES: return smallCounters;
            case  LARGE_MESSAGES: return largeCounters;
            case LEGACY_MESSAGES: return legacyCounters;
        }

        throw new IllegalArgumentException();
    }

    public long receivedCount()
    {
        return sumHandlers(h -> h.receivedCount) + closedReceivedCount;
    }

    public long receivedBytes()
    {
        return sumHandlers(h -> h.receivedBytes) + closedReceivedBytes;
    }

    public long throttledCount()
    {
        return sumHandlers(h -> h.throttledCount) + closedThrottledCount;
    }

    public long throttledNanos()
    {
        return sumHandlers(h -> h.throttledNanos) + closedThrottledNanos;
    }

    public long usingCapacity()
    {
        return sumHandlers(h -> h.queueSize);
    }

    public long usingEndpointReserveCapacity()
    {
        return endpointReserveCapacity.using();
    }

    public long corruptFramesRecovered()
    {
        return sumHandlers(h -> h.corruptFramesRecovered) + closedCorruptFramesRecovered;
    }

    public long corruptFramesUnrecovered()
    {
        return sumHandlers(h -> h.corruptFramesUnrecovered) + closedCorruptFramesUnrecovered;
    }

    public long errorCount()
    {
        return sumCounters(InboundCounters::errorCount);
    }

    public long errorBytes()
    {
        return sumCounters(InboundCounters::errorBytes);
    }

    public long expiredCount()
    {
        return sumCounters(InboundCounters::expiredCount);
    }

    public long expiredBytes()
    {
        return sumCounters(InboundCounters::expiredBytes);
    }

    public long processedCount()
    {
        return sumCounters(InboundCounters::processedCount);
    }

    public long processedBytes()
    {
        return sumCounters(InboundCounters::processedBytes);
    }

    public long scheduledCount()
    {
        return sumCounters(InboundCounters::scheduledCount);
    }

    public long scheduledBytes()
    {
        return sumCounters(InboundCounters::scheduledBytes);
    }

    /*
     * 'Archived' counter values, combined for all connections that have been closed.
     */

    private volatile long closedReceivedCount, closedReceivedBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedReceivedCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedReceivedCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedReceivedBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedReceivedBytes");

    private volatile long closedThrottledCount, closedThrottledNanos;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedThrottledCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedThrottledCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedThrottledNanosUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedThrottledNanos");

    private volatile long closedCorruptFramesRecovered, closedCorruptFramesUnrecovered;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedCorruptFramesRecoveredUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedCorruptFramesRecovered");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedCorruptFramesUnrecoveredUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedCorruptFramesUnrecovered");

    private void absorbCounters(InboundMessageHandler handler)
    {
        closedReceivedCountUpdater.addAndGet(this, handler.receivedCount);
        closedReceivedBytesUpdater.addAndGet(this, handler.receivedBytes);

        closedThrottledCountUpdater.addAndGet(this, handler.throttledCount);
        closedThrottledNanosUpdater.addAndGet(this, handler.throttledNanos);

        closedCorruptFramesRecoveredUpdater.addAndGet(this, handler.corruptFramesRecovered);
        closedCorruptFramesUnrecoveredUpdater.addAndGet(this, handler.corruptFramesUnrecovered);
    }

    private long sumHandlers(ToLongFunction<InboundMessageHandler> counter)
    {
        long sum = 0L;
        for (InboundMessageHandler h : handlers)
            sum += counter.applyAsLong(h);
        return sum;
    }

    private long sumCounters(ToLongFunction<InboundCounters> mapping)
    {
        return mapping.applyAsLong(urgentCounters)
             + mapping.applyAsLong(smallCounters)
             + mapping.applyAsLong(largeCounters)
             + mapping.applyAsLong(legacyCounters);
    }

    interface HandlerProvider
    {
        InboundMessageHandler provide(FrameDecoder decoder,

                                      ConnectionType type,
                                      Channel channel,
                                      InetAddressAndPort self,
                                      InetAddressAndPort peer,
                                      int version,
                                      int largeMessageThreshold,

                                      int queueCapacity,
                                      ResourceLimits.Limit endpointReserveCapacity,
                                      ResourceLimits.Limit globalReserveCapacity,
                                      InboundMessageHandler.WaitQueue endpointWaitQueue,
                                      InboundMessageHandler.WaitQueue globalWaitQueue,

                                      InboundMessageHandler.OnHandlerClosed onClosed,
                                      InboundMessageCallbacks callbacks,
                                      Consumer<Message<?>> consumer);
    }
}
