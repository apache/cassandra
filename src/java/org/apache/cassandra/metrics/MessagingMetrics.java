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
package org.apache.cassandra.metrics;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;

import com.codahale.metrics.Timer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.InboundMessageHandlers;
import org.apache.cassandra.net.LatencyConsumer;
import org.apache.cassandra.utils.StatusLogger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.DEFAULT_TIMER_UNIT;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for messages
 */
public class MessagingMetrics implements InboundMessageHandlers.GlobalMetricCallbacks
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");
    private static final Logger logger = LoggerFactory.getLogger(MessagingMetrics.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;
    
    public static class DCLatencyRecorder implements LatencyConsumer
    {
        public final Timer dcLatency;
        public final Timer allLatency;

        DCLatencyRecorder(Timer dcLatency, Timer allLatency)
        {
            this.dcLatency = dcLatency;
            this.allLatency = allLatency;
        }

        public void accept(long timeTaken, TimeUnit units)
        {
            if (timeTaken > 0)
            {
                dcLatency.update(timeTaken, units);
                allLatency.update(timeTaken, units);
            }
        }
    }

    private static final class DroppedForVerb
    {
        final DroppedMessageMetrics metrics;
        final AtomicInteger droppedFromSelf;
        final AtomicInteger droppedFromPeer;

        DroppedForVerb(Verb verb)
        {
            this(new DroppedMessageMetrics(verb));
        }

        DroppedForVerb(DroppedMessageMetrics metrics)
        {
            this.metrics = metrics;
            this.droppedFromSelf = new AtomicInteger(0);
            this.droppedFromPeer = new AtomicInteger(0);
        }
    }

    private final Timer allLatency;
    public final ConcurrentHashMap<String, DCLatencyRecorder> dcLatency;
    public final EnumMap<Verb, Timer> internalLatency;

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedForVerb> droppedMessages = new EnumMap<>(Verb.class);

    public MessagingMetrics()
    {
        allLatency = Metrics.timer(factory.createMetricName("CrossNodeLatency"));
        dcLatency = new ConcurrentHashMap<>();
        internalLatency = new EnumMap<>(Verb.class);
        for (Verb verb : Verb.VERBS)
            internalLatency.put(verb, Metrics.timer(factory.createMetricName(verb + "-WaitLatency")));
        for (Verb verb : Verb.values())
            droppedMessages.put(verb, new DroppedForVerb(verb));
    }

    public DCLatencyRecorder internodeLatencyRecorder(InetAddressAndPort from)
    {
        String dcName = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);
        DCLatencyRecorder dcUpdater = dcLatency.get(dcName);
        if (dcUpdater == null)
            dcUpdater = dcLatency.computeIfAbsent(dcName, k -> new DCLatencyRecorder(Metrics.timer(factory.createMetricName(dcName + "-Latency")), allLatency));
        return dcUpdater;
    }

    public void recordInternalLatency(Verb verb, long timeTaken, TimeUnit units)
    {
        if (timeTaken > 0)
            internalLatency.get(verb).update(timeTaken, units);
    }

    public void recordSelfDroppedMessage(Verb verb)
    {
        recordDroppedMessage(droppedMessages.get(verb), false);
    }

    public void recordSelfDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit)
    {
        recordDroppedMessage(verb, timeElapsed, timeUnit, false);
    }

    public void recordInternodeDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit)
    {
        recordDroppedMessage(verb, timeElapsed, timeUnit, true);
    }

    public void recordDroppedMessage(Message<?> message, long timeElapsed, TimeUnit timeUnit)
    {
        recordDroppedMessage(message.verb(), timeElapsed, timeUnit, message.isCrossNode());
    }

    public void recordDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit, boolean isCrossNode)
    {
        recordDroppedMessage(droppedMessages.get(verb), timeElapsed, timeUnit, isCrossNode);
    }

    private static void recordDroppedMessage(DroppedForVerb droppedMessages, long timeTaken, TimeUnit units, boolean isCrossNode)
    {
        if (isCrossNode)
            droppedMessages.metrics.crossNodeDroppedLatency.update(timeTaken, units);
        else
            droppedMessages.metrics.internalDroppedLatency.update(timeTaken, units);
        recordDroppedMessage(droppedMessages, isCrossNode);
    }

    private static void recordDroppedMessage(DroppedForVerb droppedMessages, boolean isCrossNode)
    {
        droppedMessages.metrics.dropped.mark();
        if (isCrossNode)
            droppedMessages.droppedFromPeer.incrementAndGet();
        else
            droppedMessages.droppedFromSelf.incrementAndGet();
    }

    public void scheduleLogging()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::logDroppedMessages,
                                                                 LOG_DROPPED_INTERVAL_IN_MS,
                                                                 LOG_DROPPED_INTERVAL_IN_MS,
                                                                 MILLISECONDS);
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<>(droppedMessages.size());
        for (Map.Entry<Verb, DroppedForVerb> entry : droppedMessages.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().metrics.dropped.getCount());
        return map;
    }

    private void logDroppedMessages()
    {
        if (resetAndConsumeDroppedErrors(logger::info) > 0)
            StatusLogger.log();
    }

    @VisibleForTesting
    public int resetAndConsumeDroppedErrors(Consumer<String> messageConsumer)
    {
        int count = 0;
        for (Map.Entry<Verb, DroppedForVerb> entry : droppedMessages.entrySet())
        {
            Verb verb = entry.getKey();
            DroppedForVerb droppedForVerb = entry.getValue();

            int droppedInternal = droppedForVerb.droppedFromSelf.getAndSet(0);
            int droppedCrossNode = droppedForVerb.droppedFromPeer.getAndSet(0);
            if (droppedInternal > 0 || droppedCrossNode > 0)
            {
                messageConsumer.accept(String.format("%s messages were dropped in last %d ms: %d internal and %d cross node."
                                      + " Mean internal dropped latency: %d ms and Mean cross-node dropped latency: %d ms",
                                      verb,
                                      LOG_DROPPED_INTERVAL_IN_MS,
                                      droppedInternal,
                                      droppedCrossNode,
                                      DEFAULT_TIMER_UNIT.toMillis((long) droppedForVerb.metrics.internalDroppedLatency.getSnapshot().getMean()),
                                      DEFAULT_TIMER_UNIT.toMillis((long) droppedForVerb.metrics.crossNodeDroppedLatency.getSnapshot().getMean())));
                ++count;
            }
        }
        return count;
    }

    @VisibleForTesting
    public void resetDroppedMessages()
    {
        droppedMessages.replaceAll((u, v) -> new DroppedForVerb(new DroppedMessageMetrics(u)));
    }

}
