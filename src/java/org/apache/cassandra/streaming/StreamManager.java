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
package org.apache.cassandra.streaming;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.management.StreamEventJMXNotifier;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;
import org.apache.cassandra.utils.TimeUUID;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operations should be created through this class to track streaming status and progress.
 */
public class StreamManager implements StreamManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(StreamManager.class);

    public static final StreamManager instance = new StreamManager();

    /**
     * Gets streaming rate limiter.
     * When stream_throughput_outbound is 0, this returns rate limiter
     * with the rate of Double.MAX_VALUE bytes per second.
     * Rate unit is bytes per sec.
     *
     * @return StreamRateLimiter with rate limit set based on peer location.
     */
    public static StreamRateLimiter getRateLimiter(InetAddressAndPort peer)
    {
        return new StreamRateLimiter(peer,
                                     StreamRateLimiter.LIMITER,
                                     StreamRateLimiter.INTER_DC_LIMITER,
                                     DatabaseDescriptor.getStreamThroughputOutboundBytesPerSec(),
                                     DatabaseDescriptor.getInterDCStreamThroughputOutboundBytesPerSec());
    }

    /**
     * Get streaming rate limiter for entire SSTable operations.
     * When {@code entire_sstable_stream_throughput_outbound}
     * is less than or equal ot {@code 0}, this returns rate limiter with the
     * rate of {@link Double.MAX_VALUE} bytes per second.
     * Rate unit is bytes per sec.
     *
     * @param peer the peer location
     * @return {@link  StreamRateLimiter} with entire SSTable rate limit set based on peer location
     */
    public static StreamRateLimiter getEntireSSTableRateLimiter(InetAddressAndPort peer)
    {
        return new StreamRateLimiter(peer,
                                     StreamRateLimiter.ENTIRE_SSTABLE_LIMITER,
                                     StreamRateLimiter.ENTIRE_SSTABLE_INTER_DC_LIMITER,
                                     DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundBytesPerSec(),
                                     DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundBytesPerSec());
    }

    public static class StreamRateLimiter implements StreamingDataOutputPlus.RateLimiter
    {
        public static final double BYTES_PER_MEBIBYTE = 1024.0 * 1024.0;
        private static final RateLimiter LIMITER = RateLimiter.create(calculateRateInBytes());
        private static final RateLimiter INTER_DC_LIMITER = RateLimiter.create(calculateInterDCRateInBytes());
        private static final RateLimiter ENTIRE_SSTABLE_LIMITER = RateLimiter.create(calculateEntireSSTableRateInBytes());
        private static final RateLimiter ENTIRE_SSTABLE_INTER_DC_LIMITER = RateLimiter.create(calculateEntireSSTableInterDCRateInBytes());

        private final RateLimiter limiter;
        private final RateLimiter interDCLimiter;
        private final boolean isLocalDC;
        private final double throughput;
        private final double interDCThroughput;

        private StreamRateLimiter(InetAddressAndPort peer, RateLimiter limiter, RateLimiter interDCLimiter, double throughput, double interDCThroughput)
        {
            this.limiter = limiter;
            this.interDCLimiter = interDCLimiter;
            this.throughput = throughput;
            this.interDCThroughput = interDCThroughput;
            if (DatabaseDescriptor.getLocalDataCenter() != null && DatabaseDescriptor.getEndpointSnitch() != null)
                isLocalDC = DatabaseDescriptor.getLocalDataCenter().equals(
                DatabaseDescriptor.getEndpointSnitch().getDatacenter(peer));
            else
                isLocalDC = true;
        }

        @Override
        public void acquire(int toTransfer)
        {
            limiter.acquire(toTransfer);
            if (!isLocalDC)
                interDCLimiter.acquire(toTransfer);
        }

        @Override
        public boolean isRateLimited()
        {
            // Rate limiting is enabled when throughput greater than 0.
            // If the peer is not local, also check whether inter-DC rate limiting is enabled.
            return throughput > 0 || (!isLocalDC && interDCThroughput > 0);
        }

        public static void updateThroughput()
        {
            LIMITER.setRate(calculateRateInBytes());
        }

        public static void updateInterDCThroughput()
        {
            INTER_DC_LIMITER.setRate(calculateInterDCRateInBytes());
        }

        public static void updateEntireSSTableThroughput()
        {
            ENTIRE_SSTABLE_LIMITER.setRate(calculateEntireSSTableRateInBytes());
        }

        public static void updateEntireSSTableInterDCThroughput()
        {
            ENTIRE_SSTABLE_INTER_DC_LIMITER.setRate(calculateEntireSSTableInterDCRateInBytes());
        }

        private static double calculateRateInBytes()
        {
            double throughput = DatabaseDescriptor.getStreamThroughputOutboundBytesPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateInterDCRateInBytes()
        {
            double throughput = DatabaseDescriptor.getInterDCStreamThroughputOutboundBytesPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateEntireSSTableRateInBytes()
        {
            double throughput = DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundBytesPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateEntireSSTableInterDCRateInBytes()
        {
            double throughput = DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundBytesPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        @VisibleForTesting
        public static double getRateLimiterRateInBytes()
        {
            return LIMITER.getRate();
        }

        @VisibleForTesting
        public static double getInterDCRateLimiterRateInBytes()
        {
            return INTER_DC_LIMITER.getRate();
        }

        @VisibleForTesting
        public static double getEntireSSTableRateLimiterRateInBytes()
        {
            return ENTIRE_SSTABLE_LIMITER.getRate();
        }

        @VisibleForTesting
        public static double getEntireSSTableInterDCRateLimiterRateInBytes()
        {
            return ENTIRE_SSTABLE_INTER_DC_LIMITER.getRate();
        }

        private static double calculateEffectiveRateInBytes(double throughput)
        {
            // if throughput is set to 0, throttling is disabled
            return throughput > 0
                   ? throughput
                   : Double.MAX_VALUE;
        }
    }

    private final StreamEventJMXNotifier notifier = new StreamEventJMXNotifier();
    private final CopyOnWriteArrayList<StreamListener> listeners = new CopyOnWriteArrayList<>();

    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
    private final Map<TimeUUID, StreamResultFuture> initiatorStreams = new NonBlockingHashMap<>();
    private final Map<TimeUUID, StreamResultFuture> followerStreams = new NonBlockingHashMap<>();

    private final Cache<TimeUUID, StreamingState> states;
    private final StreamListener listener = new StreamListener()
    {
        @Override
        public void onRegister(StreamResultFuture result)
        {
            if (!DatabaseDescriptor.getStreamingStatsEnabled())
                return;
            // reason for synchronized rather than states.get is to detect duplicates
            // streaming shouldn't be producing duplicates as that would imply a planId collision
            synchronized (states)
            {
                StreamingState previous = states.getIfPresent(result.planId);
                if (previous == null)
                {
                    StreamingState state = new StreamingState(result);
                    states.put(state.id(), state);
                    state.phase.start();
                    result.addEventListener(state);
                }
                else
                {
                    logger.warn("Duplicate streaming states detected for id {}", result.planId);
                }
            }
        }
    };

    protected void addStreamingStateAgain(StreamingState state)
    {
        if (!DatabaseDescriptor.getStreamingStatsEnabled())
            return;
        states.put(state.id(), state);
    }

    public StreamManager()
    {
        DurationSpec.LongNanosecondsBound duration = DatabaseDescriptor.getStreamingStateExpires();
        long sizeBytes = DatabaseDescriptor.getStreamingStateSize().toBytes();
        logger.info("Storing streaming state for {} or for size {}", duration, sizeBytes);
        states = CacheBuilder.newBuilder()
                             .expireAfterWrite(duration.quantity(), duration.unit())
                             .maximumWeight(sizeBytes)
                             .weigher(new StreamingStateWeigher())
                             .build();
    }

    private static class StreamingStateWeigher implements Weigher<TimeUUID,StreamingState>
    {
        @Override
        public int weigh(TimeUUID key, StreamingState val)
        {
            long costOfStreamingState = val.unsharedHeapSize() + TimeUUID.TIMEUUID_SIZE;
            return Math.toIntExact(costOfStreamingState);
        }
    }

    public void start()
    {
        addListener(listener);
    }

    public void stop()
    {
        removeListener(listener);
    }

    public Collection<StreamingState> getStreamingStates()
    {
        return states.asMap().values();
    }

    public StreamingState getStreamingState(TimeUUID id)
    {
        return states.getIfPresent(id);
    }

    @VisibleForTesting
    public void putStreamingState(StreamingState state)
    {
        synchronized (states)
        {
            StreamingState previous = states.getIfPresent(state.id());
            if (previous != null)
                throw new AssertionError("StreamPlan id " + state.id() + " already exists");
            states.put(state.id(), state);
        }
    }

    @VisibleForTesting
    public void clearStates()
    {
        // states.cleanUp() doesn't clear, it looks to only run gc on things that could be removed... this method should remove all state
        states.asMap().clear();
    }

    public Set<CompositeData> getCurrentStreams()
    {
        return Sets.newHashSet(Iterables.transform(Iterables.concat(initiatorStreams.values(), followerStreams.values()), new Function<StreamResultFuture, CompositeData>()
        {
            public CompositeData apply(StreamResultFuture input)
            {
                return StreamStateCompositeData.toCompositeData(input.getCurrentState());
            }
        }));
    }

    @Override
    public boolean getStreamingStatsEnabled()
    {
        return DatabaseDescriptor.getStreamingStatsEnabled();
    }

    @Override
    public void setStreamingStatsEnabled(boolean streamingStatsEnabled)
    {
        DatabaseDescriptor.setStreamingStatsEnabled(streamingStatsEnabled);
    }

    @Override
    public String getStreamingSlowEventsLogTimeout()
    {
        return DatabaseDescriptor.getStreamingSlowEventsLogTimeout().toString();
    }

    @Override
    public void setStreamingSlowEventsLogTimeout(String value)
    {
        DatabaseDescriptor.setStreamingSlowEventsLogTimeout(value);
    }

    public void registerInitiator(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(() -> initiatorStreams.remove(result.planId));

        initiatorStreams.put(result.planId, result);
        notifySafeOnRegister(result);
    }

    public StreamResultFuture registerFollower(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(() -> followerStreams.remove(result.planId));

        StreamResultFuture previous = followerStreams.putIfAbsent(result.planId, result);
        if (previous == null)
        {
            notifySafeOnRegister(result);
            return result;
        }
        return previous;
    }

    @VisibleForTesting
    public void putInitiatorStream(StreamResultFuture future)
    {
        StreamResultFuture current = initiatorStreams.putIfAbsent(future.planId, future);
        assert current == null: "Duplicat initiator stream for " + future.planId;
    }

    @VisibleForTesting
    public void putFollowerStream(StreamResultFuture future)
    {
        StreamResultFuture current = followerStreams.putIfAbsent(future.planId, future);
        assert current == null: "Duplicate follower stream for " + future.planId;
    }

    public void addListener(StreamListener listener)
    {
        listeners.add(listener);
    }

    public void removeListener(StreamListener listener)
    {
        listeners.remove(listener);
    }

    private void notifySafeOnRegister(StreamResultFuture result)
    {
        for (StreamListener l : listeners)
        {
            try
            {
                l.onRegister(result);
            }
            catch (Throwable t)
            {
                logger.warn("Failed to notify stream listener of new Initiator/Follower", t);
            }
        }
    }

    public StreamResultFuture getReceivingStream(TimeUUID planId)
    {
        return followerStreams.get(planId);
    }

    public StreamResultFuture getInitiatorStream(TimeUUID planId)
    {
        return initiatorStreams.get(planId);
    }

    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
    {
        notifier.addNotificationListener(listener, filter, handback);
    }

    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener);
    }

    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) throws ListenerNotFoundException
    {
        notifier.removeNotificationListener(listener, filter, handback);
    }

    public MBeanNotificationInfo[] getNotificationInfo()
    {
        return notifier.getNotificationInfo();
    }

    public StreamSession findSession(InetAddressAndPort peer, TimeUUID planId, int sessionIndex, boolean searchInitiatorSessions)
    {
        Map<TimeUUID, StreamResultFuture> streams = searchInitiatorSessions ? initiatorStreams : followerStreams;
        return findSession(streams, peer, planId, sessionIndex);
    }

    private StreamSession findSession(Map<TimeUUID, StreamResultFuture> streams, InetAddressAndPort peer, TimeUUID planId, int sessionIndex)
    {
        StreamResultFuture streamResultFuture = streams.get(planId);
        if (streamResultFuture == null)
            return null;

        return streamResultFuture.getSession(peer, sessionIndex);
    }

    public long getTotalRemainingOngoingBytes()
    {
        long total = 0;
        for (StreamResultFuture fut : Iterables.concat(initiatorStreams.values(), followerStreams.values()))
        {
            for (SessionInfo sessionInfo : fut.getCurrentState().sessions)
                total += sessionInfo.getTotalSizeToReceive() - sessionInfo.getTotalSizeReceived();
        }
        return total;
    }

    public interface StreamListener
    {
        default void onRegister(StreamResultFuture result) {}
    }
}
