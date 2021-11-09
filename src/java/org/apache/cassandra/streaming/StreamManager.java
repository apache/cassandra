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

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.management.StreamEventJMXNotifier;
import org.apache.cassandra.streaming.management.StreamStateCompositeData;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming status and progress.
 */
public class StreamManager implements StreamManagerMBean
{
    public static final StreamManager instance = new StreamManager();

    /**
     * Gets streaming rate limiter.
     * When stream_throughput_outbound_megabits_per_sec is 0, this returns rate limiter
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
                                     DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec(),
                                     DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec());
    }

    /**
     * Get streaming rate limiter for entire SSTable operations.
     * When {@code entire_sstable_stream_throughput_outbound_megabits_per_sec}
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
                                     DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMegabitsPerSec(),
                                     DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMegabitsPerSec());
    }

    public static class StreamRateLimiter implements StreamingDataOutputPlus.RateLimiter
    {
        public static final double BYTES_PER_MEGABIT = (1024 * 1024) / 8; // from bits
        private static final RateLimiter LIMITER = RateLimiter.create(calculateRateInBytes());
        private static final RateLimiter INTER_DC_LIMITER = RateLimiter.create(calculateInterDCRateInBytes());
        private static final RateLimiter ENTIRE_SSTABLE_LIMITER = RateLimiter.create(calculateEntireSSTableRateInBytes());
        private static final RateLimiter ENTIRE_SSTABLE_INTER_DC_LIMITER = RateLimiter.create(calculateEntireSSTableInterDCRateInBytes());

        private final RateLimiter limiter;
        private final RateLimiter interDCLimiter;
        private final boolean isLocalDC;
        private final int throughput;
        private final int interDCThroughput;

        private StreamRateLimiter(InetAddressAndPort peer, RateLimiter limiter, RateLimiter interDCLimiter, int throughput, int interDCThroughput)
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
            int throughput = DatabaseDescriptor.getStreamThroughputOutboundMegabitsPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateInterDCRateInBytes()
        {
            int throughput = DatabaseDescriptor.getInterDCStreamThroughputOutboundMegabitsPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateEntireSSTableRateInBytes()
        {
            int throughput = DatabaseDescriptor.getEntireSSTableStreamThroughputOutboundMegabitsPerSec();
            return calculateEffectiveRateInBytes(throughput);
        }

        private static double calculateEntireSSTableInterDCRateInBytes()
        {
            int throughput = DatabaseDescriptor.getEntireSSTableInterDCStreamThroughputOutboundMegabitsPerSec();
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

        private static double calculateEffectiveRateInBytes(int throughput)
        {
            // if throughput is set to 0 or negative value, throttling is disabled
            return throughput > 0
                   ? throughput * BYTES_PER_MEGABIT
                   : Double.MAX_VALUE;
        }
    }

    private final StreamEventJMXNotifier notifier = new StreamEventJMXNotifier();

    /*
     * Currently running streams. Removed after completion/failure.
     * We manage them in two different maps to distinguish plan from initiated ones to
     * receiving ones withing the same JVM.
     */
    private final Map<UUID, StreamResultFuture> initiatorStreams = new NonBlockingHashMap<>();
    private final Map<UUID, StreamResultFuture> followerStreams = new NonBlockingHashMap<>();

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

    public void registerInitiator(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(() -> initiatorStreams.remove(result.planId));

        initiatorStreams.put(result.planId, result);
    }

    public StreamResultFuture registerFollower(final StreamResultFuture result)
    {
        result.addEventListener(notifier);
        // Make sure we remove the stream on completion (whether successful or not)
        result.addListener(() -> followerStreams.remove(result.planId));

        StreamResultFuture previous = followerStreams.putIfAbsent(result.planId, result);
        return previous == null ? result : previous;
    }

    public StreamResultFuture getReceivingStream(UUID planId)
    {
        return followerStreams.get(planId);
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

    public StreamSession findSession(InetAddressAndPort peer, UUID planId, int sessionIndex, boolean searchInitiatorSessions)
    {
        Map<UUID, StreamResultFuture> streams = searchInitiatorSessions ? initiatorStreams : followerStreams;
        return findSession(streams, peer, planId, sessionIndex);
    }

    private StreamSession findSession(Map<UUID, StreamResultFuture> streams, InetAddressAndPort peer, UUID planId, int sessionIndex)
    {
        StreamResultFuture streamResultFuture = streams.get(planId);
        if (streamResultFuture == null)
            return null;

        return streamResultFuture.getSession(peer, sessionIndex);
    }
}
