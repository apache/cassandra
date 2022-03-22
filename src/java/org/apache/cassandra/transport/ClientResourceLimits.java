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

package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.utils.concurrent.NonBlockingRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;
import org.apache.cassandra.net.AbstractMessageHandler;
import org.apache.cassandra.net.ResourceLimits;

public class ClientResourceLimits
{
    private static final Logger logger = LoggerFactory.getLogger(ClientResourceLimits.class);

    private static final ResourceLimits.Concurrent GLOBAL_LIMIT = new ResourceLimits.Concurrent(getGlobalLimit());
    private static final AbstractMessageHandler.WaitQueue GLOBAL_QUEUE = AbstractMessageHandler.WaitQueue.global(GLOBAL_LIMIT);
    private static final ConcurrentMap<InetAddress, Allocator> PER_ENDPOINT_ALLOCATORS = new ConcurrentHashMap<>();

    public static final NonBlockingRateLimiter GLOBAL_REQUEST_LIMITER = new NonBlockingRateLimiter(getNativeTransportMaxRequestsPerSecond());

    public enum Overload { NONE, REQUESTS, BYTES_IN_FLIGHT }
    
    public static Allocator getAllocatorForEndpoint(InetAddress endpoint)
    {
        while (true)
        {
            Allocator result = PER_ENDPOINT_ALLOCATORS.computeIfAbsent(endpoint, Allocator::new);
            if (result.acquire())
                return result;

            PER_ENDPOINT_ALLOCATORS.remove(endpoint, result);
        }
    }

    public static long getGlobalLimit()
    {
        return DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightInBytes();
    }

    public static void setGlobalLimit(long newLimit)
    {
        DatabaseDescriptor.setNativeTransportConcurrentRequestDataInFlightInBytes(newLimit);
        long existingLimit = GLOBAL_LIMIT.setLimit(getGlobalLimit());
        logger.info("Changed native_max_transport_requests_in_bytes from {} to {}", existingLimit, newLimit);
    }

    public static long getCurrentGlobalUsage()
    {
        return GLOBAL_LIMIT.using();
    }

    public static long getEndpointLimit()
    {
        return DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes();
    }

    public static void setEndpointLimit(long newLimit)
    {
        long existingLimit = DatabaseDescriptor.getNativeTransportMaxRequestDataInFlightPerIpInBytes();
        DatabaseDescriptor.setNativeTransportMaxRequestDataInFlightPerIpInBytes(newLimit); // ensure new instances get the new limit
        for (Allocator allocator : PER_ENDPOINT_ALLOCATORS.values())
            existingLimit = allocator.endpointAndGlobal.endpoint().setLimit(newLimit);
        logger.info("Changed native_max_transport_requests_in_bytes_per_ip from {} to {}", existingLimit, newLimit);
    }

    public static Snapshot getCurrentIpUsage()
    {
        DecayingEstimatedHistogramReservoir histogram = new DecayingEstimatedHistogramReservoir();
        for (Allocator allocator : PER_ENDPOINT_ALLOCATORS.values())
        {
            histogram.update(allocator.endpointAndGlobal.endpoint().using());
        }
        return histogram.getSnapshot();
    }

    public static int getNativeTransportMaxRequestsPerSecond()
    {
        return DatabaseDescriptor.getNativeTransportMaxRequestsPerSecond();
    }

    public static void setNativeTransportMaxRequestsPerSecond(int newPerSecond)
    {
        int existingPerSecond = getNativeTransportMaxRequestsPerSecond();
        DatabaseDescriptor.setNativeTransportMaxRequestsPerSecond(newPerSecond);
        GLOBAL_REQUEST_LIMITER.setRate(newPerSecond);
        logger.info("Changed native_transport_max_requests_per_second from {} to {}", existingPerSecond, newPerSecond);
    }

    /**
     * This will recompute the ip usage histo on each query of the snapshot when requested instead of trying to keep
     * a histogram up to date with each request
     */
    public static Reservoir ipUsageReservoir()
    {
        return new Reservoir()
        {
            public int size()
            {
                return PER_ENDPOINT_ALLOCATORS.size();
            }

            public void update(long l)
            {
                throw new IllegalStateException();
            }

            public Snapshot getSnapshot()
            {
                return getCurrentIpUsage();
            }
        };
    }

    /**
     * Used during protocol negotiation in {@link InitialConnectionHandler} and then if protocol V4 or earlier is
     * selected. V5 connections convert this to a {@link ResourceProvider} and use the global/endpoint
     * limits and queues directly.
     */
    static class Allocator
    {
        private final AtomicInteger refCount = new AtomicInteger(0);
        private final InetAddress endpoint;

        private final ResourceLimits.EndpointAndGlobal endpointAndGlobal;
        // This is not used or exposed by instances of this class, but it is used for V5+
        // client connections so it's initialized here as the same queue must be shared
        // across all clients from the same remote address.
        private final AbstractMessageHandler.WaitQueue waitQueue;

        private Allocator(InetAddress endpoint)
        {
            this.endpoint = endpoint;
            ResourceLimits.Concurrent limit = new ResourceLimits.Concurrent(getEndpointLimit());
            endpointAndGlobal = new ResourceLimits.EndpointAndGlobal(limit, GLOBAL_LIMIT);
            waitQueue = AbstractMessageHandler.WaitQueue.endpoint(limit);
        }

        private boolean acquire()
        {
            return 0 < refCount.updateAndGet(i -> i < 0 ? i : i + 1);
        }

        /**
         * Decrement the reference count, possibly removing the instance from the cache
         * if this is its final reference
         */
        void release()
        {
            if (-1 == refCount.updateAndGet(i -> i == 1 ? -1 : i - 1))
                PER_ENDPOINT_ALLOCATORS.remove(endpoint, this);
        }

        /**
         * Attempt to allocate a number of permits representing bytes towards the inflight
         * limits. To succeed, it must be possible to allocate from both the per-endpoint
         * and global reserves.
         *
         * @param amount number permits to allocate
         * @return outcome SUCCESS if the allocation was successful. In the case of failure,
         * either INSUFFICIENT_GLOBAL or INSUFFICIENT_ENDPOINT to indicate which 
         * reserve rejected the allocation request.
         */
        ResourceLimits.Outcome tryAllocate(long amount)
        {
            return endpointAndGlobal.tryAllocate(amount);
        }

        /**
         * Force an allocation of a number of permits representing bytes from the inflight
         * limits. Permits will be acquired from both the per-endpoint and global reserves
         * which may lead to either or both reserves going over their limits.
         * @param amount number permits to allocate
         */
        void allocate(long amount)
        {
            endpointAndGlobal.allocate(amount);
        }

        /**
         * Release a number of permits representing bytes back to the both the per-endpoint and
         * global limits for inflight requests.
         *
         * @param amount number of permits to release
         * @return outcome, ABOVE_LIMIT if either reserve is above its configured limit after
         * the operation completes or, BELOW_LIMIT if neither is.
         * rejected the allocation request.
         */
        ResourceLimits.Outcome release(long amount)
        {
            return endpointAndGlobal.release(amount);
        }

        @VisibleForTesting
        long endpointUsing()
        {
           return endpointAndGlobal.endpoint().using();
        }

        @VisibleForTesting
        long globallyUsing()
        {
           return endpointAndGlobal.global().using();
        }

        @Override
        public String toString()
        {
            return String.format("Using %d/%d bytes of endpoint limit and %d/%d bytes of global limit.",
                                 endpointAndGlobal.endpoint().using(), endpointAndGlobal.endpoint().limit(),
                                 endpointAndGlobal.global().using(), endpointAndGlobal.global().limit());
        }
    }

    /**
     * Used in protocol V5 and later by the AbstractMessageHandler/CQLMessageHandler hierarchy.
     * This hides the allocate/tryAllocate/release methods from {@link ClientResourceLimits} and exposes
     * the endpoint and global limits, along with their corresponding
     * {@link org.apache.cassandra.net.AbstractMessageHandler.WaitQueue} directly.
     * Provided as an interface and single implementation for testing (see CQLConnectionTest)
     */
    interface ResourceProvider
    {
        ResourceLimits.Limit globalLimit();
        AbstractMessageHandler.WaitQueue globalWaitQueue();
        ResourceLimits.Limit endpointLimit();
        AbstractMessageHandler.WaitQueue endpointWaitQueue();
        NonBlockingRateLimiter requestRateLimiter();
        void release();

        class Default implements ResourceProvider
        {
            private final Allocator limits;

            Default(Allocator limits)
            {
                this.limits = limits;
            }

            public ResourceLimits.Limit globalLimit()
            {
                return limits.endpointAndGlobal.global();
            }

            public AbstractMessageHandler.WaitQueue globalWaitQueue()
            {
                return GLOBAL_QUEUE;
            }

            public ResourceLimits.Limit endpointLimit()
            {
                return limits.endpointAndGlobal.endpoint();
            }

            public AbstractMessageHandler.WaitQueue endpointWaitQueue()
            {
                return limits.waitQueue;
            }

            public NonBlockingRateLimiter requestRateLimiter()
            {
                return GLOBAL_REQUEST_LIMITER;
            }
            
            public void release()
            {
                limits.release();
            }
        }
    }
}
