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

package org.apache.cassandra.service.reads.range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.MultiRangeReadResponse;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.DataResolver;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.service.reads.ShortReadPartitionsProtection;
import org.apache.cassandra.service.reads.ShortReadProtection;
import org.apache.cassandra.service.reads.repair.NoopReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;

/**
 * Coordinates the process of endpoint grouping queries for given vnode ranges based on concurrency factor:
 * <ol>
 *     <li> Collect token ranges required by concurrency factor in token order and group token ranges by endpoint
 *     <li> Create single-range read callbacks corresponding to each vnode range, note that:
 *        <ul>
 *           <li> In order to maintain proper single result counting for short-read-protection, single-range read callback
 *                cannot start resolving before previous one has finished resolving.
 *        </ul>
 *     <li> Execute {@link MultiRangeReadCommand} on each selected endpoint with all its replicated ranges at once.
 *     <li> Upon receiving individual {@link MultiRangeReadResponse}:
 *        <ol>
 *           <li> It will split multi-range response into single-range responses by queried vnode ranges.
 *           <li> It will pass single-range responses to their corresponding single-range read callback to allow progressive data merging.
 *        </ol>
 *     <li> Return single-range handlers' result in token order.
 * </ol>
 */
public class EndpointGroupingCoordinator
{
    private final PartitionRangeReadCommand command;
    private final DataLimits.Counter counter;
    private final Map<InetAddressAndPort, EndpointQueryContext> endpointContexts;
    private final List<ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead>> perRangeHandlers;
    private final List<PartitionIterator> concurrentQueries;

    private final long queryStartNanoTime;
    private final int vnodeRanges;

    /**
     * @param command current range read command
     * @param counter the unlimited counter for the command
     * @param replicaPlans to be queried
     * @param concurrencyFactor number of vnode ranges to query at once
     * @param queryStartNanoTime the start time of the query
     */
    public EndpointGroupingCoordinator(PartitionRangeReadCommand command,
                                       DataLimits.Counter counter,
                                       Iterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                       int concurrencyFactor,
                                       long queryStartNanoTime)
    {
        this.command = command;
        this.counter = counter;
        this.queryStartNanoTime = queryStartNanoTime;
        this.endpointContexts = new HashMap<>();

        // Read callbacks in token order
        perRangeHandlers = new ArrayList<>(concurrencyFactor);
        // Range responses in token order
        concurrentQueries = new ArrayList<>(concurrencyFactor);
        int vnodeRanges = 0;

        while (replicaPlans.hasNext() && vnodeRanges < concurrencyFactor)
        {
            ReplicaPlan.ForRangeRead replicaPlan = replicaPlans.next();

            boolean isFirst = vnodeRanges == 0;
            vnodeRanges += replicaPlan.vnodeCount();
            concurrentQueries.add(createResponse(replicaPlan, isFirst));
        }
        this.vnodeRanges = vnodeRanges;
    }

    public int vnodeRanges()
    {
        return vnodeRanges;
    }

    public PartitionIterator execute()
    {
        for (EndpointQueryContext replica : replicas())
            replica.queryReplica();

        return counter.applyTo(PartitionIterators.concat(concurrentQueries));
    }

    @VisibleForTesting
    Collection<EndpointQueryContext> endpointRanges()
    {
        return endpointContexts.values();
    }

    /**
     * @return number of endpoints to be queried
     */
    int endpoints()
    {
        return endpointContexts.size();
    }

    private Collection<EndpointQueryContext> replicas()
    {
        return endpointContexts.values();
    }

    /**
     * Create a {@link SingleRangeResponse} for a given vnode range. The responses are collected and concatenated by
     * {@code execute}.
     */
    private SingleRangeResponse createResponse(ReplicaPlan.ForRangeRead replicaPlan, boolean isFirst)
    {
        PartitionRangeReadCommand subrangeCommand = command.forSubRange(replicaPlan.range(), isFirst);

        ReplicaPlan.SharedForRangeRead sharedReplicaPlan = ReplicaPlan.shared(replicaPlan);

        DataResolver<EndpointsForRange, ReplicaPlan.ForRangeRead> resolver =
                new EndpointDataResolver(subrangeCommand, sharedReplicaPlan, NoopReadRepair.instance, queryStartNanoTime);

        // Create a handler for the range and add it, by replica, to the endpoint contexts.
        ReadCallback<EndpointsForRange, ReplicaPlan.ForRangeRead> handler =
                new ReadCallback<>(resolver, subrangeCommand, sharedReplicaPlan, queryStartNanoTime);
        
        perRangeHandlers.add(handler);
        for (Replica replica : replicaPlan.contacts())
        {
            endpointContexts.computeIfAbsent(replica.endpoint(),
                                             k -> new EndpointQueryContext(replica.endpoint(),
                                                                           command.createLimitedCounter(false))).add(handler);
        }
        return new SingleRangeResponse(resolver, handler, NoopReadRepair.instance);
    }

    /**
     * Collect and query all involved ranges of a given endpoint
     */
    public static class EndpointQueryContext
    {
        private final InetAddressAndPort endpoint;
        private final List<ReadCallback<?, ?>> handlers;
        // used by SRP to track fetched data from each endpoint to determine if an endpoint is exhausted,
        // aka. no more data can be fetched.
        private final DataLimits.Counter singleResultCounter;

        private MultiRangeReadCommand multiRangeCommand;

        public EndpointQueryContext(InetAddressAndPort endpoint, DataLimits.Counter singleResultCounter)
        {
            this.endpoint = endpoint;
            this.handlers = new ArrayList<>();
            this.singleResultCounter = singleResultCounter;
        }

        /**
         * @param handler read callback for a given vnode range on the current endpoint
         */
        public void add(ReadCallback<?, ?> handler)
        {
            assert multiRangeCommand == null : "Cannot add range to already queried context";
            handlers.add(handler);
        }

        /**
         * Query a single endpoint with multiple vnode ranges asynchronously
         */
        public void queryReplica()
        {
            assert multiRangeCommand == null : "Can only query given endpoint once";
            this.multiRangeCommand = MultiRangeReadCommand.create(handlers);

            SingleEndpointCallback proxy = new SingleEndpointCallback();
            Message<ReadCommand> message = multiRangeCommand.createMessage(false);
            MessagingService.instance().sendWithCallback(message, endpoint, proxy);
        }

        @VisibleForTesting
        public int rangesCount()
        {
            return handlers.size();
        }

        /**
         * A proxy responsible for:
         * 0. propagating failure/timeout to single-range handlers
         * 1. receiving multi-range responses from a given endpoint
         * 2. spliting the multi-range responses by vnode ranges
         * 3. passing the split single-range response to a corresponding read callback which will
         *    start resolving responses if it has got enough responses for the consistency level requirement.
         */
        private class SingleEndpointCallback implements RequestCallback<ReadResponse>
        {
            @Override
            public void onResponse(Message<ReadResponse> response)
            {
                // split single-endpoint multi-range response into per-range handlers.
                MultiRangeReadResponse multiRangeResponse = (MultiRangeReadResponse) response.payload;
                for (ReadCallback<?, ?> handler : handlers)
                {
                    AbstractBounds<PartitionPosition> range = ((PartitionRangeReadCommand) handler.command()).dataRange().keyRange();

                    // extract subrange response in token order
                    ReadResponse subrangeResponse = multiRangeResponse.subrangeResponse(multiRangeCommand, range);
                    handler.onResponse(Message.remoteResponse(response.header.from, Verb.RANGE_RSP, subrangeResponse));
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                for (ReadCallback<?, ?> handler : handlers)
                    handler.onFailure(from, failureReason);
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }

            @Override
            public boolean trackLatencyForSnitch()
            {
                return true;
            }
        }
    }

    /**
     * Short-read-protection needs to know if an endpoint has any more data or it has already reached the limit:
     * If the endpoint has no more data, aka. the counter hasn't reached the limit, there is no point in doing SRP.
     * If the endpoint might have more data, aka. the counter has reached the limit, SRP might be needed.
     *
     * With token ordered range query or single partition query, {@link DataResolver} uses a new single result counter
     * per replica for a given range, as all replicas are queried with the same range.
     *
     * But with endpoint grouping, each source is queried with different token ranges. So we need a shared
     * cross-range counter for each replica to know if given endpoint has more data.
     */
    private class EndpointDataResolver<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E>> extends DataResolver<E, P>
    {
        public EndpointDataResolver(ReadCommand command, ReplicaPlan.Shared replicaPlan, ReadRepair readRepair, long queryStartNanoTime)
        {
            super(command, replicaPlan, readRepair, queryStartNanoTime);
        }

        @Override
        protected UnfilteredPartitionIterator shortReadProtectedResponse(int i, DataResolver.ResolveContext context)
        {
            UnfilteredPartitionIterator originalResponse = responses.get(i).payload.makeIterator(command);

            if (context.needShortReadProtection())
            {
                DataLimits.Counter singleResultCounter = endpointContexts.get(context.replicas.get(i).endpoint()).singleResultCounter;
                return ShortReadProtection.extend(originalResponse,
                                                  command,
                                                  new EndpointShortReadResponseProtection(command,
                                                                                          context.replicas.get(i),
                                                                                          () -> responses.clearUnsafe(i),
                                                                                          singleResultCounter,
                                                                                          context.mergedResultCounter(),
                                                                                          queryStartNanoTime),
                                                  singleResultCounter);
            }
            else
                return originalResponse;
        }

        /**
         * On replica, {@link MultiRangeReadCommand} stops fetching remaining ranges when it reaches limit.
         *
         * We should do short-read-protection if current range is not fetched due to limit.
         */
        public class EndpointShortReadResponseProtection extends ShortReadPartitionsProtection
        {
            public EndpointShortReadResponseProtection(ReadCommand command,
                                                       Replica source,
                                                       Runnable preFetchCallback,
                                                       DataLimits.Counter singleResultCounter,
                                                       DataLimits.Counter mergedResultCounter,
                                                       long queryStartNanoTime)
            {
                super(command, source, preFetchCallback, singleResultCounter, mergedResultCounter, queryStartNanoTime);
            }

            @Override
            public boolean rangeExhausted()
            {
                // if the range is not fetched by original request or SRP, SRP is needed.
                return super.rangeExhausted() && (rangeFetched || !singleResultCounter.isDone());
            }
        }
    }
}
