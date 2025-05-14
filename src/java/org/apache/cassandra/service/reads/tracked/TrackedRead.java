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

package org.apache.cassandra.service.reads.tracked;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.AbstractPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetrics;

public abstract class TrackedRead<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> implements RequestCallback<TrackedDataResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedRead.class);

    public static class Id
    {
        private static final int nodeId = ClusterMetadata.current().myNodeId().id();
        private static final AtomicLong lastHlc = new AtomicLong();

        private final int node;
        private final long hlc;

        public Id(int node, long hlc)
        {
            this.node = node;
            this.hlc = hlc;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            Id id = (Id) o;
            return node == id.node && hlc == id.hlc;
        }

        @Override
        public int hashCode()
        {
            return Integer.hashCode(node) * 31 + Long.hashCode(hlc);
        }

        @Override
        public String toString()
        {
            return "Id{" + node + ':' + hlc + '}';
        }

        public static final IVersionedSerializer<Id> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Id id, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(id.node);
                out.writeLong(id.hlc);
            }

            @Override
            public Id deserialize(DataInputPlus in, int version) throws IOException
            {
                int node = in.readInt();
                long hlc = in.readLong();
                return new Id(node, hlc);
            }

            @Override
            public long serializedSize(Id id, int version)
            {
                return TypeSizes.sizeof(id.node) + TypeSizes.sizeof(id.hlc);
            }
        };

        public static Id nextId()
        {
            while (true)
            {
                long lastMicros = lastHlc.get();
                long nextMicros = Math.max(lastMicros + 1, TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()));
                if (lastHlc.compareAndSet(lastMicros, nextMicros))
                    return new Id(nodeId, nextMicros);
            }
        }
    }

    private final AsyncPromise<PartitionIterator> future = new AsyncPromise<>();

    private final Id readId = Id.nextId();
    private final ReadCommand command;
    private final ReplicaPlan.AbstractForRead<E, P> replicaPlan;
    private final ConsistencyLevel consistencyLevel;

    private static class RequestFailure extends Throwable
    {
        private final InetAddressAndPort from;
        private final RequestFailureReason reason;

        public RequestFailure(InetAddressAndPort from, RequestFailureReason reason)
        {
            this.from = from;
            this.reason = reason;
        }

        public Map<InetAddressAndPort, RequestFailureReason> reasonByEndpoint()
        {
            return Map.of(from, reason);
        }
    }

    public TrackedRead(ReadCommand command, ReplicaPlan.AbstractForRead<E, P> replicaPlan, ConsistencyLevel consistencyLevel)
    {
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public String toString()
    {
        return "TrackedRead." + getClass().getSimpleName() + '{' + readId + '}';
    }

    protected abstract Verb verb();

    public static class Partition extends TrackedRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        private Partition(SinglePartitionReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForToken, ReplicaPlan.ForTokenRead> replicaPlan, ConsistencyLevel consistencyLevel)
        {
            super(command, replicaPlan, consistencyLevel);
        }

        public static Partition create(ClusterMetadata metadata, SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel)
        {
            Preconditions.checkArgument(command.metadata().replicationType().isTracked());
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;
            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(metadata,
                                                                        keyspace,
                                                                        command.partitionKey().getToken(),
                                                                        command.indexQueryPlan(),
                                                                        consistencyLevel,
                                                                        retry);
            return new Partition(command, replicaPlan, consistencyLevel);
        }

        @Override
        protected Verb verb()
        {
            return Verb.TRACKED_PARTITION_READ_REQ;
        }
    }

    public static class Range extends TrackedRead<EndpointsForRange, ReplicaPlan.ForRangeRead>
    {
        private Range(PartitionRangeReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForRange, ReplicaPlan.ForRangeRead> replicaPlan, ConsistencyLevel consistencyLevel)
        {
            super(command, replicaPlan, consistencyLevel);
        }

        public static TrackedRead.Range create(PartitionRangeReadCommand command, ReplicaPlan.ForRangeRead replicaPlan)
        {
            Preconditions.checkArgument(command.metadata().replicationType().isTracked());
            return new Range(command, replicaPlan, replicaPlan.consistencyLevel());
        }

        @Override
        protected Verb verb()
        {
            return Verb.TRACKED_RANGE_READ_REQ;
        }
    }

    private static <E extends Endpoints<E>> int[] endpointsToHostIds(E endpoints)
    {
        int[] hostids = new int[endpoints.size()];
        int idx = 0;
        ClusterMetadata metadata = ClusterMetadata.current();
        for (Replica replica : endpoints)
            hostids[idx++] = metadata.directory.peerId(replica.endpoint()).id();
        return hostids;
    }

    public void start(long expiresAt)
    {
        // TODO: skip local coordination if this node knows its recovering from an outage
        // TODO: read speculation
        Replica localReplica = replicaPlan.lookup(FBUtilities.getBroadcastAddressAndPort());
        if (localReplica != null)
            readMetrics.localRequests.mark();
        else
            readMetrics.remoteRequests.mark();

        // create an id
        // select data node
        // select summary nodes
        E selected = replicaPlan.contacts();
        Replica dataNode = localReplica != null && localReplica.isFull()
                           ? localReplica
                           : Iterables.getOnlyElement(selected.filter(Replica::isFull, 1));
        E summaryNodes = selected.filter(r -> r != dataNode);
        int[] summaryHostIds = endpointsToHostIds(summaryNodes);


        if (dataNode == localReplica)
        {
            Stage.READ.submit(() -> {
                TrackedLocalReadCoordinator coordinator = MutationTrackingService.instance.localReads().beginRead(readId, ClusterMetadata.current(), command, consistencyLevel, summaryHostIds, expiresAt);
                coordinator.addCallback((response, error) -> {
                    if (error != null)
                    {
                        // TODO: notify coordinator that read has failed
                        logger.error("Error while processing read", error);
                        return;
                    }
                    logger.trace("Finished locally coordinating {}", this);
                    onResponse(response);
                });
            });
        }
        else
        {
            DataRequest dataRequest = new DataRequest(readId, command, consistencyLevel, summaryHostIds);
            Message<DataRequest> dataMessage = Message.outWithFlag(verb(), dataRequest, MessageFlag.CALL_BACK_ON_FAILURE);
            MessagingService.instance().sendWithCallback(dataMessage, dataNode.endpoint(), this);
        }

        if (summaryNodes.isEmpty())
            return;

        SummaryRequest summaryRequest = new SummaryRequest(readId, command);
        Message<SummaryRequest> summaryMessage = Message.outWithParam(Verb.TRACKED_SUMMARY_REQ,
                                                                      summaryRequest,
                                                                      ParamType.RESPOND_TO,
                                                                      dataNode.endpoint());
        for (Replica replica : summaryNodes)
        {
            if (localReplica == replica)
            {
                Stage.READ.submit(() -> summaryRequest.executeLocally(summaryMessage, ClusterMetadata.current()));
            }
            else
            {
                MessagingService.instance().send(summaryMessage, replica.endpoint());
            }
        }
    }

    public void start(Dispatcher.RequestTime requestTime)
    {
        start(requestTime.computeDeadline(verb().expiresAfterNanos()));
    }

    private void onResponse(TrackedDataResponse response)
    {
        future.trySuccess(response.makeIterator(command));
    }

    @Override
    public void onResponse(Message<TrackedDataResponse> msg)
    {
        onResponse(msg.payload);
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        future.tryFailure(new RequestFailure(from, failureReason));
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    public Future<PartitionIterator> future()
    {
        return future;
    }

    public PartitionIterator awaitResults()
    {
        try
        {
            return future.get(command.getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            Throwable ex = e.getCause();
            Map<InetAddressAndPort, RequestFailureReason> reasons = Collections.emptyMap();
            if (ex instanceof RequestFailure)
            {
                RequestFailure failure = (RequestFailure) ex;
                if (failure.reason == RequestFailureReason.TIMEOUT)
                {
                    throw new ReadTimeoutException(replicaPlan.consistencyLevel(), 0, replicaPlan.readQuorum(), false);
                }

                reasons = failure.reasonByEndpoint();
            }

            throw new ReadFailureException(replicaPlan.consistencyLevel(), 0, replicaPlan.readQuorum(), false, reasons);
        }
        catch (TimeoutException e)
        {
            throw new ReadTimeoutException(replicaPlan.consistencyLevel(), 0, replicaPlan.readQuorum(), false);
        }
    }

    public PartitionIterator iterator()
    {
        return new AbstractPartitionIterator()
        {
            PartitionIterator result = null;

            @Override
            protected RowIterator computeNext()
            {
                if (result == null)
                    result = awaitResults();

                if (!result.hasNext())
                    return endOfData();

                return result.next();
            }
        };
    }

    public abstract static class Request
    {
        protected final Id readId;
        protected final ReadCommand command;

        protected Request(Id readId, ReadCommand command)
        {
            this.readId = readId;
            this.command = command;
        }

        public abstract void executeLocally(Message<? extends Request> message, ClusterMetadata metadata);
    }

    public static class DataRequest extends Request
    {
        private final ConsistencyLevel consistencyLevel;
        private final int[] summaryNodes;

        public DataRequest(Id readId, ReadCommand command, ConsistencyLevel consistencyLevel, int[] summaryNodes)
        {
            super(readId, command);
            this.consistencyLevel = consistencyLevel;
            this.summaryNodes = summaryNodes;
        }

        @Override
        public void executeLocally(Message<? extends Request> message, ClusterMetadata metadata)
        {
            TrackedLocalReadCoordinator coordinator = MutationTrackingService.instance.localReads().beginRead(readId, metadata, command, consistencyLevel, summaryNodes, message.expiresAtNanos());
            coordinator.addCallback((response, error) -> {
                if (error != null)
                {
                    // TODO: notify coordinator that read has failed
                    logger.error("Error while processing read", error);
                    return;
                }
                MessagingService.instance().send(message.responseWith(response), message.from());
            });
        }

        public static final IVersionedSerializer<DataRequest> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(DataRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId, out, version);
                ReadCommand.serializer.serialize(request.command, out, version);
                out.writeInt(request.consistencyLevel.code);
                out.writeInt(request.summaryNodes.length);
                for (int hostid : request.summaryNodes)
                    out.writeInt(hostid);
            }

            @Override
            public DataRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                Id readId = Id.serializer.deserialize(in, version);
                ReadCommand command = ReadCommand.serializer.deserialize(in, version);
                ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readInt());
                int[] summaryNodes = new int[in.readInt()];
                for (int i = 0; i < summaryNodes.length; i++)
                    summaryNodes[i] = in.readInt();
                return new DataRequest(readId, command, consistencyLevel, summaryNodes);
            }

            @Override
            public long serializedSize(DataRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId, version) +
                       ReadCommand.serializer.serializedSize(request.command, version) +
                       TypeSizes.sizeof(request.consistencyLevel.code) +
                       TypeSizes.sizeof(request.summaryNodes.length) +
                       ((long) TypeSizes.INT_SIZE * request.summaryNodes.length);
            }
        };
    }

    public static class SummaryRequest extends Request
    {
        public SummaryRequest(Id readId, ReadCommand command)
        {
            super(readId, command);
        }

        @Override
        public void executeLocally(Message<? extends Request> message, ClusterMetadata metadata)
        {
            MutationSummary summary = command.createMutationSummary(false);
            TrackedSummaryResponse response = new TrackedSummaryResponse(readId, summary);
            MessagingService.instance().send(message.responseWith(response), message.respondTo());
        }

        public static final IVersionedSerializer<SummaryRequest> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(SummaryRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId, out, version);
                ReadCommand.serializer.serialize(request.command, out, version);
            }

            @Override
            public SummaryRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                Id readId = Id.serializer.deserialize(in, version);
                ReadCommand command = ReadCommand.serializer.deserialize(in, version);
                return new SummaryRequest(readId, command);
            }

            @Override
            public long serializedSize(SummaryRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId, version) +
                       ReadCommand.serializer.serializedSize(request.command, version);
            }
        };
    }

    public static final IVerbHandler<Request> verbHandler = new AbstractReadCommandVerbHandler<>()
    {
        @Override
        protected void performRead(Message<Request> message, ClusterMetadata metadata)
        {
            message.payload.executeLocally(message, metadata);
        }

        @Override
        protected ReadCommand getCommand(Request payload)
        {
            return payload.command;
        }
    };
}
