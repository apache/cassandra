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
import com.google.common.collect.Sets;

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
import org.apache.cassandra.utils.CollectionSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
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

        public static final IVersionedSerializer<Id> serializer = new IVersionedSerializer<Id>()
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

    public TrackedRead(ReplicaPlan.AbstractForRead<E, P> replicaPlan, ConsistencyLevel consistencyLevel)
    {
        this.replicaPlan = replicaPlan;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public String toString()
    {
        return "TrackedRead." + getClass().getSimpleName() + '{' + readId + '}';
    }

    protected abstract ReadCommand command();
    protected abstract Verb verb();

    public static Partition create(ClusterMetadata metadata,
                                   SinglePartitionReadCommand command,
                                   ConsistencyLevel consistencyLevel)
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

    public static TrackedRead.Range create(PartitionRangeReadCommand command,
                                           ReplicaPlan.ForRangeRead replicaPlan)
    {
        Preconditions.checkArgument(command.metadata().replicationType().isTracked());
        return new Range(command, replicaPlan, replicaPlan.consistencyLevel());
    }

    public static class Partition extends TrackedRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        private final SinglePartitionReadCommand command;
        public Partition(SinglePartitionReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForToken, ReplicaPlan.ForTokenRead> replicaPlan, ConsistencyLevel consistencyLevel)
        {
            super(replicaPlan, consistencyLevel);
            this.command = command;
        }

        @Override
        protected ReadCommand command()
        {
            return command;
        }

        @Override
        protected Verb verb()
        {
            return Verb.TRACKED_READ_REQ;
        }
    }

    public static class Range extends TrackedRead<EndpointsForRange, ReplicaPlan.ForRangeRead>
    {
        private final PartitionRangeReadCommand command;

        public Range(PartitionRangeReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForRange, ReplicaPlan.ForRangeRead> replicaPlan, ConsistencyLevel consistencyLevel)
        {
            super(replicaPlan, consistencyLevel);
            this.command = command;
        }

        @Override
        protected ReadCommand command()
        {
            return command;
        }

        @Override
        protected Verb verb()
        {
            return Verb.TRACKED_RANGE_READ_REQ;
        }
    }

    public void start(long expiresAt)
    {
        // TODO: do the coordination locally if this is a replica
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
        E summaryNodes = selected.filter(r -> !r.equals(dataNode));


        if (dataNode == localReplica)
        {
            Stage.READ.submit(() -> {
                TrackedLocalReadCoordinator coordinator = MutationTrackingService.instance.localReads().beginRead(readId, ClusterMetadata.current(), command(), consistencyLevel, summaryNodes.endpoints(), expiresAt);
                coordinator.addCallback(((response, error) -> {
                    if (error != null)
                    {
                        // TODO: notify coordinator that read has failed
                        logger.error("Error while processing read", error);
                        return;
                    }
                    logger.trace("Finished locally coordinating {}", this);
                    onResponse(response);
                }));
            });
        }
        else
        {
            DataRequest dataRequest = new DataRequest(readId, command(), consistencyLevel, summaryNodes.endpoints());
            MessagingService.instance().sendWithCallback(Message.out(verb(), dataRequest), dataNode.endpoint(), this);
        }

        if (summaryNodes.isEmpty())
            return;

        SummaryRequest summaryRequest = new SummaryRequest(readId, command(), dataNode.endpoint());
        Message<SummaryRequest> summaryMessage = Message.out(verb(), summaryRequest);
        for (InetAddressAndPort endpoint : summaryNodes.endpoints())
            MessagingService.instance().send(summaryMessage, endpoint);
    }

    public void start(Dispatcher.RequestTime requestTime)
    {
        start(requestTime.computeDeadline(verb().expiresAfterNanos()));
    }

    private void onResponse(TrackedDataResponse response)
    {
        future.trySuccess(response.makeIterator(command()));
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

    public Future<PartitionIterator> future()
    {
        return future;
    }

    public PartitionIterator awaitResults()
    {
        try
        {
            return future.get(command().getTimeout(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
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
        public enum Kind
        {
            DATA, SUMMARY;

            static final IVersionedSerializer<Kind> serializer = new IVersionedSerializer<Kind>()
            {
                @Override
                public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
                {
                    switch (kind)
                    {
                        case DATA:
                            out.writeByte(0);
                            break;
                        case SUMMARY:
                            out.writeByte(1);
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported kind: " + kind);
                    }
                }

                @Override
                public Kind deserialize(DataInputPlus in, int version) throws IOException
                {
                    byte b = in.readByte();
                    switch (b)
                    {
                        case 0:
                            return Kind.DATA;
                        case 1:
                            return Kind.SUMMARY;
                        default:
                            throw new IllegalArgumentException("Unknown kind byte: " + b);
                    }
                }

                @Override
                public long serializedSize(Kind kind, int version)
                {
                    return TypeSizes.BYTE_SIZE;
                }
            };
        }

        private final TrackedRead.Id readId;
        private final ReadCommand command;

        public Request(TrackedRead.Id readId, ReadCommand command)
        {
            this.readId = readId;
            this.command = command;
        }

        public TrackedRead.Id readId()
        {
            return readId;
        }

        public ReadCommand command()
        {
            return command;
        }

        public abstract Kind kind();
        public abstract void executeLocally(Message<Request> message, ClusterMetadata metadata);

        public static final IVersionedSerializer<Request> serializer = new IVersionedSerializer<Request>()
        {
            @Override
            public void serialize(Request request, DataOutputPlus out, int version) throws IOException
            {
                Kind.serializer.serialize(request.kind(),out, version);
                switch (request.kind())
                {
                    case DATA:
                        DataRequest.serializer.serialize((DataRequest) request, out, version);
                        break;
                    case SUMMARY:
                        SummaryRequest.serializer.serialize((SummaryRequest) request, out, version);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported kind: " + request.kind());
                }
            }

            @Override
            public Request deserialize(DataInputPlus in, int version) throws IOException
            {
                Kind kind = Kind.serializer.deserialize(in, version);
                switch (kind)
                {
                    case DATA:
                        return DataRequest.serializer.deserialize(in, version);
                    case SUMMARY:
                        return SummaryRequest.serializer.deserialize(in, version);
                    default:
                        throw new IllegalArgumentException("Unsupported kind: " + kind);
                }
            }

            @Override
            public long serializedSize(Request request, int version)
            {
                long size = TypeSizes.BYTE_SIZE;
                switch (request.kind())
                {
                    case DATA:
                        return size + DataRequest.serializer.serializedSize((DataRequest) request, version);
                    case SUMMARY:
                        return size + SummaryRequest.serializer.serializedSize((SummaryRequest) request, version);
                    default:
                        throw new IllegalArgumentException("Unsupported kind: " + request.kind());
                }
            }
        };
    }

    public static class DataRequest extends Request
    {
        private final ConsistencyLevel consistencyLevel;
        private final Set<InetAddressAndPort> summaryNodes;

        public DataRequest(TrackedRead.Id readId, ReadCommand command, ConsistencyLevel consistencyLevel, Set<InetAddressAndPort> summaryNodes)
        {
            super(readId, command);
            this.consistencyLevel = consistencyLevel;
            this.summaryNodes = summaryNodes;
        }

        @Override
        public Kind kind()
        {
            return Kind.DATA;
        }

        @Override
        public void executeLocally(Message<Request> message, ClusterMetadata metadata)
        {
            TrackedLocalReadCoordinator coordinator = MutationTrackingService.instance.localReads().beginRead(readId(), metadata, command(), consistencyLevel, summaryNodes, message.expiresAtNanos());
            coordinator.addCallback((response, error) -> {
                if (error != null)
                {
                    // TODO: notify coordinator that read has failed
                    logger.error("Error while processing read", error);
                    return;
                }
                Message<TrackedDataResponse> reply = message.responseWith(response);
                MessagingService.instance().send(reply, message.from());
            });
        }

        static final IVersionedSerializer<DataRequest> serializer = new IVersionedSerializer<DataRequest>()
        {
            @Override
            public void serialize(DataRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId(), out, version);
                ReadCommand.serializer.serialize(request.command(), out, version);
                out.writeInt(request.consistencyLevel.code);
                CollectionSerializer.serializeCollection(inetAddressAndPortSerializer, request.summaryNodes, out, version);
            }

            @Override
            public DataRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                return new DataRequest(Id.serializer.deserialize(in, version),
                                       ReadCommand.serializer.deserialize(in, version),
                                       ConsistencyLevel.fromCode(in.readInt()),
                                       CollectionSerializer.deserializeCollection(inetAddressAndPortSerializer, Sets::newHashSetWithExpectedSize, in, version));
            }

            @Override
            public long serializedSize(DataRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId(), version) +
                        ReadCommand.serializer.serializedSize(request.command(), version) +
                        TypeSizes.INT_SIZE +
                        CollectionSerializer.serializedSizeCollection(inetAddressAndPortSerializer, request.summaryNodes, version);
            }
        };
    }

    public static class SummaryRequest extends Request
    {
        private final InetAddressAndPort respondTo;

        public SummaryRequest(Id readId, ReadCommand command, InetAddressAndPort respondTo)
        {
            super(readId, command);
            this.respondTo = respondTo;
        }

        @Override
        public Kind kind()
        {
            return Kind.SUMMARY;
        }

        @Override
        public void executeLocally(Message<Request> message, ClusterMetadata metadata)
        {
            // create summary
            MutationSummary summary = command().createMutationSummary(false);
            // send to data node
            TrackedReadSummary response = new TrackedReadSummary(readId(), summary);
            MessagingService.instance().send(Message.out(Verb.TRACKED_READ_SUMMARY, response), respondTo);
        }

        static final IVersionedSerializer<SummaryRequest> serializer = new IVersionedSerializer<SummaryRequest>()
        {
            @Override
            public void serialize(SummaryRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId(), out, version);
                ReadCommand.serializer.serialize(request.command(), out, version);
                inetAddressAndPortSerializer.serialize(request.respondTo, out, version);
            }

            @Override
            public SummaryRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                return new SummaryRequest(Id.serializer.deserialize(in, version),
                                          ReadCommand.serializer.deserialize(in, version),
                                          inetAddressAndPortSerializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(SummaryRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId(), version) +
                        ReadCommand.serializer.serializedSize(request.command(), version) +
                        inetAddressAndPortSerializer.serializedSize(request.respondTo, version);
            }
        };
    }

    public static final IVerbHandler<Request> verbHandler = new AbstractReadCommandVerbHandler<Request>()
    {
        @Override
        protected void performRead(Message<Request> message, ClusterMetadata metadata)
        {
            message.payload.executeLocally(message, metadata);
        }

        @Override
        protected ReadCommand getCommand(Request payload)
        {
            return payload.command();
        }
    };
}
