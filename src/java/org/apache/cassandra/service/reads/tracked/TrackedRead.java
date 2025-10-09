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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.AbstractPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.*;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.db.ReadKind.TRACKED_DATA;
import static org.apache.cassandra.db.ReadKind.TRACKED_SUMMARY;
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

    private final AsyncPromise<TrackedDataResponse> future = new AsyncPromise<>();

    private final Id readId = Id.nextId();
    private final ReadCommand command;
    private final ReplicaPlan.AbstractForRead<E, P> replicaPlan;
    private final ConsistencyLevel consistencyLevel;
    private final Dispatcher.RequestTime requestTime;

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

    public TrackedRead(ReadCommand command, ReplicaPlan.AbstractForRead<E, P> replicaPlan, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.consistencyLevel = consistencyLevel;
        this.requestTime = requestTime;
    }

    public ReplicaPlan.AbstractForRead<E, P> replicaPlan()
    {
        return replicaPlan;
    }

    @Override
    public String toString()
    {
        return "TrackedRead." + getClass().getSimpleName() + '{' + readId + '}';
    }

    protected abstract Verb verb();

    public boolean intersects(DecoratedKey key)
    {
        return command.dataRange().contains(key);
    }

    public static class Partition extends TrackedRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        private Partition(SinglePartitionReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForToken, ReplicaPlan.ForTokenRead> replicaPlan, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            super(command, replicaPlan, consistencyLevel, requestTime);
        }

        public static Partition create(ClusterMetadata metadata, SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            Preconditions.checkArgument(command.metadata().replicationType().isTracked());
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;
            ReplicaPlan.ForTokenRead replicaPlan = ReplicaPlans.forRead(metadata,
                                                                        keyspace,
                                                                        cfs.getTableId(),
                                                                        command.partitionKey().getToken(),
                                                                        command.indexQueryPlan(),
                                                                        consistencyLevel,
                                                                        retry,
                                                                        ReadCoordinator.DEFAULT);
            return new Partition(command, replicaPlan, consistencyLevel, requestTime);
        }

        @Override
        protected Verb verb()
        {
            return Verb.TRACKED_PARTITION_READ_REQ;
        }
    }

    public static class Range extends TrackedRead<EndpointsForRange, ReplicaPlan.ForRangeRead>
    {
        private Range(PartitionRangeReadCommand command, ReplicaPlan.AbstractForRead<EndpointsForRange, ReplicaPlan.ForRangeRead> replicaPlan, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            super(command, replicaPlan, consistencyLevel, requestTime);
        }

        public static TrackedRead.Range create(PartitionRangeReadCommand command, ReplicaPlan.ForRangeRead replicaPlan, Dispatcher.RequestTime requestTime)
        {
            Preconditions.checkArgument(command.metadata().replicationType().isTracked());
            return new Range(command, replicaPlan, replicaPlan.consistencyLevel(), requestTime);
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

    private static int endpointToHostId(Replica replica)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        return metadata.directory.peerId(replica.endpoint()).id();
    }

    private void start(Dispatcher.RequestTime requestTime, Consumer<PartialTrackedRead> partialReadConsumer, TrackedLocalReads.Completer completer)
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
        E selected = replicaPlan.contacts().filter(r -> FailureDetector.instance.isAlive(r.endpoint()));
        if (selected.size() < replicaPlan.readQuorum())
            throw new UnavailableException(String.format("Insufficient replicas available for read (%d < %d)", selected.size(), replicaPlan.readQuorum()),
                                           replicaPlan.consistencyLevel(), selected.size(), replicaPlan.readQuorum());
        Replica dataReplica = localReplica != null && localReplica.isFull()
                            ? localReplica
                            : Iterables.getOnlyElement(selected.filter(Replica::isFull, 1));
        E summaryReplicas = selected.filter(r -> r != dataReplica);

        int dataNode = endpointToHostId(dataReplica);
        int[] summaryNodes = endpointsToHostIds(summaryReplicas);

        if (dataReplica == localReplica)
        {
            logger.trace("Locally coordinating {}", readId);
            Stage.READ.submit(() -> {
                AsyncPromise<TrackedDataResponse> promise =
                    MutationTrackingService.instance.localReads().beginRead(readId, ClusterMetadata.current(), command, consistencyLevel, summaryNodes, requestTime, completer);
                promise.addCallback((response, error) -> {
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
            logger.trace("Sending data request for {} to {}", readId, dataReplica.endpoint());
            Preconditions.checkArgument(partialReadConsumer == null, "Cannot supply read consumer for nonlocal reads");
            DataRequest dataRequest = new DataRequest(readId, command, dataNode, summaryNodes, consistencyLevel);
            Message<DataRequest> dataMessage = Message.builder(verb(), dataRequest)
                                                      .withRequestTime(requestTime)
                                                      .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                                      .build();
            MessagingService.instance().sendWithCallback(dataMessage, dataReplica.endpoint(), this);
        }

        if (summaryReplicas.isEmpty())
            return;

        SummaryRequest summaryRequest = new SummaryRequest(readId, command, dataNode, summaryNodes);
        Message<SummaryRequest> summaryMessage = Message.outWithRequestTime(Verb.TRACKED_SUMMARY_REQ, summaryRequest, requestTime);
        for (Replica replica : summaryReplicas)
        {
            if (localReplica == replica)
            {
                logger.trace("Locally processing summary request for {}", readId);
                Stage.READ.submit(() -> summaryRequest.executeLocally(summaryMessage, ClusterMetadata.current()));
            }
            else
            {
                logger.trace("Sending summary request for {} to {}", readId, replica.endpoint());
                MessagingService.instance().send(summaryMessage, replica.endpoint());
            }
        }
    }

    public void start(Dispatcher.RequestTime requestTime)
    {
        start(requestTime, null, TrackedLocalReads.Completer.DEFAULT);
    }

    public void startLocal(Dispatcher.RequestTime requestTime, Consumer<PartialTrackedRead> partialReadConsumer, TrackedLocalReads.Completer completer)
    {
        start(requestTime, partialReadConsumer, completer);
    }

    private void onResponse(TrackedDataResponse response)
    {
        future.trySuccess(response);
    }

    @Override
    public void onResponse(Message<TrackedDataResponse> msg)
    {
        onResponse(msg.payload);
    }

    @Override
    public void onFailure(InetAddressAndPort from, org.apache.cassandra.exceptions.RequestFailure failure)
    {
        future.tryFailure(new RequestFailure(from, failure.reason));
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    public Future<TrackedDataResponse> future()
    {
        return future;
    }

    public PartitionIterator awaitResults()
    {
        try
        {
            return future.get(Math.max(0, requestTime.computeDeadline(verb().expiresAfterNanos()) - Clock.Global.nanoTime()), NANOSECONDS).makeIterator(command);
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

    public abstract static class Request implements EmbeddableSinglePartitionReadCommand
    {
        public final Id readId;
        public final ReadCommand command;
        public final int dataNode;
        public final int[] summaryNodes;

        protected Request(Id readId, ReadCommand command, int dataNode, int[] summaryNodes)
        {
            this.readId = readId;
            this.command = command;
            this.dataNode = dataNode;
            this.summaryNodes = summaryNodes;
        }

        @Override
        public TableMetadata metadata()
        {
            return command.metadata();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            // The command could be a PartitionRangeRead in which case nothing should call partitionKey
            // If something does it will generate a ClassCastException which is an acceptable way to signal the error
            return ((SinglePartitionReadCommand)command).partitionKey();
        }

        public abstract void executeLocally(Message<? extends Request> message, ClusterMetadata metadata);

        public abstract Future<? extends IReadResponse> executeLocally(Request request, ClusterMetadata metadata, RequestTime requestTime);
    }

    public static class DataRequest extends Request
    {
        private final ConsistencyLevel consistencyLevel;

        public DataRequest(Id readId, ReadCommand command, int dataNode, int[] summaryNodes, ConsistencyLevel consistencyLevel)
        {
            super(readId, command, dataNode, summaryNodes);
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public void executeLocally(Message<? extends Request> message, ClusterMetadata metadata)
        {
            // TODO This is 1000% the wrong deadline?
            Dispatcher.RequestTime requestTime = new Dispatcher.RequestTime(message.createdAtNanos());
            AsyncPromise<TrackedDataResponse> promise =
                MutationTrackingService.instance
                                       .localReads()
                                       .beginRead(readId, metadata, command, consistencyLevel, summaryNodes, requestTime, TrackedLocalReads.Completer.DEFAULT);
            promise.addCallback((response, error) -> {
                if (error != null)
                {
                    // TODO: notify coordinator that read has failed
                    logger.error("Error while processing read", error);
                    return;
                }
                MessagingService.instance().send(message.responseWith(response), message.from());
            });
        }

        @Override
        public Future<? extends IReadResponse> executeLocally(Request request, ClusterMetadata metadata, RequestTime requestTime)
        {
            return MutationTrackingService.instance
                                          .localReads()
                                          .beginRead(readId, metadata, command, consistencyLevel, summaryNodes, requestTime, TrackedLocalReads.Completer.DEFAULT);
        }

        public static final IVersionedSerializer<DataRequest> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(DataRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId, out, version);
                ReadCommand.serializer.serialize(request.command, out, version);
                out.writeInt(request.dataNode);
                out.writeInt(request.summaryNodes.length);
                for (int hostid : request.summaryNodes)
                    out.writeInt(hostid);
                out.writeInt(request.consistencyLevel.code);
            }

            @Override
            public DataRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                Id readId = Id.serializer.deserialize(in, version);
                ReadCommand command = ReadCommand.serializer.deserialize(in, version);
                int dataNode = in.readInt();
                int[] summaryNodes = new int[in.readInt()];
                for (int i = 0; i < summaryNodes.length; i++)
                    summaryNodes[i] = in.readInt();
                ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readInt());
                return new DataRequest(readId, command, dataNode, summaryNodes, consistencyLevel);
            }

            @Override
            public long serializedSize(DataRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId, version) +
                       ReadCommand.serializer.serializedSize(request.command, version) +
                       TypeSizes.sizeof(request.dataNode) +
                       TypeSizes.sizeof(request.summaryNodes.length) +
                       ((long) TypeSizes.INT_SIZE * request.summaryNodes.length) +
                       TypeSizes.sizeof(request.consistencyLevel.code);
            }
        };

        @Override
        public ReadKind kind()
        {
            return TRACKED_DATA;
        }
    }

    public static class SummaryRequest extends Request implements EmbeddableSinglePartitionReadCommand
    {
        public SummaryRequest(Id readId, ReadCommand command, int dataNode, int[] summaryNodes)
        {
            super(readId, command, dataNode, summaryNodes);
        }

        @Override
        public void executeLocally(Message<? extends Request> message, ClusterMetadata metadata)
        {
            ReadReconciliations.instance.handleSummaryRequest((SummaryRequest) message.payload);
        }

        @Override
        public Future<? extends IReadResponse> executeLocally(Request request, ClusterMetadata metadata, RequestTime requestTime)
        {
            ReadReconciliations.instance.handleSummaryRequest((SummaryRequest) request);
            return ImmediateFuture.success(null);
        }

        public static final IVersionedSerializer<SummaryRequest> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(SummaryRequest request, DataOutputPlus out, int version) throws IOException
            {
                Id.serializer.serialize(request.readId, out, version);
                ReadCommand.serializer.serialize(request.command, out, version);
                out.writeInt(request.dataNode);
                out.writeInt(request.summaryNodes.length);
                for (int hostid : request.summaryNodes)
                    out.writeInt(hostid);
            }

            @Override
            public SummaryRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                Id readId = Id.serializer.deserialize(in, version);
                ReadCommand command = ReadCommand.serializer.deserialize(in, version);
                int dataNode = in.readInt();
                int[] summaryNodes = new int[in.readInt()];
                for (int i = 0; i < summaryNodes.length; i++)
                    summaryNodes[i] = in.readInt();
                return new SummaryRequest(readId, command, dataNode, summaryNodes);
            }

            @Override
            public long serializedSize(SummaryRequest request, int version)
            {
                return Id.serializer.serializedSize(request.readId, version) +
                       ReadCommand.serializer.serializedSize(request.command, version) +
                       TypeSizes.sizeof(request.dataNode) +
                       TypeSizes.sizeof(request.summaryNodes.length) +
                       ((long) TypeSizes.INT_SIZE * request.summaryNodes.length);
            }
        };

        @Override
        public ReadKind kind()
        {
            return TRACKED_SUMMARY;
        }
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
