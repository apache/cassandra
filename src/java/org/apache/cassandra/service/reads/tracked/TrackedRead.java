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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
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
import org.apache.cassandra.utils.CollectionSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetrics;

public class TrackedRead extends AsyncPromise<PartitionIterator> implements RequestCallback<TrackedDataResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedRead.class);
    // TODO: use something durable
    private static final AtomicInteger nextReadId = new AtomicInteger();

    private final ClusterMetadata metadata;
    private final SinglePartitionReadCommand command;
    private final ReplicaPlan.ForTokenRead replicaPlan;
    private final ConsistencyLevel consistencyLevel;
    private final Dispatcher.RequestTime requestTime;

    public TrackedRead(ClusterMetadata metadata, SinglePartitionReadCommand command, ReplicaPlan.ForTokenRead replicaPlan, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        this.metadata = metadata;
        this.command = command;
        this.replicaPlan = replicaPlan;
        this.consistencyLevel = consistencyLevel;
        this.requestTime = requestTime;
    }

    public static long nextReadId()
    {

        return ((long) ClusterMetadata.current().myNodeId().id() << 32) | nextReadId.getAndIncrement();
    }

    public static TrackedRead create(ClusterMetadata metadata,
                                     SinglePartitionReadCommand command,
                                     ConsistencyLevel consistencyLevel,
                                     Dispatcher.RequestTime requestTime)
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

        return new TrackedRead(metadata, command, replicaPlan, consistencyLevel, requestTime);
    }

    public boolean hasLocalRead()
    {
        return replicaPlan.lookup(FBUtilities.getBroadcastAddressAndPort()) != null;
    }

    @Override
    public void onResponse(Message<TrackedDataResponse> msg)
    {
        trySuccess(msg.payload.makeIterator(command));
    }

    public void start()
    {
        if (hasLocalRead())
            readMetrics.localRequests.mark();
        else
            readMetrics.remoteRequests.mark();

        // create an id
        // select data node
        // select summary nodes
        EndpointsForToken selected = replicaPlan.contacts();
        Replica dataNode = Iterables.getOnlyElement(selected.filter(Replica::isFull, 1));
        EndpointsForToken summaryNodes = selected.filter(r -> !r.equals(dataNode));

        long readId = nextReadId();
        DataRequest dataRequest = new DataRequest(readId, command, consistencyLevel, summaryNodes.endpoints());
        MessagingService.instance().sendWithCallback(Message.out(Verb.TRACKED_READ_REQ, dataRequest), dataNode.endpoint(), this);

        if (summaryNodes.isEmpty())
            return;

        SummaryRequest summaryRequest = new SummaryRequest(readId, command, dataNode.endpoint());
        Message<SummaryRequest> summaryMessage = Message.out(Verb.TRACKED_READ_REQ, summaryRequest);
        for (InetAddressAndPort endpoint : summaryNodes.endpoints())
            MessagingService.instance().send(summaryMessage, endpoint);
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

        private final long readId;
        private final SinglePartitionReadCommand command;

        public Request(long readId, SinglePartitionReadCommand command)
        {
            this.readId = readId;
            this.command = command;
        }

        public long readId()
        {
            return readId;
        }

        public SinglePartitionReadCommand command()
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

        public DataRequest(long readId, SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, Set<InetAddressAndPort> summaryNodes)
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
                out.writeLong(request.readId());
                SinglePartitionReadCommand.serializer.serialize(request.command(), out, version);
                out.writeInt(request.consistencyLevel.code);
                CollectionSerializer.serializeCollection(inetAddressAndPortSerializer, request.summaryNodes, out, version);
            }

            @Override
            public DataRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                return new DataRequest(in.readLong(),
                                       (SinglePartitionReadCommand) SinglePartitionReadCommand.serializer.deserialize(in, version),
                                       ConsistencyLevel.fromCode(in.readInt()),
                                       CollectionSerializer.deserializeCollection(inetAddressAndPortSerializer, Sets::newHashSetWithExpectedSize, in, version));
            }

            @Override
            public long serializedSize(DataRequest request, int version)
            {
                return TypeSizes.LONG_SIZE +
                        SinglePartitionReadCommand.serializer.serializedSize(request.command(), version) +
                        TypeSizes.INT_SIZE +
                        CollectionSerializer.serializedSizeCollection(inetAddressAndPortSerializer, request.summaryNodes, version);
            }
        };
    }

    public static class SummaryRequest extends Request
    {
        private final InetAddressAndPort respondTo;

        public SummaryRequest(long readId, SinglePartitionReadCommand command, InetAddressAndPort respondTo)
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
                out.writeLong(request.readId());
                SinglePartitionReadCommand.serializer.serialize(request.command(), out, version);
                inetAddressAndPortSerializer.serialize(request.respondTo, out, version);
            }

            @Override
            public SummaryRequest deserialize(DataInputPlus in, int version) throws IOException
            {
                return new SummaryRequest(in.readLong(),
                                          (SinglePartitionReadCommand) SinglePartitionReadCommand.serializer.deserialize(in, version),
                                          inetAddressAndPortSerializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(SummaryRequest request, int version)
            {
                return TypeSizes.LONG_SIZE +
                        SinglePartitionReadCommand.serializer.serializedSize(request.command(), version) +
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
