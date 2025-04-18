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
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.readMetrics;

public class TrackedRead extends AsyncPromise<PartitionIterator>
{
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


    }

    public abstract static class Request
    {
        public enum Kind
        {
            DATA, SUMMARY
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
            public void serialize(Request t, DataOutputPlus out, int version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Request deserialize(DataInputPlus in, int version) throws IOException
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long serializedSize(Request t, int version)
            {
                throw new UnsupportedOperationException();
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
                Message<TrackedDataResponse> reply = message.responseWith(response);
                MessagingService.instance().send(reply, message.from());
            });
        }
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
