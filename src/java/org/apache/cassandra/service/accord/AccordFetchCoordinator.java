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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.impl.AbstractFetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.CastingSerializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.CollectionSerializers.deserializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializeMap;
import static org.apache.cassandra.utils.CollectionSerializers.serializedMapSize;

public class AccordFetchCoordinator extends AbstractFetchCoordinator implements StreamManager.StreamListener
{
    private static final Query noopQuery = (txnId, executeAt, keys, data, read, update) -> null;

    public static class StreamData implements Data
    {
        public static class SessionInfo
        {
            final TimeUUID planId;
            final boolean hasData;

            public SessionInfo(TimeUUID planId, boolean hasData)
            {
                this.planId = planId;
                this.hasData = hasData;
            }

            static final IVersionedSerializer<SessionInfo> serializer = new IVersionedSerializer<SessionInfo>()
            {
                public void serialize(SessionInfo info, DataOutputPlus out, int version) throws IOException
                {
                    TimeUUID.Serializer.instance.serialize(info.planId, out, version);
                    out.writeBoolean(info.hasData);

                }

                public SessionInfo deserialize(DataInputPlus in, int version) throws IOException
                {
                    return new SessionInfo(TimeUUID.Serializer.instance.deserialize(in, version), in.readBoolean());
                }

                public long serializedSize(SessionInfo info, int version)
                {
                    return TimeUUID.Serializer.instance.serializedSize(info.planId, version) + TypeSizes.BOOL_SIZE;
                }
            };
        }
        public static final IVersionedSerializer<StreamData> serializer = new IVersionedSerializer<StreamData>()
        {
            @Override
            public void serialize(StreamData data, DataOutputPlus out, int version) throws IOException
            {
                serializeMap(data.streams, out, version, TokenRange.serializer, SessionInfo.serializer);
            }

            @Override
            public StreamData deserialize(DataInputPlus in, int version) throws IOException
            {

                return new StreamData(ImmutableMap.copyOf(deserializeMap(in, version,
                                                                         TokenRange.serializer,
                                                                         SessionInfo.serializer)));
            }

            @Override
            public long serializedSize(StreamData data, int version)
            {
                return serializedMapSize(data.streams, version, TokenRange.serializer, SessionInfo.serializer);
            }
        };

        private final ImmutableMap<TokenRange, SessionInfo> streams;

        public StreamData(ImmutableMap<TokenRange, SessionInfo> streams)
        {
            this.streams = streams;
        }

        public static StreamData of(TokenRange range, TimeUUID streamId, boolean hasData)
        {
            return new StreamData(ImmutableMap.of(range, new SessionInfo(streamId, hasData)));
        }

        @Override
        public StreamData merge(Data data)
        {
            StreamData that = (StreamData) data;
            if (that.streams.keySet().stream().anyMatch(this.streams::containsKey))
                throw new IllegalStateException(String.format("Unable to merge: key found in multiple StreamData %s %s",
                                                              this.streams.keySet(), that.streams.keySet()));
            Invariants.checkState(!that.streams.keySet().stream().anyMatch(this.streams::containsKey));
            ImmutableMap.Builder<TokenRange, SessionInfo> builder = ImmutableMap.builder();
            builder.putAll(this.streams);
            builder.putAll(that.streams);
            return new StreamData(builder.build());
        }
    }

    // needs to be externally synchronized
    private class IncomingStream
    {
        private final TimeUUID planId;
        private Range range;
        private Node.Id from;
        private StreamResultFuture future;

        public IncomingStream(TimeUUID planId)
        {
            this.planId = planId;
        }

        private void rangeReceived(Range range, Node.Id from)
        {
            Invariants.nonNull(range);
            Invariants.nonNull(from);
            Invariants.checkState(this.range == null, "range was not null: %s", this.range);
            Invariants.checkState(this.from == null, "from was not null: %s", this.from);
            this.range = range;
            this.from = from;
            maybeListen();
        }

        private void futureReceived(StreamResultFuture future)
        {
            Invariants.nonNull(future);
            Invariants.checkState(this.future == null, "future was not null: %s", this.future);
            this.future = future;
            maybeListen();
        }

        private void maybeListen()
        {
            if (range == null || future == null)
                return;

            Invariants.nonNull(from);

            future.addCallback((state, fail) -> {
                if (fail == null) success(from, Ranges.of(range));
                else fail(from, Ranges.of(range), fail);
            }, ((AccordCommandStore) commandStore()).executor());
        }
    }

    public static class StreamingRead implements Read
    {
        public static final IVersionedSerializer<StreamingRead> serializer = new IVersionedSerializer<StreamingRead>()
        {
            @Override
            public void serialize(StreamingRead read, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(read.to, out, version);
                KeySerializers.ranges.serialize(read.ranges, out, version);
            }

            @Override
            public StreamingRead deserialize(DataInputPlus in, int version) throws IOException
            {
                return new StreamingRead(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                         KeySerializers.ranges.deserialize(in, version));
            }

            @Override
            public long serializedSize(StreamingRead read, int version)
            {
                return InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(read.to, version)
                       + KeySerializers.ranges.serializedSize(read.ranges, version);
            }
        };

        private final InetAddressAndPort to;
        private final Ranges ranges;

        public StreamingRead(InetAddressAndPort to, Ranges ranges)
        {
            this.to = to;
            this.ranges = ranges;
        }

        @Override
        public Seekables<?, ?> keys() { return ranges; }

        private static boolean hasDataToStream(StreamCoordinator coordinator, InetAddressAndPort to)
        {
            for (StreamSession session : coordinator.getAllStreamSessions())
            {
                if (!session.peer.equals(to))
                    continue;

                Invariants.checkState(session.getNumRequests() == 0, "Requested to send data: %s", session);
                if (session.getNumTransfers() > 0)
                    return true;
            }
            return false;
        }

        @Override
        public AsyncChain<Data> read(Seekable key, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
        {
            try
            {
                Invariants.checkArgument(key.domain() == Routable.Domain.Range, "Required Range but saw %s: %s", key.domain(), key);
                TokenRange range = (TokenRange) key;

                // TODO (correctness): check epoch
                // TODO (correctness): handle dropped tables
                TableId tableId = range.table();
                TableMetadata table = ClusterMetadata.current().schema.getKeyspaces().getTableOrViewNullable(tableId);
                Invariants.checkState(table != null, "Table with id %s not found", tableId);

                // FIXME: may also be relocation
                StreamPlan plan = new StreamPlan(StreamOperation.BOOTSTRAP, 1, false,
                                                 null, PreviewKind.NONE).flushBeforeTransfer(true);

                RangesAtEndpoint ranges = RangesAtEndpoint.toDummyList(Collections.singleton(range.toKeyspaceRange()));
                plan.transferRanges(to, table.keyspace, ranges, table.name);
                StreamResultFuture future = plan.execute();
                return AsyncChains.success(StreamData.of(range, future.planId, hasDataToStream(future.getCoordinator(), to)));
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
        }

        @Override
        public Read slice(Ranges ranges) { return new StreamingRead(to, this.ranges.slice(ranges)); }

        @Override
        public Read intersecting(Participants<?> participants) { return new StreamingRead(to, this.ranges.intersecting(ranges)); }

        @Override
        public Read merge(Read other) { throw new UnsupportedOperationException(); }
    }

    public static class StreamingTxn
    {
        private static final IVersionedSerializer<Read> read = new CastingSerializer<>(StreamingRead.class,
                                                                                       StreamingRead.serializer);

        private static final IVersionedSerializer<Query> query = new IVersionedSerializer<Query>()
        {
            @Override
            public void serialize(Query t, DataOutputPlus out, int version)
            {
                Invariants.checkArgument(t == noopQuery);
            }

            @Override
            public Query deserialize(DataInputPlus in, int version)
            {
                return noopQuery;
            }

            @Override
            public long serializedSize(Query t, int version)
            {
                Invariants.checkArgument(t == noopQuery);
                return 0;
            }
        };

        private static final IVersionedSerializer<Update> update = new IVersionedSerializer<Update>()
        {
            @Override
            public void serialize(Update t, DataOutputPlus out, int version)
            {
                Invariants.checkArgument(t == null);
            }

            @Override
            public Update deserialize(DataInputPlus in, int version)
            {
                return null;
            }

            @Override
            public long serializedSize(Update t, int version)
            {
                Invariants.checkArgument(t == null);
                return 0;
            }
        };

        // TODO (desired): this could be serialized as an InetAddressAndPort and Ranges if we had a special case PartialTxn implementation
        public static final IVersionedSerializer<PartialTxn> serializer = new CommandSerializers.PartialTxnSerializer(read, query, update);
    }

    private final Map<TimeUUID, IncomingStream> streams = new HashMap<>();

    public AccordFetchCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore)
    {
        super(node, ranges, syncPoint, fetchRanges, commandStore);
    }

    @Override
    public void start()
    {
        StreamManager.instance.addListener(this);
        super.start();
    }

    private IncomingStream stream(TimeUUID id)
    {
        return streams.computeIfAbsent(id, IncomingStream::new);
    }

    // called from stream thread
    @Override
    public synchronized void onRegister(StreamResultFuture result)
    {
        stream(result.planId).futureReceived(result);
    }

    protected void onDone(Ranges success, Throwable failure)
    {
        StreamManager.instance.removeListener(this);
        super.onDone(success, failure);
    }

    @Override
    protected PartialTxn rangeReadTxn(Ranges ranges)
    {
        StreamingRead read = new StreamingRead(FBUtilities.getBroadcastAddressAndPort(), ranges);
        return new PartialTxn.InMemory(Txn.Kind.Read, ranges, read, noopQuery, null);
    }

    @Override
    protected synchronized void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges received)
    {
        if (data == null)
            return;

        StreamData streamData = (StreamData) data;
        streamData.streams.forEach((range, streamInfo) -> {
            if (streamInfo.hasData)
            {
                stream(streamInfo.planId).rangeReceived(range, from);
            }
            else
            {
                // if there was no data to stream, no connection is initiated, and we aren't notified via the stream
                // listener, so the stream initiator notifies us and we mark it complete here
                success(from, Ranges.of(range));
            }
        });
    }
}
