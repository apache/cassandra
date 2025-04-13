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
import accord.primitives.PartialDeps;
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
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.IVersionedSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.tcm.ClusterMetadata;
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

            static final UnversionedSerializer<SessionInfo> serializer = new UnversionedSerializer<>()
            {
                public void serialize(SessionInfo info, DataOutputPlus out) throws IOException
                {
                    TimeUUID.Serializer.instance.serialize(info.planId, out);
                    out.writeBoolean(info.hasData);

                }

                public SessionInfo deserialize(DataInputPlus in) throws IOException
                {
                    return new SessionInfo(TimeUUID.Serializer.instance.deserialize(in), in.readBoolean());
                }

                public long serializedSize(SessionInfo info)
                {
                    return TimeUUID.Serializer.instance.serializedSize(info.planId) + TypeSizes.BOOL_SIZE;
                }
            };
        }
        public static final UnversionedSerializer<StreamData> serializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(StreamData data, DataOutputPlus out) throws IOException
            {
                serializeMap(data.streams, out, TokenRange.serializer, SessionInfo.serializer);
            }

            @Override
            public StreamData deserialize(DataInputPlus in) throws IOException
            {

                return new StreamData(ImmutableMap.copyOf(deserializeMap(in,
                                                                         TokenRange.serializer,
                                                                         SessionInfo.serializer)));
            }

            @Override
            public long serializedSize(StreamData data)
            {
                return serializedMapSize(data.streams, TokenRange.serializer, SessionInfo.serializer);
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
            Invariants.require(!that.streams.keySet().stream().anyMatch(this.streams::containsKey));
            ImmutableMap.Builder<TokenRange, SessionInfo> builder = ImmutableMap.builder();
            builder.putAll(this.streams);
            builder.putAll(that.streams);
            return new StreamData(builder.build());
        }

        @Override
        public Data without(Ranges ranges)
        {
            ImmutableMap.Builder<TokenRange, SessionInfo> copy = ImmutableMap.builder();
            for (Map.Entry<TokenRange, SessionInfo> e : streams.entrySet())
            {
                if (!ranges.intersects(e.getKey()))
                {
                    copy.put(e.getKey(), e.getValue());
                }
                else
                {
                    Ranges remainder = Ranges.of(e.getKey()).without(ranges);
                    for (Range range : remainder)
                        copy.put((TokenRange) range, e.getValue());
                }
            }
            return new StreamData(copy.build());
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
            Invariants.require(this.range == null, "range was not null: %s", this.range);
            Invariants.require(this.from == null, "from was not null: %s", this.from);
            this.range = range;
            this.from = from;
            maybeListen();
        }

        private void futureReceived(StreamResultFuture future)
        {
            Invariants.nonNull(future);
            Invariants.require(this.future == null, "future was not null: %s", this.future);
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
            }, ((AccordCommandStore) commandStore()).taskExecutor());
        }
    }

    public static class StreamingRead implements Read
    {
        public static final ParameterisedVersionedSerializer<StreamingRead, TableMetadatasAndKeys, Version> serializer = new ParameterisedVersionedSerializer<StreamingRead, TableMetadatasAndKeys, Version>()
        {
            @Override
            public void serialize(StreamingRead read, TableMetadatasAndKeys seekables, DataOutputPlus out, Version version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(read.to, out, version.messageVersion());
                KeySerializers.ranges.serialize(read.ranges, out);
            }

            @Override
            public StreamingRead deserialize(TableMetadatasAndKeys seekables, DataInputPlus in, Version version) throws IOException
            {
                return new StreamingRead(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version.messageVersion()),
                                         KeySerializers.ranges.deserialize(in));
            }

            @Override
            public long serializedSize(StreamingRead read, TableMetadatasAndKeys seekables, Version version)
            {
                return InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(read.to, version.messageVersion())
                       + KeySerializers.ranges.serializedSize(read.ranges);
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

                Invariants.require(session.getNumRequests() == 0, "Requested to send data: %s", session);
                if (session.getNumTransfers() > 0)
                    return true;
            }
            return false;
        }

        @Override
        public AsyncChain<Data> read(SafeCommandStore commandStore, Seekable key, Timestamp executeAt)
        {
            try
            {
                Invariants.requireArgument(key.domain() == Routable.Domain.Range, "Required Range but saw %s: %s", key.domain(), key);
                TokenRange range = (TokenRange) key;

                // TODO (required): check epoch
                // TODO (required): handle dropped tables
                TableId tableId = range.table();
                TableMetadata table = ClusterMetadata.current().schema.getKeyspaces().getTableOrViewNullable(tableId);
                Invariants.require(table != null, "Table with id %s not found", tableId);

                // TODO (required): may also be relocation
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
        public Read intersecting(Participants<?> participants) { return new StreamingRead(to, this.ranges.slice(ranges)); }

        @Override
        public Read merge(Read other) { throw new UnsupportedOperationException(); }
    }

    public static class StreamingTxn
    {
        private static final ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> read = (ParameterisedVersionedSerializer)StreamingRead.serializer;

        private static final UnversionedSerializer<Query> query = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(Query t, DataOutputPlus out)
            {
                Invariants.requireArgument(t == noopQuery);
            }

            @Override
            public Query deserialize(DataInputPlus in)
            {
                return noopQuery;
            }

            @Override
            public long serializedSize(Query t)
            {
                Invariants.requireArgument(t == noopQuery);
                return 0;
            }
        };

        private static final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> update = new ParameterisedVersionedSerializer<>()
        {
            @Override
            public void serialize(Update t, TableMetadatasAndKeys seekables, DataOutputPlus out, Version version)
            {
                Invariants.requireArgument(t == null);
            }

            @Override
            public Update deserialize(TableMetadatasAndKeys seekables, DataInputPlus in, Version version)
            {
                return null;
            }

            @Override
            public long serializedSize(Update t, TableMetadatasAndKeys seekables, Version version)
            {
                Invariants.requireArgument(t == null);
                return 0;
            }
        };

        // TODO (desired): this could be serialized as an InetAddressAndPort and Ranges if we had a special case PartialTxn implementation
        public static final IVersionedSerializer<PartialTxn> serializer = new CommandSerializers.PartialTxnSerializer(read, query, update, TableMetadatasAndKeys.serializer);
    }

    private final Map<TimeUUID, IncomingStream> streams = new HashMap<>();

    public AccordFetchCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore)
    {
        super(node, node.someSequentialExecutor(), ranges, syncPoint, fetchRanges, commandStore);
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
        return new PartialTxn.InMemory(Txn.Kind.Read, ranges, read, noopQuery, null, TableMetadatasAndKeys.none(Routable.Domain.Range));
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

    public static class AccordFetchRequest extends FetchRequest
    {
        public AccordFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
        {
            super(sourceEpoch, syncId, ranges, partialDeps, partialTxn);
        }

        @Override
        protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> execute)
        {
            AsyncChain<Data> result = super.beginRead(safeStore, executeAt, txn, execute);
            // TODO (required): verify that streaming snapshots have all been created by now, so we won't stream any data that arrives after this
            readStarted(safeStore);
            return result;
        }
    }

    @Override
    protected FetchRequest newFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
    {
        return new AccordFetchRequest(sourceEpoch, syncId, ranges, partialDeps, partialTxn);
    }
}
