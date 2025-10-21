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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Long2LongHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryFetchWatermarks;

/**
 * Collects watermarks of closed and retired epochs per range, and synced epochs per node.
 */
public class WatermarkCollector implements ConfigurationService.Listener
{
    private static final Comparator<Map.Entry<Range, Long>> sortByEpochThenRange = (a, b) -> {
        int c = Long.compareUnsigned(a.getValue(), b.getValue());
        if (c == 0) c = a.getKey().compare(b.getKey());
        return c;
    };

    ReducingRangeMap<Long> closed;
    ReducingRangeMap<Long> retired;
    final Long2LongHashMap synced;

    WatermarkCollector()
    {
        closed = new ReducingRangeMap<>();
        retired = new ReducingRangeMap<>();
        synced = new Long2LongHashMap(-1);
    }

    @Override public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        return null;
    }

    @Override
    public synchronized void onRemoteSyncComplete(Node.Id node, long epoch)
    {
        synced.compute(node.id, (k, prev) -> prev == -1 ? epoch : Long.max(prev, epoch));
    }

    @Override
    public synchronized void onEpochClosed(Ranges ranges, long epoch)
    {
        closed = ReducingRangeMap.merge(closed, ReducingRangeMap.create(ranges, epoch), Long::max);
    }

    @Override
    public synchronized void onEpochRetired(Ranges ranges, long epoch)
    {
        retired = ReducingRangeMap.merge(retired, ReducingRangeMap.create(ranges, epoch), Long::max);
    }

    public final IVerbHandler<Void> handler = new IVerbHandler<Void>()
    {
        public void doVerb(Message<Void> message)
        {
            Invariants.require(AccordService.started());
            Snapshot snapshot;
            synchronized (WatermarkCollector.this)
            {
                List<Map.Entry<Range, Long>> closedSnapshot = closed.foldlWithBounds((epoch, list, start, end) -> { list.add(Map.entry(start.rangeFactory().newRange(start, end), epoch)); return list; }, new ArrayList<>(), Predicates.alwaysFalse());
                List<Map.Entry<Range, Long>> retiredSnapshot = retired.foldlWithBounds((epoch, list, start, end) -> { list.add(Map.entry(start.rangeFactory().newRange(start, end), epoch)); return list; }, new ArrayList<>(), Predicates.alwaysFalse());
                Long2LongHashMap syncedSnapshot = new Long2LongHashMap(synced.size(), 0.6f, -1);
                syncedSnapshot.putAll(synced);
                snapshot = new Snapshot(closedSnapshot, retiredSnapshot, syncedSnapshot);
            }
            MessagingService.instance().respond(snapshot, message);
        }
    };

    @VisibleForTesting
    static void fetchAndReportWatermarksAsync(AccordConfigurationService configService)
    {
        SharedContext context = SharedContext.Global.instance;
        Set<InetAddressAndPort> peers = new HashSet<>();
        peers.addAll(ClusterMetadata.current().directory.allAddresses());
        peers.remove(FBUtilities.getBroadcastAddressAndPort());

        context.messaging().<NoPayload, Snapshot>sendWithRetries(retryFetchWatermarks(),
                                                                 context.optionalTasks()::schedule,
                                                                 Verb.ACCORD_FETCH_WATERMARKS_REQ,
                                                                 NoPayload.noPayload,
                                                                 Iterators.cycle(peers),
                                                                 MessageDelivery.RetryPredicate.ALWAYS_RETRY,
                                                                 MessageDelivery.RetryErrorMessage.EMPTY)
               .addCallback((m, fail) -> {
                   if (fail != null)
                       return;

                   Snapshot snapshot = m.payload;
                   long minEpoch = configService.minEpoch();
                   forEachEpoch(configService::receiveClosed, snapshot.closed);
                   forEachEpoch(configService::receiveRetired, snapshot.retired);
                   for (Map.Entry<Long, Long> e : snapshot.synced.entrySet())
                   {
                       Node.Id node = new Node.Id(Ints.saturatedCast(e.getKey()));
                       for (long epoch = minEpoch; epoch <= e.getValue(); epoch++)
                           configService.receiveRemoteSyncComplete(node, epoch);
                   }
               });
    }

    private static void forEachEpoch(BiConsumer<Ranges, Long> forEachEpoch, List<Map.Entry<Range, Long>> rangesAndEpochs)
    {
        if (rangesAndEpochs.isEmpty())
            return;

        rangesAndEpochs.sort(sortByEpochThenRange);
        long collectingEpoch = rangesAndEpochs.get(0).getValue();
        List<Range> ranges = new ArrayList<>();
        for (Map.Entry<Range, Long> e : rangesAndEpochs)
        {
            Range range = e.getKey();
            long epoch = e.getValue();
            if (epoch != collectingEpoch)
            {
                forEachEpoch.accept(Ranges.of(ranges.toArray(Range[]::new)), collectingEpoch);
                collectingEpoch = epoch;
                ranges.clear();
            }
            ranges.add(range);
        }
        forEachEpoch.accept(Ranges.of(ranges.toArray(Range[]::new)), collectingEpoch);
    }

    public static class Snapshot
    {
        public final List<Map.Entry<Range, Long>> closed;
        public final List<Map.Entry<Range, Long>> retired;
        public final Long2LongHashMap synced;

        public Snapshot(List<Map.Entry<Range, Long>> closed, List<Map.Entry<Range, Long>> retired, Long2LongHashMap synced)
        {
            this.closed = closed;
            this.retired = retired;
            this.synced = synced;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Snapshot snapshot = (Snapshot) o;
            return closed.equals(snapshot.closed) && retired.equals(snapshot.retired) && synced.equals(snapshot.synced);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(closed, retired, synced);
        }
    }

    public static final UnversionedSerializer<Snapshot> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Snapshot t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.closed.size());
            for (Map.Entry<Range, Long> e : t.closed)
            {
                TokenRange.serializer.serialize((TokenRange) e.getKey(), out);
                out.writeUnsignedVInt(e.getValue());
            }
            out.writeUnsignedVInt32(t.retired.size());
            for (Map.Entry<Range, Long> e : t.retired)
            {
                TokenRange.serializer.serialize((TokenRange) e.getKey(), out);
                out.writeUnsignedVInt(e.getValue());
            }
            out.writeUnsignedVInt32(t.synced.size());
            for (Map.Entry<Long, Long> e : t.synced.entrySet())
            {
                out.writeUnsignedVInt(e.getKey());
                out.writeUnsignedVInt(e.getValue());
            }
        }

        // TODO (desired): we do not have to deserialize to report these values
        @Override
        public Snapshot deserialize(DataInputPlus in) throws IOException
        {
            int closedSize = in.readUnsignedVInt32();
            List<Map.Entry<Range, Long>> closed = new ArrayList<>();
            for (int i = 0; i < closedSize; i++)
                closed.add(Map.entry(TokenRange.serializer.deserialize(in), in.readUnsignedVInt()));

            int retiredSize = in.readUnsignedVInt32();
            List<Map.Entry<Range, Long>> retired = new ArrayList<>();
            for (int i = 0; i < retiredSize; i++)
                retired.add(Map.entry(TokenRange.serializer.deserialize(in), in.readUnsignedVInt()));

            int syncedSize = in.readUnsignedVInt32();
            Long2LongHashMap synced = new Long2LongHashMap(-1);
            for (int i = 0; i < syncedSize; i++)
            {
                synced.put(in.readUnsignedVInt(), in.readUnsignedVInt());
            }
            return new Snapshot(closed, retired, synced);
        }

        @Override
        public long serializedSize(Snapshot t)
        {
            int size = 0;
            size += TypeSizes.sizeofUnsignedVInt(t.closed.size());
            for (Map.Entry<Range, Long> e : t.closed)
            {
                size += TokenRange.serializer.serializedSize((TokenRange) e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            size += TypeSizes.sizeofUnsignedVInt(t.retired.size());
            for (Map.Entry<Range, Long> e : t.retired)
            {
                size += TokenRange.serializer.serializedSize((TokenRange) e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            size += TypeSizes.sizeofUnsignedVInt(t.synced.size());
            for (Map.Entry<Long, Long> e : t.synced.entrySet())
            {
                size += TypeSizes.sizeofUnsignedVInt(e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            return size;
        }
    };
}
