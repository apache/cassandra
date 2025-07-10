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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;
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
    private static final Logger logger = LoggerFactory.getLogger(WatermarkCollector.class);

    final Map<Range, Long> closed;
    final Map<Range, Long> retired;
    final Int2ObjectHashMap<Long> synced;

    WatermarkCollector()
    {
        closed = new HashMap<>();
        retired = new HashMap<>();
        synced = new Int2ObjectHashMap<>();
    }

    @Override public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        return null;
    }

    @Override
    public void onRemoteSyncComplete(Node.Id node, long epoch)
    {
        synced.compute(node.id, (k, prev) -> prev == null ? epoch : Long.max(prev, epoch));
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch)
    {
        synchronized (this)
        {
            for (Range range : ranges)
                this.closed.compute(range, (k, prev) -> prev == null ? epoch : Long.max(prev, epoch));
        }
    }

    @Override
    public void onEpochRetired(Ranges ranges, long epoch)
    {
        synchronized (this)
        {
            for (Range range : ranges)
                this.retired.compute(range, (k, prev) -> prev == null ? epoch : Long.max(prev, epoch));
        }
    }

    public final IVerbHandler<Void> handler = new IVerbHandler<Void>()
    {
        public void doVerb(Message<Void> message) throws IOException
        {
            Invariants.require(AccordService.started());
            Snapshot snapshot;
            synchronized (WatermarkCollector.this)
            {
                snapshot = new Snapshot(new HashMap<>(closed), new HashMap<>(retired), new Int2ObjectHashMap<>(synced));
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
                   for (Map.Entry<Range, Long> e : snapshot.closed.entrySet())
                   {
                       Ranges r = Ranges.of(e.getKey());
                       configService.receiveClosed(r, e.getValue());
                   }
                   for (Map.Entry<Range, Long> e : snapshot.retired.entrySet())
                   {
                       Ranges r = Ranges.of(e.getKey());
                       configService.receiveRetired(r, e.getValue());
                   }
                   for (Map.Entry<Integer, Long> e : snapshot.synced.entrySet())
                   {
                       Node.Id node = new Node.Id(e.getKey());
                       for (long epoch = minEpoch; epoch <= e.getValue(); epoch++)
                           configService.receiveRemoteSyncComplete(node, epoch);
                   }
               });
    }

    public static class Snapshot
    {
        public final Map<Range, Long> closed;
        public final Map<Range, Long> retired;
        public final Int2ObjectHashMap<Long> synced;

        public Snapshot(Map<Range, Long> closed, Map<Range, Long> retired, Int2ObjectHashMap<Long> synced)
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
            for (Map.Entry<Range, Long> e : t.closed.entrySet())
            {
                TokenRange.serializer.serialize((TokenRange) e.getKey(), out);
                out.writeUnsignedVInt(e.getValue());
            }
            out.writeUnsignedVInt32(t.retired.size());
            for (Map.Entry<Range, Long> e : t.retired.entrySet())
            {
                TokenRange.serializer.serialize((TokenRange) e.getKey(), out);
                out.writeUnsignedVInt(e.getValue());
            }
            out.writeUnsignedVInt32(t.synced.size());
            for (Map.Entry<Integer, Long> e : t.synced.entrySet())
            {
                out.writeUnsignedVInt32(e.getKey());
                out.writeUnsignedVInt(e.getValue());
            }
        }

        // TODO (desired): we do not have to deserialize to report these values
        @Override
        public Snapshot deserialize(DataInputPlus in) throws IOException
        {
            int closedSize = in.readUnsignedVInt32();
            Map<Range, Long> closed = new HashMap<>();
            for (int i = 0; i < closedSize; i++)
            {
                closed.put(TokenRange.serializer.deserialize(in),
                           in.readUnsignedVInt());
            }
            int retiredSize = in.readUnsignedVInt32();
            Map<Range, Long> retired = new HashMap<>();
            for (int i = 0; i < retiredSize; i++)
            {
                retired.put(TokenRange.serializer.deserialize(in),
                            in.readUnsignedVInt());
            }
            int syncedSize = in.readUnsignedVInt32();
            Int2ObjectHashMap<Long> synced = new Int2ObjectHashMap<>();
            for (int i = 0; i < syncedSize; i++)
            {
                synced.put(in.readUnsignedVInt32(),
                           (Long) in.readUnsignedVInt());
            }
            return new Snapshot(closed, retired, synced);
        }

        @Override
        public long serializedSize(Snapshot t)
        {
            int size = 0;
            size += TypeSizes.sizeofUnsignedVInt(t.closed.size());
            for (Map.Entry<Range, Long> e : t.closed.entrySet())
            {
                size += TokenRange.serializer.serializedSize((TokenRange) e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            size += TypeSizes.sizeofUnsignedVInt(t.retired.size());
            for (Map.Entry<Range, Long> e : t.retired.entrySet())
            {
                size += TokenRange.serializer.serializedSize((TokenRange) e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            size += TypeSizes.sizeofUnsignedVInt(t.synced.size());
            for (Map.Entry<Integer, Long> e : t.synced.entrySet())
            {
                size += TypeSizes.sizeofUnsignedVInt(e.getKey());
                size += TypeSizes.sizeofUnsignedVInt(e.getValue());
            }
            return size;
        }
    };
}
