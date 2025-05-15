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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.utils.CollectionSerializer;

/**
 * Instructs a node to send mutations to other node
 */
public class ReadReconcileSend
{
    private static final Logger logger = LoggerFactory.getLogger(ReadReconcileSend.class);

    public static class PeerSync
    {
        final int syncId;
        final InetAddressAndPort to;
        final Log2OffsetsMap.Immutable plan;

        public PeerSync(int syncId, InetAddressAndPort to, Log2OffsetsMap.Immutable plan)
        {
            this.syncId = syncId;
            this.to = to;
            this.plan = plan;
        }

        @Override
        public String toString()
        {
            return "PeerSync{" +
                   "syncId=" + syncId +
                   ", to=" + to +
                   ", plan=" + plan +
                   '}';
        }

        public static final IVersionedSerializer<PeerSync> serializer = new IVersionedSerializer<PeerSync>()
        {
            @Override
            public void serialize(PeerSync sync, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(sync.syncId);
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(sync.to, out, version);
                Log2OffsetsMap.Immutable.serializer.serialize(sync.plan, out, version);
            }

            @Override
            public PeerSync deserialize(DataInputPlus in, int version) throws IOException
            {
                return new PeerSync(in.readInt(),
                                    InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                    Log2OffsetsMap.Immutable.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(PeerSync sync, int version)
            {
                return TypeSizes.sizeof(sync.syncId)
                       + InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(sync.to, version)
                       + Log2OffsetsMap.Immutable.serializer.serializedSize(sync.plan, version);
            }
        };
    }

    public final TrackedRead.Id reconcileId;
    public final ImmutableList<PeerSync> syncTasks;

    public ReadReconcileSend(TrackedRead.Id reconcileId, List<PeerSync> syncTasks)
    {
        this.reconcileId = reconcileId;
        this.syncTasks = ImmutableList.copyOf(syncTasks);
    }

    @Override
    public String toString()
    {
        return "ReadReconcileSend{" +
               "reconcileId=" + reconcileId +
               ", syncTasks=" + syncTasks +
               '}';
    }

    public static final IVerbHandler<ReadReconcileSend> verbHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<ReadReconcileSend> message)
        {
            logger.trace("Received {} from {}", message.payload, message.from());
            // TODO: check epoch and tokens?
            ReadReconcileSend payload = message.payload;
            for (PeerSync sync : message.payload.syncTasks)
            {
                // TODO (expected): do not deser just to serialize again, if same messaging versions (common case)
                // TODO (expected): don't materialize mutation ids, look up from offset collections
                int mutationCount = sync.plan.idCount();
                List<Mutation> mutations = new ArrayList<>(mutationCount);
                MutationJournal.instance.readAll(sync.plan, mutations);
                Preconditions.checkArgument(mutationCount == mutations.size());

                ReadReconcileReceive receive = new ReadReconcileReceive(payload.reconcileId, sync.syncId, message.from(), mutations);
                logger.trace("Sending {} to replica {}", receive, sync.to);
                MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_RCV, receive), sync.to);
            }
        }
    };

    public static final IVersionedSerializer<ReadReconcileSend> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ReadReconcileSend send, DataOutputPlus out, int version) throws IOException
        {
            TrackedRead.Id.serializer.serialize(send.reconcileId, out, version);
            CollectionSerializer.serializeList(PeerSync.serializer, send.syncTasks, out, version);
        }

        @Override
        public ReadReconcileSend deserialize(DataInputPlus in, int version) throws IOException
        {
            TrackedRead.Id readId = TrackedRead.Id.serializer.deserialize(in, version);
            List<PeerSync> syncTasks = CollectionSerializer.deserializeCollection(PeerSync.serializer, ArrayList::new, in, version);
            return new ReadReconcileSend(readId, syncTasks);
        }

        @Override
        public long serializedSize(ReadReconcileSend send, int version)
        {
            return TrackedRead.Id.serializer.serializedSize(send.reconcileId, version) + CollectionSerializer.serializedSizeCollection(PeerSync.serializer, send.syncTasks, version);
        }
    };
}
