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

package org.apache.cassandra.service.reads.logged;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.replication.MutationTrackingService;
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
        final ImmutableSet<MutationId> ids;
        final boolean mirrorToCoordinator;

        public PeerSync(int syncId, InetAddressAndPort to, ImmutableSet<MutationId> ids, boolean mirrorToCoordinator)
        {
            this.syncId = syncId;
            this.to = to;
            this.ids = ids;
            this.mirrorToCoordinator = mirrorToCoordinator;
        }

        @Override
        public String toString()
        {
            return "PeerSync{" +
                   "syncId=" + syncId +
                   ", to=" + to +
                   ", ids=" + ids +
                   ", mirrorToCoordinator=" + mirrorToCoordinator +
                   '}';
        }

        public static final IVersionedSerializer<PeerSync> serializer = new IVersionedSerializer<PeerSync>()
        {
            @Override
            public void serialize(PeerSync sync, DataOutputPlus out, int version) throws IOException
            {
                out.writeInt(sync.syncId);
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(sync.to, out, version);
                CollectionSerializer.serializeCollection(MutationId.serializer, sync.ids, out, version);
                out.writeBoolean(sync.mirrorToCoordinator);
            }

            @Override
            public PeerSync deserialize(DataInputPlus in, int version) throws IOException
            {
                return new PeerSync(in.readInt(),
                                    InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                    ImmutableSet.copyOf((Collection<? extends MutationId>) CollectionSerializer.deserializeCollection(MutationId.serializer, s -> new HashSet<>(), in, version)),
                                    in.readBoolean());
            }

            @Override
            public long serializedSize(PeerSync sync, int version)
            {
                return TypeSizes.sizeof(sync.syncId)
                       + InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(sync.to, version)
                       + CollectionSerializer.serializedSizeCollection(MutationId.serializer, sync.ids, version)
                       + TypeSizes.sizeof(sync.mirrorToCoordinator);
            }
        };
    }

    public final long reconcileId;
    public final ImmutableList<PeerSync> syncTasks;

    public ReadReconcileSend(long reconcileId, List<PeerSync> syncTasks)
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

    public static final IVerbHandler<ReadReconcileSend> verbHandler = new IVerbHandler<ReadReconcileSend>()
    {
        @Override
        public void doVerb(Message<ReadReconcileSend> message) throws IOException
        {
            // TODO: check epoch and tokens?
            ReadReconcileSend payload = message.payload;
            for (PeerSync sync : message.payload.syncTasks)
            {
                List<Mutation> mutations = MutationTrackingService.instance().mutations(sync.ids);

                boolean mirrorToCoordinator = sync.mirrorToCoordinator;
                ReadReconcileReceive.Kind kind = ReadReconcileReceive.Kind.REPLICA;

                if (mirrorToCoordinator && sync.to.equals(message.from()))
                {
                    mirrorToCoordinator = false;
                    kind = ReadReconcileReceive.Kind.BOTH;
                }

                {
                    ReadReconcileReceive receive = new ReadReconcileReceive(payload.reconcileId, sync.syncId, message.from(), kind, mutations);
                    MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_RCV, receive), sync.to);
                }

                if (mirrorToCoordinator)
                {
                    ReadReconcileReceive receive = new ReadReconcileReceive(payload.reconcileId, sync.syncId, message.from(), ReadReconcileReceive.Kind.COORDINATOR, mutations);
                    MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_RCV, receive), message.from());
                }
            }
        }
    };

    public static final IVersionedSerializer<ReadReconcileSend> serializer = new IVersionedSerializer<ReadReconcileSend>()
    {
        @Override
        public void serialize(ReadReconcileSend send, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(send.reconcileId);
            CollectionSerializer.serializeList(PeerSync.serializer, send.syncTasks, out, version);
        }

        @Override
        public ReadReconcileSend deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ReadReconcileSend(in.readLong(),
                                         CollectionSerializer.deserializeCollection(PeerSync.serializer, ArrayList::new, in, version));
        }

        @Override
        public long serializedSize(ReadReconcileSend send, int version)
        {
            return TypeSizes.sizeof(send.reconcileId)
                   + CollectionSerializer.serializedSizeCollection(PeerSync.serializer, send.syncTasks, version);
        }
    };
}
