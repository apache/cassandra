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
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.utils.CollectionSerializer;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class ReadReconcileReceive
{
    private static final Logger logger = LoggerFactory.getLogger(ReadReconcileReceive.class);

    public final TrackedRead.Id readId;
    public final int syncId;
    public final InetAddressAndPort coordinator;
    public final List<Mutation> mutations;

    public ReadReconcileReceive(TrackedRead.Id readId, int syncId, InetAddressAndPort coordinator, List<Mutation> mutations)
    {
        this.readId = readId;
        this.syncId = syncId;
        this.coordinator = coordinator;
        this.mutations = mutations;
    }

    private static String mutationString(List<Mutation> mutations)
    {
        StringBuilder builder = new StringBuilder('[');
        boolean isFirst = true;
        for (Mutation mutation : mutations)
        {
            if (!isFirst)
            {
                builder.append(", ");
            }
            isFirst = false;
            builder.append(mutation.id());
        }
        builder.append(']');
        return builder.toString();
    }

    @Override
    public String toString()
    {
        return "ReadReconcileReceive{" +
               "reconciliationId=" + readId +
               ", syncId=" + syncId +
               ", coordinator=" + coordinator +
               ", mutations=" + mutationString(mutations) +
               '}';
    }

    public static final IVerbHandler<ReadReconcileReceive> verbHandler = new IVerbHandler<ReadReconcileReceive>()
    {
        @Override
        public void doVerb(Message<ReadReconcileReceive> message) throws IOException
        {
            // TODO: check epoch and tokens?
            ReadReconcileReceive receive = message.payload;
            logger.trace("Received read reconciliation from {}: {}", message.from(), receive);
            receive.mutations.forEach(Mutation::apply);

            if (!MutationTrackingService.instance.localReads().receiveMutations(receive.readId, receive.syncId, receive.mutations))
            {
                // if this isn't a locally coordinated read, notify the coordinator
                ReadReconcileNotify notify = new ReadReconcileNotify(receive.readId, receive.syncId);
                MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_NOTIFY, notify), receive.coordinator);
            }
        }
    };

    public static final IVersionedSerializer<ReadReconcileReceive> serializer = new IVersionedSerializer<ReadReconcileReceive>()
    {
        @Override
        public void serialize(ReadReconcileReceive rcv, DataOutputPlus out, int version) throws IOException
        {
            TrackedRead.Id.serializer.serialize(rcv.readId, out, version);
            out.writeInt(rcv.syncId);
            inetAddressAndPortSerializer.serialize(rcv.coordinator, out, version);
            CollectionSerializer.serializeCollection(Mutation.serializer, rcv.mutations, out, version);

        }

        @Override
        public ReadReconcileReceive deserialize(DataInputPlus in, int version) throws IOException
        {
            TrackedRead.Id readId = TrackedRead.Id.serializer.deserialize(in, version);
            int syncId = in.readInt();
            InetAddressAndPort coordinator = inetAddressAndPortSerializer.deserialize(in, version);
            List<Mutation> mutations = CollectionSerializer.deserializeCollection(Mutation.serializer, ArrayList::new, in, version);
            return new ReadReconcileReceive(readId, syncId, coordinator, mutations);
        }

        @Override
        public long serializedSize(ReadReconcileReceive t, int version)
        {
            return TrackedRead.Id.serializer.serializedSize(t.readId, version)
                   + TypeSizes.sizeof(t.syncId)
                   + inetAddressAndPortSerializer.serializedSize(t.coordinator, version)
                   + CollectionSerializer.serializedSizeCollection(Mutation.serializer, t.mutations, version);
        }
    };
}
