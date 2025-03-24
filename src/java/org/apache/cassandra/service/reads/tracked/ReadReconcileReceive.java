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

    public enum Kind
    {
        REPLICA, COORDINATOR, BOTH;

        public static Kind kindFor(boolean replica, boolean coordinator)
        {
            if (replica && coordinator)
                return BOTH;
            else if (replica)
                return REPLICA;
            else if (coordinator)
                return COORDINATOR;

            throw new IllegalArgumentException("Neither replica nor coordinator are true");
        }

        public boolean writeLocally()
        {
            switch (this)
            {
                case REPLICA:
                case BOTH:
                    return true;
                default:
                    return false;
            }
        }

        public boolean applyToRead()
        {
            switch (this)
            {
                case COORDINATOR:
                case BOTH:
                    return true;
                default:
                    return false;
            }
        }

        public static IVersionedSerializer<Kind> serializer = new IVersionedSerializer<Kind>()
        {
            @Override
            public void serialize(Kind kind, DataOutputPlus out, int version) throws IOException
            {
                switch (kind)
                {
                    case REPLICA:
                        out.writeByte(0);
                        break;
                    case COORDINATOR:
                        out.writeByte(1);
                        break;
                    case BOTH:
                        out.writeByte(2);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown kind: " + kind);
                }

            }

            @Override
            public Kind deserialize(DataInputPlus in, int version) throws IOException
            {
                int kind = in.readByte();
                switch (kind)
                {
                    case 0:
                        return Kind.REPLICA;
                    case 1:
                        return Kind.COORDINATOR;
                    case 2:
                        return Kind.BOTH;
                    default:
                        throw new IllegalArgumentException("Unknown kind value: " + kind);
                }
            }

            @Override
            public long serializedSize(Kind t, int version)
            {
                return TypeSizes.BYTE_SIZE;
            }
        };
    }

    public final long reconciliationId;
    public final int syncId;
    public final InetAddressAndPort coordinator;
    public final Kind kind;
    public final List<Mutation> mutations;

    public ReadReconcileReceive(long reconciliationId, int syncId, InetAddressAndPort coordinator, Kind kind, List<Mutation> mutations)
    {
        this.reconciliationId = reconciliationId;
        this.syncId = syncId;
        this.coordinator = coordinator;
        this.kind = kind;
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
               "reconciliationId=" + reconciliationId +
               ", syncId=" + syncId +
               ", coordinator=" + coordinator +
               ", kind=" + kind +
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
            logger.trace("Received read reconciliation {} from", receive, message.from());
            if (receive.kind.writeLocally())
            {
                receive.mutations.forEach(Mutation::apply);
                ReadReconcileNotify notify = new ReadReconcileNotify(receive.reconciliationId, receive.syncId);
                MessagingService.instance().send(Message.out(Verb.READ_RECONCILE_NOTIFY, notify), receive.coordinator);
            }
            if (receive.kind.applyToRead())
            {
                MutationTrackingService.instance.reconciliations().addMutationsToRead(receive.reconciliationId, receive.mutations);
            }
        }
    };

    public static final IVersionedSerializer<ReadReconcileReceive> serializer = new IVersionedSerializer<ReadReconcileReceive>()
    {
        @Override
        public void serialize(ReadReconcileReceive rcv, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(rcv.reconciliationId);
            out.writeInt(rcv.syncId);
            inetAddressAndPortSerializer.serialize(rcv.coordinator, out, version);
            Kind.serializer.serialize(rcv.kind, out, version);
            CollectionSerializer.serializeCollection(Mutation.serializer, rcv.mutations, out, version);

        }

        @Override
        public ReadReconcileReceive deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ReadReconcileReceive(in.readLong(),
                                            in.readInt(),
                                            inetAddressAndPortSerializer.deserialize(in, version),
                                            Kind.serializer.deserialize(in, version),
                                            CollectionSerializer.deserializeCollection(Mutation.serializer, ArrayList::new, in, version));
        }

        @Override
        public long serializedSize(ReadReconcileReceive t, int version)
        {
            return TypeSizes.sizeof(t.reconciliationId)
                   + TypeSizes.sizeof(t.syncId)
                   + inetAddressAndPortSerializer.serializedSize(t.coordinator, version)
                   + Kind.serializer.serializedSize(t.kind, version)
                   + CollectionSerializer.serializedSizeCollection(Mutation.serializer, t.mutations, version);
        }
    };
}
