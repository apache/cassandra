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
package org.apache.cassandra.net;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.utils.vint.VIntCoding.computeUnsignedVIntSize;

/**
 * A container used to store a node -> message_id map for inter-DC write forwarding.
 * We pick one node in each external DC to forward the message to its local peers.
 *
 * TODO: in the next protocol version only serialize peers, message id will become redundant once 3.0 is out of the picture
 */
public final class ForwardingInfo implements Serializable
{
    final List<InetAddressAndPort> targets;
    final long[] messageIds;

    public ForwardingInfo(List<InetAddressAndPort> targets, long[] messageIds)
    {
        Preconditions.checkArgument(targets.size() == messageIds.length);
        this.targets = targets;
        this.messageIds = messageIds;
    }

    /**
     * @return {@code true} if all host are to use the same message id, {@code false} otherwise. Starting with 4.0 and
     * above, we should be reusing the same id, always, but it won't always be true until 3.0/3.11 are phased out.
     */
    public boolean useSameMessageID(long id)
    {
        for (int i = 0; i < messageIds.length; i++)
            if (id != messageIds[i])
                return false;

        return true;
    }

    /**
     * Apply the provided consumer to all (host, message_id) pairs.
     */
    public void forEach(BiConsumer<Long, InetAddressAndPort> biConsumer)
    {
        for (int i = 0; i < messageIds.length; i++)
            biConsumer.accept(messageIds[i], targets.get(i));
    }

    static final IVersionedSerializer<ForwardingInfo> serializer = new IVersionedSerializer<ForwardingInfo>()
    {
        public void serialize(ForwardingInfo forwardTo, DataOutputPlus out, int version) throws IOException
        {
            long[] ids = forwardTo.messageIds;
            List<InetAddressAndPort> targets = forwardTo.targets;

            int count = ids.length;
            if (version >= VERSION_40)
                out.writeUnsignedVInt(count);
            else
                out.writeInt(count);

            for (int i = 0; i < count; i++)
            {
                inetAddressAndPortSerializer.serialize(targets.get(i), out, version);
                if (version >= VERSION_40)
                    out.writeUnsignedVInt(ids[i]);
                else
                    out.writeInt(Ints.checkedCast(ids[i]));
            }
        }

        public long serializedSize(ForwardingInfo forwardTo, int version)
        {
            long[] ids = forwardTo.messageIds;
            List<InetAddressAndPort> targets = forwardTo.targets;

            int count = ids.length;
            long size = version >= VERSION_40 ? computeUnsignedVIntSize(count) : TypeSizes.sizeof(count);

            for (int i = 0; i < count; i++)
            {
                size += inetAddressAndPortSerializer.serializedSize(targets.get(i), version);
                size += version >= VERSION_40 ? computeUnsignedVIntSize(ids[i]) : 4;
            }

            return size;
        }

        public ForwardingInfo deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = version >= VERSION_40 ? Ints.checkedCast(in.readUnsignedVInt()) : in.readInt();

            long[] ids = new long[count];
            List<InetAddressAndPort> targets = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
            {
                targets.add(inetAddressAndPortSerializer.deserialize(in, version));
                ids[i] = version >= VERSION_40 ? Ints.checkedCast(in.readUnsignedVInt()) : in.readInt();
            }

            return new ForwardingInfo(targets, ids);
        }
    };
}
