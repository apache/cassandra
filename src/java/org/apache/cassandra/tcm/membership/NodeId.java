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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class NodeId implements Comparable<NodeId>, MultiStepOperation.SequenceKey
{
    private final static long NODE_ID_UUID_MAGIC = 7861390860069061072L;
    public static final Serializer serializer = new Serializer();

    private final int id;

    public NodeId(int id)
    {
        this.id = id;
    }

    public static NodeId fromString(String nodeOrHostId)
    {
        if (nodeOrHostId.length() == UUID.randomUUID().toString().length())
            return NodeId.fromUUID(UUID.fromString(nodeOrHostId));
        return new NodeId(Integer.parseInt(nodeOrHostId));
    }

    public static NodeId fromUUID(UUID uuid)
    {
        if (!isValidNodeId(uuid))
            throw new UnsupportedOperationException("Not a node id: " + uuid); // see RemoveTest#testBadHostId

        long id = 0x0FFFFFFFFFFFFFFFL & uuid.getLeastSignificantBits();
        return new NodeId(Ints.checkedCast(id));
    }

    public static boolean isValidNodeId(UUID uuid)
    {
        long id = 0x0FFFFFFFFFFFFFFFL & uuid.getLeastSignificantBits();
        return (uuid.getMostSignificantBits() == NODE_ID_UUID_MAGIC && id < Integer.MAX_VALUE) ||
                (uuid.getMostSignificantBits() == 0 && uuid.getLeastSignificantBits() < Integer.MAX_VALUE); // old check, for existing cluster upgrades, no need upstream
    }

    @Deprecated(since = "CEP-21")
    public UUID toUUID()
    {
        long lsb = 0xC000000000000000L | id;
        return new UUID(NODE_ID_UUID_MAGIC, lsb);
    }

    public int id()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeId nodeId = (NodeId) o;
        return Objects.equals(id, nodeId.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public String toString()
    {
        return "NodeId{" +
               "id=" + id +
               '}';
    }

    public int compareTo(NodeId o)
    {
        return Integer.compare(id, o.id);
    }

    public static class Serializer implements MetadataSerializer<NodeId>
    {
        public void serialize(NodeId n, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(n.id);
        }

        public NodeId deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new NodeId(in.readUnsignedVInt32());
        }

        public long serializedSize(NodeId t, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(t.id);
        }
    }
}
