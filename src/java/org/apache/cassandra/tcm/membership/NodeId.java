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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class NodeId implements Comparable<NodeId>
{
    public static final Serializer serializer = new Serializer();

    public final UUID uuid;

    public NodeId(UUID id)
    {
        this.uuid = id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeId nodeId = (NodeId) o;
        return Objects.equals(uuid, nodeId.uuid);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(uuid);
    }

    @Override
    public String toString()
    {
        return "NodeId{" +
               "id=" + uuid +
               '}';
    }

    public int compareTo(NodeId o)
    {
        return uuid.compareTo(o.uuid);
    }

    public static class Serializer implements MetadataSerializer<NodeId>
    {
        public void serialize(NodeId n, DataOutputPlus out, Version version) throws IOException
        {
            out.writeLong(n.uuid.getMostSignificantBits());
            out.writeLong(n.uuid.getLeastSignificantBits());
        }

        public NodeId deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new NodeId(new UUID(in.readLong(), in.readLong()));
        }

        public long serializedSize(NodeId t, Version version)
        {
            return TypeSizes.LONG_SIZE + TypeSizes.LONG_SIZE;
        }
    }

}
