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

package org.apache.cassandra.db.streaming;

import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ComponentInfo
{
    final Component.Type type;
    final long length;

    public ComponentInfo(Component.Type type, long length)
    {
        assert length >= 0 : "Component length cannot be negative";
        this.type = type;
        this.length = length;
    }

    @Override
    public String toString()
    {
        return "ComponentInfo{" +
               "type=" + type +
               ", length=" + length +
               '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ComponentInfo that = (ComponentInfo) o;

        return new EqualsBuilder()
               .append(length, that.length)
               .append(type, that.type)
               .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(17, 37)
               .append(type)
               .append(length)
               .toHashCode();
    }

    public static final IVersionedSerializer<ComponentInfo> serializer = new IVersionedSerializer<ComponentInfo>()
    {
        public void serialize(ComponentInfo info, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(info.type.id);
            out.writeLong(info.length);
        }

        public ComponentInfo deserialize(DataInputPlus in, int version) throws IOException
        {
            Component.Type type = Component.Type.fromRepresentation(in.readByte());
            long size = in.readLong();
            return new ComponentInfo(type, size);
        }

        public long serializedSize(ComponentInfo info, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(info.type.id);
            size += TypeSizes.sizeof(info.length);
            return size;
        }
    };
}
