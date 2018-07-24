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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ComponentManifest
{
    private final LinkedHashMap<Component.Type, Long> manifest;
    private final Set<Component> components = new LinkedHashSet<>(Component.Type.values().length);
    private final long totalSize;

    public ComponentManifest(Map<Component.Type, Long> componentManifest)
    {
        this.manifest = new LinkedHashMap<>(componentManifest);

        long size = 0;
        for (Map.Entry<Component.Type, Long> entry : this.manifest.entrySet())
        {
            size += entry.getValue();
            this.components.add(Component.parse(entry.getKey().repr));
        }

        this.totalSize = size;
    }

    public Long getSizeForType(Component.Type type)
    {
        return manifest.get(type);
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public Set<Component> getComponents()
    {
        return Collections.unmodifiableSet(components);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComponentManifest that = (ComponentManifest) o;
        return totalSize == that.totalSize &&
               Objects.equals(manifest, that.manifest);
    }

    public int hashCode()
    {

        return Objects.hash(manifest, totalSize);
    }

    public static final IVersionedSerializer<ComponentManifest> serializer = new IVersionedSerializer<ComponentManifest>()
    {
        public void serialize(ComponentManifest manifest, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(manifest.manifest.size());
            for (Map.Entry<Component.Type, Long> entry : manifest.manifest.entrySet())
                serialize(entry.getKey(), entry.getValue(), out);
        }

        public ComponentManifest deserialize(DataInputPlus in, int version) throws IOException
        {
            LinkedHashMap<Component.Type, Long> components = new LinkedHashMap<>(Component.Type.values().length);

            int size = in.readInt();
            assert size >= 0 : "Invalid number of components";

            for (int i = 0; i < size; i++)
            {
                Component.Type type = Component.Type.fromRepresentation(in.readByte());
                long length = in.readLong();
                components.put(type, length);
            }

            return new ComponentManifest(components);
        }

        public long serializedSize(ComponentManifest manifest, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(manifest.manifest.size());
            for (Map.Entry<Component.Type, Long> entry : manifest.manifest.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey().id);
                size += TypeSizes.sizeof(entry.getValue());
            }
            return size;
        }

        private void serialize(Component.Type type, long size, DataOutputPlus out) throws IOException
        {
            out.writeByte(type.id);
            out.writeLong(size);
        }
    };
}
