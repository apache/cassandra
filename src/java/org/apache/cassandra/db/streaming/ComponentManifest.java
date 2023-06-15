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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;

/**
 * SSTable components and their sizes to be tranfered via entire-sstable-streaming
 */
public final class ComponentManifest implements Iterable<Component>
{
    private final LinkedHashMap<Component, Long> components;

    public ComponentManifest(Map<Component, Long> components)
    {
        this.components = new LinkedHashMap<>(components);
    }

    @VisibleForTesting
    public static ComponentManifest create(SSTable sstable)
    {
        Set<Component> streamingComponents = sstable.getStreamingComponents();
        LinkedHashMap<Component, Long> components = new LinkedHashMap<>(streamingComponents.size());

        for (Component component : streamingComponents)
        {
            File file = sstable.descriptor.fileFor(component);
            if (!file.exists())
                continue;

            components.put(component, file.length());
        }

        return new ComponentManifest(components);
    }

    public long sizeOf(Component component)
    {
        Long size = components.get(component);
        if (size == null)
            throw new IllegalArgumentException("Component " + component + " is not present in the manifest");
        return size;
    }

    public long totalSize()
    {
        long totalSize = 0;
        for (Long size : components.values())
            totalSize += size;
        return totalSize;
    }

    public List<Component> components()
    {
        return new ArrayList<>(components.keySet());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ComponentManifest))
            return false;

        ComponentManifest that = (ComponentManifest) o;
        return components.equals(that.components);
    }

    @Override
    public int hashCode()
    {
        return components.hashCode();
    }

    @Override
    public String toString()
    {
        return "ComponentManifest{" +
               "components=" + components +
               '}';
    }

    public static final Map<String, IVersionedSerializer<ComponentManifest>> serializers;

    static
    {
        ImmutableMap.Builder<String, IVersionedSerializer<ComponentManifest>> b = ImmutableMap.builder();
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            IVersionedSerializer<ComponentManifest> serializer = new IVersionedSerializer<ComponentManifest>()
            {

                public void serialize(ComponentManifest manifest, DataOutputPlus out, int version) throws IOException
                {
                    out.writeUnsignedVInt32(manifest.components.size());
                    for (Map.Entry<Component, Long> entry : manifest.components.entrySet())
                    {
                        out.writeUTF(entry.getKey().name);
                        out.writeUnsignedVInt(entry.getValue());
                    }
                }

                public ComponentManifest deserialize(DataInputPlus in, int version) throws IOException
                {
                    int size = in.readUnsignedVInt32();

                    LinkedHashMap<Component, Long> components = new LinkedHashMap<>(size);

                    for (int i = 0; i < size; i++)
                    {
                        Component component = Component.parse(in.readUTF(), format);
                        long length = in.readUnsignedVInt();
                        components.put(component, length);
                    }

                    return new ComponentManifest(components);
                }

                public long serializedSize(ComponentManifest manifest, int version)
                {
                    long size = TypeSizes.sizeofUnsignedVInt(manifest.components.size());
                    for (Map.Entry<Component, Long> entry : manifest.components.entrySet())
                    {
                        size += TypeSizes.sizeof(entry.getKey().name);
                        size += TypeSizes.sizeofUnsignedVInt(entry.getValue());
                    }
                    return size;
                }
            };

            b.put(format.name(), serializer);
        }
        serializers = b.build();
    }

    @Override
    public Iterator<Component> iterator()
    {
        return Iterators.unmodifiableIterator(components.keySet().iterator());
    }
}
