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
package org.apache.cassandra.io.sstable.metadata;

import java.io.*;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Metadata serializer for SSTables {@code version >= 'k'}.
 *
 * <pre>
 * File format := | number of components (4 bytes) | toc | component1 | component2 | ... |
 * toc         := | component type (4 bytes) | position of component |
 * </pre>
 *
 * IMetadataComponent.Type's ordinal() defines the order of serialization.
 */
public class MetadataSerializer implements IMetadataSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);

    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException
    {
        // sort components by type
        List<MetadataComponent> sortedComponents = Lists.newArrayList(components.values());
        Collections.sort(sortedComponents);

        // write number of component
        out.writeInt(components.size());
        // build and write toc
        int lastPosition = 4 + (8 * sortedComponents.size());
        for (MetadataComponent component : sortedComponents)
        {
            MetadataType type = component.getType();
            // serialize type
            out.writeInt(type.ordinal());
            // serialize position
            out.writeInt(lastPosition);
            lastPosition += type.serializer.serializedSize(version, component);
        }
        // serialize components
        for (MetadataComponent component : sortedComponents)
        {
            component.getType().serializer.serialize(version, component, out);
        }
    }

    public Map<MetadataType, MetadataComponent> deserialize( Descriptor descriptor, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components;
        logger.trace("Load metadata for {}", descriptor);
        File statsFile = new File(descriptor.filenameFor(Component.STATS));
        if (!statsFile.exists())
        {
            logger.trace("No sstable stats for {}", descriptor);
            components = Maps.newHashMap();
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        }
        else
        {
            try (RandomAccessReader r = RandomAccessReader.open(statsFile))
            {
                components = deserialize(descriptor, r, types);
            }
        }
        return components;
    }

    public MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException
    {
        return deserialize(descriptor, EnumSet.of(type)).get(type);
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, FileDataInput in, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components = Maps.newHashMap();
        // read number of components
        int numComponents = in.readInt();
        // read toc
        Map<MetadataType, Integer> toc = new HashMap<>(numComponents);
        MetadataType[] values = MetadataType.values();
        for (int i = 0; i < numComponents; i++)
        {
            toc.put(values[in.readInt()], in.readInt());
        }
        for (MetadataType type : types)
        {
            Integer offset = toc.get(type);
            if (offset != null)
            {
                in.seek(offset);
                MetadataComponent component = type.serializer.deserialize(descriptor.version, in);
                components.put(type, component);
            }
        }
        return components;
    }

    public void mutateLevel(Descriptor descriptor, int newLevel) throws IOException
    {
        logger.trace("Mutating {} to level {}", descriptor.filenameFor(Component.STATS), newLevel);
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);
        // mutate level
        currentComponents.put(MetadataType.STATS, stats.mutateLevel(newLevel));
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    public void mutateRepairedAt(Descriptor descriptor, long newRepairedAt) throws IOException
    {
        logger.trace("Mutating {} to repairedAt time {}", descriptor.filenameFor(Component.STATS), newRepairedAt);
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);
        // mutate level
        currentComponents.put(MetadataType.STATS, stats.mutateRepairedAt(newRepairedAt));
        rewriteSSTableMetadata(descriptor, currentComponents);
    }

    private void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents) throws IOException
    {
        String filePath = descriptor.tmpFilenameFor(Component.STATS);
        try (DataOutputStreamPlus out = new BufferedDataOutputStreamPlus(new FileOutputStream(filePath)))
        {
            serialize(currentComponents, out, descriptor.version);
            out.flush();
        }
        // we cant move a file on top of another file in windows:
        if (FBUtilities.isWindows())
            FileUtils.delete(descriptor.filenameFor(Component.STATS));
        FileUtils.renameWithConfirm(filePath, descriptor.filenameFor(Component.STATS));

    }
}
