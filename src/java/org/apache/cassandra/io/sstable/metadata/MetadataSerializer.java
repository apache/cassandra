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
import java.util.zip.CRC32;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.updateChecksumInt;

/**
 * Metadata serializer for SSTables {@code version >= 'na'}.
 *
 * <pre>
 * File format := | number of components (4 bytes) | crc | toc | crc | component1 | c1 crc | component2 | c2 crc | ... |
 * toc         := | component type (4 bytes) | position of component |
 * </pre>
 *
 * IMetadataComponent.Type's ordinal() defines the order of serialization.
 */
public class MetadataSerializer implements IMetadataSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);

    private static final int CHECKSUM_LENGTH = 4; // CRC32

    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException
    {
        boolean checksum = version.hasMetadataChecksum();
        CRC32 crc = new CRC32();
        // sort components by type
        List<MetadataComponent> sortedComponents = Lists.newArrayList(components.values());
        Collections.sort(sortedComponents);

        // write number of component
        out.writeInt(components.size());
        updateChecksumInt(crc, components.size());
        maybeWriteChecksum(crc, out, version);

        // build and write toc
        int lastPosition = 4 + (8 * sortedComponents.size()) + (checksum ? 2 * CHECKSUM_LENGTH : 0);
        Map<MetadataType, Integer> sizes = new EnumMap<>(MetadataType.class);
        for (MetadataComponent component : sortedComponents)
        {
            MetadataType type = component.getType();
            // serialize type
            out.writeInt(type.ordinal());
            updateChecksumInt(crc, type.ordinal());
            // serialize position
            out.writeInt(lastPosition);
            updateChecksumInt(crc, lastPosition);
            int size = type.serializer.serializedSize(version, component);
            lastPosition += size + (checksum ? CHECKSUM_LENGTH : 0);
            sizes.put(type, size);
        }
        maybeWriteChecksum(crc, out, version);

        // serialize components
        for (MetadataComponent component : sortedComponents)
        {
            byte[] bytes;
            try (DataOutputBuffer dob = new DataOutputBuffer(sizes.get(component.getType())))
            {
                component.getType().serializer.serialize(version, component, dob);
                bytes = dob.getData();
            }
            out.write(bytes);

            crc.reset(); crc.update(bytes);
            maybeWriteChecksum(crc, out, version);
        }
    }

    private static void maybeWriteChecksum(CRC32 crc, DataOutputPlus out, Version version) throws IOException
    {
        if (version.hasMetadataChecksum())
            out.writeInt((int) crc.getValue());
    }

    public Map<MetadataType, MetadataComponent> deserialize( Descriptor descriptor, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components;
        logger.trace("Load metadata for {}", descriptor);
        File statsFile = new File(descriptor.filenameFor(Component.STATS));
        if (!statsFile.exists())
        {
            logger.trace("No sstable stats for {}", descriptor);
            components = new EnumMap<>(MetadataType.class);
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

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor,
                                                            FileDataInput in,
                                                            EnumSet<MetadataType> selectedTypes)
    throws IOException
    {
        boolean isChecksummed = descriptor.version.hasMetadataChecksum();
        CRC32 crc = new CRC32();

        /*
         * Read TOC
         */

        int length = (int) in.bytesRemaining();

        int count = in.readInt();
        updateChecksumInt(crc, count);
        maybeValidateChecksum(crc, in, descriptor);

        int[] ordinals = new int[count];
        int[]  offsets = new int[count];
        int[]  lengths = new int[count];

        for (int i = 0; i < count; i++)
        {
            ordinals[i] = in.readInt();
            updateChecksumInt(crc, ordinals[i]);

            offsets[i] = in.readInt();
            updateChecksumInt(crc, offsets[i]);
        }
        maybeValidateChecksum(crc, in, descriptor);

        lengths[count - 1] = length - offsets[count - 1];
        for (int i = 0; i < count - 1; i++)
            lengths[i] = offsets[i + 1] - offsets[i];

        /*
         * Read components
         */

        MetadataType[] allMetadataTypes = MetadataType.values();

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);

        for (int i = 0; i < count; i++)
        {
            MetadataType type = allMetadataTypes[ordinals[i]];

            if (!selectedTypes.contains(type))
            {
                in.skipBytes(lengths[i]);
                continue;
            }

            byte[] buffer = new byte[isChecksummed ? lengths[i] - CHECKSUM_LENGTH : lengths[i]];
            in.readFully(buffer);

            crc.reset(); crc.update(buffer);
            maybeValidateChecksum(crc, in, descriptor);
            try (DataInputBuffer dataInputBuffer = new DataInputBuffer(buffer))
            {
                components.put(type, type.serializer.deserialize(descriptor.version, dataInputBuffer));
            }
        }

        return components;
    }

    private static void maybeValidateChecksum(CRC32 crc, FileDataInput in, Descriptor descriptor) throws IOException
    {
        if (!descriptor.version.hasMetadataChecksum())
            return;

        int actualChecksum = (int) crc.getValue();
        int expectedChecksum = in.readInt();

        if (actualChecksum != expectedChecksum)
        {
            String filename = descriptor.filenameFor(Component.STATS);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + filename), filename);
        }
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

    public void mutateRepaired(Descriptor descriptor, long newRepairedAt, UUID newPendingRepair) throws IOException
    {
        logger.trace("Mutating {} to repairedAt time {} and pendingRepair {}",
                     descriptor.filenameFor(Component.STATS), newRepairedAt, newPendingRepair);
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);
        // mutate time & id
        currentComponents.put(MetadataType.STATS, stats.mutateRepairedAt(newRepairedAt).mutatePendingRepair(newPendingRepair));
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
        if (FBUtilities.isWindows)
            FileUtils.delete(descriptor.filenameFor(Component.STATS));
        FileUtils.renameWithConfirm(filePath, descriptor.filenameFor(Component.STATS));

    }
}
