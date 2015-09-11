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
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus.DataInputStreamPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * Serializer for SSTable from legacy versions
 */
@Deprecated
public class LegacyMetadataSerializer extends MetadataSerializer
{
    /**
     * Legacy serialization is only used for SSTable level reset.
     */
    @Override
    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException
    {
        ValidationMetadata validation = (ValidationMetadata) components.get(MetadataType.VALIDATION);
        StatsMetadata stats = (StatsMetadata) components.get(MetadataType.STATS);
        CompactionMetadata compaction = (CompactionMetadata) components.get(MetadataType.COMPACTION);

        assert validation != null && stats != null && compaction != null && validation.partitioner != null;

        EstimatedHistogram.serializer.serialize(stats.estimatedPartitionSize, out);
        EstimatedHistogram.serializer.serialize(stats.estimatedColumnCount, out);
        ReplayPosition.serializer.serialize(stats.replayPosition, out);
        out.writeLong(stats.minTimestamp);
        out.writeLong(stats.maxTimestamp);
        out.writeInt(stats.maxLocalDeletionTime);
        out.writeDouble(validation.bloomFilterFPChance);
        out.writeDouble(stats.compressionRatio);
        out.writeUTF(validation.partitioner);
        out.writeInt(0); // compaction ancestors
        StreamingHistogram.serializer.serialize(stats.estimatedTombstoneDropTime, out);
        out.writeInt(stats.sstableLevel);
        out.writeInt(stats.minClusteringValues.size());
        for (ByteBuffer value : stats.minClusteringValues)
            ByteBufferUtil.writeWithShortLength(value, out);
        out.writeInt(stats.maxClusteringValues.size());
        for (ByteBuffer value : stats.maxClusteringValues)
            ByteBufferUtil.writeWithShortLength(value, out);
    }

    /**
     * Legacy serializer deserialize all components no matter what types are specified.
     */
    @Override
    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components = Maps.newHashMap();

        File statsFile = new File(descriptor.filenameFor(Component.STATS));
        if (!statsFile.exists() && types.contains(MetadataType.STATS))
        {
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        }
        else
        {
            try (DataInputStreamPlus in = new DataInputStreamPlus(new BufferedInputStream(new FileInputStream(statsFile))))
            {
                EstimatedHistogram partitionSizes = EstimatedHistogram.serializer.deserialize(in);
                EstimatedHistogram columnCounts = EstimatedHistogram.serializer.deserialize(in);
                ReplayPosition replayPosition = ReplayPosition.serializer.deserialize(in);
                long minTimestamp = in.readLong();
                long maxTimestamp = in.readLong();
                int maxLocalDeletionTime = in.readInt();
                double bloomFilterFPChance = in.readDouble();
                double compressionRatio = in.readDouble();
                String partitioner = in.readUTF();
                int nbAncestors = in.readInt(); //skip compaction ancestors
                in.skipBytes(nbAncestors * TypeSizes.sizeof(nbAncestors));
                StreamingHistogram tombstoneHistogram = StreamingHistogram.serializer.deserialize(in);
                int sstableLevel = 0;
                if (in.available() > 0)
                    sstableLevel = in.readInt();

                int colCount = in.readInt();
                List<ByteBuffer> minColumnNames = new ArrayList<>(colCount);
                for (int i = 0; i < colCount; i++)
                    minColumnNames.add(ByteBufferUtil.readWithShortLength(in));

                colCount = in.readInt();
                List<ByteBuffer> maxColumnNames = new ArrayList<>(colCount);
                for (int i = 0; i < colCount; i++)
                    maxColumnNames.add(ByteBufferUtil.readWithShortLength(in));

                if (types.contains(MetadataType.VALIDATION))
                    components.put(MetadataType.VALIDATION,
                                   new ValidationMetadata(partitioner, bloomFilterFPChance));
                if (types.contains(MetadataType.STATS))
                    components.put(MetadataType.STATS,
                                   new StatsMetadata(partitionSizes,
                                                     columnCounts,
                                                     replayPosition,
                                                     minTimestamp,
                                                     maxTimestamp,
                                                     Integer.MAX_VALUE,
                                                     maxLocalDeletionTime,
                                                     0,
                                                     Integer.MAX_VALUE,
                                                     compressionRatio,
                                                     tombstoneHistogram,
                                                     sstableLevel,
                                                     minColumnNames,
                                                     maxColumnNames,
                                                     true,
                                                     ActiveRepairService.UNREPAIRED_SSTABLE,
                                                     -1,
                                                     -1));
                if (types.contains(MetadataType.COMPACTION))
                    components.put(MetadataType.COMPACTION,
                                   new CompactionMetadata(null));
            }
        }
        return components;
    }
}
