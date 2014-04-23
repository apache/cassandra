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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.EstimatedHistogram;

import static org.junit.Assert.assertEquals;

public class MetadataSerializerTest
{
    @Test
    public void testSerialization() throws IOException
    {
        EstimatedHistogram rowSizes = new EstimatedHistogram(new long[] { 1L, 2L },
                                                             new long[] { 3L, 4L, 5L });
        EstimatedHistogram columnCounts = new EstimatedHistogram(new long[] { 6L, 7L },
                                                                 new long[] { 8L, 9L, 10L });
        ReplayPosition rp = new ReplayPosition(11L, 12);
        long minTimestamp = 2162517136L;
        long maxTimestamp = 4162517136L;

        MetadataCollector collector = new MetadataCollector(new SimpleDenseCellNameType(BytesType.instance))
                                                      .estimatedRowSize(rowSizes)
                                                      .estimatedColumnCount(columnCounts)
                                                      .replayPosition(rp);
        collector.updateMinTimestamp(minTimestamp);
        collector.updateMaxTimestamp(maxTimestamp);

        Set<Integer> ancestors = Sets.newHashSet(1, 2, 3, 4);
        for (int i : ancestors)
            collector.addAncestor(i);

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        Map<MetadataType, MetadataComponent> originalMetadata = collector.finalizeMetadata(partitioner, bfFpChance, 0);

        MetadataSerializer serializer = new MetadataSerializer();
        // Serialize to tmp file
        File statsFile = File.createTempFile(Component.STATS.name, null);
        try (DataOutputStreamAndChannel out = new DataOutputStreamAndChannel(new FileOutputStream(statsFile)))
        {
            serializer.serialize(originalMetadata, out);
        }

        Descriptor desc = new Descriptor(Descriptor.Version.CURRENT, statsFile.getParentFile(), "", "", 0, Descriptor.Type.FINAL);
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(originalMetadata.get(type), deserialized.get(type));
            }
        }
    }
}
