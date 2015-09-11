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
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.assertEquals;

public class MetadataSerializerTest
{
    @Test
    public void testSerialization() throws IOException
    {

        CFMetaData cfm = SchemaLoader.standardCFMD("ks1", "cf1");

        ReplayPosition rp = new ReplayPosition(11L, 12);

        MetadataCollector collector = new MetadataCollector(cfm.comparator).replayPosition(rp);

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        Map<MetadataType, MetadataComponent> originalMetadata = collector.finalizeMetadata(partitioner, bfFpChance, 0, SerializationHeader.make(cfm, Collections.EMPTY_LIST));

        MetadataSerializer serializer = new MetadataSerializer();
        // Serialize to tmp file
        File statsFile = File.createTempFile(Component.STATS.name, null);
        try (DataOutputStreamPlus out = new BufferedDataOutputStreamPlus(new FileOutputStream(statsFile)))
        {
            serializer.serialize(originalMetadata, out, BigFormat.latestVersion);
        }

        Descriptor desc = new Descriptor( statsFile.getParentFile(), "", "", 0);
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
