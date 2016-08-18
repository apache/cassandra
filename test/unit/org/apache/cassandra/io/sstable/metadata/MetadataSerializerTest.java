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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.assertEquals;

public class MetadataSerializerTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerialization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata();

        MetadataSerializer serializer = new MetadataSerializer();
        File statsFile = serialize(originalMetadata, serializer, BigFormat.latestVersion);

        Descriptor desc = new Descriptor(statsFile.getParentFile(), "", "", 0, SSTableFormat.Type.BIG);
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(originalMetadata.get(type), deserialized.get(type));
            }
        }
    }

    public File serialize(Map<MetadataType, MetadataComponent> metadata, MetadataSerializer serializer, Version version)
            throws IOException
    {
        // Serialize to tmp file
        File statsFile = File.createTempFile(Component.STATS.name, null);
        try (DataOutputStreamPlus out = new BufferedDataOutputStreamPlus(new FileOutputStream(statsFile)))
        {
            serializer.serialize(metadata, out, version);
        }
        return statsFile;
    }

    public Map<MetadataType, MetadataComponent> constructMetadata()
    {
        CommitLogPosition club = new CommitLogPosition(11L, 12);
        CommitLogPosition cllb = new CommitLogPosition(9L, 12);

        CFMetaData cfm = SchemaLoader.standardCFMD("ks1", "cf1");
        MetadataCollector collector = new MetadataCollector(cfm.comparator)
                                          .commitLogIntervals(new IntervalSet<>(cllb, club));

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        Map<MetadataType, MetadataComponent> originalMetadata = collector.finalizeMetadata(partitioner, bfFpChance, 0, SerializationHeader.make(cfm, Collections.emptyList()));
        return originalMetadata;
    }

    @Test
    public void testLaReadLb() throws IOException
    {
        testOldReadsNew("la", "lb");
    }

    @Test
    public void testMaReadMb() throws IOException
    {
        testOldReadsNew("ma", "mb");
    }

    @Test
    public void testMaReadMc() throws IOException
    {
        testOldReadsNew("ma", "mc");
    }

    @Test
    public void testMbReadMc() throws IOException
    {
        testOldReadsNew("mb", "mc");
    }

    public void testOldReadsNew(String oldV, String newV) throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata();

        MetadataSerializer serializer = new MetadataSerializer();
        // Write metadata in two minor formats.
        File statsFileLb = serialize(originalMetadata, serializer, BigFormat.instance.getVersion(newV));
        File statsFileLa = serialize(originalMetadata, serializer, BigFormat.instance.getVersion(oldV));
        // Reading both as earlier version should yield identical results.
        Descriptor desc = new Descriptor(oldV, statsFileLb.getParentFile(), "", "", 0, SSTableFormat.Type.current());
        try (RandomAccessReader inLb = RandomAccessReader.open(statsFileLb);
             RandomAccessReader inLa = RandomAccessReader.open(statsFileLa))
        {
            Map<MetadataType, MetadataComponent> deserializedLb = serializer.deserialize(desc, inLb, EnumSet.allOf(MetadataType.class));
            Map<MetadataType, MetadataComponent> deserializedLa = serializer.deserialize(desc, inLa, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(deserializedLa.get(type), deserializedLb.get(type));
                if (!originalMetadata.get(type).equals(deserializedLb.get(type)))
                {
                    // Currently only STATS can be different. Change if no longer the case
                    assertEquals(MetadataType.STATS, type);
                }
            }
        }
    }
}
