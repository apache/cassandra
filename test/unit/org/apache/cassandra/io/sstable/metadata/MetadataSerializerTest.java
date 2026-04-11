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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataSerializerTest
{
    private final static Logger logger = LoggerFactory.getLogger(MetadataSerializerTest.class);

    private static SSTableFormat<?, ?> format;

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        format = DatabaseDescriptor.getSelectedSSTableFormat();
    }

    @Test
    public void testSerialization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        MetadataSerializer serializer = new MetadataSerializer();
        Version latestVersion = DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion();
        File statsFile = serialize(originalMetadata, serializer, DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion());

        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), DatabaseDescriptor.getSelectedSSTableFormat());
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                if ((type != MetadataType.STATS) || latestVersion.hasImprovedMinMax())
                    assertEquals(originalMetadata.get(type), deserialized.get(type));

            }
        }
    }

    @Test
    public void testHistogramSterilization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        // Modify the histograms to overflow:
        StatsMetadata originalStats = (StatsMetadata) originalMetadata.get(MetadataType.STATS);
        originalStats.estimatedCellPerPartitionCount.add(Long.MAX_VALUE);
        originalStats.estimatedPartitionSize.add(Long.MAX_VALUE);
        assertTrue(originalStats.estimatedCellPerPartitionCount.isOverflowed());
        assertTrue(originalStats.estimatedPartitionSize.isOverflowed());

        // Serialize w/ overflowed histograms:
        MetadataSerializer serializer = new MetadataSerializer();
        File statsFile = serialize(originalMetadata, serializer, format.getLatestVersion());
        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), format);

        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            // Deserialie and verify that the two histograms have had their overflow buckets cleared:
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));
            StatsMetadata deserializedStats = (StatsMetadata) deserialized.get(MetadataType.STATS);
            assertFalse(deserializedStats.estimatedCellPerPartitionCount.isOverflowed());
            assertFalse(deserializedStats.estimatedPartitionSize.isOverflowed());
        }
    }

    public File serialize(Map<MetadataType, MetadataComponent> metadata, MetadataSerializer serializer, Version version)
    throws IOException
    {
        // Serialize to tmp file
        File statsFile = FileUtils.createTempFile(Components.STATS.name, null);
        try (DataOutputStreamPlus out = new FileOutputStreamPlus(statsFile))
        {
            serializer.serialize(metadata, out, version);
        }
        return statsFile;
    }

    public Map<MetadataType, MetadataComponent> constructMetadata(boolean withNulls)
    {
        CommitLogPosition club = new CommitLogPosition(11L, 12);
        CommitLogPosition cllb = new CommitLogPosition(9L, 12);

        TableMetadata cfm = TableMetadata.builder("ks1", "cf1")
                                         .addPartitionKeyColumn("k", AsciiType.instance)
                                         .addClusteringColumn("c1", UTF8Type.instance)
                                         .addClusteringColumn("c2", Int32Type.instance)
                                         .addRegularColumn("v", Int32Type.instance)
                                         .build();
        MetadataCollector collector = new MetadataCollector(cfm.comparator)
                                      .commitLogIntervals(new IntervalSet<>(cllb, club));
        if (DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion().hasTokenSpaceCoverage())
            collector.tokenSpaceCoverage(0.7);

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        collector.updateClusteringValues(Clustering.make(UTF8Type.instance.decompose("abc"), Int32Type.instance.decompose(123)));
        collector.updateClusteringValues(Clustering.make(UTF8Type.instance.decompose("cba"), withNulls ? null : Int32Type.instance.decompose(234)));
        ByteBuffer first = AsciiType.instance.decompose("a");
        ByteBuffer last = AsciiType.instance.decompose("b");
        return collector.finalizeMetadata(partitioner, bfFpChance, 0, null, false, SerializationHeader.make(cfm, Collections.emptyList()), first, last);
    }

    private void testVersions(List<String> versions) throws Throwable
    {
        logger.info("Testing minor versions {} compatibility for sstable format {}", versions, format.getClass().getName());
        Throwable t = null;
        for (int oldIdx = 0; oldIdx < versions.size(); oldIdx++)
        {
            for (int newIdx = oldIdx; newIdx < versions.size(); newIdx++)
            {
                try
                {
                    testOldReadsNew(versions.get(oldIdx), versions.get(newIdx));
                }
                catch (Exception | AssertionError e)
                {
                    t = Throwables.merge(t, new AssertionError("Failed to test " + versions.get(oldIdx) + " -> " + versions.get(newIdx), e));
                }
            }
        }
        if (t != null)
        {
            throw t;
        }
    }

    @Test
    public void testMinorVersionsCompatibilty() throws Throwable
    {
        Map<Character, List<String>> supportedVersions = new LinkedHashMap<>();

        for (char major = 'a'; major <= 'z'; major++){
            for (char minor = 'a'; minor <= 'z'; minor++){
                Version version = format.getVersion(String.format("%s%s", major, minor));
                if (version.isCompatible())
                    supportedVersions.computeIfAbsent(major, ignored -> new ArrayList<>()).add(version.version);
            }
        }

        for (List<String> minorVersions : supportedVersions.values())
            testVersions(minorVersions);
    }

    public void testOldReadsNew(String oldV, String newV) throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(true);

        MetadataSerializer serializer = new MetadataSerializer();
        // Write metadata in two minor formats.
        File statsFileLb = serialize(originalMetadata, serializer, format.getVersion(newV));
        File statsFileLa = serialize(originalMetadata, serializer, format.getVersion(oldV));
        // Reading both as earlier version should yield identical results.
        Descriptor desc = new Descriptor(format.getVersion(oldV), statsFileLb.parent(), "", "", new SequenceBasedSSTableId(0));
        try (RandomAccessReader inLb = RandomAccessReader.open(statsFileLb);
             RandomAccessReader inLa = RandomAccessReader.open(statsFileLa))
        {
            Map<MetadataType, MetadataComponent> deserializedLb = serializer.deserialize(desc, inLb, EnumSet.allOf(MetadataType.class));
            Map<MetadataType, MetadataComponent> deserializedLa = serializer.deserialize(desc, inLa, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(deserializedLa.get(type), deserializedLb.get(type));

                if (MetadataType.STATS != type)
                    assertEquals(originalMetadata.get(type), deserializedLb.get(type));
            }
        }
    }

}
