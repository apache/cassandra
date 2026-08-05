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
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertArrayEquals;
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
        return constructMetadata(withNulls, false);
    }

    /**
     * @param hasUnindexedRegions value for {@link StatsMetadata#hasUnindexedRegions}. {@link MetadataCollector} never
     *                            sets it -- only the zero-copy paths do, and they do it by rebuilding the component,
     *                            which is what is replicated here (see {@code ZeroCopySSTableSplitter.writeStatistics}
     *                            for why the comparator's subtypes are the right clustering types to pass).
     */
    public Map<MetadataType, MetadataComponent> constructMetadata(boolean withNulls, boolean hasUnindexedRegions)
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
        Map<MetadataType, MetadataComponent> metadata = collector.finalizeMetadata(partitioner, bfFpChance, 0, null, false, SerializationHeader.make(cfm, Collections.emptyList()), first, last);

        if (hasUnindexedRegions)
        {
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            metadata.put(MetadataType.STATS, new StatsMetadata(stats.estimatedPartitionSize,
                                                               stats.estimatedCellPerPartitionCount,
                                                               stats.commitLogIntervals,
                                                               stats.minTimestamp,
                                                               stats.maxTimestamp,
                                                               stats.minLocalDeletionTime,
                                                               stats.maxLocalDeletionTime,
                                                               stats.minTTL,
                                                               stats.maxTTL,
                                                               stats.compressionRatio,
                                                               stats.estimatedTombstoneDropTime,
                                                               stats.sstableLevel,
                                                               cfm.comparator.subtypes(),
                                                               stats.coveredClustering,
                                                               stats.hasLegacyCounterShards,
                                                               stats.repairedAt,
                                                               stats.totalColumnsSet,
                                                               stats.totalRows,
                                                               stats.tokenSpaceCoverage,
                                                               stats.originatingHostId,
                                                               stats.pendingRepair,
                                                               stats.isTransient,
                                                               stats.hasPartitionLevelDeletions,
                                                               stats.firstKey,
                                                               stats.lastKey,
                                                               true));
        }

        return metadata;
    }

    /**
     * Every two letter version of the selected format, whether it ever existed or not -- the feature predicates are
     * plain string comparisons, so asking for a version is enough to know what it can hold.
     */
    private static List<Version> allVersions()
    {
        List<Version> versions = new ArrayList<>();
        for (char major = 'a'; major <= 'z'; major++)
            for (char minor = 'a'; minor <= 'z'; minor++)
                versions.add(format.getVersion(String.format("%s%s", major, minor)));
        return versions;
    }

    private static List<Version> compatibleVersions()
    {
        return allVersions().stream().filter(Version::isCompatible).collect(Collectors.toList());
    }

    /**
     * The two versions either side of the {@link Version#hasUnindexedRegionsMarker()} gate -- {@code pa}/{@code pb} for
     * BIG, {@code ea}/{@code eb} for BTI -- found rather than named so that the next version bump leaves this test
     * alone. Deliberately not restricted to {@link Version#isCompatible()} versions: which ones are compatible depends
     * on the storage compatibility mode, whereas what the gate lets a version hold does not.
     *
     * @param withMarker whether to return the first version that records the marker, or the last one that cannot
     */
    private static Version versionAtUnindexedRegionsMarkerGate(boolean withMarker)
    {
        Version previous = null;
        for (Version version : allVersions())
        {
            if (version.hasUnindexedRegionsMarker())
                return withMarker ? version : previous;
            previous = version;
        }
        throw new AssertionError("No version of " + format.name() + " records hasUnindexedRegions");
    }

    private byte[] serializeToBytes(Map<MetadataType, MetadataComponent> metadata, MetadataSerializer serializer, Version version)
    throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(metadata, out, version);
            return out.toByteArray();
        }
    }

    private StatsMetadata deserializeStats(File statsFile, MetadataSerializer serializer, Version version) throws IOException
    {
        Descriptor desc = new Descriptor(version, statsFile.parent(), "", "", new SequenceBasedSSTableId(0));
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            return (StatsMetadata) serializer.deserialize(desc, in, EnumSet.of(MetadataType.STATS)).get(MetadataType.STATS);
        }
    }

    /**
     * {@link StatsMetadata#hasUnindexedRegions} survives a round trip in a version that records it, and reads back as
     * false from one that cannot -- the reader of an older sstable has no marker to find, and none was ever written
     * there ({@code ZeroCopySSTableSlice} refuses).
     */
    @Test
    public void testUnindexedRegionsRoundTrip() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false, true);
        StatsMetadata originalStats = (StatsMetadata) originalMetadata.get(MetadataType.STATS);
        assertTrue(originalStats.hasUnindexedRegions);

        MetadataSerializer serializer = new MetadataSerializer();

        Version newVersion = versionAtUnindexedRegionsMarkerGate(true);
        StatsMetadata newDeserialized = deserializeStats(serialize(originalMetadata, serializer, newVersion), serializer, newVersion);
        assertTrue("Marker lost by " + newVersion, newDeserialized.hasUnindexedRegions);
        // The whole component, not just the flag: the marker must not have displaced anything else. Sound only
        // because StatsMetadata.equals covers hasUnindexedRegions, which is what makes the assertion above fallible.
        assertEquals(originalStats, newDeserialized);

        Version oldVersion = versionAtUnindexedRegionsMarkerGate(false);
        StatsMetadata oldDeserialized = deserializeStats(serialize(originalMetadata, serializer, oldVersion), serializer, oldVersion);
        assertFalse("Marker read back from " + oldVersion + ", which cannot hold it", oldDeserialized.hasUnindexedRegions);
    }

    /**
     * A version that cannot express the marker must serialize a marked {@link StatsMetadata} to exactly the bytes an
     * unmarked one produces: an sstable written in an old version has to be byte for byte what it was before the field
     * existed, not merely readable by an old reader. Conversely, a version that does record it must produce different
     * bytes, or the assertion above would hold vacuously.
     */
    @Test
    public void testUnindexedRegionsNotWrittenToOldVersions() throws IOException
    {
        Map<MetadataType, MetadataComponent> marked = constructMetadata(false, true);
        Map<MetadataType, MetadataComponent> unmarked = constructMetadata(false, false);

        MetadataSerializer serializer = new MetadataSerializer();

        // Checked outside the loop as well: which versions are compatible depends on the storage compatibility mode,
        // so the ones that record the marker can be missing from it entirely, leaving nothing to contrast against.
        Version newVersion = versionAtUnindexedRegionsMarkerGate(true);
        assertFalse("Version " + newVersion + " records the marker, so the bytes must differ",
                    Arrays.equals(serializeToBytes(unmarked, serializer, newVersion),
                                  serializeToBytes(marked, serializer, newVersion)));

        for (Version version : compatibleVersions())
        {
            byte[] markedBytes = serializeToBytes(marked, serializer, version);
            byte[] unmarkedBytes = serializeToBytes(unmarked, serializer, version);

            if (version.hasUnindexedRegionsMarker())
                assertFalse("Version " + version + " records the marker, so the bytes must differ",
                            Arrays.equals(unmarkedBytes, markedBytes));
            else
                assertArrayEquals("Version " + version + " cannot record the marker, so it must write the same bytes either way",
                                  unmarkedBytes, markedBytes);
        }
    }

    /**
     * The marker byte a new version writes must be invisible to a reader of an older minor version: it is appended
     * past everything that version knows about, and MetadataSerializer hands each component only its own bytes.
     */
    @Test
    public void testOldReadsNewUnindexedRegionsMarker() throws Throwable
    {
        Map<MetadataType, MetadataComponent> markedMetadata = constructMetadata(true, true);
        Version oldVersion = versionAtUnindexedRegionsMarkerGate(false);

        Throwable t = null;
        for (Version newVersion : allVersions())
        {
            // Same major only, like testMinorVersionsCompatibilty: reading across majors is not a claim this format makes.
            if (!newVersion.hasUnindexedRegionsMarker() || newVersion.version.charAt(0) != oldVersion.version.charAt(0))
                continue;

            try
            {
                testOldReadsNew(oldVersion.version, newVersion.version, markedMetadata);
            }
            catch (Exception | AssertionError e)
            {
                t = Throwables.merge(t, new AssertionError("Failed to test " + oldVersion + " -> " + newVersion, e));
            }
        }
        if (t != null)
        {
            throw t;
        }
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
        testOldReadsNew(oldV, newV, constructMetadata(true));
    }

    public void testOldReadsNew(String oldV, String newV, Map<MetadataType, MetadataComponent> originalMetadata) throws IOException
    {
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
