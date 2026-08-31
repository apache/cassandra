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

package org.apache.cassandra.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.PartitionDescriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableCursorReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandaloneSplitterWithCQLTesterTest extends CQLTester
{
    private static String sstableFileName;
    private static File sstablesDir;
    private static List<File> origSstables;

    @Before
    public void before() throws Throwable
    {
        setupTestSstables();
        // Stop server as this is exercising an offline tool
        tearDownClass();
        SSTableReader.resetTidying();
    }

    @After
    public void unsafeRemoveSSTables() throws Throwable
    {
        // Before resetting the CMS in CQLTester::afterClass, manually remove the original SSTables from the
        // CFS. If we don't do this, restoring the schema to a pre-test state causes the CFS to be dropped
        // which attempts to remove the SSTables in the tracker. Because we've unsafely modified these with
        // a tool that should only be used offline, this causes an error in test tear down. In a real node,
        // running the tool while offline, or even just restarting the node after the tool has been unsafely
        // run like this, would avoid/fix this issue.
        Tracker tracker = getCurrentColumnFamilyStore(KEYSPACE).getTracker();
        Set<SSTableReader> toRemove = new HashSet<>();
        tracker.getView().allKnownSSTables().forEach(toRemove::add);
        tracker.removeUnsafe(toRemove);
    }

    @Test
    public void testMinFileSizeCheck() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, sstableFileName);
        Assertions.assertThat(tool.getStdout()).contains("is less than the split size");
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testSplittingSSTable() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", sstableFileName);
        List<File> splitFiles = Arrays.asList(sstablesDir.tryList());
        splitFiles.stream().forEach(f -> {
            if (f.name().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.name() + " is way bigger than 1MiB: [" + f.length() + "] bytes",
                           f.length() <= 1024 * 1024 * 1.2); //give a 20% margin on size check
        });
        assertTrue(origSstables.size() < splitFiles.size());
        Assertions.assertThat(tool.getStdout()).contains("snapshotted into snapshot");
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testSnapshotFailureDoesNotSplitSSTable() throws Throwable
    {
        Set<String> originalFiles = Arrays.stream(sstablesDir.tryList())
                                          .map(File::name)
                                          .collect(Collectors.toSet());
        StandaloneSplitter.setFailSnapshotForTesting(true);
        try
        {
            ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", sstableFileName);
            assertEquals(1, tool.getExitCode());
            Assertions.assertThat(tool.getCleanedStderr()).contains("Error Snapshotting");
            Assertions.assertThat(tool.getStdout()).doesNotContain("snapshotted into");
            assertEquals(originalFiles,
                         Arrays.stream(sstablesDir.tryList())
                               .map(File::name)
                               .collect(Collectors.toSet()));
        }
        finally
        {
            StandaloneSplitter.setFailSnapshotForTesting(false);
        }
    }

    @Test
    public void testZeroCopySplittingSSTable() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Descriptor source = SSTable.tryDescriptorFromFile(new File(sstableFileName));
        assertEquals("qa", source.version.version);
        assertTrue(source.version.hasSplitPrefixMarker());
        assertEquals("an ordinary writer must start at position zero",
                     0, StatsComponent.load(source).statsMetadata().firstPartitionPosition);

        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                 "-s", "1", "--zero-copy", sstableFileName);
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        Assertions.assertThat(tool.getStdout())
                  .contains("Zero-copy split committed", "bytes cloned=", "bytes written=", "reflink used=");

        long partitions = 0;
        boolean sawDeadPrefix = false;
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE);
        for (File file : sstablesDir.tryList())
        {
            if (!file.name().endsWith("Data.db") || origSstables.contains(file))
                continue;

            Descriptor descriptor = SSTable.tryDescriptorFromFile(file);
            assertTrue(BigFormat.is(descriptor.getFormat()));
            assertEquals("qa", descriptor.version.version);
            assertTrue(descriptor.version.hasSplitPrefixMarker());
            assertTrue(file.name() + " exceeds the requested 1 MiB maximum: " + file.length(),
                       file.length() <= 1024 * 1024);
            Set<Component> components = TOCComponent.loadTOC(descriptor);
            SSTableReader reader = SSTableReader.openNoValidation(descriptor, components, cfs);
            try
            {
                SSTableReader.PartitionPositionBounds fullRange = reader.getPositionsForFullRange();
                assertEquals("Statistics.db must describe this child's exact start",
                             fullRange.lowerPosition,
                             reader.getSSTableMetadata().firstPartitionPosition);
                if (fullRange.lowerPosition > 0)
                {
                    sawDeadPrefix = true;
                    PartitionDescriptor partition = new PartitionDescriptor(reader.getPartitioner().createReusableKey(0));
                    try (SSTableCursorReader cursor = new SSTableCursorReader(reader))
                    {
                        cursor.readPartitionHeader(partition);
                        assertEquals(reader.getFirst(), partition.key());
                    }
                }

                try (ISSTableScanner scanner = reader.getScanner())
                {
                    while (scanner.hasNext())
                    {
                        try (UnfilteredRowIterator partition = scanner.next())
                        {
                            partitions++;
                        }
                    }
                }
            }
            finally
            {
                reader.selfRef().release();
            }
        }

        assertTrue("expected at least one split child to retain a compression-chunk prefix", sawDeadPrefix);
        assertEquals(100000, partitions);
    }

    @Test
    public void testZeroCopySplittingSSTableWithoutTOC() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Descriptor source = SSTable.tryDescriptorFromFile(new File(sstableFileName));
        File toc = source.fileFor(Components.TOC);
        assertTrue(toc.tryDelete());

        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                 "-s", "1", "--zero-copy", "--no-snapshot", sstableFileName);

        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
        assertTrue("expected split children after reconstructing the missing TOC",
                   Arrays.stream(sstablesDir.tryList())
                         .filter(file -> file.name().endsWith("Data.db"))
                         .anyMatch(file -> !origSstables.contains(file)));
    }

    @Test
    public void testZeroCopyRefusesSSTableAttachedIndex() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        createIndex("CREATE CUSTOM INDEX splitter_sai ON %s (val) USING 'StorageAttachedIndex'");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore(KEYSPACE);
        assertTrue(cfs.indexManager.hasSSTableAttachedIndexes());

        Descriptor source = SSTable.tryDescriptorFromFile(new File(sstableFileName));
        assertTrue(TOCComponent.loadTOC(source).stream().anyMatch(c -> c.name().startsWith("SAI+")));
        Map<String, Long> sourceFiles = Arrays.stream(sstablesDir.tryList())
                                              .collect(Collectors.toMap(File::name, File::length));

        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                 "-s", "1", "--zero-copy", "--no-snapshot", sstableFileName);

        assertEquals(1, tool.getExitCode());
        Assertions.assertThat(tool.getCleanedStderr())
                  .contains("Cannot zero-copy split SSTables", "SSTable-attached indexes");
        Assertions.assertThat(Arrays.stream(sstablesDir.tryList())
                                    .collect(Collectors.toMap(File::name, File::length)))
                  .isEqualTo(sourceFiles);
    }

    @Test
    public void testZeroCopyRefusesCustomComponent() throws Throwable
    {
        Assume.assumeTrue(BigFormat.isSelected());
        Descriptor source = SSTable.tryDescriptorFromFile(new File(sstableFileName));
        Component custom = Component.parse("CustomZeroCopy.db", source.getFormat());
        assertTrue(source.fileFor(custom).createFileIfNotExists());
        Set<Component> components = new HashSet<>(TOCComponent.loadTOC(source));
        components.add(custom);
        TOCComponent.updateTOC(source, components);

        Map<String, Long> sourceFiles = Arrays.stream(sstablesDir.tryList())
                                              .collect(Collectors.toMap(File::name, File::length));
        ToolResult tool = ToolRunner.invokeClass(StandaloneSplitter.class,
                                                 "-s", "1", "--zero-copy", "--no-snapshot", sstableFileName);

        assertEquals(1, tool.getExitCode());
        Assertions.assertThat(tool.getCleanedStderr())
                  .contains("CustomZeroCopy.db", "which this class cannot produce for a child");
        Assertions.assertThat(Arrays.stream(sstablesDir.tryList())
                                    .collect(Collectors.toMap(File::name, File::length)))
                  .isEqualTo(sourceFiles);
    }

    @Test
    public void testSplittingMultipleSSTables() throws Throwable
    {
        ArrayList<String> args = new ArrayList<>(Arrays.asList("-s", "1"));

        args.addAll(Arrays.asList(sstablesDir.tryList())
                          .stream()
                          .map(f -> f.absolutePath())
                          .collect(Collectors.toList()));

        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, args.toArray(new String[args.size()]));
        List<File> splitFiles = Arrays.asList(sstablesDir.tryList());
        splitFiles.stream().forEach(f -> {
            if (f.name().endsWith("Data.db") && !origSstables.contains(f))
                assertTrue(f.name() + " is way bigger than 1MiB: [" + f.length() + "] bytes",
                           f.length() <= 1024 * 1024 * 1.2); //give a 20% margin on size check
        });
        assertTrue(origSstables.size() < splitFiles.size());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    @Test
    public void testNoSnapshotOption() throws Throwable
    {
        ToolResult tool  = ToolRunner.invokeClass(StandaloneSplitter.class, "-s", "1", "--no-snapshot", sstableFileName);
        assertTrue(origSstables.size() < Arrays.asList(sstablesDir.tryList()).size());
        assertTrue(tool.getStdout(), tool.getStdout().isEmpty());
        assertTrue(tool.getCleanedStderr(), tool.getCleanedStderr().isEmpty());
        assertEquals(0, tool.getExitCode());
    }

    private void setupTestSstables() throws Throwable
    {
        SSTableReader.resetTidying();
        createTable("CREATE TABLE %s (id text primary key, val text)");
        for (int i = 0; i < 100000; i++)
            executeFormattedQuery(formatQuery("INSERT INTO %s (id, val) VALUES (?, ?)"), "mockData" + i, "mockData" + i);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        org.apache.cassandra.Util.flush(cfs);

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        SSTableReader sstable = sstables.iterator().next();
        sstableFileName = sstable.getFilename();
        assertTrue("Generated sstable must be at least 1MiB", (new File(sstableFileName)).length() > 1024*1024);
        sstablesDir = new File(sstableFileName).parent();
        origSstables = Arrays.asList(sstablesDir.tryList());
        TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.setBoolean(true);
    }
}
