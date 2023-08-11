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

package org.apache.cassandra.tools.nodetool;

import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ScrubTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tools.StandaloneScrubber;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.SchemaLoader.counterCFMD;
import static org.apache.cassandra.SchemaLoader.createKeyspace;
import static org.apache.cassandra.SchemaLoader.getCompressionParameters;
import static org.apache.cassandra.SchemaLoader.loadSchema;
import static org.apache.cassandra.SchemaLoader.standardCFMD;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST;
import static org.apache.cassandra.io.sstable.ScrubTest.CF_INDEX1;
import static org.apache.cassandra.io.sstable.ScrubTest.CF_INDEX1_BYTEORDERED;
import static org.apache.cassandra.io.sstable.ScrubTest.CF_INDEX2;
import static org.apache.cassandra.io.sstable.ScrubTest.CF_INDEX2_BYTEORDERED;
import static org.apache.cassandra.io.sstable.ScrubTest.CF_UUID;
import static org.apache.cassandra.io.sstable.ScrubTest.COMPRESSION_CHUNK_LENGTH;
import static org.apache.cassandra.io.sstable.ScrubTest.COUNTER_CF;
import static org.apache.cassandra.io.sstable.ScrubTest.assertOrderedAll;
import static org.apache.cassandra.io.sstable.ScrubTest.fillCF;
import static org.apache.cassandra.io.sstable.ScrubTest.fillCounterCF;
import static org.apache.cassandra.io.sstable.ScrubTest.overrideWithGarbage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ScrubToolTest
{
    private static final ImmutableList<String> CLEANERS = ImmutableList.<String>builder()
                                                                       .addAll(ToolRunner.DEFAULT_CLEANERS)
                                                                       .add("(?im)^.*header-fix is deprecated and no longer functional.*\\R")
                                                                       .build();
    private static final String CF = "scrub_tool_test";
    private static final AtomicInteger seq = new AtomicInteger();

    String ksName;
    Keyspace keyspace;
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        loadSchema();
    }

    @Before
    public void setup()
    {
        ksName = "scrub_test_" + seq.incrementAndGet();
        createKeyspace(ksName,
                       KeyspaceParams.simple(1),
                       standardCFMD(ksName, CF),
                       counterCFMD(ksName, COUNTER_CF).compression(getCompressionParameters(COMPRESSION_CHUNK_LENGTH)),
                       standardCFMD(ksName, CF_UUID, 0, UUIDType.instance),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1, true),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2, true),
                       SchemaLoader.keysIndexCFMD(ksName, CF_INDEX1_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance),
                       SchemaLoader.compositeIndexCFMD(ksName, CF_INDEX2_BYTEORDERED, true).partitioner(ByteOrderedPartitioner.instance));
        keyspace = Keyspace.open(ksName);

        CompactionManager.instance.disableAutoCompaction();
        TEST_UTIL_ALLOW_TOOL_REINIT_FOR_TEST.setBoolean(true);
    }

    @Test
    public void testScrubOnePartitionWithTool()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, ksName, CF);
        Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
        Assertions.assertThat(tool.getStdout()).contains("1 partitions in new sstable and 0 empty");
        tool.assertOnCleanExit();

        // check data is still there
        assertOrderedAll(cfs, 1);
    }

    @Test
    public void testSkipScrubCorruptedCounterPartitionWithTool() throws IOException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        int numPartitions = 1000;

        fillCounterCF(cfs, numPartitions);
        assertOrderedAll(cfs, numPartitions);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        ScrubTest.overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"), (byte) 0x7A);

        // with skipCorrupted == true, the corrupt rows will be skipped
        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-s", ksName, COUNTER_CF);
        Assertions.assertThat(tool.getStdout()).contains("0 empty");
        Assertions.assertThat(tool.getStdout()).contains("partitions that were skipped");
        tool.assertOnCleanExit();

        assertEquals(1, cfs.getLiveSSTables().size());
    }

    @Test
    public void testNoSkipScrubCorruptedCounterPartitionWithTool() throws IOException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(COUNTER_CF);
        int numPartitions = 1000;

        fillCounterCF(cfs, numPartitions);
        assertOrderedAll(cfs, numPartitions);
        assertEquals(1, cfs.getLiveSSTables().size());
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        //use 0x00 instead of the usual 0x7A because if by any chance it's able to iterate over the corrupt
        //section, then we get many out-of-order errors, which we don't want
        overrideWithGarbage(sstable, ByteBufferUtil.bytes("0"), ByteBufferUtil.bytes("1"), (byte) 0x0);

        // with skipCorrupted == false, the scrub is expected to fail
        try
        {
            ToolRunner.invokeClass(StandaloneScrubber.class, ksName, COUNTER_CF);
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (IOError err)
        {
            Throwables.assertAnyCause(err, CorruptSSTableException.class);
        }
    }

    @Test
    public void testNoCheckScrubMultiPartitionWithTool()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        // insert data and verify we get it back w/ range query
        fillCF(cfs, 10);
        assertOrderedAll(cfs, 10);

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-n", ksName, CF);
        Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
        Assertions.assertThat(tool.getStdout()).contains("10 partitions in new sstable and 0 empty");
        tool.assertOnCleanExit();

        // check data is still there
        assertOrderedAll(cfs, 10);
    }

    @Test
    public void testHeaderFixValidateWithTool()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-e", "validate", ksName, CF);
        Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
        Assertions.assertThat(tool.getStdout()).contains("1 partitions in new sstable and 0 empty");
        // TODO cleaner that ignores
        tool.assertOnCleanExit(CLEANERS);
        assertOrderedAll(cfs, 1);
    }

    @Test
    public void testHeaderFixWithTool()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-e", "fix", ksName, CF);
        Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
        Assertions.assertThat(tool.getStdout()).contains("1 partitions in new sstable and 0 empty");
        tool.assertOnCleanExit(CLEANERS);
        assertOrderedAll(cfs, 1);
    }

    @Test
    public void testHeaderFixNoChecksWithTool()
    {
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(CF);

        fillCF(cfs, 1);
        assertOrderedAll(cfs, 1);

        ToolRunner.ToolResult tool = ToolRunner.invokeClass(StandaloneScrubber.class, "-e", "off", ksName, CF);
        Assertions.assertThat(tool.getStdout()).contains("Pre-scrub sstables snapshotted into");
        Assertions.assertThat(tool.getStdout()).contains("1 partitions in new sstable and 0 empty");
        tool.assertOnCleanExit(CLEANERS);
        assertOrderedAll(cfs, 1);
    }
}