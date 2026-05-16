/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.io.sstable.format;

import java.nio.file.Files;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.Config.FlushCompression;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.DirectCompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DirectIoTestUtils;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertTrue;

public class DataComponentDirectWriteSelectionTest
{
    private static final OperationType[] SUPPORTED_OPS = {
        OperationType.WRITE,
        OperationType.COMPACTION,
        OperationType.MAJOR_COMPACTION,
        OperationType.GARBAGE_COLLECT,
        OperationType.ANTICOMPACTION,
        OperationType.TOMBSTONE_COMPACTION,
        OperationType.CLEANUP,
        OperationType.UPGRADE_SSTABLES,
        OperationType.STREAM,
    };

    private static final String KS = "ks_dio_selection";
    private static final String CF = "cf_dio_selection";

    private File tmpDir;
    private int nextId;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws Exception
    {
        tmpDir = new File(Files.createTempDirectory("dio_selection"));
        nextId = 0;
    }

    @After
    public void tearDown()
    {
        if (tmpDir != null)
            FileUtils.deleteRecursive(tmpDir);
    }

    @Test
    public void testSupportedOpsReturnDirectWriterWhenCompressedAndDirectMode() throws Exception
    {
        DirectIoTestUtils.withDirectWrites(() -> {
            for (OperationType op : SUPPORTED_OPS)
            {
                try (SequentialWriter w = build(compressedMetadata(), op))
                {
                    assertTrue("Expected DirectCompressedSequentialWriter for " + op + ", got " + w.getClass().getSimpleName(),
                               w instanceof DirectCompressedSequentialWriter);
                }
            }
        });
    }

    @Test
    public void testFlushFallsBackToStandardEvenInDirectMode() throws Exception
    {
        DirectIoTestUtils.withDirectWrites(() -> {
            try (SequentialWriter w = build(compressedMetadata(), OperationType.FLUSH))
            {
                assertTrue("FLUSH must not use DirectCompressedSequentialWriter (UNSUPPORTED_POLICY), got " + w.getClass().getSimpleName(),
                           w instanceof CompressedSequentialWriter && !(w instanceof DirectCompressedSequentialWriter));
            }
        });
    }

    @Test
    public void testScrubFallsBackToStandardEvenInDirectMode() throws Exception
    {
        DirectIoTestUtils.withDirectWrites(() -> {
            try (SequentialWriter w = build(compressedMetadata(), OperationType.SCRUB))
            {
                assertTrue("SCRUB must not use DirectCompressedSequentialWriter (UNSUPPORTED_CORRECTNESS), got " + w.getClass().getSimpleName(),
                           w instanceof CompressedSequentialWriter && !(w instanceof DirectCompressedSequentialWriter));
            }
        });
    }

    @Test
    public void testUncompressedAlwaysUsesStandardWriter() throws Exception
    {
        DirectIoTestUtils.withDirectWrites(() -> {
            for (OperationType op : SUPPORTED_OPS)
            {
                try (SequentialWriter w = build(uncompressedMetadata(), op))
                {
                    assertTrue("Uncompressed tables must use ChecksummedSequentialWriter for " + op + ", got " + w.getClass().getSimpleName(),
                               w instanceof ChecksummedSequentialWriter);
                }
            }
        });
    }

    @Test
    public void testBufferedModeNeverPicksDirectWriter()
    {
        DiskAccessMode original = DatabaseDescriptor.getBackgroundWriteDiskAccessMode();
        DatabaseDescriptor.setBackgroundWriteDiskAccessMode(DiskAccessMode.standard);
        try
        {
            for (OperationType op : SUPPORTED_OPS)
            {
                try (SequentialWriter w = build(compressedMetadata(), op))
                {
                    assertTrue("Buffered mode must not produce DirectCompressedSequentialWriter for " + op + ", got " + w.getClass().getSimpleName(),
                               w instanceof CompressedSequentialWriter && !(w instanceof DirectCompressedSequentialWriter));
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setBackgroundWriteDiskAccessMode(original);
        }
    }

    private SequentialWriter build(TableMetadata metadata, OperationType op)
    {
        Descriptor descriptor = new Descriptor(tmpDir, KS, CF, new SequenceBasedSSTableId(nextId++),
                                               DatabaseDescriptor.getSelectedSSTableFormat());
        MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
        // finishOnClose(false) routes try-with-resources close through abort, avoiding finish() side effects on an empty writer.
        SequentialWriterOption options = SequentialWriterOption.newBuilder().finishOnClose(false).build();
        return DataComponent.buildWriter(descriptor, metadata, options, collector, op, FlushCompression.fast, null);
    }

    private static TableMetadata compressedMetadata()
    {
        return TableMetadata.builder(KS, CF)
                            .addPartitionKeyColumn("k", BytesType.instance)
                            .compression(CompressionParams.lz4())
                            .build();
    }

    private static TableMetadata uncompressedMetadata()
    {
        return TableMetadata.builder(KS, CF)
                            .addPartitionKeyColumn("k", BytesType.instance)
                            .compression(CompressionParams.noCompression())
                            .build();
    }
}
