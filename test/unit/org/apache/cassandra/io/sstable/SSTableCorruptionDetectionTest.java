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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.*;
import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.schema.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SSTableCorruptionDetectionTest extends SSTableWriterTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableCorruptionDetectionTest.class);

    private static final int numberOfPks = 1000;
    private static final int numberOfRuns = 100;
    private static final int valueSize = 512 * 1024;
    // Set corruption size larger or in comparable size to value size, otherwise
    // chance for corruption to land in the middle of value is quite high.
    private static final int maxCorruptionSize = 2 * 1024 * 1024;

    private static final String keyspace = "SSTableCorruptionDetectionTest";
    private static final String table = "corrupted_table";

    private static int maxValueSize;
    private static Random random;
    private static SSTableWriter writer;
    private static LifecycleTransaction txn;
    private static ColumnFamilyStore cfs;
    private static SSTableReader ssTableReader;

    @BeforeClass
    public static void setUp()
    {
        CFMetaData cfm = CFMetaData.Builder.create(keyspace, table)
                                           .addPartitionKey("pk", AsciiType.instance)
                                           .addClusteringColumn("ck1", AsciiType.instance)
                                           .addClusteringColumn("ck2", AsciiType.instance)
                                           .addRegularColumn("reg1", BytesType.instance)
                                           .addRegularColumn("reg2", BytesType.instance)
                                           .build();

        cfm.compression(CompressionParams.noCompression());
        SchemaLoader.createKeyspace(keyspace,
                                    KeyspaceParams.simple(1),
                                    cfm);

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024);

        long seed = System.nanoTime();
        logger.info("Seed {}", seed);
        random = new Random(seed);

        truncate(cfs);
        File dir = cfs.getDirectories().getDirectoryForNewSSTables();
        txn = LifecycleTransaction.offline(OperationType.WRITE);

        // Setting up/writing large values is an expensive operation, we only want to do it once per run
        writer = getWriter(cfs, dir, txn);
        for (int i = 0; i < numberOfPks; i++)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata, String.format("pkvalue_%07d", i)).withTimestamp(1);
            byte[] reg1 = new byte[valueSize];
            random.nextBytes(reg1);
            byte[] reg2 = new byte[valueSize];
            random.nextBytes(reg2);
            builder.newRow("clustering_" + i, "clustering_" + (i + 1))
                   .add("reg1", ByteBuffer.wrap(reg1))
                   .add("reg2", ByteBuffer.wrap(reg2));
            writer.append(builder.build().unfilteredIterator());
        }
        cfs.forceBlockingFlush();

        ssTableReader = writer.finish(true);
        txn.update(ssTableReader, false);
        LifecycleTransaction.waitForDeletions();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);

        txn.abort();
        writer.close();
    }

    @Test
    public void testSinglePartitionIterator() throws Throwable
    {
        bruteForceCorruptionTest(ssTableReader, partitionIterator());
    }

    @Test
    public void testSSTableScanner() throws Throwable
    {
        bruteForceCorruptionTest(ssTableReader, sstableScanner());
    }

    private void bruteForceCorruptionTest(SSTableReader ssTableReader, Consumer<SSTableReader> walker) throws Throwable
    {
        RandomAccessFile raf = new RandomAccessFile(ssTableReader.getFilename(), "rw");

        int corruptedCounter = 0;

        int fileLength = (int)raf.length(); // in current test, it does fit into int
        for (int i = 0; i < numberOfRuns; i++)
        {
            final int corruptionPosition = random.nextInt(fileLength - 1); //ensure at least one byte will be corrupted
            // corrupt max from position to end of file
            final int corruptionSize = Math.min(maxCorruptionSize, random.nextInt(fileLength - corruptionPosition));

            byte[] backup = corruptSstable(raf, corruptionPosition, corruptionSize);

            try
            {
                walker.accept(ssTableReader);
            }
            catch (CorruptSSTableException t)
            {
                corruptedCounter++;
            }
            finally
            {
                if (ChunkCache.instance != null)
                    ChunkCache.instance.invalidateFile(ssTableReader.getFilename());

                restore(raf, corruptionPosition, backup);
            }
        }

        assertTrue(corruptedCounter > 0);
        FileUtils.closeQuietly(raf);
    }

    private Consumer<SSTableReader> sstableScanner()
    {
        return (SSTableReader sstable) -> {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator rowIter = scanner.next())
                    {
                        if (rowIter.hasNext())
                        {
                            Unfiltered unfiltered = rowIter.next();
                            if (unfiltered.isRow())
                            {
                                Row row = (Row) unfiltered;
                                assertEquals(2, row.clustering().size());
                                // no-op read
                            }
                        }
                    }

                }
            }
        };
    }

    private Consumer<SSTableReader> partitionIterator()
    {
        return (SSTableReader sstable) -> {
            for (int i = 0; i < numberOfPks; i++)
            {
                DecoratedKey dk = Util.dk(String.format("pkvalue_%07d", i));
                try (UnfilteredRowIterator rowIter = sstable.iterator(dk,
                                                                      Slices.ALL,
                                                                      ColumnFilter.all(cfs.metadata),
                                                                      false,
                                                                      false,
                                                                      SSTableReadsListener.NOOP_LISTENER))
                {
                    while (rowIter.hasNext())
                    {
                        Unfiltered unfiltered = rowIter.next();
                        if (unfiltered.isRow())
                        {
                            Row row = (Row) unfiltered;
                            assertEquals(2, row.clustering().size());
                            // no-op read
                        }
                    }
                    rowIter.close();
                }
            }
        };
    }

    private byte[] corruptSstable(RandomAccessFile raf, int position, int corruptionSize) throws IOException
    {
        byte[] backup = new byte[corruptionSize];
        raf.seek(position);
        raf.read(backup);

        raf.seek(position);
        byte[] corruption = new byte[corruptionSize];
        random.nextBytes(corruption);
        raf.write(corruption);

        return backup;
    }

    private void restore(RandomAccessFile raf, int position, byte[] backup) throws IOException
    {
        raf.seek(position);
        raf.write(backup);
    }
}
