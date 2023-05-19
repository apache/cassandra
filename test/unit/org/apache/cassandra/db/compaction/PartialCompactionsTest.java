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
package org.apache.cassandra.db.compaction;

import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DirectoriesTest;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;

public class PartialCompactionsTest extends SchemaLoader
{
    static final String KEYSPACE = PartialCompactionsTest.class.getSimpleName();
    static final String TABLE = "testtable";

    @BeforeClass
    public static void initSchema()
    {
        CompactionManager.instance.disableAutoCompaction();

        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));

        LimitableDataDirectory.applyTo(KEYSPACE, TABLE);
    }

    @Before
    public void prepareCFS()
    {
        LimitableDataDirectory.setAvailableSpace(cfStore(), null);
    }

    @After
    public void truncateCF()
    {
        cfStore().truncateBlocking();
        LifecycleTransaction.waitForDeletions();
    }

    private static ColumnFamilyStore cfStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @Test
    public void shouldNotResurrectDataFromSSTableExcludedDueToInsufficientSpace()
    {
        // given
        ColumnFamilyStore cfs = cfStore();
        int few = 10, many = 10 * few;

        // a large sstable as the oldest
        createDataSSTable(cfs, 0, many);
        // more inserts (to have more than one sstable to compact)
        createDataSSTable(cfs, many, many + few);
        // delete data that's in both of the prior sstables
        createTombstonesSSTable(cfs, many - few / 2, many + few / 2);

        // emulate there not being enough space to compact all sstables
        LimitableDataDirectory.setAvailableSpace(cfs, enoughSpaceForAllButTheLargestSSTable(cfs));

        // when - run a compaction where all tombstones have timed out
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE, false));

        // then - the tombstones should not be removed
        assertEquals("live sstables after compaction", 2, cfs.getLiveSSTables().size());
        assertEquals("remaining live rows after compaction", many, liveRows(cfs));
    }

    private static long enoughSpaceForAllButTheLargestSSTable(ColumnFamilyStore cfs)
    {
        long totalSize = 1, maxSize = 0;
        for (SSTableReader ssTable : cfs.getLiveSSTables())
        {
            long size = ssTable.onDiskLength();
            if (size > maxSize) maxSize = size;
            totalSize += size;
        }
        return totalSize - maxSize;
    }

    private static int liveRows(ColumnFamilyStore cfs)
    {
        return Util.getAll(Util.cmd(cfs, "key1").build()).stream()
                   .map(partition -> count(partition.rowIterator()))
                   .reduce(Integer::sum)
                   .orElse(0);
    }

    private static int count(Iterator<?> iter)
    {
        try (CloseableIterator<?> unused = iter instanceof CloseableIterator ? (CloseableIterator<?>) iter : null)
        {
            int count = 0;
            for (; iter.hasNext(); iter.next())
            {
                count++;
            }
            return count;
        }
    }

    private static void createDataSSTable(ColumnFamilyStore cfs, int firstKey, int endKey)
    {
        for (int i = firstKey; i < endKey; i++)
        {
            new RowUpdateBuilder(cfs.metadata(), 0, "key1")
            .clustering(String.valueOf(i))
            .add("val", String.valueOf(i))
            .build()
            .applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private static void createTombstonesSSTable(ColumnFamilyStore cfs, int firstKey, int endKey)
    {
        for (int i = firstKey; i < endKey; i++)
        {
            RowUpdateBuilder.deleteRow(cfs.metadata(), 1, "key1", String.valueOf(i)).applyUnsafe();
        }
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private static class LimitableDataDirectory extends Directories.DataDirectory
    {
        private Long availableSpace;

        LimitableDataDirectory(Directories.DataDirectory dataDirectory)
        {
            super(dataDirectory.location);
        }

        @Override
        public long getAvailableSpace()
        {
            if (availableSpace != null)
                return availableSpace;
            return super.getAvailableSpace();
        }

        public static void setAvailableSpace(ColumnFamilyStore cfs, Long availableSpace)
        {
            for (Directories.DataDirectory location : cfs.getDirectories().getWriteableLocations())
            {
                assertThat("ColumnFamilyStore set up with ability to emulate limited disk space",
                           location, instanceOf(LimitableDataDirectory.class));
                ((LimitableDataDirectory) location).availableSpace = availableSpace;
            }
        }

        public static void applyTo(String ks, String cf)
        {
            Keyspace keyspace = Keyspace.open(ks);
            ColumnFamilyStore store = keyspace.getColumnFamilyStore(cf);
            TableMetadataRef metadata = store.metadata;
            keyspace.dropCf(metadata.id, true);
            ColumnFamilyStore cfs = ColumnFamilyStore.createColumnFamilyStore(keyspace, cf, metadata, wrapDirectoriesOf(store), false, false, true);
            keyspace.initCfCustom(cfs);
        }

        private static Directories wrapDirectoriesOf(ColumnFamilyStore cfs)
        {
            Directories.DataDirectory[] original = cfs.getDirectories().getWriteableLocations();
            Directories.DataDirectory[] wrapped = new Directories.DataDirectory[original.length];
            for (int i = 0; i < wrapped.length; i++)
            {
                wrapped[i] = new LimitableDataDirectory(original[i]);
            }
            return new Directories(cfs.metadata(), wrapped)
            {
                @Override
                public boolean hasDiskSpaceForCompactionsAndStreams(Map<File, Long> expectedNewWriteSizes, Map<File, Long> totalCompactionWriteRemaining)
                {
                    return hasDiskSpaceForCompactionsAndStreams(expectedNewWriteSizes, totalCompactionWriteRemaining, file -> {
                        for (DataDirectory location : getWriteableLocations())
                        {
                            if (file.toPath().startsWith(location.location.toPath())) {
                                LimitableDataDirectory directory = (LimitableDataDirectory) location;
                                if (directory.availableSpace != null)
                                {
                                    DirectoriesTest.FakeFileStore store = new DirectoriesTest.FakeFileStore();
                                    // reverse the computation in Directories.getAvailableSpaceForCompactions
                                    store.usableSpace = Math.round(directory.availableSpace / DatabaseDescriptor.getMaxSpaceForCompactionsPerDrive()) + DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
                                    return store;
                                }
                            }
                        }
                        return Directories.getFileStore(file);
                    });
                }
            };
        }
    }
}
