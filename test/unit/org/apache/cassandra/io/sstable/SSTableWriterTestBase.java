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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SSTableWriterTestBase extends SchemaLoader
{

    protected static final String KEYSPACE = "SSTableRewriterTest";
    protected static final String CF = "Standard1";
    protected static final String CF_SMALL_MAX_VALUE = "Standard_SmallMaxValue";

    private static Config.DiskAccessMode standardMode;
    private static Config.DiskAccessMode indexMode;

    private static int maxValueSize;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_SMALL_MAX_VALUE));

        maxValueSize = DatabaseDescriptor.getMaxValueSize();
        DatabaseDescriptor.setMaxValueSize(1024 * 1024); // set max value size to 1MiB
    }

    @AfterClass
    public static void revertConfiguration()
    {
        DatabaseDescriptor.setMaxValueSize(maxValueSize);
        DatabaseDescriptor.setDiskAccessMode(standardMode);
        DatabaseDescriptor.setIndexAccessMode(indexMode);
    }

    @After
    public void truncateCF()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
        LifecycleTransaction.waitForDeletions();
    }

    public static void truncate(ColumnFamilyStore cfs)
    {
        cfs.truncateBlocking();
        LifecycleTransaction.waitForDeletions();
        Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);
        assertEquals(0, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(0, cfs.metric.totalDiskSpaceUsed.getCount());
        validateCFS(cfs);
    }

    /**
     * Validate the column family store by checking that all live
     * sstables are referenced only once and are not marked as
     * compacting. It also checks that the generation of the data
     * files on disk is the same as that of the live sstables,
     * to ensure that the data files on disk belong to the live
     * sstables. Finally, it checks that the metrics contain the
     * correct disk space used, live and total.
     *
     * Note that this method will submit a maximal compaction task
     * if there are live sstables, in order to check that there is at least
     * a maximal task when there are live sstables.
     *
     * This method has therefore side effects and should be called after
     * performing any other checks on previous operations, especially
     * checks involving files on disk.
     *
     * @param cfs - the column family store to validate
     */
    public static void validateCFS(ColumnFamilyStore cfs)
    {
        Set<SSTableId> liveDescriptors = new HashSet<>();
        long spaceUsed = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            assertFalse(sstable.isMarkedCompacted());
            assertEquals(1, sstable.selfRef().globalCount());
            liveDescriptors.add(sstable.descriptor.id);
            spaceUsed += sstable.bytesOnDisk();
        }
        for (File dir : cfs.getDirectories().getCFDirectories())
        {
            for (File f : dir.tryList())
            {
                if (f.name().contains("Data"))
                {
                    Descriptor d = Descriptor.fromFileWithComponent(f, false).left;
                    assertTrue(d.toString(), liveDescriptors.contains(d.id));
                }
            }
        }
        assertEquals(spaceUsed, cfs.metric.liveDiskSpaceUsed.getCount());
        assertEquals(spaceUsed, cfs.metric.totalDiskSpaceUsed.getCount());
        assertTrue(cfs.getTracker().getCompacting().isEmpty());

        if(cfs.getLiveSSTables().size() > 0)
            assertFalse(CompactionManager.instance.submitMaximal(cfs, cfs.gcBefore((int) (System.currentTimeMillis() / 1000)), false).isEmpty());
    }

    public static SSTableWriter getWriter(ColumnFamilyStore cfs, File directory, LifecycleTransaction txn, long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        Descriptor desc = cfs.newSSTableDescriptor(directory);
        return desc.getFormat().getWriterFactory().builder(desc)
                   .setTableMetadataRef(cfs.metadata)
                   .setKeyCount(0)
                   .setRepairedAt(repairedAt)
                   .setPendingRepair(pendingRepair)
                   .setTransientSSTable(isTransient)
                   .setSerializationHeader(new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS))
                   .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                   .setMetadataCollector(new MetadataCollector(cfs.metadata().comparator))
                   .addDefaultComponents(cfs.indexManager.listIndexGroups())
                   .build(txn, cfs);
    }

    public static SSTableWriter getWriter(ColumnFamilyStore cfs, File directory, LifecycleTransaction txn)
    {
        return getWriter(cfs, directory, txn, 0, null, false);
    }

    public static ByteBuffer random(int i, int size)
    {
        byte[] bytes = new byte[size + 4];
        ThreadLocalRandom.current().nextBytes(bytes);
        ByteBuffer r = ByteBuffer.wrap(bytes);
        r.putInt(0, i);
        return r;
    }

    public static int assertFileCounts(String [] files)
    {
        int tmplinkcount = 0;
        int tmpcount = 0;
        int datacount = 0;
        for (String f : files)
        {
            if (f.endsWith("-CRC.db"))
                continue;
            if (f.contains("tmplink-"))
                tmplinkcount++;
            else if (f.contains("tmp-"))
                tmpcount++;
            else if (f.contains("Data"))
                datacount++;
        }
        assertEquals(0, tmplinkcount);
        assertEquals(0, tmpcount);
        return datacount;
    }
}
