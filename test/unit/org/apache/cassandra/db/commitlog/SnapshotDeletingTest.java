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

package org.apache.cassandra.db.commitlog;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SnapshotDeletingTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "CF_STANDARD1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testSnapshotDeletionFailure() throws Exception
    {
        Assume.assumeTrue(FBUtilities.isWindows());

        GCInspector.register();

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.clearUnsafe();

        // Needed to init the output file where we print failed snapshots. This is called on node startup.
        WindowsFailedSnapshotTracker.deleteOldSnapshots();

        populate(10000);
        store.snapshot("snapshot1");

        // Confirm snapshot deletion fails. Sleep for a bit just to make sure the SnapshotDeletingTask has
        // time to run and fail.
        Thread.sleep(500);
        store.clearSnapshot("snapshot1");
        assertEquals(1, SnapshotDeletingTask.pendingDeletionCount());

        // Compact the cf and confirm that the executor's after hook calls rescheduleDeletion
        populate(20000);
        store.forceBlockingFlush();
        store.forceMajorCompaction();

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 1000 && SnapshotDeletingTask.pendingDeletionCount() > 0)
        {
            Thread.yield();
        }

        assertEquals(0, SnapshotDeletingTask.pendingDeletionCount());
    }

    private long populate(int rowCount)
    {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i <= rowCount; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add(CF_STANDARD1,  Util.cellname(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       0);
            rm.applyUnsafe();
        }
        return timestamp;
    }
}
