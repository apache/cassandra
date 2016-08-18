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
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.WindowsFailedSnapshotTracker;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SnapshotDeletingTest
{
    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF_STANDARD1 = "CF_STANDARD1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        GCInspector.register();
        // Needed to init the output file where we print failed snapshots. This is called on node startup.
        WindowsFailedSnapshotTracker.deleteOldSnapshots();
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    @Test
    public void testCompactionHook() throws Exception
    {
        Assume.assumeTrue(FBUtilities.isWindows);

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF_STANDARD1);
        store.clearUnsafe();

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

    private void populate(int rowCount) {
        long timestamp = System.currentTimeMillis();
        CFMetaData cfm = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF_STANDARD1).metadata;
        for (int i = 0; i <= rowCount; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            for (int j = 0; j < 10; j++)
            {
                new RowUpdateBuilder(cfm, timestamp, 0, key.getKey())
                    .clustering(Integer.toString(j))
                    .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                    .build()
                    .applyUnsafe();
            }
        }
    }
}
