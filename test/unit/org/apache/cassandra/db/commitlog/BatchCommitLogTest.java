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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class BatchCommitLogTest
{
    private static final long CL_BATCH_SYNC_WINDOW = 1000; // 1 second
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "Standard1";

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);
        DatabaseDescriptor.setCommitLogSyncBatchWindow(CL_BATCH_SYNC_WINDOW);

        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testBatchCLSyncImmediately()
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation m = new RowUpdateBuilder(cfs1.metadata, 0, "key")
                     .clustering("bytes")
                     .add("val", ByteBuffer.allocate(10 * 1024))
                     .build();

        long startNano = System.nanoTime();
        CommitLog.instance.add(m);
        long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        assertTrue("Expect batch commitlog sync immediately, but took " + delta, delta < CL_BATCH_SYNC_WINDOW);
    }

    @Test
    public void testBatchCLShutDownImmediately() throws InterruptedException
    {
        long startNano = System.nanoTime();
        CommitLog.instance.shutdownBlocking();
        long delta = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
        assertTrue("Expect batch commitlog shutdown immediately, but took " + delta, delta < CL_BATCH_SYNC_WINDOW);
        CommitLog.instance.start();
    }
}
