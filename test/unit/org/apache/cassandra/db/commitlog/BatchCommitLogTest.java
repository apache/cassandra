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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.security.EncryptionContext;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class BatchCommitLogTest extends CommitLogTest
{
    private static final long CL_BATCH_SYNC_WINDOW = 1000; // 1 second
    
    public BatchCommitLogTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        super(commitLogCompression, encryptionContext);
    }

    @BeforeClass
    public static void setCommitLogModeDetails()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);
        beforeClass();
    }

    @Test
    public void testBatchCLSyncImmediately()
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "key")
                     .clustering("bytes")
                     .add("val", ByteBuffer.allocate(10 * 1024))
                     .build();

        long startNano = nanoTime();
        CommitLog.instance.add(m);
        long delta = TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNano);
        Assert.assertTrue("Expect batch commitlog sync immediately, but took " + delta, delta < CL_BATCH_SYNC_WINDOW);
    }

    @Test
    public void testBatchCLShutDownImmediately() throws InterruptedException
    {
        long startNano = nanoTime();
        CommitLog.instance.shutdownBlocking();
        long delta = TimeUnit.NANOSECONDS.toMillis(nanoTime() - startNano);
        Assert.assertTrue("Expect batch commitlog shutdown immediately, but took " + delta, delta < CL_BATCH_SYNC_WINDOW);
        CommitLog.instance.start();
    }

    @Test
    public void testFlushAndWaitingMetrics()
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        Mutation m = new RowUpdateBuilder(cfs1.metadata.get(), 0, "key").clustering("bytes")
                                                                        .add("val", ByteBuffer.allocate(10 * 1024))
                                                                        .build();

        long startingFlushCount = CommitLog.instance.metrics.waitingOnFlush.getCount();
        long startingWaitCount = CommitLog.instance.metrics.waitingOnCommit.getCount();

        CommitLog.instance.add(m);

        // We should register single new flush and waiting data points.
        assertEquals(startingFlushCount + 1, CommitLog.instance.metrics.waitingOnFlush.getCount());
        assertEquals(startingWaitCount + 1, CommitLog.instance.metrics.waitingOnCommit.getCount());
    }
}
