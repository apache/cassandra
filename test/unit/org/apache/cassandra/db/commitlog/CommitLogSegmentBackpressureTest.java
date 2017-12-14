/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Semaphore;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

/**
 * Since this test depends on byteman rules being setup during initialization, you shouldn't add tests to this class
 */
@RunWith(BMUnitRunner.class)
public class CommitLogSegmentBackpressureTest
{
    //Block commit log service from syncing
    private static final Semaphore allowSync = new Semaphore(1);

    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "Standard1";
    private static final String STANDARD2 = "Standard2";

    private final static byte[] entropy = new byte[1024 * 256];

    @Test
    @BMRules(rules = {@BMRule(name = "Acquire Semaphore before sync",
                              targetClass = "AbstractCommitLogService$SyncRunnable",
                              targetMethod = "sync",
                              targetLocation = "AT INVOKE org.apache.cassandra.db.commitlog.CommitLog.sync(boolean)",
                              action = "org.apache.cassandra.db.commitlog.CommitLogSegmentBackpressureTest.allowSync.acquire()"),
                      @BMRule(name = "Release Semaphore after sync",
                              targetClass = "AbstractCommitLogService$SyncRunnable",
                              targetMethod = "sync",
                              targetLocation = "AFTER INVOKE org.apache.cassandra.db.commitlog.CommitLog.sync(boolean)",
                              action = "org.apache.cassandra.db.commitlog.CommitLogSegmentBackpressureTest.allowSync.release()")})
    public void testCompressedCommitLogBackpressure() throws Throwable
    {
        // Perform all initialization before making CommitLog.Sync blocking
        // Doing the initialization within the method guarantee that Byteman has performed its injections when we start
        new Random().nextBytes(entropy);
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        DatabaseDescriptor.setCommitLogSegmentSize(1);
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10 * 1000);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD2, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();

        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        final Mutation m = new RowUpdateBuilder(cfs1.metadata, 0, "k").clustering("bytes")
                                                                      .add("val", ByteBuffer.wrap(entropy))
                                                                      .build();

        Thread dummyThread = new Thread(() -> {
            for (int i = 0; i < 20; i++)
                CommitLog.instance.add(m);
        });

        try
        {
            // Makes sure any call to CommitLog.sync is blocking
            allowSync.acquire();

            dummyThread.start();

            AbstractCommitLogSegmentManager clsm = CommitLog.instance.segmentManager;

            Util.spinAssertEquals(3, () -> clsm.getActiveSegments().size(), 5);

            Thread.sleep(1000);

            // Should only be able to create 3 segments not 7 because it blocks waiting for truncation that never comes
            Assert.assertEquals(3, clsm.getActiveSegments().size());

            // Discard the currently active segments so allocation can continue.
            // Take snapshot of the list, otherwise this will also discard newly allocated segments.
            new ArrayList<>(clsm.getActiveSegments()).forEach( clsm::archiveAndDiscard );

            // The allocated count should reach the limit again.
            Util.spinAssertEquals(3, () -> clsm.getActiveSegments().size(), 5);
        }
        finally
        {
            // Allow the CommitLog.sync to perform normally.
            allowSync.release();
        }
        try
        {
            // Wait for the dummy thread to die
            dummyThread.join();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
