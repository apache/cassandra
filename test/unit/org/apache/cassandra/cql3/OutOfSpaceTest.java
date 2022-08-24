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
package org.apache.cassandra.cql3;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMKiller;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;

import static junit.framework.Assert.fail;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;

/**
 * Test that TombstoneOverwhelmingException gets thrown when it should be and doesn't when it shouldn't be.
 */
public class OutOfSpaceTest
{
    private final static String KEYSPACE = "ks";
    private final static String TABLE = "tab";
    private static EmbeddedCassandraService service; // we use EmbeddedCassandraService instead of CqlTester because we want CassandraDaemon

    @BeforeClass
    public static void beforeClass() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.mkdirs();
        ServerTestUtils.cleanup();
        service = new EmbeddedCassandraService();
        service.start();
    }

    public static void afterClass()
    {
        service.stop();
    }

    @Before
    public void before()
    {
        if (!StorageService.instance.isNativeTransportRunning())
            StorageService.instance.startNativeTransport();
        if (!StorageService.instance.isGossipActive())
            StorageService.instance.startGossiping();
    }

    @After
    public void after()
    {
        SchemaTestUtil.dropKeyspaceIfExist(KEYSPACE, false);
    }

    @Test
    public void testFlushUnwriteableDie() throws Throwable
    {
        makeTable();

        KillerForTests killerForTests = new KillerForTests();
        JVMKiller originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);
        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(ColumnFamilyStore.getIfExists(KEYSPACE, TABLE)))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.die);
            flushAndExpectError();
            Assert.assertTrue(killerForTests.wasKilled());
            Assert.assertFalse(killerForTests.wasKilledQuietly()); //only killed quietly on startup failure
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
            JVMStabilityInspector.replaceKiller(originalKiller);
        }
    }

    @Test
    public void testFlushUnwriteableStop() throws Throwable
    {
        makeTable();

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(ColumnFamilyStore.getIfExists(KEYSPACE, TABLE)))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.stop);
            Assert.assertTrue(Gossiper.instance.isEnabled()); // sanity check
            flushAndExpectError();
            Assert.assertFalse(Gossiper.instance.isEnabled());
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }
    }

    @Test
    public void testFlushUnwriteableIgnore() throws Throwable
    {
        makeTable();

        DiskFailurePolicy oldPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        try (Closeable c = Util.markDirectoriesUnwriteable(ColumnFamilyStore.getIfExists(KEYSPACE, TABLE)))
        {
            DatabaseDescriptor.setDiskFailurePolicy(DiskFailurePolicy.ignore);
            flushAndExpectError();
        }
        finally
        {
            DatabaseDescriptor.setDiskFailurePolicy(oldPolicy);
        }

        // Next flush should succeed.
        flush();
    }

    public void makeTable() throws Throwable
    {
        SchemaTestUtil.announceNewKeyspace(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1)));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (a text, b text, c text, PRIMARY KEY (a, b));", KEYSPACE, TABLE));

        // insert exactly the amount of tombstones that shouldn't trigger an exception
        for (int i = 0; i < 10; i++)
            QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (a, b, c) VALUES ('key', 'column%d', null);", KEYSPACE, TABLE, i));
    }

    public void flushAndExpectError() throws InterruptedException, ExecutionException
    {
        try
        {
            Keyspace.open(KEYSPACE)
                    .getColumnFamilyStore(TABLE)
                    .forceFlush(UNIT_TESTS)
                    .get();
            fail("FSWriteError expected.");
        }
        catch (ExecutionException e)
        {
            // Correct path.
            Assert.assertTrue(e.getCause() instanceof FSWriteError);
        }

        // Make sure commit log wasn't discarded.
        TableId tableId = currentTableMetadata().id;
        for (CommitLogSegment segment : CommitLog.instance.getSegmentManager().getActiveSegments())
            if (segment.getDirtyTableIds().contains(tableId))
                return;
        fail("Expected commit log to remain dirty for the affected table.");
    }

    private TableMetadata currentTableMetadata()
    {
        return ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).metadata();
    }

    private void flush()
    {
        ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).forceBlockingFlush(UNIT_TESTS);
    }
}
