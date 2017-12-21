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
package org.apache.cassandra.index;

import java.io.FileNotFoundException;
import java.net.SocketException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SecondaryIndexManagerTest extends CQLTester
{

    @After
    public void after()
    {
        TestingIndex.clear();
    }

    @Test
    public void creatingIndexMarksTheIndexAsBuilt() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");

        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void rebuildingIndexMarksTheIndexAsBuilt() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");

        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        assertTrue(tryRebuild(indexName, false));
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void recreatingIndexMarksTheIndexAsBuilt() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");

        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // drop the index and verify that it has been removed from the built indexes table
        dropIndex("DROP INDEX %s." + indexName);
        assertNotMarkedAsBuilt(indexName);

        // create the index again and verify that it's added to the built indexes table
        createIndex(String.format("CREATE INDEX %s ON %%s(c)", indexName));
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void addingSSTablesMarksTheIndexAsBuilt() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex("CREATE INDEX ON %s(c)");

        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.indexManager.markAllIndexesRemoved();
        assertNotMarkedAsBuilt(indexName);

        try (Refs<SSTableReader> sstables = Refs.ref(cfs.getSSTables(SSTableSet.CANONICAL)))
        {
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertMarkedAsBuilt(indexName);
        }
    }

    @Test
    public void cannotRebuildWhileInitializationIsInProgress() throws Throwable
    {
        // create an index which blocks on creation
        TestingIndex.blockCreate();
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));

        // try to rebuild the index before the index creation task has finished
        assertFalse(tryRebuild(indexName, false));
        assertNotMarkedAsBuilt(indexName);

        // check that the index is marked as built when the creation finishes
        TestingIndex.unblockCreate();
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // now verify you can rebuild
        assertTrue(tryRebuild(indexName, false));
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void cannotRebuildWhileAnotherRebuildIsInProgress() throws Throwable
    {
        final String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        final String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        final AtomicBoolean error = new AtomicBoolean();

        // wait for index initialization and verify it's built:
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // rebuild the index in another thread, but make it block:
        TestingIndex.blockBuild();
        Thread asyncBuild = new Thread()
        {

            @Override
            public void run()
            {
                try
                {
                    tryRebuild(indexName, false);
                }
                catch (Throwable ex)
                {
                    error.set(true);
                }
            }
        };
        asyncBuild.start();

        // wait for the rebuild to block, so that we can proceed unblocking all further operations:
        TestingIndex.waitBlockedOnBuild();

        // do not block further builds:
        TestingIndex.shouldBlockBuild = false;

        // verify rebuilding the index before the previous index build task has finished fails
        assertFalse(tryRebuild(indexName, false));
        assertNotMarkedAsBuilt(indexName);

        // check that the index is marked as built when the build finishes
        TestingIndex.unblockBuild();
        asyncBuild.join();
        assertMarkedAsBuilt(indexName);

        // now verify you can rebuild
        assertTrue(tryRebuild(indexName, false));
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void cannotRebuildWhileAnSSTableBuildIsInProgress() throws Throwable
    {
        final String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        final String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        final AtomicBoolean error = new AtomicBoolean();

        // wait for index initialization and verify it's built:
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // add sstables in another thread, but make it block:
        TestingIndex.blockBuild();
        Thread asyncBuild = new Thread()
        {

            @Override
            public void run()
            {
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                try (Refs<SSTableReader> sstables = Refs.ref(cfs.getSSTables(SSTableSet.CANONICAL)))
                {
                    cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
                }
                catch (Throwable ex)
                {
                    error.set(true);
                }
            }
        };
        asyncBuild.start();

        // wait for the build to block, so that we can proceed unblocking all further operations:
        TestingIndex.waitBlockedOnBuild();

        // do not block further builds:
        TestingIndex.shouldBlockBuild = false;

        // verify rebuilding the index before the previous index build task has finished fails
        assertFalse(tryRebuild(indexName, false));
        assertNotMarkedAsBuilt(indexName);

        // check that the index is marked as built when the build finishes
        TestingIndex.unblockBuild();
        asyncBuild.join();
        assertMarkedAsBuilt(indexName);

        // now verify you can rebuild
        assertTrue(tryRebuild(indexName, false));
        assertMarkedAsBuilt(indexName);
    }

    @Test
    public void addingSSTableWhileRebuildIsInProgress() throws Throwable
    {
        final String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        final String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        final AtomicBoolean error = new AtomicBoolean();

        // wait for index initialization and verify it's built:
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // rebuild the index in another thread, but make it block:
        TestingIndex.blockBuild();
        Thread asyncBuild = new Thread()
        {

            @Override
            public void run()
            {
                try
                {
                    tryRebuild(indexName, false);
                }
                catch (Throwable ex)
                {
                    error.set(true);
                }
            }
        };
        asyncBuild.start();

        // wait for the rebuild to block, so that we can proceed unblocking all further operations:
        TestingIndex.waitBlockedOnBuild();

        // do not block further builds:
        TestingIndex.shouldBlockBuild = false;

        // try adding sstables and verify they are built but the index is not marked as built because of the pending build:
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try (Refs<SSTableReader> sstables = Refs.ref(cfs.getSSTables(SSTableSet.CANONICAL)))
        {
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            assertNotMarkedAsBuilt(indexName);
        }

        // unblock the pending build:
        TestingIndex.unblockBuild();
        asyncBuild.join();

        // verify the index is now built:
        assertMarkedAsBuilt(indexName);
        assertFalse(error.get());
    }

    @Test
    public void addingSSTableWithBuildFailureWhileRebuildIsInProgress() throws Throwable
    {
        final String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        final String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        final AtomicBoolean error = new AtomicBoolean();

        // wait for index initialization and verify it's built:
        waitForIndex(KEYSPACE, tableName, indexName);
        assertMarkedAsBuilt(indexName);

        // rebuild the index in another thread, but make it block:
        TestingIndex.blockBuild();
        Thread asyncBuild = new Thread()
        {

            @Override
            public void run()
            {
                try
                {
                    tryRebuild(indexName, false);
                }
                catch (Throwable ex)
                {
                    error.set(true);
                }
            }
        };
        asyncBuild.start();

        // wait for the rebuild to block, so that we can proceed unblocking all further operations:
        TestingIndex.waitBlockedOnBuild();

        // do not block further builds:
        TestingIndex.shouldBlockBuild = false;

        // try adding sstables but make the build fail:
        TestingIndex.shouldFailBuild = true;
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        try (Refs<SSTableReader> sstables = Refs.ref(cfs.getSSTables(SSTableSet.CANONICAL)))
        {
            cfs.indexManager.handleNotification(new SSTableAddedNotification(sstables, null), cfs.getTracker());
            fail("Should have failed!");
        }
        catch (Throwable ex)
        {
            assertTrue(ex.getMessage().contains("configured to fail"));
        }

        // disable failures:
        TestingIndex.shouldFailBuild = false;

        // unblock the pending build:
        TestingIndex.unblockBuild();
        asyncBuild.join();

        // verify the index is *not* built due to the failing sstable build:
        assertNotMarkedAsBuilt(indexName);
        assertFalse(error.get());
    }

    @Test
    public void rebuildWithFailure() throws Throwable
    {
        final String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        final String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        waitForIndex(KEYSPACE, tableName, indexName);

        // Rebuild the index with failure and verify it is not marked as built
        TestingIndex.shouldFailBuild = true;
        try
        {
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(indexName));
            fail("Should have failed!");
        }
        catch (Throwable ex)
        {
            assertTrue(ex.getMessage().contains("configured to fail"));
        }
        assertNotMarkedAsBuilt(indexName);
    }

    @Test
    public void initializingIndexNotQueryable() throws Throwable
    {
        TestingIndex.blockCreate();
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));

        // the index shouldn't be queryable while the initialization hasn't finished
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Index index = cfs.indexManager.getIndexByName(indexName);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        // the index should be queryable once the initialization has finished
        TestingIndex.unblockCreate();
        waitForIndex(KEYSPACE, tableName, indexName);
        assertTrue(cfs.indexManager.isIndexQueryable(index));
    }

    @Test
    public void initializingIndexNotQueryableAfterPartialRebuild() throws Throwable
    {
        TestingIndex.blockCreate();
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));

        // the index shouldn't be queryable while the initialization hasn't finished
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Index index = cfs.indexManager.getIndexByName(indexName);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        // a failing partial build doesn't set the index as queryable
        TestingIndex.shouldFailBuild = true;
        try
        {
            cfs.indexManager.handleNotification(new SSTableAddedNotification(cfs.getLiveSSTables(), null), this);
            fail("Should have failed!");
        }
        catch (Throwable ex)
        {
            assertTrue(ex.getMessage().contains("configured to fail"));
        }
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        // a successful partial build doesn't set the index as queryable
        TestingIndex.shouldFailBuild = false;
        cfs.indexManager.handleNotification(new SSTableAddedNotification(cfs.getLiveSSTables(), null), this);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        // the index should be queryable once the initialization has finished
        TestingIndex.unblockCreate();
        waitForIndex(KEYSPACE, tableName, indexName);
        assertTrue(cfs.indexManager.isIndexQueryable(index));
    }

    @Test
    public void indexWithFailedInitializationIsQueryableAfterFullRebuild() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");

        TestingIndex.shouldFailCreate = true;
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));

        tryRebuild(indexName, true);
        TestingIndex.shouldFailCreate = false;

        // a successfull full rebuild should set the index as queryable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Index index = cfs.indexManager.getIndexByName(indexName);
        cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(indexName));
        assertTrue(cfs.indexManager.isIndexQueryable(index));
    }

    @Test
    public void indexWithFailedInitializationIsNotQueryableAfterPartialRebuild() throws Throwable
    {
        TestingIndex.shouldFailCreate = true;
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        assertTrue(waitForIndexBuilds(KEYSPACE, indexName));
        TestingIndex.shouldFailCreate = false;

        // the index shouldn't be queryable after the failed initialization
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Index index = cfs.indexManager.getIndexByName(indexName);
        assertFalse(cfs.indexManager.isIndexQueryable(index));

        // a successful partial build doesn't set the index as queryable
        cfs.indexManager.handleNotification(new SSTableAddedNotification(cfs.getLiveSSTables(), null), this);
        assertTrue(waitForIndexBuilds(KEYSPACE, indexName));
        assertFalse(cfs.indexManager.isIndexQueryable(index));
    }

    @Test
    public void handleJVMStablityOnFailedCreate() throws Throwable
    {
        handleJVMStablityOnFailedCreate(new SocketException("Should not fail"), false);
        handleJVMStablityOnFailedCreate(new FileNotFoundException("Should not fail"), false);
        handleJVMStablityOnFailedCreate(new SocketException("Too many open files"), true);
        handleJVMStablityOnFailedCreate(new FileNotFoundException("Too many open files"), true);
        handleJVMStablityOnFailedCreate(new RuntimeException("Should not fail"), false);
    }

    private void handleJVMStablityOnFailedCreate(Throwable throwable, boolean shouldKillJVM) throws Throwable
    {
        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        try
        {
            TestingIndex.shouldFailCreate = true;
            TestingIndex.failedCreateThrowable = throwable;

            createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
            String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
            tryRebuild(indexName, true);
            fail("Should have failed!");
        }
        catch (Throwable t)
        {
            assertEquals(shouldKillJVM, killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            TestingIndex.shouldFailCreate = false;
            TestingIndex.failedCreateThrowable = null;
        }
    }

    @Test
    public void handleJVMStablityOnFailedRebuild() throws Throwable
    {
        handleJVMStablityOnFailedRebuild(new SocketException("Should not fail"), false);
        handleJVMStablityOnFailedRebuild(new FileNotFoundException("Should not fail"), false);
        handleJVMStablityOnFailedRebuild(new SocketException("Too many open files"), true);
        handleJVMStablityOnFailedRebuild(new FileNotFoundException("Too many open files"), true);
        handleJVMStablityOnFailedRebuild(new RuntimeException("Should not fail"), false);
    }

    private void handleJVMStablityOnFailedRebuild(Throwable throwable, boolean shouldKillJVM) throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        String indexName = createIndex(String.format("CREATE CUSTOM INDEX ON %%s(c) USING '%s'", TestingIndex.class.getName()));
        waitForIndex(KEYSPACE, tableName, indexName);

        KillerForTests killerForTests = new KillerForTests();
        JVMStabilityInspector.Killer originalKiller = JVMStabilityInspector.replaceKiller(killerForTests);

        try
        {
            TestingIndex.shouldFailBuild = true;
            TestingIndex.failedBuildTrowable = throwable;

            getCurrentColumnFamilyStore().indexManager.rebuildIndexesBlocking(Collections.singleton(indexName));
            fail("Should have failed!");
        }
        catch (Throwable t)
        {
            assertEquals(shouldKillJVM, killerForTests.wasKilled());
        }
        finally
        {
            JVMStabilityInspector.replaceKiller(originalKiller);
            TestingIndex.shouldFailBuild = false;
            TestingIndex.failedBuildTrowable = null;
        }
    }

    private static void assertMarkedAsBuilt(String indexName)
    {
        List<String> indexes = SystemKeyspace.getBuiltIndexes(KEYSPACE, Collections.singleton(indexName));
        assertEquals(1, indexes.size());
        assertEquals(indexName, indexes.get(0));
    }

    private static void assertNotMarkedAsBuilt(String indexName)
    {
        List<String> indexes = SystemKeyspace.getBuiltIndexes(KEYSPACE, Collections.singleton(indexName));
        assertTrue(indexes.isEmpty());
    }

    private boolean tryRebuild(String indexName, boolean wait) throws Throwable
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        boolean done = false;
        do
        {
            try
            {
                cfs.indexManager.rebuildIndexesBlocking(Collections.singleton(indexName));
                done = true;
            }
            catch (IllegalStateException e)
            {
                assertTrue(e.getMessage().contains("currently in progress"));
            }
            Thread.sleep(500);
        }
        while (!done && wait);

        return done;
    }

    public static class TestingIndex extends StubIndex
    {
        private static volatile CountDownLatch createLatch;
        private static volatile CountDownLatch buildLatch;
        private static volatile CountDownLatch createWaitLatch;
        private static volatile CountDownLatch buildWaitLatch;
        public static volatile boolean shouldBlockCreate = false;
        public static volatile boolean shouldBlockBuild = false;
        public static volatile boolean shouldFailCreate = false;
        public static volatile boolean shouldFailBuild = false;
        public static volatile Throwable failedCreateThrowable;
        public static volatile Throwable failedBuildTrowable;

        public TestingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }

        public static void blockCreate()
        {
            shouldBlockCreate = true;
            createLatch = new CountDownLatch(1);
            createWaitLatch = new CountDownLatch(1);
        }

        public static void blockBuild()
        {
            shouldBlockBuild = true;
            buildLatch = new CountDownLatch(1);
            buildWaitLatch = new CountDownLatch(1);
        }

        public static void unblockCreate()
        {
            createLatch.countDown();
        }

        public static void unblockBuild()
        {
            buildLatch.countDown();
        }

        public static void waitBlockedOnCreate() throws InterruptedException
        {
            createWaitLatch.await();
        }

        public static void waitBlockedOnBuild() throws InterruptedException
        {
            buildWaitLatch.await();
        }

        public static void clear()
        {
            createLatch = null;
            createWaitLatch = null;
            buildLatch = null;
            buildWaitLatch = null;
            shouldBlockCreate = false;
            shouldBlockBuild = false;
            shouldFailBuild = false;
            failedCreateThrowable = null;
            failedBuildTrowable = null;
        }

        public Callable<?> getInitializationTask()
        {
            return () ->
            {
                if (shouldBlockCreate && createLatch != null)
                {
                    createWaitLatch.countDown();
                    createLatch.await();
                }

                if (shouldFailCreate)
                {
                    throw failedCreateThrowable == null
                          ? new IllegalStateException("Index is configured to fail.")
                          : new RuntimeException(failedCreateThrowable);
                }

                return null;
            };
        }

        public IndexBuildingSupport getBuildTaskSupport()
        {
            return new CollatedViewIndexBuildingSupport()
            {
                public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables)
                {
                    try
                    {
                        if (shouldBlockBuild && buildLatch != null)
                        {
                            buildWaitLatch.countDown();
                            buildLatch.await();
                        }
                        final SecondaryIndexBuilder builder = super.getIndexBuildTask(cfs, indexes, sstables);
                        return new SecondaryIndexBuilder()
                        {

                            @Override
                            public void build()
                            {
                                if (shouldFailBuild)
                                {
                                    throw failedBuildTrowable == null
                                          ? new IllegalStateException("Index is configured to fail.")
                                          : new RuntimeException(failedBuildTrowable);
                                }
                                builder.build();
                            }

                            @Override
                            public CompactionInfo getCompactionInfo()
                            {
                                return builder.getCompactionInfo();
                            }
                        };
                    }
                    catch (InterruptedException ex)
                    {
                        throw new RuntimeException(ex);
                    }
                }
            };
        }

        public boolean shouldBuildBlocking()
        {
            return true;
        }
    }
}
