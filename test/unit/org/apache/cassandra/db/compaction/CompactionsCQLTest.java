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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.serializers.MarshalException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CompactionsCQLTest extends CQLTester
{

    public static final int SLEEP_TIME = 5000;

    private Config.CorruptedTombstoneStrategy strategy;

    @Before
    public void before()
    {
        strategy = DatabaseDescriptor.getCorruptedTombstoneStrategy();
    }

    @After
    public void after()
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(DatabaseDescriptor.getCorruptedTombstoneStrategy());
    }


    @Test
    public void testTriggerMinorCompactionSTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionLCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'fanout_size':5};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }


    @Test
    public void testTriggerMinorCompactionDTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'DateTieredCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1') using timestamp 1000"); // same timestamp = same window = minor compaction triggered
        flush();
        execute("insert into %s (id) values ('1') using timestamp 1000");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerMinorCompactionTWCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY) WITH compaction = {'class':'TimeWindowCompactionStrategy', 'min_threshold':2};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }


    @Test
    public void testTriggerNoMinorCompactionSTCSDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSNodetoolEnabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().enableAutoCompaction();
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        // Alter keyspace replication settings to force compaction strategy reload and check strategy is still enabled
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        getCurrentColumnFamilyStore().getCompactionStrategyManager().maybeReloadDiskBoundaries();
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSNodetoolDisabled() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerNoMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':true};");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, false);
    }

    @Test
    public void testTriggerMinorCompactionSTCSAlterTable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)  WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':2, 'enabled':false};");
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("ALTER TABLE %s WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'enabled': true}");
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        execute("insert into %s (id) values ('1')");
        flush();
        execute("insert into %s (id) values ('1')");
        flush();
        waitForMinor(KEYSPACE, currentTable(), SLEEP_TIME, true);
    }

    @Test
    public void testSetLocalCompactionStrategy() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // Invalidate disk boundaries to ensure that boundary invalidation will not cause the old strategy to be reloaded
        getCurrentColumnFamilyStore().invalidateDiskBoundaries();
        // altering something non-compaction related
        execute("ALTER TABLE %s WITH gc_grace_seconds = 1000");
        // should keep the local compaction strat
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // Alter keyspace replication settings to force compaction strategy reload
        execute("alter keyspace "+keyspace()+" with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 }");
        // should keep the local compaction strat
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), DateTieredCompactionStrategy.class));
        // altering a compaction option
        execute("ALTER TABLE %s WITH compaction = {'class':'SizeTieredCompactionStrategy', 'min_threshold':3}");
        // will use the new option
        assertTrue(verifyStrategies(getCurrentColumnFamilyStore().getCompactionStrategyManager(), SizeTieredCompactionStrategy.class));
    }

    @Test
    public void testSetLocalCompactionStrategyDisable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");
        localOptions.put("enabled", "false");
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
        localOptions.clear();
        localOptions.put("class", "DateTieredCompactionStrategy");
        // localOptions.put("enabled", "true"); - this is default!
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }


    @Test
    public void testSetLocalCompactionStrategyEnable() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class", "DateTieredCompactionStrategy");

        getCurrentColumnFamilyStore().disableAutoCompaction();
        assertFalse(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());

        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
        assertTrue(getCurrentColumnFamilyStore().getCompactionStrategyManager().isEnabled());
    }



    @Test(expected = IllegalArgumentException.class)
    public void testBadLocalCompactionStrategyOptions()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY)");
        Map<String, String> localOptions = new HashMap<>();
        localOptions.put("class","SizeTieredCompactionStrategy");
        localOptions.put("sstable_size_in_mb","1234"); // not for STCS
        getCurrentColumnFamilyStore().setCompactionParameters(localOptions);
    }

    @Test
    public void testPerCFSNeverPurgeTombstonesCell() throws Throwable
    {
        testPerCFSNeverPurgeTombstonesHelper(true);
    }

    @Test
    public void testPerCFSNeverPurgeTombstones() throws Throwable
    {
        testPerCFSNeverPurgeTombstonesHelper(false);
    }

    @Test
    public void testCompactionInvalidRTs() throws Throwable
    {
        // set the corruptedTombstoneStrategy to exception since these tests require it - if someone changed the default
        // in test/conf/cassandra.yaml they would start failing
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        prepare();
        // write a range tombstone with negative local deletion time (LDTs are not set by user and should not be negative):
        RangeTombstone rt = new RangeTombstone(Slice.ALL, new DeletionTime(System.currentTimeMillis(), -1));
        RowUpdateBuilder rub = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), System.currentTimeMillis() * 1000, 22).clustering(33).addRangeTombstone(rt);
        rub.build().apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        compactAndValidate();
        readAndValidate(true);
        readAndValidate(false);
    }

    @Test
    public void testCompactionInvalidTombstone() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        prepare();
        // write a standard tombstone with negative local deletion time (LDTs are not set by user and should not be negative):
        RowUpdateBuilder rub = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), -1, System.currentTimeMillis() * 1000, 22).clustering(33).delete("b");
        rub.build().apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        compactAndValidate();
        readAndValidate(true);
        readAndValidate(false);
    }

    @Test
    public void testCompactionInvalidPartitionDeletion() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        prepare();
        // write a partition deletion with negative local deletion time (LDTs are not set by user and should not be negative)::
        PartitionUpdate pu = PartitionUpdate.simpleBuilder(getCurrentColumnFamilyStore().metadata(), 22).nowInSec(-1).delete().build();
        new Mutation(pu).apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        compactAndValidate();
        readAndValidate(true);
        readAndValidate(false);
    }

    @Test
    public void testCompactionInvalidRowDeletion() throws Throwable
    {
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        prepare();
        // write a row deletion with negative local deletion time (LDTs are not set by user and should not be negative):
        RowUpdateBuilder.deleteRowAt(getCurrentColumnFamilyStore().metadata(), System.currentTimeMillis() * 1000, -1, 22, 33).apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        compactAndValidate();
        readAndValidate(true);
        readAndValidate(false);
    }

    private void prepare() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, id2 int, b text, primary key (id, id2))");
        for (int i = 0; i < 2; i++)
            execute("INSERT INTO %s (id, id2, b) VALUES (?, ?, ?)", i, i, String.valueOf(i));
    }

    @Test
    public void testIndexedReaderRowDeletion() throws Throwable
    {
        // write enough data to make sure we use an IndexedReader when doing a read, and make sure it fails when reading a corrupt row deletion
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        int maxSizePre = DatabaseDescriptor.getColumnIndexSize();
        DatabaseDescriptor.setColumnIndexSize(1024);
        prepareWide();
        RowUpdateBuilder.deleteRowAt(getCurrentColumnFamilyStore().metadata(), System.currentTimeMillis() * 1000, -1, 22, 33).apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        readAndValidate(true);
        readAndValidate(false);
        DatabaseDescriptor.setColumnIndexSize(maxSizePre);
    }

    @Test
    public void testIndexedReaderTombstone() throws Throwable
    {
        // write enough data to make sure we use an IndexedReader when doing a read, and make sure it fails when reading a corrupt standard tombstone
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        int maxSizePre = DatabaseDescriptor.getColumnIndexSize();
        DatabaseDescriptor.setColumnIndexSize(1024);
        prepareWide();
        RowUpdateBuilder rub = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), -1, System.currentTimeMillis() * 1000, 22).clustering(33).delete("b");
        rub.build().apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        readAndValidate(true);
        readAndValidate(false);
        DatabaseDescriptor.setColumnIndexSize(maxSizePre);
    }

    @Test
    public void testIndexedReaderRT() throws Throwable
    {
        // write enough data to make sure we use an IndexedReader when doing a read, and make sure it fails when reading a corrupt range tombstone
        DatabaseDescriptor.setCorruptedTombstoneStrategy(Config.CorruptedTombstoneStrategy.exception);
        int maxSizePre = DatabaseDescriptor.getColumnIndexSize();
        DatabaseDescriptor.setColumnIndexSize(1024);
        prepareWide();
        RangeTombstone rt = new RangeTombstone(Slice.ALL, new DeletionTime(System.currentTimeMillis(), -1));
        RowUpdateBuilder rub = new RowUpdateBuilder(getCurrentColumnFamilyStore().metadata(), System.currentTimeMillis() * 1000, 22).clustering(33).addRangeTombstone(rt);
        rub.build().apply();
        getCurrentColumnFamilyStore().forceBlockingFlush();
        readAndValidate(true);
        readAndValidate(false);
        DatabaseDescriptor.setColumnIndexSize(maxSizePre);
    }


    @Test
    public void testLCSThresholdParams() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t blob, primary key (id, id2)) with compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':'1', 'max_threshold':'60'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        for (int i = 0; i < 50; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (id, id2, t) values (?, ?, ?)", i, j, value);
            }
            cfs.forceBlockingFlush();
        }
        assertEquals(50, cfs.getLiveSSTables().size());
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getUnrepairedUnsafe().first();
        AbstractCompactionTask act = lcs.getNextBackgroundTask(0);
        // we should be compacting all 50 sstables:
        assertEquals(50, act.transaction.originals().size());
        act.execute(ActiveCompactionsTracker.NOOP);
    }

    @Test
    public void testSTCSinL0() throws Throwable
    {
        createTable("create table %s (id int, id2 int, t blob, primary key (id, id2)) with compaction = {'class':'LeveledCompactionStrategy', 'sstable_size_in_mb':'1', 'max_threshold':'60'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        execute("insert into %s (id, id2, t) values (?, ?, ?)", 1,1,"L1");
        cfs.forceBlockingFlush();
        cfs.forceMajorCompaction();
        SSTableReader l1sstable = cfs.getLiveSSTables().iterator().next();
        assertEquals(1, l1sstable.getSSTableLevel());
        // now we have a single L1 sstable, create many L0 ones:
        byte [] b = new byte[100 * 1024];
        new Random().nextBytes(b);
        ByteBuffer value = ByteBuffer.wrap(b);
        for (int i = 0; i < 50; i++)
        {
            for (int j = 0; j < 10; j++)
            {
                execute("insert into %s (id, id2, t) values (?, ?, ?)", i, j, value);
            }
            cfs.forceBlockingFlush();
        }
        assertEquals(51, cfs.getLiveSSTables().size());

        // mark the L1 sstable as compacting to make sure we trigger STCS in L0:
        LifecycleTransaction txn = cfs.getTracker().tryModify(l1sstable, OperationType.COMPACTION);
        LeveledCompactionStrategy lcs = (LeveledCompactionStrategy) cfs.getCompactionStrategyManager().getUnrepairedUnsafe().first();
        AbstractCompactionTask act = lcs.getNextBackgroundTask(0);
        // note that max_threshold is 60 (more than the amount of L0 sstables), but MAX_COMPACTING_L0 is 32, which means we will trigger STCS with at most max_threshold sstables
        assertEquals(50, act.transaction.originals().size());
        assertEquals(0, ((LeveledCompactionTask)act).getLevel());
        assertTrue(act.transaction.originals().stream().allMatch(s -> s.getSSTableLevel() == 0));
        txn.abort(); // unmark the l1 sstable compacting
        act.execute(ActiveCompactionsTracker.NOOP);
    }

    private void prepareWide() throws Throwable
    {
        createTable("CREATE TABLE %s (id int, id2 int, b text, primary key (id, id2))");
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (id, id2, b) VALUES (?, ?, ?)", 22, i, StringUtils.repeat("ABCDEFG", 10));
    }

    private void compactAndValidate()
    {
        boolean gotException = false;
        try
        {
            getCurrentColumnFamilyStore().forceMajorCompaction();
        }
        catch(Throwable t)
        {
            gotException = true;
            Throwable cause = t;
            while (cause != null && !(cause instanceof MarshalException))
                cause = cause.getCause();
            assertNotNull(cause);
            MarshalException me = (MarshalException) cause;
            assertTrue(me.getMessage().contains(getCurrentColumnFamilyStore().metadata.keyspace+"."+getCurrentColumnFamilyStore().metadata.name));
            assertTrue(me.getMessage().contains("Key 22"));
        }
        assertTrue(gotException);
        assertSuspectAndReset(getCurrentColumnFamilyStore().getLiveSSTables());
    }

    private void readAndValidate(boolean asc) throws Throwable
    {
        execute("select * from %s where id = 0 order by id2 "+(asc ? "ASC" : "DESC"));

        boolean gotException = false;
        try
        {
            for (UntypedResultSet.Row r : execute("select * from %s")) {}
        }
        catch (Throwable t)
        {
            assertTrue(t instanceof CorruptSSTableException);
            gotException = true;
            Throwable cause = t;
            while (cause != null && !(cause instanceof MarshalException))
                cause = cause.getCause();
            assertNotNull(cause);
            MarshalException me = (MarshalException) cause;
            assertTrue(me.getMessage().contains("Key 22"));
        }
        assertSuspectAndReset(getCurrentColumnFamilyStore().getLiveSSTables());
        assertTrue(gotException);
        gotException = false;
        try
        {
            execute("select * from %s where id = 22 order by id2 "+(asc ? "ASC" : "DESC"));
        }
        catch (Throwable t)
        {
            assertTrue(t instanceof CorruptSSTableException);
            gotException = true;
            Throwable cause = t;
            while (cause != null && !(cause instanceof MarshalException))
                cause = cause.getCause();
            assertNotNull(cause);
            MarshalException me = (MarshalException) cause;
            assertTrue(me.getMessage().contains("Key 22"));
        }
        assertTrue(gotException);
        assertSuspectAndReset(getCurrentColumnFamilyStore().getLiveSSTables());
    }

    public void testPerCFSNeverPurgeTombstonesHelper(boolean deletedCell) throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, b text) with gc_grace_seconds = 0");
        for (int i = 0; i < 100; i++)
        {
            execute("INSERT INTO %s (id, b) VALUES (?, ?)", i, String.valueOf(i));
        }
        getCurrentColumnFamilyStore().forceBlockingFlush();

        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        if (deletedCell)
            execute("UPDATE %s SET b=null WHERE id = ?", 50);
        else
            execute("DELETE FROM %s WHERE id = ?", 50);
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(false);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Thread.sleep(2000); // wait for gcgs to pass
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        if (deletedCell)
            execute("UPDATE %s SET b=null WHERE id = ?", 44);
        else
            execute("DELETE FROM %s WHERE id = ?", 44);
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(true);
        getCurrentColumnFamilyStore().forceBlockingFlush();
        Thread.sleep(1100);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), true);
        // disable it again and make sure the tombstone is gone:
        getCurrentColumnFamilyStore().setNeverPurgeTombstones(false);
        getCurrentColumnFamilyStore().forceMajorCompaction();
        assertTombstones(getCurrentColumnFamilyStore().getLiveSSTables().iterator().next(), false);
        getCurrentColumnFamilyStore().truncateBlocking();
    }

    private void assertSuspectAndReset(Collection<SSTableReader> sstables)
    {
        assertFalse(sstables.isEmpty());
        for (SSTableReader sstable : sstables)
        {
            assertTrue(sstable.isMarkedSuspect());
            sstable.unmarkSuspect();
        }
    }

    private void assertTombstones(SSTableReader sstable, boolean expectTS)
    {
        boolean foundTombstone = false;
        try(ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        foundTombstone = true;
                    while (iter.hasNext())
                    {
                        Unfiltered unfiltered = iter.next();
                        assertTrue(unfiltered instanceof Row);
                        for (Cell c : ((Row)unfiltered).cells())
                        {
                            if (c.isTombstone())
                                foundTombstone = true;
                        }

                    }
                }
            }
        }
        assertEquals(expectTS, foundTombstone);
    }

    public boolean verifyStrategies(CompactionStrategyManager manager, Class<? extends AbstractCompactionStrategy> expected)
    {
        boolean found = false;
        for (List<AbstractCompactionStrategy> strategies : manager.getStrategies())
        {
            if (!strategies.stream().allMatch((strategy) -> strategy.getClass().equals(expected)))
                return false;
            found = true;
        }
        return found;
    }

    private void waitForMinor(String keyspace, String cf, long maxWaitTime, boolean shouldFind) throws Throwable
    {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < maxWaitTime)
        {
            UntypedResultSet res = execute("SELECT * FROM system.compaction_history");
            for (UntypedResultSet.Row r : res)
            {
                if (r.getString("keyspace_name").equals(keyspace) && r.getString("columnfamily_name").equals(cf))
                    if (shouldFind)
                        return;
                    else
                        fail("Found minor compaction");
            }
            Thread.sleep(100);
        }
        if (shouldFind)
            fail("No minor compaction triggered in "+maxWaitTime+"ms");
    }
}
