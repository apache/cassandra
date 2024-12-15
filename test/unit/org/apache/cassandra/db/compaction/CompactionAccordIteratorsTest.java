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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Result;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.Serialize;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnFamilyStore.FlushReason;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionIteratorTest.Scanner;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static accord.impl.TimestampsForKey.NO_LAST_EXECUTED_HLC;
import static accord.local.KeyHistory.SYNC;
import static accord.local.PreLoadContext.contextFor;
import static accord.primitives.Routable.Domain.Range;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.MAJORITY;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.NOT_DURABLE;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.UNIVERSAL;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.COMMANDS_FOR_KEY;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandRows;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandsColumns;
import static org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeysAccessor;
import static org.apache.cassandra.service.accord.AccordKeyspace.TIMESTAMPS_FOR_KEY;
import static org.apache.cassandra.service.accord.AccordKeyspace.TimestampsForKeyRows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompactionAccordIteratorsTest
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionAccordIteratorsTest.class);
    private static final long CLOCK_START = 44;
    private static final long HLC_START = 41;
    private static final int NODE = 1;
    private static final int EPOCH = 1;
    private static final AtomicLong clock = new AtomicLong(CLOCK_START);
    private static final TxnId LT_TXN_ID = AccordTestUtils.txnId(EPOCH, HLC_START, NODE);
    private static final TxnId TXN_ID = AccordTestUtils.txnId(EPOCH, LT_TXN_ID.hlc() + 1, NODE);
    private static final TxnId SECOND_TXN_ID = AccordTestUtils.txnId(EPOCH, TXN_ID.hlc() + 1, NODE, Kind.Read);
    private static final TxnId RANGE_TXN_ID = AccordTestUtils.txnId(EPOCH, TXN_ID.hlc() + 2, NODE, Kind.Read, Range);
    private static final TxnId GT_TXN_ID = SECOND_TXN_ID;
    // For CommandsForKey where we test with two commands
    private static final TxnId[] TXN_IDS = new TxnId[]{ TXN_ID, SECOND_TXN_ID };
    private static final TxnId GT_SECOND_TXN_ID = AccordTestUtils.txnId(EPOCH, SECOND_TXN_ID.hlc() + 1, NODE);

    static ColumnFamilyStore commands;
    static ColumnFamilyStore timestampsForKey;
    static ColumnFamilyStore commandsForKey;
    static TableMetadata table;
    static FullRoute<?> route;
    Random random;

    /*
     * Whether to compact all tables at once in a single merge or forcing two random tables
     * to merge at a time
     */
    private boolean singleCompaction;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        SchemaLoader.prepareServer();
        // Schema doesn't matter since this is a metadata only test
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode = 'full'", "ks"));
        StorageService.instance.initServer();

        commands = ColumnFamilyStore.getIfExists(SchemaConstants.ACCORD_KEYSPACE_NAME, AccordKeyspace.COMMANDS);
        commands.disableAutoCompaction();

        timestampsForKey = ColumnFamilyStore.getIfExists(SchemaConstants.ACCORD_KEYSPACE_NAME, TIMESTAMPS_FOR_KEY);
        timestampsForKey.disableAutoCompaction();

        commandsForKey = ColumnFamilyStore.getIfExists(SchemaConstants.ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY);
        commandsForKey.disableAutoCompaction();

        table = ColumnFamilyStore.getIfExists("ks", "tbl").metadata();
        route = AccordTestUtils.keys(table, 42).toRoute(AccordTestUtils.key(table, 42).toUnseekable());
    }

    @Before
    public void setUp()
    {
        // This attempt at determinism doesn't work because the order of the SSTableScanners is not determinisitc
        long seed = System.nanoTime();
        logger.info("Seed " + seed + "L");
        random = new Random(seed);
    }

    // This isn't attempting to be an exhaustive test of Commands.shouldCleanup just that the return values
    // are handled correctly and that the interaction between the CompactionIterator and shoudCleanup seems reasonable
    @Test
    public void testAccordCommandsPurgerSingleCompaction() throws Throwable
    {
        testAccordCommandsPurger(true);
    }

    @Test
    public void testAccordCommandsPurgerMultipleCompactions() throws Throwable
    {
        testAccordCommandsPurger(false);
    }

    private void testAccordCommandsPurger(boolean singleCompaction) throws Throwable
    {
        this.singleCompaction = singleCompaction;
        // Null redudnant before should make no change since we have no information on this CommandStore
        testAccordCommandsPurger(null, DurableBefore.EMPTY, expectAccordCommandsNoChange());
        // Universally and locally durable (and global to boot) should be erased since literally everyone knows about it
        testAccordCommandsPurger(redundantBefore(GT_TXN_ID), durableBefore(UNIVERSAL), expectAccordCommandsErase());
        // Universally durable but not locally; we're stale, but shouldn't erase
        testAccordCommandsPurger(redundantBefore(LT_TXN_ID), durableBefore(UNIVERSAL), expectAccordCommandsNoChange());
        // redundantBefore will be > txnId as must be exclusive sync point, so will truncate
        testAccordCommandsPurger(redundantBefore(TXN_ID), durableBefore(MAJORITY), expectAccordCommandsTruncated());
        testAccordCommandsPurger(redundantBefore(LT_TXN_ID), durableBefore(MAJORITY), expectAccordCommandsNoChange());
        // Durable at a majority can be truncated with minimal data preserved, it must be redundant for this to occur
        testAccordCommandsPurger(redundantBefore(GT_TXN_ID), durableBefore(MAJORITY), expectAccordCommandsTruncated());
        // Not durable can be truncated, but needs the outcome preserved, it must be redundant for this to occur
        testAccordCommandsPurger(redundantBefore(GT_TXN_ID), durableBefore(NOT_DURABLE), expectAccordCommandsTruncatedWithOutcome());
        // Since it is redudnant but not known durable (outside of local)
        testAccordCommandsPurger(redundantBefore(GT_TXN_ID), durableBefore(DurableBeforeType.EMPTY), expectAccordCommandsTruncatedWithOutcome());
        // Never makes it past redundantBefore being LT_TXN_ID
        testAccordCommandsPurger(redundantBefore(LT_TXN_ID), durableBefore(DurableBeforeType.EMPTY), expectAccordCommandsNoChange());
    }

    private void testAccordCommandsPurger(RedundantBefore redundantBefore, DurableBefore durableBefore, Consumer<List<Partition>> expectedResult) throws Throwable
    {
        testWithCommandStore((commandStore) -> {
            IAccordService mockAccordService = mockAccordService(commandStore, redundantBefore, durableBefore);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ACCORD_KEYSPACE_NAME, AccordKeyspace.COMMANDS);
            List<Partition> result = compactCFS(mockAccordService, cfs);
            expectedResult.accept(result);
        }, false);
    }

    @Test
    public void testAccordCommandsForKeyPurgerSingleCompaction() throws Throwable
    {
        testAccordCommandsForKeyPurger(true);
    }

    @Test
    public void testAccordCommandsForKeyPurgerMultipleCompactions() throws Throwable
    {
        testAccordCommandsForKeyPurger(false);
    }

    private void testAccordCommandsForKeyPurger(boolean singleCompaction) throws Throwable
    {
        this.singleCompaction = singleCompaction;
        testAccordTimestampsForKeyPurger(null, expectedAccordTimestampsForKeyNoChange());
        testAccordCommandsForKeyPurger(null, expectedAccordCommandsForKeyNoChange());
        testAccordTimestampsForKeyPurger(redundantBefore(LT_TXN_ID), expectedAccordTimestampsForKeyNoChange());
        testAccordCommandsForKeyPurger(redundantBefore(LT_TXN_ID), expectedAccordCommandsForKeyNoChange());
        // will erase one more than expected as converted to ExclusiveSyncPoint id which is > base id
        testAccordTimestampsForKeyPurger(redundantBefore(TXN_ID), expectedAccordTimestampsForKeyEraseOne());
        testAccordCommandsForKeyPurger(redundantBefore(TXN_ID), expectedAccordCommandsForKeyEraseOne());
        testAccordTimestampsForKeyPurger(redundantBefore(GT_TXN_ID), expectedAccordTimestampsForKeyEraseAll());
        testAccordCommandsForKeyPurger(redundantBefore(GT_TXN_ID), expectedAccordCommandsForKeyEraseAll());
        testAccordTimestampsForKeyPurger(redundantBefore(GT_SECOND_TXN_ID), expectedAccordTimestampsForKeyEraseAll());
        testAccordCommandsForKeyPurger(redundantBefore(GT_SECOND_TXN_ID), expectedAccordCommandsForKeyEraseAll());
    }

    private static Consumer<List<Partition>> expectedAccordTimestampsForKeyNoChange()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            Row row = partition.getRow(Clustering.EMPTY);

            assertEquals(TXN_ID, TimestampsForKeyRows.getLastExecutedTimestamp(row));
            assertEquals(TXN_ID, TimestampsForKeyRows.getLastWriteTimestamp(row));

            // last_executed_micros is only persisted if it doesn't match txnId.hlc, which only happens in the
            // case of an hlc collision. Each txnId in this test has a unique hlc
            assertEquals(NO_LAST_EXECUTED_HLC, TimestampsForKeyRows.getLastExecutedMicros(row));
        };
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyNoChange()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            TokenKey partitionKey = new TokenKey(partition.metadata().id, partition.partitionKey().getToken());
            CommandsForKey cfk = CommandsForKeysAccessor.getCommandsForKey(partitionKey, ((Row) partition.unfilteredIterator().next()));
            assertEquals(TXN_IDS.length, cfk.size());
            for (int i = 0; i < TXN_IDS.length; ++i)
                assertEquals(TXN_IDS[i], cfk.txnId(i));
        };
    }

    private static Consumer<List<Partition>> expectedAccordTimestampsForKeyEraseOne()
    {
        return partitions -> assertEquals(0, partitions.size());
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyEraseOne()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            assertEquals(1, Iterators.size(partition.unfilteredIterator()));
            UnfilteredRowIterator rows = partition.unfilteredIterator();
//            assertEquals(TXN_IDS[1], CommandsForKeysAccessor.getTimestamp((Row)rows.next()));
        };
    }

    private static Consumer<List<Partition>> expectedAccordTimestampsForKeyEraseAll()
    {
        return partitions -> assertEquals(0, partitions.size());
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyEraseAll()
    {
        return partitions -> assertEquals(0, partitions.size());
    }

    private void testAccordTimestampsForKeyPurger(RedundantBefore redundantBefore, Consumer<List<Partition>> expectedResult) throws Throwable
    {
        testWithCommandStore((commandStore) -> {
            IAccordService mockAccordService = mockAccordService(commandStore, redundantBefore, DurableBefore.EMPTY);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ACCORD_KEYSPACE_NAME, TIMESTAMPS_FOR_KEY);
            List<Partition> result = compactCFS(mockAccordService, cfs);
            expectedResult.accept(result);
        }, true);
    }

    private void testAccordCommandsForKeyPurger(RedundantBefore redundantBefore, Consumer<List<Partition>> expectedResult) throws Throwable
    {
        testWithCommandStore((commandStore) -> {
            IAccordService mockAccordService = mockAccordService(commandStore, redundantBefore, DurableBefore.EMPTY);
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY);
            List<Partition> result = compactCFS(mockAccordService, cfs);
            expectedResult.accept(result);
        }, true);
    }

    Consumer<List<Partition>> expectAccordCommandsErase()
    {
        return partitions -> assertTrue(partitions.isEmpty());
    }

    Consumer<List<Partition>> expectAccordCommandsTruncatedWithOutcome()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            assertEquals(1, Iterators.size(partition.unfilteredIterator()));
            ByteBuffer[] partitionKeyComponents = CommandRows.splitPartitionKey(partition.partitionKey());
            Row row = (Row) partition.unfilteredIterator().next();
            assertEquals(CommandsColumns.TRUNCATE_FIELDS.length, row.columnCount());
            for (ColumnMetadata cm : CommandsColumns.TRUNCATE_FIELDS)
                assertNotNull(row.getColumnData(cm));
            assertEquals(TXN_ID, CommandRows.getTxnId(partitionKeyComponents));
            assertEquals(Durability.Local, CommandRows.getDurability(row));
            assertEquals(TXN_ID, CommandRows.getExecuteAt(row));
            assertEquals(route, CommandRows.getRoute(row));
            assertEquals(SaveStatus.TruncatedApplyWithOutcome, AccordKeyspace.CommandRows.getStatus(row));
        };
    }

    Consumer<List<Partition>> expectAccordCommandsTruncated()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            assertEquals(1, Iterators.size(partition.unfilteredIterator()));
            ByteBuffer[] partitionKeyComponents = CommandRows.splitPartitionKey(partition.partitionKey());
            Row row = (Row) partition.unfilteredIterator().next();
            assertEquals(CommandsColumns.TRUNCATE_FIELDS.length, row.columnCount());
            for (ColumnMetadata cm : CommandsColumns.TRUNCATE_FIELDS)
                assertNotNull(row.getColumnData(cm));
            assertEquals(TXN_ID, CommandRows.getTxnId(partitionKeyComponents));
            assertEquals(Durability.Local, CommandRows.getDurability(row));
            assertEquals(TXN_ID, CommandRows.getExecuteAt(row));
            assertEquals(route, CommandRows.getRoute(row));
            assertEquals(SaveStatus.TruncatedApply, AccordKeyspace.CommandRows.getStatus(row));
        };
    }

    Consumer<List<Partition>> expectAccordCommandsNoChange()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            assertEquals(1, Iterators.size(partition.unfilteredIterator()));
            ByteBuffer[] partitionKeyComponents = CommandRows.splitPartitionKey(partition.partitionKey());
            Row row = (Row) partition.unfilteredIterator().next();

            // execute_atleast is null, so when we read from the scanner the column won't be present in the partition
            Assertions.assertThat(new ArrayList<>(row.columns())).isEqualTo(commands.metadata().regularColumns().stream().filter(c -> !c.name.toString().equals("execute_atleast")).collect(Collectors.toList()));
            for (ColumnMetadata cm : commands.metadata().regularColumns())
            {
                if (cm.name.toString().equals("execute_atleast")) continue;
                assertNotNull(row.getColumnData(cm));
            }
            assertEquals(TXN_ID, CommandRows.getTxnId(partitionKeyComponents));
            assertEquals(SaveStatus.Applied, AccordKeyspace.CommandRows.getStatus(row));
        };
    }

    private static RedundantBefore redundantBefore(TxnId txnId)
    {
        Ranges ranges = AccordTestUtils.fullRange(AccordTestUtils.keys(table, 42));
        txnId = txnId.as(Kind.ExclusiveSyncPoint, Range);
        return RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, txnId, txnId, txnId, txnId, LT_TXN_ID.as(Range));
    }

    enum DurableBeforeType
    {
        UNIVERSAL,
        MAJORITY,
        NOT_DURABLE,
        EMPTY
    }

    private static DurableBefore durableBefore(DurableBeforeType durableBeforeType)
    {
        Ranges ranges = AccordTestUtils.fullRange(AccordTestUtils.keys(table, 42));
        switch (durableBeforeType)
        {
            case UNIVERSAL:
                return DurableBefore.create(ranges, GT_TXN_ID, GT_TXN_ID);
            case MAJORITY:
                return DurableBefore.create(ranges, GT_TXN_ID, LT_TXN_ID);
            case NOT_DURABLE:
                return DurableBefore.create(ranges, LT_TXN_ID, LT_TXN_ID);
            case EMPTY:
                return DurableBefore.EMPTY;
            default:
                throw new IllegalStateException();
        }
    }

    private static IAccordService mockAccordService(CommandStore commandStore, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        IAccordService mockAccordService = mock(IAccordService.class);
        Int2ObjectHashMap<RedundantBefore> redundantBefores = new Int2ObjectHashMap<>();
        if (redundantBefore != null)
            redundantBefores.put(commandStore.id(), redundantBefore);
        Int2ObjectHashMap<DurableBefore> durableBefores = new Int2ObjectHashMap<>();
        if (durableBefore != null)
            durableBefores.put(commandStore.id(), durableBefore);
        Int2ObjectHashMap<CommandStores.RangesForEpoch> rangesForEpochs = new Int2ObjectHashMap<>();
        rangesForEpochs.put(commandStore.id(), commandStore.unsafeGetRangesForEpoch());
        when(mockAccordService.getCompactionInfo()).thenReturn(new IAccordService.CompactionInfo(redundantBefores, rangesForEpochs, durableBefores));
        return mockAccordService;
    }

    interface TestWithCommandStore
    {
        void test(AccordCommandStore commandStore) throws Throwable;
    }


    private static void flush(AccordCommandStore commandStore)
    {
        commandStore.executeBlocking(() -> {
            // clear cache and wait for post-eviction writes to complete
            try (AccordExecutor.ExclusiveGlobalCaches cache = commandStore.executor().lockCaches();)
            {
                long cacheSize = cache.global.capacity();
                cache.global.setCapacity(0);
                cache.global.setCapacity(cacheSize);
            }
        });
        commands.forceBlockingFlush(FlushReason.UNIT_TESTS);
        timestampsForKey.forceBlockingFlush(FlushReason.UNIT_TESTS);
        commandsForKey.forceBlockingFlush(FlushReason.UNIT_TESTS);
        while (commandStore.executor().hasTasks())
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
    }

    private void testWithCommandStore(TestWithCommandStore test, boolean additionalCommand) throws Throwable
    {
        try (WithProperties wp = new WithProperties().set(CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED, "true"))
        {
            testWithCommandStoreInternal(test, additionalCommand);
        }
    }

    private void testWithCommandStoreInternal(TestWithCommandStore test, boolean additionalCommand) throws Throwable
    {
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
        ((AccordService) AccordService.instance()).journal().truncateForTesting();
        clock.set(CLOCK_START);
        AccordCommandStore commandStore = AccordTestUtils.createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId[] txnIds = additionalCommand ? TXN_IDS : new TxnId[]{ TXN_ID };
        Txn writeTxn = AccordTestUtils.createWriteTxn(42);
        Txn readTxn = AccordTestUtils.createTxn(42);
        Seekable key = writeTxn.keys().get(0);
        for (TxnId txnId : txnIds)
        {
            Txn txn = txnId.kind().isWrite() ? writeTxn : readTxn;
            PartialDeps partialDeps = Deps.NONE.intersecting(AccordTestUtils.fullRange(txn));
            PartialTxn partialTxn = txn.slice(commandStore.unsafeGetRangesForEpoch().currentRanges(), true);
            Route<?> partialRoute = route.slice(commandStore.unsafeGetRangesForEpoch().currentRanges());
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, appendDiffToKeyspace(commandStore));
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC), safe -> {
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, txnId, partialDeps, appendDiffToKeyspace(commandStore));
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC), safe -> {
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, partialTxn, txnId, partialDeps, appendDiffToKeyspace(commandStore));
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC), safe -> {
                Pair<Writes, Result> result = AccordTestUtils.processTxnResultDirect(safe, txnId, partialTxn, txnId);
                CheckedCommands.apply(safe, txnId, route, txnId, partialDeps, partialTxn, result.left, result.right, appendDiffToKeyspace(commandStore));
            }).beginAsResult());
            flush(commandStore);
            // The apply chain is asychronous, so it is easiest to just spin until it is applied
            // in order to have the updated state in the system table
            spinAssertEquals(true, 5, () -> {
                return getUninterruptibly(commandStore.submit(contextFor(txnId, route, SYNC), safe -> {
                    StoreParticipants participants = StoreParticipants.all(route);
                    Command command = safe.get(txnId, participants).current();
                    appendDiffToKeyspace(commandStore).accept(null, command);
                    return command.hasBeen(Status.Applied);
                }).beginAsResult());
            });
            flush(commandStore);
        }
        UntypedResultSet commandsTable = QueryProcessor.executeInternal("SELECT * FROM " + ACCORD_KEYSPACE_NAME + "." + AccordKeyspace.COMMANDS + ";");
        logger.info(commandsTable.toStringUnsafe());
        assertEquals(txnIds.length, commandsTable.size());
        Iterator<UntypedResultSet.Row> commandsTableIterator = commandsTable.iterator();
        for (TxnId txnId : txnIds)
            assertEquals(txnId, AccordKeyspace.deserializeTimestampOrNull(commandsTableIterator.next().getBytes("txn_id"), TxnId::fromBits));
        UntypedResultSet commandsForKeyTable = QueryProcessor.executeInternal("SELECT * FROM " + ACCORD_KEYSPACE_NAME + "." + COMMANDS_FOR_KEY + ";");
        logger.info(commandsForKeyTable.toStringUnsafe());
        assertEquals(1, commandsForKeyTable.size());
        CommandsForKey cfk = Serialize.fromBytes(((Key) key).toUnseekable(), commandsForKeyTable.iterator().next().getBytes("data"));
        assertEquals(txnIds.length, cfk.size());
        for (int i = 0; i < txnIds.length; ++i)
            assertEquals(txnIds[i], cfk.txnId(i));
        test.test(commandStore);
    }

    // This little bit of magic is required because we do not expose range commands explicitly, but still need to compact them
    private static BiConsumer<Command, Command> appendDiffToKeyspace(AccordCommandStore commandStore)
    {
        return (before, after) -> {
            AccordKeyspace.getCommandMutation(commandStore.id(), after, commandStore.nextSystemTimestampMicros()).applyUnsafe();
        };
    }

    private List<Partition> compactCFS(IAccordService mockAccordService, ColumnFamilyStore cfs)
    {
        List<ISSTableScanner> scanners = cfs.getLiveSSTables().stream().map(SSTableReader::getScanner).collect(Collectors.toList());
        int numScanners = scanners.size();
        List<Partition> result = null;
        do
        {
            List<Partition> outputPartitions = new ArrayList<>();
            List<ISSTableScanner> nextInputScanners = new ArrayList<>();
            if (singleCompaction || numScanners == 1)
            {
                nextInputScanners = ImmutableList.copyOf(scanners);
                scanners.clear();
            }
            else
            {
                // Process the rows only two sstables at a time to force compacting random slices of command state
                nextInputScanners.add(scanners.remove(random.nextInt(scanners.size())));
                nextInputScanners.add(scanners.remove(random.nextInt(scanners.size())));
            }
            try (CompactionController controller = new CompactionController(ColumnFamilyStore.getIfExists(ACCORD_KEYSPACE_NAME, cfs.name), Collections.emptySet(), 0);
                 CompactionIterator compactionIterator = new CompactionIterator(OperationType.COMPACTION, nextInputScanners, controller, FBUtilities.nowInSeconds(), null, ActiveCompactionsTracker.NOOP, null, () -> mockAccordService))
            {
                while (compactionIterator.hasNext())
                {
                    try (UnfilteredRowIterator partition = compactionIterator.next())
                    {
                        outputPartitions.add(ImmutableBTreePartition.create(partition));
                    }
                }
            }

            if (scanners.isEmpty())
                result = outputPartitions;
            else
                scanners.add(random.nextInt(scanners.size()), new Scanner(cfs.metadata(), outputPartitions.stream().map(Partition::unfilteredIterator).collect(Collectors.toList())));
        } while (!scanners.isEmpty());

        verify(mockAccordService, times(singleCompaction || numScanners == 1 ? 1 : numScanners - 1)).getCompactionInfo();
        return result;
    }
}
