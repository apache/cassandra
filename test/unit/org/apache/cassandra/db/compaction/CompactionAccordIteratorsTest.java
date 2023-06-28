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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.local.CheckedCommands;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.Commit;
import accord.messages.PreAccept;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.SchemaLoader;
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
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandRows;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsColumns;
import org.apache.cassandra.service.accord.AccordKeyspace.CommandsForKeyRows;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static accord.impl.CommandsForKey.NO_LAST_EXECUTED_HLC;
import static accord.local.PreLoadContext.contextFor;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.MAJORITY;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.NOT_DURABLE;
import static org.apache.cassandra.db.compaction.CompactionAccordIteratorsTest.DurableBeforeType.UNIVERSAL;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.COMMANDS;
import static org.apache.cassandra.service.accord.AccordKeyspace.COMMANDS_FOR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
    private static final TxnId GT_TXN_ID = SECOND_TXN_ID;
    // For CommandsForKey where we test with two commands
    private static final TxnId[] TXN_IDS = new TxnId[] {TXN_ID, SECOND_TXN_ID};
    private static final TxnId GT_SECOND_TXN_ID = AccordTestUtils.txnId(EPOCH, SECOND_TXN_ID.hlc() + 1, NODE);

    static ColumnFamilyStore commands;
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
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
        commands = ColumnFamilyStore.getIfExists(SchemaConstants.ACCORD_KEYSPACE_NAME, COMMANDS);
        commands.disableAutoCompaction();
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
        // Universally durable (and global to boot) should be erased since literally everyone knows about it
        // The way Commands.shouldCleanup was implemented (when this was written) it doesn't check redundantBefore
        // at all for this
        testAccordCommandsPurger(redundantBefore(LT_TXN_ID), durableBefore(UNIVERSAL), expectAccordCommandsErase());
        // With redundantBefore at the txnId there should be no change because it is < not <=
        testAccordCommandsPurger(redundantBefore(TXN_ID), durableBefore(MAJORITY), expectAccordCommandsNoChange());
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
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(ACCORD_KEYSPACE_NAME, COMMANDS);
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
        testAccordCommandsForKeyPurger(null, expectedAccordCommandsForKeyNoChange());
        testAccordCommandsForKeyPurger(redundantBefore(LT_TXN_ID), expectedAccordCommandsForKeyNoChange());
        testAccordCommandsForKeyPurger(redundantBefore(TXN_ID), expectedAccordCommandsForKeyNoChange());
        testAccordCommandsForKeyPurger(redundantBefore(GT_TXN_ID), expectedAccordCommandsForKeyEraseOne());
        testAccordCommandsForKeyPurger(redundantBefore(GT_SECOND_TXN_ID), expectedAccordCommandsForKeyEraseAll());
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyNoChange()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            Row staticRow = partition.getRow(Clustering.STATIC_CLUSTERING);
            assertEquals(4, Iterables.size(staticRow));
            assertEquals(SECOND_TXN_ID, CommandsForKeyRows.getMaxTimestamp(staticRow));
            assertEquals(TXN_ID, CommandsForKeyRows.getLastExecutedTimestamp(staticRow));
            assertEquals(TXN_ID, CommandsForKeyRows.getLastWriteTimestamp(staticRow));
            assertEquals(TXN_ID.hlc(), CommandsForKeyRows.getLastExecutedMicros(staticRow));
            assertEquals(4, Iterators.size(partition.unfilteredIterator()));
            UnfilteredRowIterator rows = partition.unfilteredIterator();
            // One row per txn per series
            for (int i = 0; i < 2; i++)
                for (TxnId txnId : TXN_IDS)
                    assertEquals(txnId, CommandsForKeyRows.getTimestamp((Row)rows.next()));
        };
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyEraseOne()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            Row staticRow = partition.getRow(Clustering.STATIC_CLUSTERING);
            // Only expect one column to remain because the second transaction is a read
            assertEquals(1, Iterables.size(staticRow));
            assertEquals(SECOND_TXN_ID, CommandsForKeyRows.getMaxTimestamp(staticRow));
            assertNull(CommandsForKeyRows.getLastExecutedTimestamp(staticRow));
            assertNull(CommandsForKeyRows.getLastWriteTimestamp(staticRow));
            assertEquals(NO_LAST_EXECUTED_HLC, CommandsForKeyRows.getLastExecutedMicros(staticRow));
            assertEquals(2, Iterators.size(partition.unfilteredIterator()));
            UnfilteredRowIterator rows = partition.unfilteredIterator();
            assertEquals(TXN_IDS[1], CommandsForKeyRows.getTimestamp((Row)rows.next()));
            assertEquals(TXN_IDS[1], CommandsForKeyRows.getTimestamp((Row)rows.next()));
        };
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyEraseAll()
    {
        return partitions -> assertEquals(0, partitions.size());
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
            Row row = (Row)partition.unfilteredIterator().next();
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
            Row row = (Row)partition.unfilteredIterator().next();
            assertEquals(commands.metadata().regularColumns().size(), row.columnCount());
            for (ColumnMetadata cm : commands.metadata().regularColumns())
                assertNotNull(row.getColumnData(cm));
            assertEquals(TXN_ID, CommandRows.getTxnId(partitionKeyComponents));
            assertEquals(SaveStatus.Applied, AccordKeyspace.CommandRows.getStatus(row));
        };
    }


    private static RedundantBefore redundantBefore(TxnId txnId)
    {
        Ranges ranges = AccordTestUtils.fullRange(AccordTestUtils.keys(table, 42));
        return RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, txnId, LT_TXN_ID);
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
        when(mockAccordService.getRedundantBeforesAndDurableBefore()).thenReturn(Pair.create(redundantBefores, durableBefore));
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
            long cacheSize = commandStore.getCacheSize();
            commandStore.setCacheSize(0);
            commandStore.setCacheSize(cacheSize);
            commandStore.cache().awaitSaveResults();
        });
        commands.forceBlockingFlush(FlushReason.UNIT_TESTS);
        commandsForKey.forceBlockingFlush(FlushReason.UNIT_TESTS);
    }

    private void testWithCommandStore(TestWithCommandStore test, boolean additionalCommand) throws Throwable
    {
        Keyspace.open(ACCORD_KEYSPACE_NAME).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
        clock.set(CLOCK_START);
        AccordCommandStore commandStore = AccordTestUtils.createAccordCommandStore(clock::incrementAndGet, "ks", "tbl");
        TxnId[] txnIds = additionalCommand ? TXN_IDS : new TxnId[] {TXN_ID};
        for (TxnId txnId : txnIds)
        {
            Txn txn = txnId.rw().isWrite() ? AccordTestUtils.createWriteTxn(42) : AccordTestUtils.createTxn(42);
            Seekable key = txn.keys().get(0);
            PartialDeps partialDeps = Deps.NONE.slice(AccordTestUtils.fullRange(txn));
            PartialTxn partialTxn = txn.slice(commandStore.unsafeRangesForEpoch().currentRanges(), true);
            PartialRoute<?> partialRoute = route.slice(commandStore.unsafeRangesForEpoch().currentRanges());
            getUninterruptibly(commandStore.execute(contextFor(txnId, txn.keys()), safe -> {
                PreAccept preAccept =
                    PreAccept.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), txnId.epoch(), false, txnId.epoch(), partialTxn, route);
                commandStore.appendToJournal(preAccept);
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, null);
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, txn.keys()), safe -> {
                Accept accept =
                    Accept.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), txnId.epoch(), false, Ballot.ZERO, txnId, partialTxn.keys(), partialDeps);
                commandStore.appendToJournal(accept);
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, partialTxn.keys(), null, txnId, partialDeps);
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, txn.keys()), safe -> {
                Commit commit =
                    Commit.SerializerSupport.create(txnId, partialRoute, txnId.epoch(), Commit.Kind.Minimal, txnId, partialTxn, partialDeps, route, null);
                commandStore.appendToJournal(commit);
                CheckedCommands.commit(safe, txnId, route, null, partialTxn, txnId, partialDeps);
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, txn.keys()), safe -> {
                Pair<Writes, Result> result = AccordTestUtils.processTxnResultDirect(safe, txnId, partialTxn, txnId);
                Apply apply =
                    Apply.SerializationSupport.create(txnId, partialRoute, txnId.epoch(), Apply.Kind.Minimal, partialTxn.keys(), txnId, partialDeps, partialTxn, result.left, result.right);
                commandStore.appendToJournal(apply);
                CheckedCommands.apply(safe, txnId, route, null, txnId, partialDeps, partialTxn, result.left, result.right);
            }).beginAsResult());
            flush(commandStore);
            // The apply chain is asychronous, so it is easiest to just spin until it is applied
            // in order to have the updated state in the system table
            spinAssertEquals(true, 5, () ->
                                      getUninterruptibly(commandStore.submit(contextFor(txnId, txn.keys()), safe -> safe.get(txnId, route.homeKey()).current().hasBeen(Status.Applied)
                                      ).beginAsResult()));
            flush(commandStore);
        }

        UntypedResultSet commandsTable = QueryProcessor.executeInternal("SELECT * FROM " + ACCORD_KEYSPACE_NAME + "." + COMMANDS + ";");
        logger.info(commandsTable.toStringUnsafe());
        assertEquals(txnIds.length, commandsTable.size());
        Iterator<UntypedResultSet.Row> commandsTableIterator = commandsTable.iterator();
        for (TxnId txnId : txnIds)
            assertEquals(txnId, AccordKeyspace.deserializeTimestampOrNull(commandsTableIterator.next().getBytes("txn_id"), TxnId::fromBits));
        UntypedResultSet commandsForKeyTable = QueryProcessor.executeInternal("SELECT * FROM " + ACCORD_KEYSPACE_NAME + "." + COMMANDS_FOR_KEY + ";");
        logger.info(commandsForKeyTable.toStringUnsafe());
        assertEquals(txnIds.length * 2, commandsForKeyTable.size());
        Iterator<UntypedResultSet.Row> commandsForKeyTableIterator = commandsTable.iterator();
        for (TxnId txnId : txnIds)
            assertEquals(txnId, AccordKeyspace.deserializeTimestampOrNull(commandsForKeyTableIterator.next().getBytes("txn_id"), TxnId::fromBits));
        test.test(commandStore);
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
            if (singleCompaction)
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

        verify(mockAccordService, times(singleCompaction ? 1 : numScanners - 1)).getRedundantBeforesAndDurableBefore();
        return result;
    }
}
