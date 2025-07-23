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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Key;
import accord.local.CheckedCommands;
import accord.local.Command;
import accord.local.CommandStore;
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
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
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
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.FBUtilities;

import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static org.apache.cassandra.Util.spinAssertEquals;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.AccordKeyspace.COMMANDS_FOR_KEY;
import static org.apache.cassandra.service.accord.AccordKeyspace.CFKAccessor;
import static org.junit.Assert.assertEquals;
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
    private static final TxnId GT_TXN_ID = SECOND_TXN_ID.addFlag(HLC_BOUND);
    // For CommandsForKey where we test with two commands
    private static final TxnId[] TXN_IDS = new TxnId[]{ TXN_ID, SECOND_TXN_ID };
    private static final TxnId GT_SECOND_TXN_ID = AccordTestUtils.txnId(EPOCH, SECOND_TXN_ID.hlc() + 1, NODE).addFlag(HLC_BOUND);

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
        testAccordCommandsForKeyPurger(RedundantBefore.EMPTY, expectedAccordCommandsForKeyNoChange());
        testAccordCommandsForKeyPurger(redundantBefore(LT_TXN_ID), expectedAccordCommandsForKeyNoChange());
        // will erase one more than expected as converted to ExclusiveSyncPoint id which is > base id
        testAccordCommandsForKeyPurger(redundantBefore(TXN_ID), expectedAccordCommandsForKeyEraseOne());
        testAccordCommandsForKeyPurger(redundantBefore(GT_TXN_ID), expectedAccordCommandsForKeyEraseAll());
        testAccordCommandsForKeyPurger(redundantBefore(GT_SECOND_TXN_ID), expectedAccordCommandsForKeyEraseAll());
    }

    private static Consumer<List<Partition>> expectedAccordCommandsForKeyNoChange()
    {
        return partitions -> {
            assertEquals(1, partitions.size());
            Partition partition = partitions.get(0);
            TokenKey partitionKey = new TokenKey(partition.metadata().id, partition.partitionKey().getToken());
            CommandsForKey cfk = CFKAccessor.fromRow(partitionKey, ((Row) partition.unfilteredIterator().next()));
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

    private static RedundantBefore redundantBefore(TxnId txnId)
    {
        Ranges ranges = AccordTestUtils.fullRange(AccordTestUtils.keys(table, 42));
        txnId = txnId.as(Kind.ExclusiveSyncPoint, Range).addFlag(SHARD_BOUND);
        return RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, txnId, GC_BEFORE_AND_LOCALLY_DURABLE, LT_TXN_ID.as(Range));
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
        IAccordService.AccordCompactionInfo compactionInfo = new IAccordService.AccordCompactionInfo(commandStore.id(), redundantBefore, commandStore.unsafeGetRangesForEpoch(), ((AccordCommandStore)commandStore).tableId());
        IAccordService.AccordCompactionInfos compactionInfos = new IAccordService.AccordCompactionInfos(durableBefore, 0);
        compactionInfos.put(commandStore.id(), compactionInfo);
        when(mockAccordService.agent()).thenReturn(mock(Agent.class));
        when(mockAccordService.getCompactionInfo()).thenReturn(compactionInfos);
        when(mockAccordService.journalConfiguration()).thenReturn(new TestParams());
        return mockAccordService;
    }

    interface TestWithCommandStore
    {
        void test(AccordCommandStore commandStore) throws Throwable;
    }


    private static void flush(AccordCommandStore commandStore)
    {
        try (AccordExecutor.ExclusiveGlobalCaches cache = commandStore.executor().lockCaches();)
        {
            long cacheSize = cache.global.capacity();
            cache.global.setCapacity(0);
            cache.global.setCapacity(cacheSize);
        }
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
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                CheckedCommands.preaccept(safe, txnId, partialTxn, route, (a, b) -> {});
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                CheckedCommands.accept(safe, txnId, Ballot.ZERO, partialRoute, txnId, partialDeps, (a, b) -> {});
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.execute(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                CheckedCommands.commit(safe, SaveStatus.Stable, Ballot.ZERO, txnId, route, partialTxn, txnId, partialDeps, (a, b) -> {});
            }).beginAsResult());
            flush(commandStore);
            getUninterruptibly(commandStore.submit(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                return AccordTestUtils.processTxnResultDirect(safe, txnId, partialTxn, txnId);
            }).flatMap(i -> i).flatMap(result -> commandStore.execute(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                CheckedCommands.apply(safe, txnId, route, txnId, partialDeps, partialTxn, result.left, result.right, (a, b) -> {});
            })));
            flush(commandStore);
            // The apply chain is asychronous, so it is easiest to just spin until it is applied
            // in order to have the updated state in the system table
            spinAssertEquals(true, 5, () -> {
                return getUninterruptibly(commandStore.submit(contextFor(txnId, route, SYNC, READ_WRITE, "Test"), safe -> {
                    StoreParticipants participants = StoreParticipants.all(route);
                    Command command = safe.get(txnId, participants).current();
                    return command.hasBeen(Status.Applied);
                }).beginAsResult());
            });
            flush(commandStore);
        }
        UntypedResultSet commandsForKeyTable = QueryProcessor.executeInternal("SELECT * FROM " + ACCORD_KEYSPACE_NAME + "." + COMMANDS_FOR_KEY + ";");
        logger.info(commandsForKeyTable.toStringUnsafe());
        assertEquals(1, commandsForKeyTable.size());
        CommandsForKey cfk = Serialize.fromBytes(((Key) key).toUnseekable(), commandsForKeyTable.iterator().next().getBytes("data"));
        assertEquals(txnIds.length, cfk.size());
        for (int i = 0; i < txnIds.length; ++i)
            assertEquals(txnIds[i], cfk.txnId(i));
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
                 CompactionIterator compactionIterator = new CompactionIterator(OperationType.COMPACTION, nextInputScanners, controller, FBUtilities.nowInSeconds(), null, ActiveCompactionsTracker.NOOP, null, mockAccordService))
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
