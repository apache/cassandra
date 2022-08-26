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

package org.apache.cassandra.db.partitions;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.NativeCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.MemtableCleaner;
import org.apache.cassandra.utils.memory.MemtablePool;

import static org.assertj.core.api.Assertions.assertThat;

/* Test memory pool accounting when updating atomic btree partitions.  CASSANDRA-18125 hit an issue
 * where cells were doubly-counted when releasing causing negative allocator onHeap ownership which
 * crashed memtable flushing.
 *
 * The aim of the test is to exhaustively test updates to simple and complex cells in all possible
 * state and check the accounting is reasonable. It generates an initial row, then an update row
 * and checks the allocator ownership is reasonable, then compares usage to a freshly recreated
 * instance of the partition.
 *
 * Replacing existing values does not free up memory and is accounted for when comparing
 * the fresh build.
 */
@RunWith(Parameterized.class)
public class AtomicBTreePartitionMemtableAccountingTest
{
    public static final int INITIAL_TS = 2000;
    public static final int EARLIER_TS = 1000;
    public static final int LATER_TS = 3000;

    public static final long NOW_LDT = FBUtilities.nowInSeconds();
    public static final long LATER_LDT = NOW_LDT + 1000;
    public static final long EARLIER_LDT = NOW_LDT - 1000;

    public static final int EXPIRED_TTL = 1;
    public static final int EXPIRING_TTL = 10000;

    public static final long HEAP_LIMIT = 1 << 20;
    public static final long OFF_HEAP_LIMIT = 1 << 20;
    public static final float MEMTABLE_CLEANUP_THRESHOLD = 0.25f;
    public static final MemtableCleaner DUMMY_CLEANER = () -> ImmediateFuture.failure(new IllegalStateException());

    @Parameterized.Parameters(name="allocationType={0}")
    public static Iterable<? extends Object> data()
    {
        return Arrays.asList(Config.MemtableAllocationType.values());
    }

    @Parameterized.Parameter
    public Config.MemtableAllocationType allocationType;

    static TableMetadata metadata;
    static DecoratedKey partitionKey;
    static  ColumnMetadata r1md;
    static  ColumnMetadata c2md;
    static  ColumnMetadata s3md;
    static  ColumnMetadata c4md;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("dummy_ks", "dummy_tbl")
                                                           .addPartitionKeyColumn("pk", Int32Type.instance)
                                                           .addRegularColumn("r1", Int32Type.instance)
                                                           .addRegularColumn("c2", SetType.getInstance(Int32Type.instance, true))
                                                           .addStaticColumn("s3", Int32Type.instance)
                                                           .addStaticColumn("c4", SetType.getInstance(Int32Type.instance, true))
                                                           .build();
        partitionKey = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(0));
        r1md = metadata.getColumn(new ColumnIdentifier("r1", false));
        c2md = metadata.getColumn(new ColumnIdentifier("c2", false));
        s3md = metadata.getColumn(new ColumnIdentifier("s3", false));
        c4md = metadata.getColumn(new ColumnIdentifier("c4", false));
    }

    @Ignore
    @Test
    public void repro() // For running in the IDE, update with failing testCase parameters to run
    {
        new TestCase(INITIAL_TS, Cell.NO_TTL, Cell.NO_DELETION_TIME, DeletionTime.build(EARLIER_TS, EARLIER_LDT), 1,
                     EARLIER_TS, Cell.NO_TTL, Cell.NO_DELETION_TIME, DeletionTime.LIVE, 3).execute();
    }

    @Test
    public void exhaustiveTest()
    {
        // TTLs for initial and updated cells
        List<Integer> ttls = Arrays.asList(Cell.NO_TTL, EXPIRING_TTL, EXPIRED_TTL);

        // Initital local deleted times - a live cell, and a tombstone from now
        List<Long> initialLDTs = Arrays.asList(Cell.NO_DELETION_TIME, NOW_LDT);

        // Initial complex deletion time for c2 - no deletion, earlier than c2 elements, or concurrent with c2 elements
        List<DeletionTime> initialComplexDeletionTimes = Arrays.asList(DeletionTime.LIVE,
                                                                       DeletionTime.build(EARLIER_TS, EARLIER_LDT),
                                                                       DeletionTime.build(INITIAL_TS, NOW_LDT));

        // Update timestamps - earlier - ignore update, same as initial, after initial - supercedes
        List<Integer> updateTimestamps = Arrays.asList(EARLIER_TS, INITIAL_TS, LATER_TS);

        // Update local deleted times - live cell, earlier tombstone, concurrent tombstone, or future deletion
        List<Long> updateLDTs = Arrays.asList(Cell.NO_DELETION_TIME, EARLIER_LDT, NOW_LDT, LATER_LDT);

        // Update complex deletion time for c2 - no deletion, earlier than c2 elements,
        // or concurrent with c2 elements, after c2 elements
        List<DeletionTime> updateComplexDeletionTimes = Arrays.asList(DeletionTime.LIVE,
                                                                      DeletionTime.build(EARLIER_TS, EARLIER_LDT),
                                                                      DeletionTime.build(INITIAL_TS, NOW_LDT),
                                                                      DeletionTime.build(LATER_TS, LATER_LDT));

        // Number of cells to put in the update collection - overlapping by one cell
        List<Integer> initialComplexCellCount = Arrays.asList(3, 1);
        List<Integer> updateComplexCellCount = Arrays.asList(3, 1);

        ttls.forEach(initialTTL -> {
            initialLDTs.forEach(initialLDT -> {
                initialComplexDeletionTimes.forEach(initialCDT -> {
                    initialComplexCellCount.forEach(numC2InitialCells -> {
                        updateTimestamps.forEach(updateTS -> {
                            ttls.forEach(updateTTL -> {
                                updateLDTs.forEach(updateLDT -> {
                                    updateComplexDeletionTimes.forEach(updateCDT -> {
                                        updateComplexCellCount.forEach(numC2UpdateCells -> {
                                            new TestCase(INITIAL_TS, initialTTL, initialLDT, initialCDT, numC2InitialCells,
                                                         updateTS, updateTTL, updateLDT, updateCDT, numC2UpdateCells).execute();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }

    class TestCase
    {
        int initialTS;
        int initialTTL;
        long initialLDT;
        DeletionTime initialCDT;
        int numC2InitialCells;
        int updateTS;
        int updateTTL;
        long updateLDT;
        DeletionTime updateCDT;
        Integer numC2UpdateCells;

        public TestCase(int initialTS, int initialTTL, long initialLDT, DeletionTime initialCDT, int numC2InitialCells,
                        int updateTS, int updateTTL, long updateLDT, DeletionTime updateCDT, Integer numC2UpdateCells)
        {
            this.initialTS = initialTS;
            this.initialTTL = initialTTL;
            this.initialLDT = initialLDT;
            this.initialCDT = initialCDT;
            this.numC2InitialCells = numC2InitialCells;
            this.updateTS = updateTS;
            this.updateTTL = updateTTL;
            this.updateLDT = updateLDT;
            this.updateCDT = updateCDT;
            this.numC2UpdateCells = numC2UpdateCells;
        }

        void execute()
        {
            // Test regular row updates
            Pair<Row, Row> regularRows = makeInitialAndUpdate(r1md, c2md);
            PartitionUpdate initial = PartitionUpdate.singleRowUpdate(metadata, partitionKey, regularRows.left, null);
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, partitionKey, regularRows.right, null);
            validateUpdates(metadata, partitionKey, Arrays.asList(initial, update));

            // Test static row updates
            Pair<Row, Row> staticRows = makeInitialAndUpdate(s3md, c4md);
            PartitionUpdate staticInitial = PartitionUpdate.singleRowUpdate(metadata, partitionKey, null, staticRows.left);
            PartitionUpdate staticUpdate = PartitionUpdate.singleRowUpdate(metadata, partitionKey, null, staticRows.right);
            validateUpdates(metadata, partitionKey, Arrays.asList(staticInitial, staticUpdate));
        }

        private Pair<Row, Row> makeInitialAndUpdate(ColumnMetadata regular, ColumnMetadata complex)
        {
            final ByteBuffer initialValueBB = ByteBufferUtil.bytes(111);
            final ByteBuffer updateValueBB = ByteBufferUtil.bytes(222);

            // Create the initial row to populate the partition with
            Row.Builder initialRowBuilder = BTreeRow.unsortedBuilder();
            initialRowBuilder.newRow(regular.isStatic() ? Clustering.STATIC_CLUSTERING : Clustering.EMPTY);

            initialRowBuilder.addCell(makeCell(regular, initialTS, initialTTL, initialLDT, initialValueBB, null));
            if (initialCDT != DeletionTime.LIVE)
                initialRowBuilder.addComplexDeletion(complex, initialCDT);
            int cellPath = 1000;
            for (int i = 0; i < numC2InitialCells; i++)
                initialRowBuilder.addCell(makeCell(complex, initialTS, initialTTL, initialLDT,
                                                   ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                   CellPath.create(ByteBufferUtil.bytes(cellPath--))));
            Row initialRow = initialRowBuilder.build();

            // Create the update row to modify the partition with
            Row.Builder updateRowBuilder = BTreeRow.unsortedBuilder();
            updateRowBuilder.newRow(regular.isStatic() ? Clustering.STATIC_CLUSTERING : Clustering.EMPTY);

            updateRowBuilder.addCell(makeCell(regular, updateTS, updateTTL, updateLDT, updateValueBB, null));
            if (updateCDT != DeletionTime.LIVE)
                updateRowBuilder.addComplexDeletion(complex, updateCDT);

            // Make multiple update cells to make any issues more pronounced
            cellPath = 1000;
            for (int i = 0; i < numC2UpdateCells; i++)
                updateRowBuilder.addCell(makeCell(complex, updateTS, updateTTL, updateLDT,
                                                  ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                                  CellPath.create(ByteBufferUtil.bytes(cellPath++))));
            Row updateRow = updateRowBuilder.build();
            return Pair.create(initialRow, updateRow);
        }

        Cell<?> makeCell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, ByteBuffer value, CellPath path)
        {
            if (localDeletionTime != Cell.NO_DELETION_TIME) // never a ttl for a tombstone
            {
                ttl = Cell.NO_TTL;
                value = ByteBufferUtil.EMPTY_BYTE_BUFFER;
            }
            return new BufferCell(column, timestamp, ttl, localDeletionTime, value, path);
        }
    }

    void validateUpdates(TableMetadata metadata, DecoratedKey partitionKey, List<PartitionUpdate> updates)
    {
        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(metadata);

        OpOrder opOrder = new OpOrder();
        opOrder.start();
        UpdateTransaction indexer = UpdateTransaction.NO_OP;

        MemtablePool memtablePool = AbstractAllocatorMemtable.createMemtableAllocatorPoolInternal(allocationType,
                                                                                                  HEAP_LIMIT, OFF_HEAP_LIMIT, MEMTABLE_CLEANUP_THRESHOLD, DUMMY_CLEANER);
        MemtableAllocator allocator = memtablePool.newAllocator("test");
        MemtableAllocator recreatedAllocator = memtablePool.newAllocator("recreated");
        try
        {
            // Prepare a partition to receive updates
            AtomicBTreePartition partition = new AtomicBTreePartition(metadataRef, partitionKey, allocator);

            // For each update, apply it and verify the allocator is positive
            long unreleasable = updates.stream().mapToLong(update -> {
                DeletionTime exsDeletion = partition.deletionInfo().getPartitionDeletion();
                DeletionTime updDeletion = update.deletionInfo().getPartitionDeletion();
                long updateUnreleasable = 0;
                if (!BTree.isEmpty(partition.unsafeGetHolder().tree))
                {
                    for (Row updRow : BTree.<Row>iterable(update.holder().tree))
                    {
                        Row exsRow = BTree.find(partition.unsafeGetHolder().tree, partition.metadata().comparator, updRow);
                        updateUnreleasable += getUnreleasableSize(updRow, exsRow, exsDeletion, updDeletion);
                    }
                }
                if (partition.staticRow() != null)
                {
                    updateUnreleasable += getUnreleasableSize(update.staticRow(), partition.unsafeGetHolder().staticRow, exsDeletion, updDeletion);
                }

                OpOrder.Group writeOp = opOrder.getCurrent();
                Cloner cloner = allocator.cloner(writeOp);
                partition.addAll(update, cloner, writeOp, indexer);
                opOrder.newBarrier().issue();

                assertThat(allocator.onHeap().owns()).isGreaterThanOrEqualTo(0L);
                assertThat(allocator.offHeap().owns()).isGreaterThanOrEqualTo(0L);
                return updateUnreleasable;
            }).sum();

            // Now recreate the partition to see if there's a leak in the accounting
            AtomicBTreePartition recreated = new AtomicBTreePartition(metadataRef, partitionKey, recreatedAllocator);
            try (UnfilteredRowIterator iter = partition.unfilteredIterator())
            {
                PartitionUpdate update = PartitionUpdate.fromIterator(iter, ColumnFilter.NONE);
                opOrder.newBarrier().issue();
                OpOrder.Group writeOp = opOrder.getCurrent();
                Cloner cloner = recreatedAllocator.cloner(writeOp);
                recreated.addAll(update, cloner, writeOp, indexer);
            }

            // offheap allocators don't release on heap memory, so expect the same
            long unreleasableOnHeap = 0, unreleasableOffHeap = 0;
            if (allocator.offHeap().owns() > 0) unreleasableOffHeap = unreleasable;
            else unreleasableOnHeap = unreleasable;

            assertThat(recreatedAllocator.offHeap().owns()).isEqualTo(allocator.offHeap().owns() - unreleasableOffHeap);
            assertThat(recreatedAllocator.onHeap().owns()).isEqualTo(allocator.onHeap().owns() - unreleasableOnHeap);
        }
        finally
        {
            // Release test resources
            recreatedAllocator.setDiscarding();
            recreatedAllocator.setDiscarded();
            allocator.setDiscarding();
            allocator.setDiscarded();
            try
            {
                memtablePool.shutdownAndWait(1, TimeUnit.SECONDS);
            }
            catch (Throwable tr)
            {
                // too bad
            }
        }
    }

    private long getUnreleasableSize(Row updRow, Row exsRow, DeletionTime exsDeletion, DeletionTime updDeletion)
    {
        if (exsRow.deletion().supersedes(exsDeletion))
            exsDeletion = exsRow.deletion().time();
        if (updRow.deletion().supersedes(updDeletion))
            updDeletion = updRow.deletion().time();

        long size = 0;
        for (ColumnData exsCd : exsRow.columnData())
        {
            ColumnData updCd = updRow.getColumnData(exsCd.column());
            if (exsCd instanceof Cell)
            {
                Cell exsCell = (Cell) exsCd, updCell = (Cell) updCd;
                if (updDeletion.deletes(exsCell))
                    size += sizeOf(exsCell);
                else if (updCell != null && Cells.reconcile(exsCell, updCell) != exsCell && !exsDeletion.deletes(updCell))
                    size += sizeOf(exsCell);
            }
            else
            {
                ComplexColumnData exsCcd = (ComplexColumnData) exsCd;
                ComplexColumnData updCcd = (ComplexColumnData) updCd;

                DeletionTime activeExsDeletion = exsDeletion;
                DeletionTime activeUpdDeletion = updDeletion;
                if (exsCcd.complexDeletion().supersedes(exsDeletion))
                    activeExsDeletion = exsCcd.complexDeletion();
                if (updCcd != null && updCcd.complexDeletion().supersedes(updDeletion))
                    activeUpdDeletion = updCcd.complexDeletion();

                for (Cell exsCell : exsCcd)
                {
                    Cell updCell = updCcd == null ? null : updCcd.getCell(exsCell.path());

                    if (activeUpdDeletion.deletes(exsCell))
                        size += sizeOf(exsCell);
                    else if (updCell != null && (Cells.reconcile(exsCell, updCell) != exsCell && !activeExsDeletion.deletes(updCell)))
                        size += sizeOf(exsCell);
                }
            }
        }
        return size;
    }

    private static long sizeOf(Cell cell)
    {
        if (cell instanceof NativeCell)
            return ((NativeCell) cell).offHeapSize();
        return cell.valueSize() + (cell.path() == null ? 0 : cell.path().dataSize());
    }
}