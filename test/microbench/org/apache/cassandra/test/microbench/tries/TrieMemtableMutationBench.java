/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench.tries;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/// Measures the time to apply partition updates to a trie memtable, i.e. the deletion-aware in-memory trie mutation
/// path, with and without deletions in the updates and in the memtable. The updates are prepared in trie form, so that
/// the conversion of the updates is not included.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class TrieMemtableMutationBench extends CQLTester
{
    static final boolean PRINT_DIAGNOSTICS = false;

    public enum Scenario
    {
        /// Insert rows into an empty memtable.
        INSERT,
        /// Insert rows into partitions which have range tombstones (i.e. a deletion branch) in the memtable.
        INSERT_OVER_DELETIONS,
        /// Delete existing rows using row deletions.
        ROW_DELETE,
        /// Delete existing rows using range tombstones, each covering 10 rows.
        RANGE_DELETE,
        /// Delete existing partitions using partition deletions.
        PARTITION_DELETE
    }

    @Param({"INSERT", "INSERT_OVER_DELETIONS", "ROW_DELETE", "RANGE_DELETE", "PARTITION_DELETE"})
    Scenario scenario;

    @Param({"1", "100", "10000"})
    int rowsPerPartition;

    @Param({"100000"})
    int rowCount;

    @Param({"false"})
    boolean blockedBarriers;

    public enum Mode
    {
        /// Apply the updates using `Memtable.put`.
        PUT,
        /// Apply the updates directly to one memtable shard's trie using its updater. This excludes the memtable's
        /// locking and memory accounting, and measures the trie mutation (including the updater's callbacks) only.
        MERGE
    }

    @Param({"PUT", "MERGE"})
    Mode mode;

    ColumnFamilyStore cfs;
    List<PartitionUpdate> existing;
    List<PartitionUpdate> updates;
    Memtable memtable;
    TriePartitionUpdater updater;
    Object cellDataBufferManager;
    Field opOrderGroupField;
    OpOrder.Group barrierBlock;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.setAutoSnapshot(false);
        String keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        String table = createTable(keyspace, "CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY(pk, ck)) with compression = {'enabled': false} and memtable = 'trie';");
        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        TableMetadata metadata = cfs.metadata();

        int partitions = Math.max(1, rowCount / rowsPerPartition);
        int rows = Math.min(rowCount, rowsPerPartition);
        existing = new ArrayList<>();
        updates = new ArrayList<>();
        for (long pk = 0; pk < partitions; ++pk)
        {
            switch (scenario)
            {
                case INSERT:
                    for (long ck = 0; ck < rows; ++ck)
                        updates.add(rowsUpdate(metadata, pk, ck, ck + 1, 2));
                    break;
                case INSERT_OVER_DELETIONS:
                {
                    PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk).timestamp(1);
                    for (long ck = 0; ck < rows; ck += 10)
                        builder.addRangeTombstone().start(ck).end(ck + 5).inclStart();
                    existing.add(asTrie(builder.build()));
                    for (long ck = 0; ck < rows; ++ck)
                        updates.add(rowsUpdate(metadata, pk, ck, ck + 1, 2));
                    break;
                }
                case ROW_DELETE:
                    existing.add(rowsUpdate(metadata, pk, 0, rows, 1));
                    for (long ck = 0; ck < rows; ++ck)
                    {
                        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk).timestamp(2);
                        builder.row(ck).delete();
                        updates.add(asTrie(builder.build()));
                    }
                    break;
                case RANGE_DELETE:
                    existing.add(rowsUpdate(metadata, pk, 0, rows, 1));
                    for (long ck = 0; ck < rows; ck += 10)
                    {
                        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk).timestamp(2);
                        builder.addRangeTombstone().start(ck).end(ck + 10).inclStart();
                        updates.add(asTrie(builder.build()));
                    }
                    break;
                case PARTITION_DELETE:
                    existing.add(rowsUpdate(metadata, pk, 0, rows, 1));
                    updates.add(asTrie(PartitionUpdate.simpleBuilder(metadata, pk).timestamp(2).delete().build()));
                    break;
                default:
                    throw new AssertionError();
            }
        }
        System.err.format("%s: %d existing updates, %d measured updates in %d partitions\n",
                          scenario, existing.size(), updates.size(), partitions);
    }

    private static PartitionUpdate rowsUpdate(TableMetadata metadata, long pk, long ckStart, long ckEnd, long timestamp)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, pk).timestamp(timestamp);
        for (long ck = ckStart; ck < ckEnd; ++ck)
            builder.row(ck).add("v1", ck).add("v2", "value" + ck);
        return asTrie(builder.build());
    }

    private static PartitionUpdate asTrie(PartitionUpdate update)
    {
        return TriePartitionUpdate.asTrieUpdate(update);
    }

    @Setup(Level.Invocation)
    public void prepareMemtable()
    {
        memtable = cfs.createMemtable(new AtomicReference<>(CommitLogPosition.NONE));
        if (mode == Mode.MERGE)
        {
            try
            {
                Field shardsField = memtable.getClass().getDeclaredField("shards");
                shardsField.setAccessible(true);
                TrieMemtable.MemtableShard shard = ((TrieMemtable.MemtableShard[]) shardsField.get(memtable))[0];
                Field dataField = shard.getClass().getDeclaredField("data");
                dataField.setAccessible(true);
                @SuppressWarnings("unchecked")
                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data = (InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>) dataField.get(shard);
                updater = new TriePartitionUpdater(shard, data);
                Field cellDataField = shard.getClass().getDeclaredField("cellDataBufferManager");
                cellDataField.setAccessible(true);
                cellDataBufferManager = cellDataField.get(shard);
                opOrderGroupField = findField(cellDataBufferManager.getClass(), "opOrderGroup");
                opOrderGroupField.setAccessible(true);
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException(e);
            }
        }
        apply(existing);
        if (blockedBarriers)
            barrierBlock = cfs.readOrdering().start();
    }

    private static Field findField(Class<?> cls, String name) throws NoSuchFieldException
    {
        for (Class<?> c = cls; c != null; c = c.getSuperclass())
        {
            try
            {
                return c.getDeclaredField(name);
            }
            catch (NoSuchFieldException e)
            {
                // try superclass
            }
        }
        throw new NoSuchFieldException(name);
    }

    @TearDown(Level.Invocation)
    public void discardMemtable() throws Exception
    {
        if (PRINT_DIAGNOSTICS)
            printDiagnostics();
        if (barrierBlock != null)
        {
            barrierBlock.close();
            barrierBlock = null;
        }

        OpOrder.Barrier barrier = cfs.keyspace.writeOrder.newBarrier();
        Memtable.LastCommitLogPosition position = new Memtable.LastCommitLogPosition(CommitLog.instance.getCurrentPosition());
        memtable.switchOut(barrier, new AtomicReference<>(position));
        barrier.issue();

        memtable.discard();
        memtable = null;
    }

    /// Prints the allocated space and the size of the cell recycling pipeline of the memtable's tries.
    private void printDiagnostics() throws Exception
    {
        long allocated = 0;
        long inPipeline = 0;
        Field shardsField = memtable.getClass().getDeclaredField("shards");
        shardsField.setAccessible(true);
        for (Object shard : (Object[]) shardsField.get(memtable))
        {
            Field dataField = shard.getClass().getDeclaredField("data");
            dataField.setAccessible(true);
            Object trie = dataField.get(shard);
            Field bufferManagerField = Class.forName("org.apache.cassandra.db.tries.InMemoryReadTrie").getDeclaredField("bufferManager");
            bufferManagerField.setAccessible(true);
            Object bufferManager = bufferManagerField.get(trie);
            Method allocatedPos = bufferManager.getClass().getDeclaredMethod("getAllocatedPos");
            allocatedPos.setAccessible(true);
            allocated += (Integer) allocatedPos.invoke(bufferManager);
            Field allocatorField = bufferManager.getClass().getDeclaredField("cellAllocator");
            allocatorField.setAccessible(true);
            Object allocator = allocatorField.get(bufferManager);
            Method count = Class.forName("org.apache.cassandra.db.tries.MemoryAllocationStrategy").getDeclaredMethod("indexCountInPipeline");
            count.setAccessible(true);
            inPipeline += (Long) count.invoke(allocator);
        }
        System.err.format("DIAG %s rowsPerPartition=%d: allocated %,d bytes, %,d cells in recycling pipeline\n",
                          scenario, rowsPerPartition, allocated, inPipeline);
    }

    @TearDown(Level.Trial)
    public void teardown() throws Throwable
    {
        CommitLog.instance.shutdownBlocking();
        CQLTester.tearDownClass();
        CQLTester.cleanup();
    }

    @Benchmark
    public Memtable applyUpdates()
    {
        apply(updates);
        return memtable;
    }

    private void apply(List<PartitionUpdate> list)
    {
        try (OpOrder.Group opGroup = Keyspace.writeOrder.start())
        {
            if (mode == Mode.PUT)
            {
                for (PartitionUpdate update : list)
                    memtable.put(update, UpdateTransaction.NO_OP, opGroup);
            }
            else
            {
                opOrderGroupField.set(cellDataBufferManager, opGroup);
                for (PartitionUpdate update : list)
                    updater.mergeUpdate(TriePartitionUpdate.asMergableTrie(update));
                opOrderGroupField.set(cellDataBufferManager, null);
            }
        }
        catch (ReflectiveOperationException | TrieSpaceExhaustedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
