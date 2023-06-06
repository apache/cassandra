package org.apache.cassandra;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information * regarding copyright ownership.  The ASF licenses this file
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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.AbstractReadCommandBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Directories.DataDirectory;
import org.apache.cassandra.db.DisallowedDirectories;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionTasks;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderWithFilter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Throwables;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.defaultanswers.ForwardsInvocations;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Util
{
    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    private static List<UUID> hostIdPool = new ArrayList<>();

    public static IPartitioner testPartitioner()
    {
        return DatabaseDescriptor.getPartitioner();
    }

    public static DecoratedKey dk(String key)
    {
        return testPartitioner().decorateKey(ByteBufferUtil.bytes(key));
    }

    public static DecoratedKey dk(String key, AbstractType<?> type)
    {
        return testPartitioner().decorateKey(type.fromString(key));
    }

    public static DecoratedKey dk(ByteBuffer key)
    {
        return testPartitioner().decorateKey(key);
    }

    public static PartitionPosition rp(String key)
    {
        return rp(key, testPartitioner());
    }

    public static PartitionPosition rp(String key, IPartitioner partitioner)
    {
        return PartitionPosition.ForKey.get(ByteBufferUtil.bytes(key), partitioner);
    }

    public static Clustering<?> clustering(ClusteringComparator comparator, Object... o)
    {
        return comparator.make(o);
    }

    public static Token token(int key)
    {
        return testPartitioner().getToken(ByteBufferUtil.bytes(key));
    }

    public static Token token(String key)
    {
        return testPartitioner().getToken(ByteBufferUtil.bytes(key));
    }

    public static Range<PartitionPosition> range(String left, String right)
    {
        return new Range<>(rp(left), rp(right));
    }

    public static Range<PartitionPosition> range(IPartitioner p, String left, String right)
    {
        return new Range<>(rp(left, p), rp(right, p));
    }

    //Test helper to make an iterator iterable once
    public static <T> Iterable<T> once(final Iterator<T> source)
    {
        return new Iterable<T>()
        {
            private AtomicBoolean exhausted = new AtomicBoolean();
            public Iterator<T> iterator()
            {
                Preconditions.checkState(!exhausted.getAndSet(true));
                return source;
            }
        };
    }

    public static ByteBuffer getBytes(long v)
    {
        byte[] bytes = new byte[8];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putLong(v);
        bb.rewind();
        return bb;
    }

    public static ByteBuffer getBytes(int v)
    {
        byte[] bytes = new byte[4];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        bb.putInt(v);
        bb.rewind();
        return bb;
    }

    /**
     * Writes out a bunch of mutations for a single column family.
     *
     * @param mutations A group of Mutations for the same keyspace and column family.
     * @return The ColumnFamilyStore that was used.
     */
    public static ColumnFamilyStore writeColumnFamily(List<Mutation> mutations)
    {
        IMutation first = mutations.get(0);
        String keyspaceName = first.getKeyspaceName();
        TableId tableId = first.getTableIds().iterator().next();

        for (Mutation rm : mutations)
            rm.applyUnsafe();

        ColumnFamilyStore store = Keyspace.open(keyspaceName).getColumnFamilyStore(tableId);
        Util.flush(store);
        return store;
    }

    public static boolean equalsCounterId(CounterId n, ByteBuffer context, int offset)
    {
        return CounterId.wrap(context, context.position() + offset).equals(n);
    }

    /**
     * Creates initial set of nodes and tokens. Nodes are added to StorageService as 'normal'
     */
    public static void createInitialRing(StorageService ss, IPartitioner partitioner, List<Token> endpointTokens,
                                         List<Token> keyTokens, List<InetAddressAndPort> hosts, List<UUID> hostIds, int howMany)
        throws UnknownHostException
    {
        // Expand pool of host IDs as necessary
        for (int i = hostIdPool.size(); i < howMany; i++)
            hostIdPool.add(UUID.randomUUID());

        boolean endpointTokenPrefilled = endpointTokens != null && !endpointTokens.isEmpty();
        for (int i=0; i<howMany; i++)
        {
            if(!endpointTokenPrefilled)
                endpointTokens.add(new BigIntegerToken(String.valueOf(10 * i)));
            keyTokens.add(new BigIntegerToken(String.valueOf(10 * i + 5)));
            hostIds.add(hostIdPool.get(i));
        }

        for (int i=0; i<endpointTokens.size(); i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByName("127.0.0." + String.valueOf(i + 1));
            Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), MessagingService.current_version, 1);
            Gossiper.instance.injectApplicationState(ep, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS_WITH_PORT,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

    public static Future<?> compactAll(ColumnFamilyStore cfs, long gcBefore)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(cfs, descriptors, gcBefore);
    }

    public static void compact(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        long gcBefore = cfs.gcBefore(FBUtilities.nowInSeconds());
        try (CompactionTasks tasks = cfs.getCompactionStrategyManager().getUserDefinedTasks(sstables, gcBefore))
        {
            for (AbstractCompactionTask task : tasks)
                task.execute(ActiveCompactionsTracker.NOOP);
        }
    }

    public static void expectEOF(Callable<?> callable)
    {
        expectException(callable, EOFException.class);
    }

    public static void expectException(Callable<?> callable, Class<?> exception)
    {
        boolean thrown = false;

        try
        {
            callable.call();
        }
        catch (Throwable e)
        {
            assert e.getClass().equals(exception) : e.getClass().getName() + " is not " + exception.getName();
            thrown = true;
        }

        assert thrown : exception.getName() + " not received";
    }

    public static AbstractReadCommandBuilder.SinglePartitionBuilder cmd(ColumnFamilyStore cfs, Object... partitionKey)
    {
        return new AbstractReadCommandBuilder.SinglePartitionBuilder(cfs, makeKey(cfs.metadata(), partitionKey));
    }

    public static AbstractReadCommandBuilder.PartitionRangeBuilder cmd(ColumnFamilyStore cfs)
    {
        return new AbstractReadCommandBuilder.PartitionRangeBuilder(cfs);
    }

    static DecoratedKey makeKey(TableMetadata metadata, Object... partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = metadata.partitionKeyAsClusteringComparator().make(partitionKey).serializeAsPartitionKey();
        return metadata.partitioner.decorateKey(key);
    }

    public static void assertEmptyUnfiltered(ReadCommand command)
    {
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
        {
            if (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().partitionKeyType.getString(partition.partitionKey().getKey()));
                }
            }
        }
    }

    public static void assertEmpty(ReadCommand command)
    {
        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = command.executeInternal(executionController))
        {
            if (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().partitionKeyType.getString(partition.partitionKey().getKey()));
                }
            }
        }
    }

    public static List<ImmutableBTreePartition> getAllUnfiltered(ReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController())
        {
            return getAllUnfiltered(command, controller);
        }
    }
    
    public static List<ImmutableBTreePartition> getAllUnfiltered(ReadCommand command, ReadExecutionController controller)
    {
        List<ImmutableBTreePartition> results = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    results.add(ImmutableBTreePartition.create(partition));
                }
            }
        }
        return results;
    }

    public static List<FilteredPartition> getAll(ReadCommand command)
    {
        try (ReadExecutionController controller = command.executionController())
        {
            return getAll(command, controller);
        }
    }
    
    public static List<FilteredPartition> getAll(ReadCommand command, ReadExecutionController controller)
    {
        List<FilteredPartition> results = new ArrayList<>();
        try (PartitionIterator iterator = command.executeInternal(controller))
        {
            while (iterator.hasNext())
            {
                try (RowIterator partition = iterator.next())
                {
                    results.add(FilteredPartition.create(partition));
                }
            }
        }
        return results;
    }

    public static Row getOnlyRowUnfiltered(ReadCommand cmd)
    {
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
        {
            assert iterator.hasNext() : "Expecting one row in one partition but got nothing";
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";

                assert partition.hasNext() : "Expecting one row in one partition but got an empty partition";
                Row row = ((Row)partition.next());
                assert !partition.hasNext() : "Expecting a single row but got more";
                return row;
            }
        }
    }

    public static Row getOnlyRow(ReadCommand cmd)
    {
        try (ReadExecutionController executionController = cmd.executionController();
             PartitionIterator iterator = cmd.executeInternal(executionController))
        {
            assert iterator.hasNext() : "Expecting one row in one partition but got nothing";
            try (RowIterator partition = iterator.next())
            {
                assert partition.hasNext() : "Expecting one row in one partition but got an empty partition";
                Row row = partition.next();
                assert !partition.hasNext() : "Expecting a single row but got more";
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                return row;
            }
        }
    }

    public static ImmutableBTreePartition getOnlyPartitionUnfiltered(ReadCommand cmd)
    {
        try (ReadExecutionController controller = cmd.executionController())
        {
            return getOnlyPartitionUnfiltered(cmd, controller);
        }
    }
    
    public static ImmutableBTreePartition getOnlyPartitionUnfiltered(ReadCommand cmd, ReadExecutionController controller)
    {
        try (UnfilteredPartitionIterator iterator = cmd.executeLocally(controller))
        {
            assert iterator.hasNext() : "Expecting a single partition but got nothing";
            try (UnfilteredRowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                return ImmutableBTreePartition.create(partition);
            }
        }
    }

    public static FilteredPartition getOnlyPartition(ReadCommand cmd)
    {
        return getOnlyPartition(cmd, false);
    }
    
    public static FilteredPartition getOnlyPartition(ReadCommand cmd, boolean trackRepairedStatus)
    {
        try (ReadExecutionController executionController = cmd.executionController(trackRepairedStatus);
             PartitionIterator iterator = cmd.executeInternal(executionController))
        {
            assert iterator.hasNext() : "Expecting a single partition but got nothing";
            try (RowIterator partition = iterator.next())
            {
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                return FilteredPartition.create(partition);
            }
        }
    }

    public static UnfilteredRowIterator apply(Mutation mutation)
    {
        mutation.apply();
        assert mutation.getPartitionUpdates().size() == 1;
        return mutation.getPartitionUpdates().iterator().next().unfilteredIterator();
    }

    public static Cell<?> cell(ColumnFamilyStore cfs, Row row, String columnName)
    {
        ColumnMetadata def = cfs.metadata().getColumn(ByteBufferUtil.bytes(columnName));
        assert def != null;
        return row.getCell(def);
    }

    public static Row row(Partition partition, Object... clustering)
    {
        return partition.getRow(partition.metadata().comparator.make(clustering));
    }

    public static void assertCellValue(Object value, ColumnFamilyStore cfs, Row row, String columnName)
    {
        Cell<?> cell = cell(cfs, row, columnName);
        assert cell != null : "Row " + row.toString(cfs.metadata()) + " has no cell for " + columnName;
        assertEquals(value, Cells.composeValue(cell, cell.column().type));
    }

    public static void consume(UnfilteredRowIterator iter)
    {
        try (UnfilteredRowIterator iterator = iter)
        {
            while (iter.hasNext())
                iter.next();
        }
    }

    public static void consume(UnfilteredPartitionIterator iterator)
    {
        while (iterator.hasNext())
        {
            consume(iterator.next());
        }
    }

    public static int size(PartitionIterator iter)
    {
        int size = 0;
        while (iter.hasNext())
        {
            ++size;
            iter.next().close();
        }
        return size;
    }

    public static boolean equal(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.columns(), b.columns())
            && Objects.equals(a.stats(), b.stats())
            && sameContent(a, b);
    }

    // Test equality of the iterators, but without caring too much about the "metadata" of said iterator. This is often
    // what we want in tests. In particular, the columns() reported by the iterators will sometimes differ because they
    // are a superset of what the iterator actually contains, and depending on the method used to get each iterator
    // tested, one may include a defined column the other don't while there is not actual content for that column.
    public static boolean sameContent(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.metadata(), b.metadata())
            && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
            && Objects.equals(a.partitionKey(), b.partitionKey())
            && Objects.equals(a.partitionLevelDeletion(), b.partitionLevelDeletion())
            && Objects.equals(a.staticRow(), b.staticRow())
            && Iterators.elementsEqual(a, b);
    }

    public static boolean sameContent(RowIterator a, RowIterator b)
    {
        return Objects.equals(a.metadata(), b.metadata())
               && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
               && Objects.equals(a.partitionKey(), b.partitionKey())
               && Objects.equals(a.staticRow(), b.staticRow())
               && Iterators.elementsEqual(a, b);
    }

    public static boolean sameContent(Mutation a, Mutation b)
    {
        if (!a.key().equals(b.key()) || !a.getTableIds().equals(b.getTableIds()))
            return false;

        for (PartitionUpdate update : a.getPartitionUpdates())
        {
            if (!sameContent(update.unfilteredIterator(), b.getPartitionUpdate(update.metadata()).unfilteredIterator()))
                return false;
        }
        return true;
    }

    // moved & refactored from KeyspaceTest in < 3.0
    public static void assertColumns(Row row, String... expectedColumnNames)
    {
        Iterator<Cell<?>> cells = row == null ? Collections.emptyIterator() : row.cells().iterator();
        String[] actual = Iterators.toArray(Iterators.transform(cells, new Function<Cell<?>, String>()
        {
            public String apply(Cell<?> cell)
            {
                return cell.column().name.toString();
            }
        }), String.class);

        assert Arrays.equals(actual, expectedColumnNames)
        : String.format("Columns [%s])] is not expected [%s]",
                        ((row == null) ? "" : row.columns().toString()),
                        StringUtils.join(expectedColumnNames, ","));
    }

    public static void assertColumn(TableMetadata cfm, Row row, String name, String value, long timestamp)
    {
        Cell<?> cell = row.getCell(cfm.getColumn(new ColumnIdentifier(name, true)));
        assertColumn(cell, value, timestamp);
    }

    public static void assertColumn(Cell<?> cell, String value, long timestamp)
    {
        assertNotNull(cell);
        assertEquals(0, ByteBufferUtil.compareUnsigned(cell.buffer(), ByteBufferUtil.bytes(value)));
        assertEquals(timestamp, cell.timestamp());
    }

    public static void assertClustering(TableMetadata cfm, Row row, Object... clusteringValue)
    {
        assertEquals(row.clustering().size(), clusteringValue.length);
        assertEquals(0, cfm.comparator.compare(row.clustering(), cfm.comparator.make(clusteringValue)));
    }

    public static PartitionerSwitcher switchPartitioner(IPartitioner p)
    {
        return new PartitionerSwitcher(p);
    }

    public static class PartitionerSwitcher implements AutoCloseable
    {
        final IPartitioner oldP;
        final IPartitioner newP;

        public PartitionerSwitcher(IPartitioner partitioner)
        {
            newP = partitioner;
            oldP = StorageService.instance.setPartitionerUnsafe(partitioner);
        }

        public void close()
        {
            IPartitioner p = StorageService.instance.setPartitionerUnsafe(oldP);
            assert p == newP;
        }
    }

    public static void spinAssertEquals(Object expected, Supplier<Object> actualSupplier, int timeoutInSeconds)
    {
        spinAssertEquals(null, expected, actualSupplier, timeoutInSeconds, TimeUnit.SECONDS);
    }

    public static <T> void spinAssertEquals(String message, T expected, Supplier<? extends T> actualSupplier, long timeout, TimeUnit timeUnit)
    {
        Awaitility.await()
                  .pollInterval(Duration.ofMillis(100))
                  .pollDelay(0, TimeUnit.MILLISECONDS)
                  .atMost(timeout, timeUnit)
                  .untilAsserted(() -> assertThat(message, actualSupplier.get(), equalTo(expected)));
    }

    public static void joinThread(Thread thread) throws InterruptedException
    {
        thread.join(10000);
    }

    public static AssertionError runCatchingAssertionError(Runnable test)
    {
        try
        {
            test.run();
            return null;
        }
        catch (AssertionError e)
        {
            return e;
        }
    }

    /**
     * Wrapper function used to run a test that can sometimes flake for uncontrollable reasons.
     *
     * If the given test fails on the first run, it is executed the given number of times again, expecting all secondary
     * runs to succeed. If they do, the failure is understood as a flake and the test is treated as passing.
     *
     * Do not use this if the test is deterministic and its success is not influenced by external factors (such as time,
     * selection of random seed, network failures, etc.). If the test can be made independent of such factors, it is
     * probably preferable to do so rather than use this method.
     *
     * @param test The test to run.
     * @param rerunsOnFailure How many times to re-run it if it fails. All reruns must pass.
     * @param message Message to send to System.err on initial failure.
     */
    public static void flakyTest(Runnable test, int rerunsOnFailure, String message)
    {
        AssertionError e = runCatchingAssertionError(test);
        if (e == null)
            return;     // success

        logger.info("Test failed. {}", message, e);
        logger.info("Re-running {} times to verify it isn't failing more often than it should.", rerunsOnFailure);

        int rerunsFailed = 0;
        for (int i = 0; i < rerunsOnFailure; ++i)
        {
            AssertionError t = runCatchingAssertionError(test);
            if (t != null)
            {
                ++rerunsFailed;
                e.addSuppressed(t);

                logger.debug("Test failed again, total num failures: {}", rerunsFailed, t);
            }
        }
        if (rerunsFailed > 0)
        {
            logger.error("Test failed in {} of the {} reruns.", rerunsFailed, rerunsOnFailure);
            throw e;
        }

        logger.info("All reruns succeeded. Failure treated as flake.");
    }

    // for use with Optional in tests, can be used as an argument to orElseThrow
    public static Supplier<AssertionError> throwAssert(final String message)
    {
        return () -> new AssertionError(message);
    }

    public static class UnfilteredSource extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        public UnfilteredSource(TableMetadata metadata, DecoratedKey partitionKey, Row staticRow, Iterator<Unfiltered> content)
        {
            super(metadata,
                  partitionKey,
                  DeletionTime.LIVE,
                  metadata.regularAndStaticColumns(),
                  staticRow != null ? staticRow : Rows.EMPTY_STATIC_ROW,
                  false,
                  EncodingStats.NO_STATS);
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    public static UnfilteredPartitionIterator executeLocally(PartitionRangeReadCommand command,
                                                             ColumnFamilyStore cfs,
                                                             ReadExecutionController controller)
    {
        return command.queryStorage(cfs, controller);
    }

    public static Closeable markDirectoriesUnwriteable(ColumnFamilyStore cfs)
    {
        try
        {
            for ( ; ; )
            {
                DataDirectory dir = cfs.getDirectories().getWriteableLocation(1);
                DisallowedDirectories.maybeMarkUnwritable(cfs.getDirectories().getLocationForDisk(dir));
            }
        }
        catch (IOError e)
        {
            // Expected -- marked all directories as unwritable
        }
        return () -> DisallowedDirectories.clearUnwritableUnsafe();
    }

    public static PagingState makeSomePagingState(ProtocolVersion protocolVersion)
    {
        return makeSomePagingState(protocolVersion, Integer.MAX_VALUE);
    }

    public static PagingState makeSomePagingState(ProtocolVersion protocolVersion, int remainingInPartition)
    {
        TableMetadata metadata =
            TableMetadata.builder("ks", "tbl")
                         .addPartitionKeyColumn("k", AsciiType.instance)
                         .addClusteringColumn("c1", AsciiType.instance)
                         .addClusteringColumn("c2", Int32Type.instance)
                         .addRegularColumn("myCol", AsciiType.instance)
                         .build();

        ByteBuffer pk = ByteBufferUtil.bytes("someKey");

        ColumnMetadata def = metadata.getColumn(new ColumnIdentifier("myCol", false));
        Clustering<?> c = Clustering.make(ByteBufferUtil.bytes("c1"), ByteBufferUtil.bytes(42));
        Row row = BTreeRow.singleCellRow(c, BufferCell.live(def, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        PagingState.RowMark mark = PagingState.RowMark.create(metadata, row, protocolVersion);
        return new PagingState(pk, mark, 10, remainingInPartition);
    }

    public static void assertRCEquals(ReplicaCollection<?> a, ReplicaCollection<?> b)
    {
        assertTrue(a + " not equal to " + b, Iterables.elementsEqual(a, b));
    }

    public static void assertNotRCEquals(ReplicaCollection<?> a, ReplicaCollection<?> b)
    {
        assertFalse(a + " equal to " + b, Iterables.elementsEqual(a, b));
    }

    /**
     * Makes sure that the sstables on disk are the same ones as the cfs live sstables (that they have the same generation)
     */
    public static void assertOnDiskState(ColumnFamilyStore cfs, int expectedSSTableCount)
    {
        LifecycleTransaction.waitForDeletions();
        assertEquals(expectedSSTableCount, cfs.getLiveSSTables().size());
        Set<SSTableId> liveIdentifiers = cfs.getLiveSSTables().stream()
                                            .map(sstable -> sstable.descriptor.id)
                                            .collect(Collectors.toSet());
        int fileCount = 0;
        for (File f : cfs.getDirectories().getCFDirectories())
        {
            for (File sst : f.tryList())
            {
                if (sst.name().contains("Data"))
                {
                    Descriptor d = Descriptor.fromFileWithComponent(sst, false).left;
                    assertTrue(liveIdentifiers.contains(d.id));
                    fileCount++;
                }
            }
        }
        assertEquals(expectedSSTableCount, fileCount);
    }

    public static ByteBuffer generateMurmurCollision(ByteBuffer original, byte... bytesToAdd)
    {
        // Round size up to 16, and add another 16 bytes
        ByteBuffer collision = ByteBuffer.allocate((original.remaining() + bytesToAdd.length + 31) & -16);
        collision.put(original);    // we can use this as a copy of original with 0s appended at the end

        original.flip();

        long c1 = 0x87c37b91114253d5L;
        long c2 = 0x4cf5ad432745937fL;

        long h1 = 0;
        long h2 = 0;

        // Get hash of original
        int index = 0;
        final int length = original.limit();
        while (index <= length - 16)
        {
            long k1 = Long.reverseBytes(collision.getLong(index + 0));
            long k2 = Long.reverseBytes(collision.getLong(index + 8));

            // 16 bytes
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;

            index += 16;
        }

        long oh1 = h1;
        long oh2 = h2;

        // Process final unfilled chunk, but only adjust the original hash value
        if (index < length)
        {
            long k1 = Long.reverseBytes(collision.getLong(index + 0));
            long k2 = Long.reverseBytes(collision.getLong(index + 8));

            // 16 bytes
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            oh1 ^= k1;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            oh2 ^= k2;
        }

        // These are the hashes the original would provide, before final mixing
        oh1 ^= original.capacity();
        oh2 ^= original.capacity();

        // Fill in the remaining bytes before the last 16 and get their hash
        collision.put(bytesToAdd);
        while ((collision.position() & 0x0f) != 0)
            collision.put((byte) 0);

        while (index < collision.position())
        {
            long k1 = Long.reverseBytes(collision.getLong(index + 0));
            long k2 = Long.reverseBytes(collision.getLong(index + 8));

            // 16 bytes
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;

            index += 16;
        }

        // Working backwards, we must get this hash pair
        long th1 = h1;
        long th2 = h2;

        // adjust ohx with length
        h1 = oh1 ^ collision.capacity();
        h2 = oh2 ^ collision.capacity();

        // Get modulo-long inverses of the multipliers used in the computation
        long i5i = inverse(5L);
        long c1i = inverse(c1);
        long c2i = inverse(c2);

        // revert one step
        h2 -= 0x38495ab5;
        h2 *= i5i;
        h2 -= h1;
        h2 = rotl64(h2, 33);

        h1 -= 0x52dce729;
        h1 *= i5i;
        h1 -= th2;  // use h2 before it's adjusted with k2
        h1 = rotl64(h1, 37);

        // extract the required modifiers and applies the inverse of their transformation
        long k1 = h1 ^ th1;
        k1 = c2i * k1;
        k1 = rotl64(k1, 33);
        k1 = c1i * k1;

        long k2 = h2 ^ th2;
        k2 = c1i * k2;
        k2 = rotl64(k2, 31);
        k2 = c2i * k2;

        collision.putLong(Long.reverseBytes(k1));
        collision.putLong(Long.reverseBytes(k2));
        collision.flip();

        return collision;
    }

    // Assumes a and b are positive
    private static BigInteger[] xgcd(BigInteger a, BigInteger b) {
        BigInteger x = a, y = b;
        BigInteger[] qrem;
        BigInteger[] result = new BigInteger[3];
        BigInteger x0 = BigInteger.ONE, x1 = BigInteger.ZERO;
        BigInteger y0 = BigInteger.ZERO, y1 = BigInteger.ONE;
        while (true)
        {
            qrem = x.divideAndRemainder(y);
            x = qrem[1];
            x0 = x0.subtract(y0.multiply(qrem[0]));
            x1 = x1.subtract(y1.multiply(qrem[0]));
            if (x.equals(BigInteger.ZERO))
            {
                result[0] = y;
                result[1] = y0;
                result[2] = y1;
                return result;
            }

            qrem = y.divideAndRemainder(x);
            y = qrem[1];
            y0 = y0.subtract(x0.multiply(qrem[0]));
            y1 = y1.subtract(x1.multiply(qrem[0]));
            if (y.equals(BigInteger.ZERO))
            {
                result[0] = x;
                result[1] = x0;
                result[2] = x1;
                return result;
            }
        }
    }

    /**
     * Find a mupltiplicative inverse for the given multiplier for long, i.e.
     * such that x * inverse(x) = 1 where * is long multiplication.
     * In other words, such an integer that x * inverse(x) == 1 (mod 2^64).
     */
    public static long inverse(long multiplier)
    {
        final BigInteger modulus = BigInteger.ONE.shiftLeft(64);
        // Add the modulus to the multiplier to avoid problems with negatives (a + m == a (mod m))
        BigInteger[] gcds = xgcd(BigInteger.valueOf(multiplier).add(modulus), modulus);
        // xgcd gives g, a and b, such that ax + bm = g
        // ie, ax = g (mod m). Return a
        assert gcds[0].equals(BigInteger.ONE) : "Even number " + multiplier + " has no long inverse";
        return gcds[1].longValueExact();
    }

    public static long rotl64(long v, int n)
    {
        return ((v << n) | (v >>> (64 - n)));
    }

    /**
     * Disable bloom filter on all sstables of given table
     */
    public static void disableBloomFilter(ColumnFamilyStore cfs)
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.UNKNOWN))
        {
            for (SSTableReader sstable : sstables)
            {
                sstable = ((SSTableReaderWithFilter) sstable).cloneAndReplace(FilterFactory.AlwaysPresent);
                txn.update(sstable, true);
                txn.checkpoint();
            }
            txn.finish();
        }

        for (SSTableReader reader : cfs.getLiveSSTables())
            assertEquals(0, ((SSTableReaderWithFilter) reader).getFilterOffHeapSize());
    }

    /**
     * Setups Gossiper to mimic the upgrade behaviour when {@link Gossiper#isUpgradingFromVersionLowerThan(CassandraVersion)}
     * or {@link Gossiper#hasMajorVersion3Nodes()} is called.
     */
    public static void setUpgradeFromVersion(String version)
    {
        int v = Optional.ofNullable(Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddressAndPort()))
                        .map(ep -> ep.getApplicationState(ApplicationState.RELEASE_VERSION))
                        .map(rv -> rv.version)
                        .orElse(0);

        Gossiper.instance.addLocalApplicationState(ApplicationState.RELEASE_VERSION,
                                                   VersionedValue.unsafeMakeVersionedValue(version, v + 1));
        try
        {
            // add dummy host to avoid returning early in Gossiper.instance.upgradeFromVersionSupplier
            Gossiper.instance.initializeNodeUnsafe(InetAddressAndPort.getByName("127.0.0.2"), UUID.randomUUID(), 1);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        Gossiper.instance.expireUpgradeFromVersion();
    }

    /**
     * Sets the length of the file to given size. File will be created if not exist.
     *
     * @param file file for which length needs to be set
     * @param size new szie
     * @throws IOException on any I/O error.
     */
    public static void setFileLength(File file, long size) throws IOException
    {
        try (FileChannel fileChannel = file.newReadWriteChannel())
        {
            if (file.length() >= size)
            {
                fileChannel.truncate(size);
            }
            else
            {
                fileChannel.position(size - 1);
                fileChannel.write(ByteBuffer.wrap(new byte[1]));
            }
        }
    }

    public static Supplier<SequenceBasedSSTableId> newSeqGen(int ... existing)
    {
        return SequenceBasedSSTableId.Builder.instance.generator(IntStream.of(existing).mapToObj(SequenceBasedSSTableId::new));
    }

    public static Supplier<UUIDBasedSSTableId> newUUIDGen()
    {
        return UUIDBasedSSTableId.Builder.instance.generator(Stream.empty());
    }

    public static Set<Descriptor> getSSTables(String ks, String tableName)
    {
        return Keyspace.open(ks)
                       .getColumnFamilyStore(tableName)
                       .getLiveSSTables()
                       .stream()
                       .map(sstr -> sstr.descriptor)
                       .collect(Collectors.toSet());
    }

    public static Set<Descriptor> getSnapshots(String ks, String tableName, String snapshotTag)
    {
        try
        {
            return Keyspace.open(ks)
                           .getColumnFamilyStore(tableName)
                           .getSnapshotSSTableReaders(snapshotTag)
                           .stream()
                           .map(sstr -> sstr.descriptor)
                           .collect(Collectors.toSet());
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }

    public static Set<Descriptor> getBackups(String ks, String tableName)
    {
        return Keyspace.open(ks)
                       .getColumnFamilyStore(tableName)
                       .getDirectories()
                       .sstableLister(Directories.OnTxnErr.THROW)
                       .onlyBackups(true)
                       .list()
                       .keySet();
    }

    public static StreamState bulkLoadSSTables(File dir, String targetKeyspace)
    {
        SSTableLoader.Client client = new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                for (Replica replica : StorageService.instance.getLocalReplicas(keyspace))
                    addRangeForEndpoint(replica.range(), FBUtilities.getBroadcastAddressAndPort());
            }

            public TableMetadataRef getTableMetadata(String tableName)
            {
                return Schema.instance.getTableMetadataRef(keyspace, tableName);
            }
        };

        SSTableLoader loader = new SSTableLoader(dir, client, new OutputHandler.LogOutput(), 1, targetKeyspace);
        StreamResultFuture result = loader.stream();
        return FBUtilities.waitOnFuture(result);
    }

    public static File relativizePath(File targetBasePath, File path, int components)
    {
        Preconditions.checkArgument(components > 0);
        Preconditions.checkArgument(path.toPath().getNameCount() >= components);
        Path relative = path.toPath().subpath(path.toPath().getNameCount() - components, path.toPath().getNameCount());
        return new File(targetBasePath.toPath().resolve(relative));
    }

    public static void flush(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public static void flushTable(Keyspace keyspace, String table)
    {
        flush(keyspace.getColumnFamilyStore(table));
    }

    public static void flushTable(Keyspace keyspace, TableId table)
    {
        flush(keyspace.getColumnFamilyStore(table));
    }

    public static void flushTable(String keyspace, String table)
    {
        flushTable(Keyspace.open(keyspace), table);
    }

    public static void flush(Keyspace keyspace)
    {
        FBUtilities.waitOnFutures(keyspace.flush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
    }

    public static void flushKeyspace(String keyspaceName)
    {
        flush(Keyspace.open(keyspaceName));
    }

    public static void flush(TableViews view)
    {
        view.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    public static class DataInputStreamPlusImpl extends DataInputStream implements DataInputPlus
    {
        private DataInputStreamPlusImpl(InputStream in)
        {
            super(in);
        }

        public static DataInputStreamPlus wrap(InputStream in)
        {
            DataInputStreamPlusImpl impl = new DataInputStreamPlusImpl(in);
            return Mockito.mock(DataInputStreamPlus.class, new ForwardsInvocations(impl));
        }
    }

    public static RuntimeException testMustBeImplementedForSSTableFormat()
    {
        return new UnsupportedOperationException("Test must be implemented for sstable format " + DatabaseDescriptor.getSelectedSSTableFormat().getClass().getName());
    }
}
