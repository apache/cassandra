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

import java.io.EOFException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Util
{
    private static List<UUID> hostIdPool = new ArrayList<UUID>();

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

    public static Cell getRegularCell(CFMetaData metadata, Row row, String name)
    {
        ColumnDefinition column = metadata.getColumnDefinition(ByteBufferUtil.bytes(name));
        assert column != null;
        return row.getCell(column);
    }

    public static Clustering clustering(ClusteringComparator comparator, Object... o)
    {
        return comparator.make(o);
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
        UUID cfid = first.getColumnFamilyIds().iterator().next();

        for (Mutation rm : mutations)
            rm.applyUnsafe();

        ColumnFamilyStore store = Keyspace.open(keyspaceName).getColumnFamilyStore(cfid);
        store.forceBlockingFlush();
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
                                   List<Token> keyTokens, List<InetAddress> hosts, List<UUID> hostIds, int howMany)
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
            InetAddress ep = InetAddress.getByName("127.0.0." + String.valueOf(i + 1));
            Gossiper.instance.initializeNodeUnsafe(ep, hostIds.get(i), 1);
            Gossiper.instance.injectApplicationState(ep, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(endpointTokens.get(i))));
            ss.onChange(ep,
                        ApplicationState.STATUS,
                        new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(endpointTokens.get(i))));
            hosts.add(ep);
        }

        // check that all nodes are in token metadata
        for (int i=0; i<endpointTokens.size(); ++i)
            assertTrue(ss.getTokenMetadata().isMember(hosts.get(i)));
    }

    public static Future<?> compactAll(ColumnFamilyStore cfs, int gcBefore)
    {
        List<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            descriptors.add(sstable.descriptor);
        return CompactionManager.instance.submitUserDefined(cfs, descriptors, gcBefore);
    }

    public static void compact(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        int gcBefore = cfs.gcBefore(FBUtilities.nowInSeconds());
        AbstractCompactionTask task = cfs.getCompactionStrategyManager().getUserDefinedTask(sstables, gcBefore);
        task.execute(null);
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
        return new AbstractReadCommandBuilder.SinglePartitionBuilder(cfs, makeKey(cfs.metadata, partitionKey));
    }

    public static AbstractReadCommandBuilder.PartitionRangeBuilder cmd(ColumnFamilyStore cfs)
    {
        return new AbstractReadCommandBuilder.PartitionRangeBuilder(cfs);
    }

    static DecoratedKey makeKey(CFMetaData metadata, Object... partitionKey)
    {
        if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
            return (DecoratedKey)partitionKey[0];

        ByteBuffer key = CFMetaData.serializePartitionKey(metadata.getKeyValidatorAsClusteringComparator().make(partitionKey));
        return metadata.decorateKey(key);
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
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
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
                    throw new AssertionError("Expected no results for query " + command.toCQLString() + " but got key " + command.metadata().getKeyValidator().getString(partition.partitionKey().getKey()));
                }
            }
        }
    }

    public static List<ImmutableBTreePartition> getAllUnfiltered(ReadCommand command)
    {
        List<ImmutableBTreePartition> results = new ArrayList<>();
        try (ReadExecutionController executionController = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(executionController))
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
        List<FilteredPartition> results = new ArrayList<>();
        try (ReadExecutionController executionController = command.executionController();
             PartitionIterator iterator = command.executeInternal(executionController))
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
                assert !iterator.hasNext() : "Expecting a single partition but got more";
                assert partition.hasNext() : "Expecting one row in one partition but got an empty partition";
                Row row = partition.next();
                assert !partition.hasNext() : "Expecting a single row but got more";
                return row;
            }
        }
    }

    public static ImmutableBTreePartition getOnlyPartitionUnfiltered(ReadCommand cmd)
    {
        try (ReadExecutionController executionController = cmd.executionController();
             UnfilteredPartitionIterator iterator = cmd.executeLocally(executionController))
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
        try (ReadExecutionController executionController = cmd.executionController();
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

    public static Cell cell(ColumnFamilyStore cfs, Row row, String columnName)
    {
        ColumnDefinition def = cfs.metadata.getColumnDefinition(ByteBufferUtil.bytes(columnName));
        assert def != null;
        return row.getCell(def);
    }

    public static Row row(Partition partition, Object... clustering)
    {
        return partition.getRow(partition.metadata().comparator.make(clustering));
    }

    public static void assertCellValue(Object value, ColumnFamilyStore cfs, Row row, String columnName)
    {
        Cell cell = cell(cfs, row, columnName);
        assert cell != null : "Row " + row.toString(cfs.metadata) + " has no cell for " + columnName;
        assertEquals(value, cell.column().type.compose(cell.value()));
    }

    public static void consume(UnfilteredRowIterator iter)
    {
        try (UnfilteredRowIterator iterator = iter)
        {
            while (iter.hasNext())
                iter.next();
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

    public static CBuilder getCBuilderForCFM(CFMetaData cfm)
    {
        List<ColumnDefinition> clusteringColumns = cfm.clusteringColumns();
        List<AbstractType<?>> types = new ArrayList<>(clusteringColumns.size());
        for (ColumnDefinition def : clusteringColumns)
            types.add(def.type);
        return CBuilder.create(new ClusteringComparator(types));
    }

    public static boolean equal(UnfilteredRowIterator a, UnfilteredRowIterator b)
    {
        return Objects.equals(a.columns(), b.columns())
            && Objects.equals(a.metadata(), b.metadata())
            && Objects.equals(a.isReverseOrder(), b.isReverseOrder())
            && Objects.equals(a.partitionKey(), b.partitionKey())
            && Objects.equals(a.partitionLevelDeletion(), b.partitionLevelDeletion())
            && Objects.equals(a.staticRow(), b.staticRow())
            && Objects.equals(a.stats(), b.stats())
            && Iterators.elementsEqual(a, b);
    }

    // moved & refactored from KeyspaceTest in < 3.0
    public static void assertColumns(Row row, String... expectedColumnNames)
    {
        Iterator<Cell> cells = row == null ? Iterators.<Cell>emptyIterator() : row.cells().iterator();
        String[] actual = Iterators.toArray(Iterators.transform(cells, new Function<Cell, String>()
        {
            public String apply(Cell cell)
            {
                return cell.column().name.toString();
            }
        }), String.class);

        assert Arrays.equals(actual, expectedColumnNames)
        : String.format("Columns [%s])] is not expected [%s]",
                        ((row == null) ? "" : row.columns().toString()),
                        StringUtils.join(expectedColumnNames, ","));
    }

    public static void assertColumn(CFMetaData cfm, Row row, String name, String value, long timestamp)
    {
        Cell cell = row.getCell(cfm.getColumnDefinition(new ColumnIdentifier(name, true)));
        assertColumn(cell, value, timestamp);
    }

    public static void assertColumn(Cell cell, String value, long timestamp)
    {
        assertNotNull(cell);
        assertEquals(0, ByteBufferUtil.compareUnsigned(cell.value(), ByteBufferUtil.bytes(value)));
        assertEquals(timestamp, cell.timestamp());
    }

    public static void assertClustering(CFMetaData cfm, Row row, Object... clusteringValue)
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

    public static void spinAssertEquals(Object expected, Supplier<Object> s, int timeoutInSeconds)
    {
        long now = System.currentTimeMillis();
        while (System.currentTimeMillis() - now < now + (1000 * timeoutInSeconds))
        {
            if (s.get().equals(expected))
                break;
            Thread.yield();
        }
        assertEquals(expected, s.get());
    }

    public static void joinThread(Thread thread) throws InterruptedException
    {
        thread.join(10000);
    }

    // for use with Optional in tests, can be used as an argument to orElseThrow
    public static Supplier<AssertionError> throwAssert(final String message)
    {
        return () -> new AssertionError(message);
    }

    public static class UnfilteredSource extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        public UnfilteredSource(CFMetaData cfm, DecoratedKey partitionKey, Row staticRow, Iterator<Unfiltered> content)
        {
            super(cfm,
                  partitionKey,
                  DeletionTime.LIVE,
                  cfm.partitionColumns(),
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
        return new InternalPartitionRangeReadCommand(command).queryStorageInternal(cfs, controller);
    }

    private static final class InternalPartitionRangeReadCommand extends PartitionRangeReadCommand
    {

        private InternalPartitionRangeReadCommand(PartitionRangeReadCommand original)
        {
            super(original.isDigestQuery(),
                  original.digestVersion(),
                  original.isForThrift(),
                  original.metadata(),
                  original.nowInSec(),
                  original.columnFilter(),
                  original.rowFilter(),
                  original.limits(),
                  original.dataRange(),
                  Optional.empty());
        }

        private UnfilteredPartitionIterator queryStorageInternal(ColumnFamilyStore cfs,
                                                                 ReadExecutionController controller)
        {
            return queryStorage(cfs, controller);
        }
    }
}
