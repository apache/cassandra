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
package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static org.apache.cassandra.db.ClusteringPrefix.Kind.STATIC_CLUSTERING;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * An abstract virtual table implementation that builds the resultset on demand.
 */
public abstract class AbstractLazyVirtualTable implements VirtualTable
{
    public enum OnTimeout { BEST_EFFORT, FAIL }

    // in the special case where we know we have enough rows in the collector, throw this exception to terminate early
    public static class InternalDoneException extends RuntimeException {}
    // in the special case where we have timed out, throw this exception to terminate early
    public static class InternalTimeoutException extends RuntimeException {}

    public static class FilterRange<V>
    {
        final V min, max;
        public FilterRange(V min, V max)
        {
            this.min = min;
            this.max = max;
        }
    }

    public interface PartitionsCollector
    {
        DataRange dataRange();
        RowFilter rowFilter();
        ColumnFilter columnFilter();
        DataLimits limits();
        long nowInSeconds();
        long timestampMicros();
        long deadlineNanos();
        boolean isEmpty();

        RowCollector row(Object... primaryKeys);
        PartitionCollector partition(Object... partitionKeys);
        UnfilteredPartitionIterator finish();

        @Nullable Object[] singlePartitionKey();
        <I, O> FilterRange<O> filters(String column, Function<I, O> translate, UnaryOperator<O> exclusiveStart, UnaryOperator<O> exclusiveEnd);
    }

    public interface PartitionCollector
    {
        void collect(Consumer<RowsCollector> addTo);
    }

    public interface RowsCollector
    {
        RowCollector add(Object... clusteringKeys);
    }

    public interface RowCollector
    {
        default void lazyCollect(Consumer<ColumnsCollector> addToIfNeeded) { eagerCollect(addToIfNeeded); }
        void eagerCollect(Consumer<ColumnsCollector> addToNow);
    }

    public interface ColumnsCollector
    {
        /**
         * equivalent to
         * {@code
         * if (value == null) add(columnName, null);
         * else if (f1.apply(value) == null) add(columnName, f1.apply(value));
         * else add(columnName, f2.apply(f1.apply(value)));
         * }
         */
        <V1, V2> ColumnsCollector add(String columnName, V1 value, Function<? super V1, ? extends V2> f1, Function<? super V2, ?> f2);

        default <V> ColumnsCollector add(String columnName, V value, Function<? super V, ?> transform)
        {
            return add(columnName, value, Function.identity(), transform);
        }
        default ColumnsCollector add(String columnName, Object value)
        {
            return add(columnName, value, Function.identity());
        }
    }

    public static class SimplePartitionsCollector implements PartitionsCollector
    {
        final TableMetadata metadata;
        final boolean isSorted;
        final boolean isSortedByPartitionKey;

        final Map<String, ColumnMetadata> columnLookup = new HashMap<>();
        final NavigableMap<DecoratedKey, SimplePartition> partitions;

        final DataRange dataRange;
        final ColumnFilter columnFilter;
        final RowFilter rowFilter;
        final DataLimits limits;

        final long startedAtNanos = Clock.Global.nanoTime();
        final long deadlineNanos;

        final long nowInSeconds = Clock.Global.nowInSeconds();
        final long timestampMicros;

        int totalRowCount;
        int lastFilteredTotalRowCount, lastFilteredPartitionCount;

        @Override public DataRange dataRange() { return dataRange; }
        @Override public RowFilter rowFilter() { return rowFilter; }
        @Override public ColumnFilter columnFilter() { return columnFilter; }
        @Override public DataLimits limits() { return limits; }
        @Override public long nowInSeconds() { return nowInSeconds; }
        @Override public long timestampMicros() { return timestampMicros; }
        @Override public long deadlineNanos() { return deadlineNanos; }
        @Override public boolean isEmpty() { return totalRowCount == 0; }

        public SimplePartitionsCollector(TableMetadata metadata, boolean isSorted, boolean isSortedByPartitionKey,
                                         DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
        {
            this.metadata = metadata;
            this.isSorted = isSorted;
            this.isSortedByPartitionKey = isSortedByPartitionKey;
            this.dataRange = dataRange;
            this.columnFilter = columnFilter;
            this.rowFilter = rowFilter;
            this.limits = limits;
            this.timestampMicros = FBUtilities.timestampMicros();
            this.deadlineNanos = startedAtNanos + DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS);
            this.partitions = new TreeMap<>(dataRange.isReversed() ? DecoratedKey.comparator.reversed() : DecoratedKey.comparator);
            for (ColumnMetadata cm : metadata.columns())
                columnLookup.put(cm.name.toString(), cm);
        }

        public Object[] singlePartitionKey()
        {
            AbstractBounds<?> bounds = dataRange().keyRange();
            if (!bounds.isStartInclusive() || !bounds.isEndInclusive() || !bounds.left.equals(bounds.right) || !(bounds.left instanceof DecoratedKey))
                return null;

            return composePartitionKeys((DecoratedKey) bounds.left, metadata);
        }

        @Override
        public PartitionCollector partition(Object ... partitionKeys)
        {
            int pkSize = metadata.partitionKeyColumns().size();
            if (pkSize != partitionKeys.length)
                throw new IllegalArgumentException();

            DecoratedKey partitionKey = decomposePartitionKeys(metadata, partitionKeys);
            if (!dataRange.contains(partitionKey))
                return dropCks -> {};



            return partitions.computeIfAbsent(partitionKey, SimplePartition::new);
        }

        @Override
        public UnfilteredPartitionIterator finish()
        {
            final Iterator<SimplePartition> partitions = this.partitions.values().iterator();
            return new UnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;

                @Override public TableMetadata metadata() { return metadata; }
                @Override public void close() {}

                @Override
                public boolean hasNext()
                {
                    while (next == null && partitions.hasNext())
                    {
                        SimplePartition partition = partitions.next();
                        Iterator<Row> rows = partition.rows();

                        if (!rows.hasNext())
                            continue;

                        next = new UnfilteredRowIterator()
                        {
                            @Override public TableMetadata metadata() { return metadata; }
                            @Override public boolean isReverseOrder() { return dataRange.isReversed(); }
                            @Override public RegularAndStaticColumns columns() { return columnFilter.fetchedColumns(); }
                            @Override public DecoratedKey partitionKey() { return partition.key; }

                            @Override public Row staticRow() { return partition.staticRow(); }
                            @Override public boolean hasNext() { return rows.hasNext(); }
                            @Override public Unfiltered next() { return rows.next(); }

                            @Override public void close() {}
                            @Override public DeletionTime partitionLevelDeletion() { return DeletionTime.LIVE; }
                            @Override public EncodingStats stats() { return EncodingStats.NO_STATS; }
                        };
                    }
                    return next != null;
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    UnfilteredRowIterator result = next;
                    next = null;
                    return result;
                }
            };
        }

        @Override
        @Nullable
        public <I, O> FilterRange<O> filters(String columnName, Function<I, O> translate, UnaryOperator<O> exclusiveStart, UnaryOperator<O> exclusiveEnd)
        {
            ColumnMetadata column = columnLookup.get(columnName);
            O min = null, max = null;
            for (RowFilter.Expression expression : rowFilter().getExpressions())
            {
                if (!expression.column().equals(column))
                    continue;

                O bound = translate.apply((I)column.type.compose(expression.getIndexValue()));
                switch (expression.operator())
                {
                    default: throw new InvalidRequestException("Operator " + expression.operator() + " not supported for txn_id");
                    case EQ:  min = max = bound; break;
                    case LTE: max = bound; break;
                    case LT:  max = exclusiveEnd.apply(bound); break;
                    case GTE: min = bound; break;
                    case GT:  min = exclusiveStart.apply(bound); break;
                }
            }

            return new FilterRange<>(min, max);
        }

        @Override
        public RowCollector row(Object... primaryKeys)
        {
            int pkSize = metadata.partitionKeyColumns().size();
            int ckSize = metadata.clusteringColumns().size();
            if (pkSize + ckSize != primaryKeys.length)
                throw new IllegalArgumentException();

            Object[] partitionKeyValues = new Object[pkSize];
            Object[]   clusteringValues = new Object[ckSize];

            System.arraycopy(primaryKeys, 0, partitionKeyValues, 0, pkSize);
            System.arraycopy(primaryKeys, pkSize, clusteringValues, 0, ckSize);

            DecoratedKey partitionKey = decomposePartitionKeys(metadata, partitionKeyValues);
            Clustering<?> clustering = decomposeClusterings(metadata, clusteringValues);

            if (!dataRange.contains(partitionKey) || !dataRange.clusteringIndexFilter(partitionKey).selects(clustering))
                return drop -> {};

            if (isSortedByPartitionKey)
                checkCorrectlySorted(partitionKey);

            return partitions.computeIfAbsent(partitionKey, SimplePartition::new).row(clustering);
        }

        private void checkCorrectlySorted(DecoratedKey newPartitionKey)
        {
            if (partitions.isEmpty())
                return;

            DecoratedKey prevKey = partitions.lastKey();
            int c = metadata.partitionKeyType.compare(prevKey.getKey(), newPartitionKey.getKey());
            if (dataRange.isReversed() ? c < 0 : c > 0)
                throw new IllegalArgumentException(Arrays.toString(composePartitionKeys(prevKey, metadata)) + (dataRange.isReversed() ? " < " : " > ") + Arrays.toString(composePartitionKeys(newPartitionKey, metadata)));
        }

        private final class SimplePartition implements PartitionCollector, RowsCollector
        {
            private final DecoratedKey key;
            // we assume no duplicate rows, and impose the condition lazily
            private SimpleRow[] rows;
            private int rowCount;
            private SimpleRow staticRow;
            private boolean dropRows;
            private boolean isSortedAndFiltered = true;

            private SimplePartition(DecoratedKey key)
            {
                this.key = key;
                this.rows = new SimpleRow[1];
            }

            @Override
            public void collect(Consumer<RowsCollector> addTo)
            {
                addTo.accept(this);
            }

            @Override
            public RowCollector add(Object... clusteringKeys)
            {
                int ckSize = metadata.clusteringColumns().size();
                if (ckSize != clusteringKeys.length)
                    throw new IllegalArgumentException();

                return row(decomposeClusterings(metadata, clusteringKeys));
            }

            private void checkCorrectlySorted(Clustering<?> newClustering)
            {
                if (rowCount == 0)
                    return;

                Clustering<?> prevClustering = rows[rowCount - 1].clustering;
                int c = metadata.comparator.compare(prevClustering, newClustering);
                if (dataRange.isReversed() ? c <= 0 : c >= 0)
                    throw new IllegalArgumentException(Arrays.toString(composeClusterings(prevClustering, metadata)) + (dataRange.isReversed() ? " <= " : " >= ") + Arrays.toString(composeClusterings(newClustering, metadata)));
            }

            RowCollector row(Clustering<?> clustering)
            {
                if (nanoTime() > deadlineNanos)
                    throw new InternalTimeoutException();

                if (dropRows || !dataRange.clusteringIndexFilter(key).selects(clustering))
                    return drop -> {};

                if (isSorted)
                    checkCorrectlySorted(clustering);

                if (totalRowCount >= limits.count())
                {
                    boolean filter;
                    if (!isSortedByPartitionKey || lastFilteredPartitionCount == partitions.size())
                    {
                        filter = totalRowCount / 2 >= Math.max(1024, limits.count());
                    }
                    else
                    {
                        int rowsAddedSinceLastFiltered = totalRowCount - lastFilteredTotalRowCount;
                        int threshold = Math.max(32, Math.min(1024, lastFilteredTotalRowCount / 2));
                        filter = lastFilteredTotalRowCount == 0 || rowsAddedSinceLastFiltered >= threshold;
                    }

                    if (filter)
                    {
                        for (SimplePartition partition : partitions.values())
                        {
                            // first filter within each partition
                            partition.filterAndSort();
                            // and truncate if there are per-partition limits
                            partition.truncate(limits.perPartitionCount());
                        }

                        // then drop any partitions that completely fall outside our limit
                        Iterator<SimplePartition> iter = partitions.descendingMap().values().iterator();
                        SimplePartition last;
                        while (true)
                        {
                            SimplePartition next = last = iter.next();
                            if (totalRowCount - next.rowCount < limits.count())
                                break;

                            iter.remove();
                            totalRowCount -= next.rowCount;
                            if (next == this)
                                dropRows = true;
                        }

                        // possibly truncate the last partition if it partially falls outside the limit
                        int overflow = Math.max(0, totalRowCount - limits.count());
                        int newCount = last.truncate(last.rowCount - overflow);
                        lastFilteredTotalRowCount = totalRowCount;
                        lastFilteredPartitionCount = partitions.size();

                        if (isSortedByPartitionKey && totalRowCount - newCount >= limits.count())
                            throw new InternalDoneException();

                        if (isSorted && totalRowCount >= limits.count())
                            throw new InternalDoneException();

                        if (dropRows)
                            return drop -> {};
                    }
                }

                SimpleRow result = new SimpleRow(clustering);
                if (clustering.kind() == STATIC_CLUSTERING)
                {
                    Invariants.require(staticRow == null);
                    staticRow = result;
                }
                else
                {
                    totalRowCount++;
                    if (rowCount == rows.length)
                        rows = Arrays.copyOf(rows, Math.max(8, rowCount * 2));
                    rows[rowCount++] = result;
                    isSortedAndFiltered = false;
                }
                return result;
            }

            void filterAndSort()
            {
                if (isSortedAndFiltered)
                    return;

                int newCount = 0;
                for (int i = 0 ; i < rowCount; ++i)
                {
                    if (rows[i].rowFilterIncludes())
                    {
                        if (newCount != i)
                            rows[newCount] = rows[i];
                        newCount++;
                    }
                }
                if (newCount != rowCount)
                {
                    Arrays.fill(rows, newCount, rowCount, null);
                    totalRowCount -= (rowCount - newCount);
                    rowCount = newCount;
                }
                Arrays.sort(rows, 0, newCount, rowComparator());
                isSortedAndFiltered = true;
            }

            int truncate(int newCount)
            {
                if (rowCount <= newCount)
                    return rowCount;

                filterAndSort();

                if (rowCount <= newCount)
                    return rowCount;

                Arrays.fill(rows, newCount, rowCount, null);
                totalRowCount -= (rowCount - newCount);
                rowCount = newCount;
                return newCount;
            }

            private Comparator<SimpleRow> rowComparator()
            {
                Comparator<Clusterable> cmp = dataRange.isReversed() ? metadata.comparator.reversed() : metadata.comparator;
                return (a, b) -> cmp.compare(a.clustering, b.clustering);
            }

            Row staticRow()
            {
                if (staticRow == null)
                    return null;

                return staticRow.materialiseAndFilter();
            }

            Iterator<Row> rows()
            {
                filterAndSort();
                return Arrays.stream(rows, 0, rowCount).map(SimpleRow::materialiseAndFilter).iterator();
            }

            private final class SimpleRow implements RowCollector
            {
                final Clustering<?> clustering;
                SomeColumns state;

                private SimpleRow(Clustering<?> clustering)
                {
                    this.clustering = clustering;
                }

                @Override
                public void lazyCollect(Consumer<ColumnsCollector> addToIfNeeded)
                {
                    Invariants.require(state == null);
                    state = new LazyColumnsCollector(addToIfNeeded);
                }

                @Override
                public void eagerCollect(Consumer<ColumnsCollector> addToNow)
                {
                    Invariants.require(state == null);
                    state = new EagerColumnsCollector(addToNow);
                }

                boolean rowFilterIncludes()
                {
                    return null != materialiseAndFilter();
                }

                Row materialiseAndFilter()
                {
                    if (state == null)
                        return null;

                    FilteredRow filtered = state.materialiseAndFilter(this);
                    state = filtered;
                    return filtered == null ? null : filtered.row;
                }

                DecoratedKey partitionKey()
                {
                    return SimplePartition.this.key;
                }

                SimplePartitionsCollector collector()
                {
                    return SimplePartitionsCollector.this;
                }
            }
        }

        static abstract class SomeColumns
        {
            abstract FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent);
        }

        static class LazyColumnsCollector extends SomeColumns
        {
            final Consumer<ColumnsCollector> lazy;
            LazyColumnsCollector(Consumer<ColumnsCollector> lazy)
            {
                this.lazy = lazy;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                return parent.collector().new EagerColumnsCollector(lazy).materialiseAndFilter(parent);
            }
        }

        class EagerColumnsCollector extends SomeColumns implements ColumnsCollector
        {
            Object[] columns = new Object[4];
            int columnCount;

            public EagerColumnsCollector(Consumer<ColumnsCollector> add)
            {
                add.accept(this);
            }

            @Override
            public <V1, V2> ColumnsCollector add(String name, V1 v1, Function<? super V1, ? extends V2> f1, Function<? super V2, ?> f2)
            {
                if (v1 == null)
                    return this;

                ColumnMetadata cm = columnLookup.get(name);
                if (cm == null)
                    throw new IllegalArgumentException("Unknown column name " + name);

                if (!columnFilter.fetches(cm))
                    return this;

                V2 v2 = f1.apply(v1);
                if (v2 == null)
                    return this;

                Object result = f2.apply(v2);
                if (result == null)
                    return this;

                if (columnCount * 2 == columns.length)
                    columns = Arrays.copyOf(columns, columnCount * 4);

                columns[columnCount * 2] = cm;
                columns[columnCount * 2 + 1] = result;
                ++columnCount;
                return this;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                for (int i = 0 ; i < columnCount ; i++)
                {
                    ColumnMetadata cm = (ColumnMetadata) columns[i * 2];
                    Object value = columns[i * 2 + 1];
                    ByteBuffer bb = value instanceof ByteBuffer ? (ByteBuffer)value : decompose(cm.type, value);
                    columns[i] = BufferCell.live(cm, timestampMicros, bb);
                }
                Arrays.sort(columns, 0, columnCount, (a, b) -> ColumnData.comparator.compare((BufferCell)a, (BufferCell)b));
                Object[] btree = BTree.build(BulkIterator.of(columns), columnCount, UpdateFunction.noOp);
                BTreeRow row = BTreeRow.create(parent.clustering, LivenessInfo.create(timestampMicros), Row.Deletion.LIVE, btree);
                if (!rowFilter.isSatisfiedBy(metadata, parent.partitionKey(), row, nowInSeconds))
                    return null;
                return new FilteredRow(row);
            }
        }

        static class FilteredRow extends SomeColumns
        {
            final Row row;
            FilteredRow(Row row)
            {
                this.row = row;
            }

            @Override
            FilteredRow materialiseAndFilter(SimplePartition.SimpleRow parent)
            {
                return this;
            }
        }
    }

    protected final TableMetadata metadata;
    private final OnTimeout onTimeout;
    private final Sorted sorted, sortedByPartitionKey;

    protected AbstractLazyVirtualTable(TableMetadata metadata, OnTimeout onTimeout, Sorted sorted)
    {
        this(metadata, onTimeout, sorted, sorted);
    }

    protected AbstractLazyVirtualTable(TableMetadata metadata, OnTimeout onTimeout, Sorted sorted, Sorted sortedByPartitionKey)
    {
        if (!metadata.isVirtual())
            throw new IllegalArgumentException("Cannot instantiate a non-virtual table");

        if (!metadata.keyspace.startsWith(SchemaConstants.ACCORD_KEYSPACE_NAME))
        {
            // NOTE: there is nothing stopping other use cases from using this facility, but there was
            // feedback on the ticket questioning the reliance on Accord integration tests for validating the API.
            // If another use case wishes to use the facility, simply satisfy reviewers in this regard. See PR #4373 for details.
            throw new IllegalArgumentException("This facility is only currently supported by Accord keyspaces");
        }

        this.metadata = metadata;
        this.onTimeout = onTimeout;
        this.sorted = sorted;
        this.sortedByPartitionKey = sortedByPartitionKey;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    public OnTimeout onTimeout() { return onTimeout; }

    protected PartitionsCollector collector(DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        boolean isSorted = isSorted(sorted, !dataRange.isReversed());
        boolean isSortedByPartitionKey = isSorted || isSorted(sortedByPartitionKey, !dataRange.isReversed());
        return new SimplePartitionsCollector(metadata, isSorted, isSortedByPartitionKey, dataRange, columnFilter, rowFilter, limits);
    }


    private static boolean isSorted(Sorted sorted, boolean asc)
    {
        return sorted == Sorted.SORTED || sorted == (asc ? Sorted.ASC : Sorted.DESC);
    }

    protected abstract void collect(PartitionsCollector collector);

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        return select(new DataRange(new Bounds<>(partitionKey, partitionKey), clusteringIndexFilter), columnFilter, rowFilter, limits);
    }

    @Override
    public final UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        PartitionsCollector collector = collector(dataRange, columnFilter, rowFilter, limits);
        try
        {
            collect(collector);
        }
        catch (InternalDoneException ignore) {}
        catch (InternalTimeoutException ignore)
        {
            if (onTimeout != OnTimeout.BEST_EFFORT || collector.isEmpty())
                throw new ReadTimeoutException(ONE, 0, 1, false);
            ClientWarn.instance.warn("Ran out of time. Returning best effort.");
        }
        return collector.finish();
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncation is not supported by table " + metadata);
    }

    @Override
    public String toString()
    {
        return metadata().toString();
    }

    static Object[] composePartitionKeys(DecoratedKey decoratedKey, TableMetadata metadata)
    {
        if (metadata.partitionKeyColumns().size() == 1)
            return new Object[] { metadata.partitionKeyType.compose(decoratedKey.getKey()) };

        ByteBuffer[] split = ((CompositeType)metadata.partitionKeyType).split(decoratedKey.getKey());
        Object[] result = new Object[split.length];
        for (int i = 0 ; i < split.length ; ++i)
            result[i] = metadata.partitionKeyColumns().get(i).type.compose(split[i]);
        return result;
    }

    static Object[] composeClusterings(ClusteringPrefix clustering, TableMetadata metadata)
    {
        Object[] result = new Object[clustering.size()];
        for (int i = 0 ; i < result.length ; ++i)
            result[i] = metadata.clusteringColumns().get(i).type.compose(clustering.get(i), clustering.accessor());
        return result;
    }

    private static ByteBuffer decompose(AbstractType<?> type, Object value)
    {
        return type.decomposeUntyped(value);
    }

    static DecoratedKey decomposePartitionKeys(TableMetadata metadata, Object... partitionKeys)
    {
        ByteBuffer partitionKey = partitionKeys.length == 1
                                  ? decompose(metadata.partitionKeyType, partitionKeys[0])
                                  : ((CompositeType) metadata.partitionKeyType).decompose(partitionKeys);
        return metadata.partitioner.decorateKey(partitionKey);
    }

    static Clustering<?> decomposeClusterings(TableMetadata metadata, Object... clusteringKeys)
    {
        if (clusteringKeys.length == 0)
            return Clustering.EMPTY;

        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringKeys.length];
        for (int i = 0; i < clusteringKeys.length; i++)
        {
            if (clusteringKeys[i] instanceof ByteBuffer) clusteringByteBuffers[i] = (ByteBuffer) clusteringKeys[i];
            else clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringKeys[i]);
        }
        return Clustering.make(clusteringByteBuffers);
    }

    /**
     * An empty {@code IndexRegistry}
     */
    static IndexRegistry indexes(Index... indexes)
    {
        final List<Index> list = ImmutableList.copyOf(indexes);
        return new IndexRegistry()
        {
            @Override
            public void registerIndex(Index index, Index.Group.Key groupKey, Supplier<Index.Group> groupSupplier)
            {
            }

            @Override
            public void unregisterIndex(Index index, Index.Group.Key groupKey)
            {
            }

            @Override
            public Collection<Index> listIndexes()
            {
                return list;
            }

            @Override
            public Collection<Index.Group> listIndexGroups()
            {
                return Collections.emptySet();
            }

            @Override
            public Index getIndex(IndexMetadata indexMetadata)
            {
                for (Index index : indexes)
                {
                    if (index.getIndexMetadata() == indexMetadata)
                        return index;
                }
                return null;
            }

            @Override
            public Index getIndexByName(String indexName)
            {
                for (Index index : indexes)
                {
                    if (index.getIndexMetadata().name.equals(indexName))
                        return index;
                }
                return null;
            }

            @Override
            public Optional<Index> getBestIndexFor(RowFilter.Expression expression, IndexHints indexHints)
            {
                return Optional.empty();
            }

            @Override
            public void validate(PartitionUpdate update, ClientState state)
            {
            }

            @Override
            public boolean supportsMultipleIndexExpressions()
            {
                return true;
            }
        };
    }

}
