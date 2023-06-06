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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.makeKey;

public abstract class AbstractReadCommandBuilder<T extends ReadQuery>
{
    protected final ColumnFamilyStore cfs;
    protected long nowInSeconds;

    private int cqlLimit = -1;
    private PageSize pageSize = PageSize.NONE;
    private PageSize subPageSize = PageSize.NONE;
    private int perPartitionLimit = -1;
    protected boolean reversed = false;

    protected Set<ColumnIdentifier> columns;
    protected final RowFilter filter = RowFilter.create();

    private ClusteringBound<?> lowerClusteringBound;
    private ClusteringBound<?> upperClusteringBound;

    private NavigableSet<Clustering<?>> clusterings;

    private AggregationSpecification aggregationSpecification;

    // Use Util.cmd() instead of this ctor directly
    AbstractReadCommandBuilder(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.nowInSeconds = FBUtilities.nowInSeconds();
    }

    public AbstractReadCommandBuilder<T> withNowInSeconds(long nowInSec)
    {
        this.nowInSeconds = nowInSec;
        return this;
    }

    public AbstractReadCommandBuilder<T> fromIncl(Object... values)
    {
        assert lowerClusteringBound == null && clusterings == null;
        this.lowerClusteringBound = ClusteringBound.create(cfs.metadata().comparator, true, true, values);
        return this;
    }

    public AbstractReadCommandBuilder<T> fromExcl(Object... values)
    {
        assert lowerClusteringBound == null && clusterings == null;
        this.lowerClusteringBound = ClusteringBound.create(cfs.metadata().comparator, true, false, values);
        return this;
    }

    public AbstractReadCommandBuilder<T> toIncl(Object... values)
    {
        assert upperClusteringBound == null && clusterings == null;
        this.upperClusteringBound = ClusteringBound.create(cfs.metadata().comparator, false, true, values);
        return this;
    }

    public AbstractReadCommandBuilder<T> toExcl(Object... values)
    {
        assert upperClusteringBound == null && clusterings == null;
        this.upperClusteringBound = ClusteringBound.create(cfs.metadata().comparator, false, false, values);
        return this;
    }

    public AbstractReadCommandBuilder<T> includeRow(Object... values)
    {
        assert lowerClusteringBound == null && upperClusteringBound == null;

        if (this.clusterings == null)
            this.clusterings = new TreeSet<>(cfs.metadata().comparator);

        this.clusterings.add(cfs.metadata().comparator.make(values));
        return this;
    }

    public AbstractReadCommandBuilder<T> reverse()
    {
        this.reversed = true;
        return this;
    }

    public AbstractReadCommandBuilder<T> withLimit(int newLimit)
    {
        this.cqlLimit = newLimit;
        return this;
    }

    public AbstractReadCommandBuilder<T> withPageSize(PageSize pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public AbstractReadCommandBuilder<T> withPerPartitionLimit(int perPartitionLimit)
    {
        this.perPartitionLimit = perPartitionLimit;
        return this;
    }

    public AbstractReadCommandBuilder<T> columns(String... columns)
    {
        if (this.columns == null)
            this.columns = Sets.newHashSetWithExpectedSize(columns.length);

        for (String column : columns)
            this.columns.add(ColumnIdentifier.getInterned(column, true));
        return this;
    }

    public AbstractReadCommandBuilder<T> withAggregationSpecification(AggregationSpecification spec)
    {
        this.aggregationSpecification = spec;
        return this;
    }

    public AbstractReadCommandBuilder<T> withSubPageSize(PageSize subPageSize)
    {
        this.subPageSize = subPageSize;
        return this;
    }

    private ByteBuffer bb(Object value, AbstractType<?> type)
    {
        return value instanceof ByteBuffer ? (ByteBuffer)value : ((AbstractType)type).decompose(value);
    }

    private AbstractType<?> forValues(AbstractType<?> collectionType)
    {
        assert collectionType instanceof CollectionType;
        CollectionType ct = (CollectionType)collectionType;
        switch (ct.kind)
        {
            case LIST:
            case MAP:
                return ct.valueComparator();
            case SET:
                return ct.nameComparator();
        }
        throw new AssertionError();
    }

    private AbstractType<?> forKeys(AbstractType<?> collectionType)
    {
        assert collectionType instanceof CollectionType;
        CollectionType ct = (CollectionType)collectionType;
        switch (ct.kind)
        {
            case LIST:
            case MAP:
                return ct.nameComparator();
        }
        throw new AssertionError();
    }

    public AbstractReadCommandBuilder<T> filterOn(String column, Operator op, Object value)
    {
        ColumnMetadata def = cfs.metadata().getColumn(ColumnIdentifier.getInterned(column, true));
        assert def != null;

        AbstractType<?> type = def.type;
        if (op == Operator.CONTAINS)
            type = forValues(type);
        else if (op == Operator.CONTAINS_KEY)
            type = forKeys(type);

        this.filter.add(def, op, bb(value, type));
        return this;
    }

    protected ColumnFilter makeColumnFilter()
    {
        if (columns == null || columns.isEmpty())
            return ColumnFilter.all(cfs.metadata());

        ColumnFilter.Builder filter = ColumnFilter.selectionBuilder();
        for (ColumnIdentifier column : columns)
            filter.add(cfs.metadata().getColumn(column));
        return filter.build();
    }

    protected ClusteringIndexFilter makeFilter()
    {
        // StatementRestrictions.isColumnRange() returns false for static compact tables, which means
        // SelectStatement.makeClusteringIndexFilter uses a names filter with no clusterings for static
        // compact tables, here we reproduce this behavior (CASSANDRA-11223). Note that this code is only
        // called by tests.
        if (cfs.metadata().isStaticCompactTable())
            return new ClusteringIndexNamesFilter(new TreeSet<>(cfs.metadata().comparator), reversed);

        if (clusterings != null)
        {
            return new ClusteringIndexNamesFilter(clusterings, reversed);
        }
        else
        {
            Slice slice = Slice.make(lowerClusteringBound == null ? BufferClusteringBound.BOTTOM : lowerClusteringBound,
                                     upperClusteringBound == null ? BufferClusteringBound.TOP : upperClusteringBound);
            return new ClusteringIndexSliceFilter(Slices.with(cfs.metadata().comparator, slice), reversed);
        }
    }

    protected DataLimits makeLimits()
    {
        DataLimits limits;
        if (aggregationSpecification != null)
        {
            limits = DataLimits.groupByLimits(cqlLimit < 0 ? DataLimits.NO_LIMIT : cqlLimit,
                                              perPartitionLimit < 0 ? DataLimits.NO_LIMIT : perPartitionLimit,
                                              subPageSize,
                                              aggregationSpecification);
        }
        else
        {
            limits = DataLimits.cqlLimits(cqlLimit < 0 ? DataLimits.NO_LIMIT : cqlLimit,
                                          perPartitionLimit < 0 ? DataLimits.NO_LIMIT : perPartitionLimit);
        }
        if (pageSize.isDefined())
            limits = limits.forPaging(pageSize);
        return limits;
    }

    public abstract T build();

    public static class SinglePartitionBuilder extends AbstractReadCommandBuilder<SinglePartitionReadCommand>
    {
        private final DecoratedKey partitionKey;

        public SinglePartitionBuilder(ColumnFamilyStore cfs, DecoratedKey key)
        {
            super(cfs);
            this.partitionKey = key;
        }

        @Override
        public SinglePartitionReadCommand build()
        {
            return SinglePartitionReadCommand.create(cfs.metadata(), nowInSeconds, makeColumnFilter(), filter, makeLimits(), partitionKey, makeFilter());
        }
    }

    public static class MultiPartitionBuilder extends AbstractReadCommandBuilder<SinglePartitionReadCommand.Group>
    {
        private final List<DecoratedKey> keys = new ArrayList<>();

        public MultiPartitionBuilder(ColumnFamilyStore cfs)
        {
            super(cfs);
        }

        public MultiPartitionBuilder addPartition(DecoratedKey key)
        {
            keys.add(key);
            return this;
        }

        public MultiPartitionBuilder addPartition(Object... values)
        {
            return addPartition(makeKey(cfs.metadata(), values));
        }

        @Override
        public SinglePartitionReadCommand.Group build()
        {
            return SinglePartitionReadCommand.Group.create(cfs.metadata(), nowInSeconds, makeColumnFilter(), filter, makeLimits(), keys, makeFilter());
        }
    }

    public static class PartitionRangeBuilder extends AbstractReadCommandBuilder<PartitionRangeReadCommand>
    {
        private DecoratedKey startKey;
        private boolean startInclusive;
        private DecoratedKey endKey;
        private boolean endInclusive;

        public PartitionRangeBuilder(ColumnFamilyStore cfs)
        {
            super(cfs);
        }

        public PartitionRangeBuilder fromKeyIncl(Object... values)
        {
            assert startKey == null;
            this.startInclusive = true;
            this.startKey = makeKey(cfs.metadata(), values);
            return this;
        }

        public PartitionRangeBuilder fromKeyExcl(Object... values)
        {
            assert startKey == null;
            this.startInclusive = false;
            this.startKey = makeKey(cfs.metadata(), values);
            return this;
        }

        public PartitionRangeBuilder toKeyIncl(Object... values)
        {
            assert endKey == null;
            this.endInclusive = true;
            this.endKey = makeKey(cfs.metadata(), values);
            return this;
        }

        public PartitionRangeBuilder toKeyExcl(Object... values)
        {
            assert endKey == null;
            this.endInclusive = false;
            this.endKey = makeKey(cfs.metadata(), values);
            return this;
        }

        @Override
        public PartitionRangeReadCommand build()
        {
            PartitionPosition start = startKey;
            if (start == null)
            {
                start = cfs.getPartitioner().getMinimumToken().maxKeyBound();
                startInclusive = false;
            }
            PartitionPosition end = endKey;
            if (end == null)
            {
                end = cfs.getPartitioner().getMinimumToken().maxKeyBound();
                endInclusive = true;
            }

            AbstractBounds<PartitionPosition> bounds;
            if (startInclusive && endInclusive)
                bounds = new Bounds<>(start, end);
            else if (startInclusive && !endInclusive)
                bounds = new IncludingExcludingBounds<>(start, end);
            else if (!startInclusive && endInclusive)
                bounds = new Range<>(start, end);
            else
                bounds = new ExcludingBounds<>(start, end);

            return PartitionRangeReadCommand.create(cfs.metadata(), nowInSeconds, makeColumnFilter(), filter, makeLimits(), new DataRange(bounds, makeFilter()));
        }

        static DecoratedKey makeKey(TableMetadata metadata, Object... partitionKey)
        {
            if (partitionKey.length == 1 && partitionKey[0] instanceof DecoratedKey)
                return (DecoratedKey)partitionKey[0];

            ByteBuffer key = metadata.partitionKeyAsClusteringComparator().make(partitionKey).serializeAsPartitionKey();
            return metadata.partitioner.decorateKey(key);
        }
    }
}