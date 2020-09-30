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
import java.util.*;

import com.google.common.collect.Sets;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractReadCommandBuilder
{
    protected final ColumnFamilyStore cfs;
    protected int nowInSeconds;

    private int cqlLimit = -1;
    private int pagingLimit = -1;
    protected boolean reversed = false;

    protected Set<ColumnIdentifier> columns;
    protected final RowFilter filter = RowFilter.create();

    private ClusteringBound<?> lowerClusteringBound;
    private ClusteringBound<?> upperClusteringBound;

    private NavigableSet<Clustering<?>> clusterings;

    // Use Util.cmd() instead of this ctor directly
    AbstractReadCommandBuilder(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.nowInSeconds = FBUtilities.nowInSeconds();
    }

    public AbstractReadCommandBuilder withNowInSeconds(int nowInSec)
    {
        this.nowInSeconds = nowInSec;
        return this;
    }

    public AbstractReadCommandBuilder fromIncl(Object... values)
    {
        assert lowerClusteringBound == null && clusterings == null;
        this.lowerClusteringBound = ClusteringBound.create(cfs.metadata().comparator, true, true, values);
        return this;
    }

    public AbstractReadCommandBuilder fromExcl(Object... values)
    {
        assert lowerClusteringBound == null && clusterings == null;
        this.lowerClusteringBound = ClusteringBound.create(cfs.metadata().comparator, true, false, values);
        return this;
    }

    public AbstractReadCommandBuilder toIncl(Object... values)
    {
        assert upperClusteringBound == null && clusterings == null;
        this.upperClusteringBound = ClusteringBound.create(cfs.metadata().comparator, false, true, values);
        return this;
    }

    public AbstractReadCommandBuilder toExcl(Object... values)
    {
        assert upperClusteringBound == null && clusterings == null;
        this.upperClusteringBound = ClusteringBound.create(cfs.metadata().comparator, false, false, values);
        return this;
    }

    public AbstractReadCommandBuilder includeRow(Object... values)
    {
        assert lowerClusteringBound == null && upperClusteringBound == null;

        if (this.clusterings == null)
            this.clusterings = new TreeSet<>(cfs.metadata().comparator);

        this.clusterings.add(cfs.metadata().comparator.make(values));
        return this;
    }

    public AbstractReadCommandBuilder reverse()
    {
        this.reversed = true;
        return this;
    }

    public AbstractReadCommandBuilder withLimit(int newLimit)
    {
        this.cqlLimit = newLimit;
        return this;
    }

    public AbstractReadCommandBuilder withPagingLimit(int newLimit)
    {
        this.pagingLimit = newLimit;
        return this;
    }

    public AbstractReadCommandBuilder columns(String... columns)
    {
        if (this.columns == null)
            this.columns = Sets.newHashSetWithExpectedSize(columns.length);

        for (String column : columns)
            this.columns.add(ColumnIdentifier.getInterned(column, true));
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

    public AbstractReadCommandBuilder filterOn(String column, Operator op, Object value)
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
        DataLimits limits = cqlLimit < 0 ? DataLimits.NONE : DataLimits.cqlLimits(cqlLimit);
        if (pagingLimit >= 0)
            limits = limits.forPaging(pagingLimit);
        return limits;
    }

    public abstract ReadCommand build();

    public static class SinglePartitionBuilder extends AbstractReadCommandBuilder
    {
        private final DecoratedKey partitionKey;

        public SinglePartitionBuilder(ColumnFamilyStore cfs, DecoratedKey key)
        {
            super(cfs);
            this.partitionKey = key;
        }

        @Override
        public ReadCommand build()
        {
            return SinglePartitionReadCommand.create(cfs.metadata(), nowInSeconds, makeColumnFilter(), filter, makeLimits(), partitionKey, makeFilter());
        }
    }

    public static class PartitionRangeBuilder extends AbstractReadCommandBuilder
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
        public ReadCommand build()
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
