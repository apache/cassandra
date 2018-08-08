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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.AbstractIterator;

/**
 * An abstract virtual table that will iteratively build its rows. This is
 * for when the data set is too large to fit in memory. It requires that the partition
 * keys are provided in the order of the partitioner of the table metadata.
 */
public abstract class OrderedVirtualTable implements VirtualTable
{
    final protected TableMetadata metadata;

    protected OrderedVirtualTable(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    /**
     * @param partitionKey
     * @return boolean if the partition key would exist in this table
     */
    protected abstract boolean hasKey(DecoratedKey partitionKey);

    /**
     * Returns an in order iterator (metadata.partitioner) of decorated partition keys for this table. A DataRange is
     * provided and all the keys for that range must be provided, but it is not required that the keys fall in this
     * range. If your partition key set is small enough it is Ok to provide entire set.
     * 
     * @param dataRange
     *            optional range of keys to return
     * @return Iterator of keys in token order
     */
    protected abstract Iterator<DecoratedKey> getPartitionKeys(DataRange dataRange);

    /**
     * @param isReversed if orderby reverse requested
     * @param key partition key
     * @param columns queried columns
     * @return iterator of rows in order for a given partition key
     */
    protected abstract Iterator<Row> getRows(boolean isReversed, DecoratedKey key, RegularAndStaticColumns columns);

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringFilter,
            ColumnFilter columnFilter)
    {
        if (!hasKey(partitionKey))
        {
            return EmptyIterators.unfilteredPartition(metadata);
        }
        Iterator<Row> iter = getRows(clusteringFilter.isReversed(), partitionKey, columnFilter.queriedColumns());
        if (iter == null || !iter.hasNext())
        {
            return EmptyIterators.unfilteredPartition(metadata);
        }
        UnfilteredRowIterator partition = new AbstractUnfilteredRowIterator(metadata,
                partitionKey,
                DeletionTime.LIVE,
                columnFilter.queriedColumns(),
                Rows.EMPTY_STATIC_ROW,
                false,
                EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                while (iter.hasNext())
                {
                    Row row = iter.next();
                    if (clusteringFilter.selects(row.clustering()))
                        return row;
                }
                return endOfData();
            }
        };
        return new SingletonUnfilteredPartitionIterator(partition);
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        Iterator<DecoratedKey> iter = getPartitionKeys(dataRange);
        return partitionIterator(new AbstractIterator<UnfilteredRowIterator>()
        {
            protected UnfilteredRowIterator computeNext()
            {
                while (iter.hasNext())
                {
                    DecoratedKey key = iter.next();
                    if (dataRange.contains(key))
                    {
                        return makePartition(key, dataRange, columnFilter);
                    }
                }
                return endOfData();
            }
        });
    }

    private UnfilteredRowIterator makePartition(DecoratedKey key, DataRange dataRange, ColumnFilter columnFilter)
    {
        return new AbstractUnfilteredRowIterator(metadata,
                key,
                DeletionTime.LIVE,
                columnFilter.queriedColumns(),
                Rows.EMPTY_STATIC_ROW,
                false,
                EncodingStats.NO_STATS)
        {
            Iterator<Row> iter = null;
            ClusteringIndexFilter clusteringFilter = null;;
            protected Unfiltered computeNext()
            {
                if (iter == null)
                {
                    clusteringFilter = dataRange.clusteringIndexFilter(key);
                    iter = getRows(clusteringFilter.isReversed(), key, columnFilter.queriedColumns());
                }

                while (iter.hasNext())
                {
                    Row row = iter.next();
                    if (clusteringFilter.selects(row.clustering()))
                        return row;
                }
                return endOfData();
            }
        };
    }

    private UnfilteredPartitionIterator partitionIterator(Iterator<UnfilteredRowIterator> partitions)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            public UnfilteredRowIterator next()
            {
                return partitions.next();
            }

            public boolean hasNext()
            {
                return partitions.hasNext();
            }

            public TableMetadata metadata()
            {
                return metadata;
            }
        };
    }

    public TableMetadata metadata()
    {
        return this.metadata;
    }

    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    protected RowBuilder row(Object... clusteringValues)
    {
        if (clusteringValues.length == 0)
            return new RowBuilder(Clustering.EMPTY);

        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);
        return new RowBuilder(Clustering.make(clusteringByteBuffers));
    }

    protected class RowBuilder
    {
        private final Clustering clustering;

        private final Map<ColumnMetadata, Object> values = new HashMap<>();

        private RowBuilder(Clustering clustering)
        {
            this.clustering = clustering;
        }

        public RowBuilder add(String columnName, Object value)
        {
            ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(columnName));
            if (null == column || !column.isRegular())
                throw new IllegalArgumentException();
            values.put(column, value);
            return this;
        }

        public Row build(RegularAndStaticColumns columns)
        {
            int now = FBUtilities.nowInSeconds();
            Row.Builder builder = BTreeRow.unsortedBuilder((int) TimeUnit.MILLISECONDS.toSeconds(now));
            builder.newRow(clustering);

            columns.forEach(c ->
            {
                Object value = values.get(c);
                if (null != value)
                    builder.addCell(BufferCell.live(c, now, decompose(c.type, value)));
            });

            return builder.build();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }
}
