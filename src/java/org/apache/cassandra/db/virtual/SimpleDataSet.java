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
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * A DataSet implementation that is filled on demand and has an easy to use API for adding rows.
 */
public class SimpleDataSet extends AbstractVirtualTable.AbstractDataSet
{
    private final TableMetadata metadata;

    private Row currentRow;

    public SimpleDataSet(TableMetadata metadata)
    {
        super(new TreeMap<>(DecoratedKey.comparator));
        this.metadata = metadata;
    }

    public SimpleDataSet row(Object... primaryKeyValues)
    {
        if (Iterables.size(metadata.primaryKeyColumns()) != primaryKeyValues.length)
            throw new IllegalArgumentException();

        Object[] partitionKeyValues = new Object[metadata.partitionKeyColumns().size()];
        Object[]   clusteringValues = new Object[metadata.clusteringColumns().size()];

        System.arraycopy(primaryKeyValues, 0, partitionKeyValues, 0, partitionKeyValues.length);
        System.arraycopy(primaryKeyValues, partitionKeyValues.length, clusteringValues, 0, clusteringValues.length);

        DecoratedKey partitionKey = makeDecoratedKey(partitionKeyValues);
        Clustering clustering = makeClustering(clusteringValues);

        currentRow = new Row(metadata, clustering);
        SimplePartition partition = (SimplePartition) partitions.computeIfAbsent(partitionKey, pk -> new SimplePartition(metadata, pk));
        partition.add(currentRow);

        return this;
    }

    public SimpleDataSet column(String columnName, Object value)
    {
        if (null == currentRow)
            throw new IllegalStateException();
        currentRow.add(columnName, value);
        return this;
    }

    private DecoratedKey makeDecoratedKey(Object... partitionKeyValues)
    {
        ByteBuffer partitionKey = partitionKeyValues.length == 1
                                ? decompose(metadata.partitionKeyType, partitionKeyValues[0])
                                : ((CompositeType) metadata.partitionKeyType).decompose(partitionKeyValues);
        return metadata.partitioner.decorateKey(partitionKey);
    }

    private Clustering makeClustering(Object... clusteringValues)
    {
        if (clusteringValues.length == 0)
            return Clustering.EMPTY;

        ByteBuffer[] clusteringByteBuffers = new ByteBuffer[clusteringValues.length];
        for (int i = 0; i < clusteringValues.length; i++)
            clusteringByteBuffers[i] = decompose(metadata.clusteringColumns().get(i).type, clusteringValues[i]);
        return Clustering.make(clusteringByteBuffers);
    }

    private static final class SimplePartition implements AbstractVirtualTable.Partition
    {
        private final DecoratedKey key;
        private final NavigableMap<Clustering, Row> rows;

        private SimplePartition(TableMetadata metadata, DecoratedKey key)
        {
            this.key = key;
            this.rows = new TreeMap<>(metadata.comparator);
        }

        private void add(Row row)
        {
            rows.put(row.clustering, row);
        }

        public DecoratedKey key()
        {
            return key;
        }

        public UnfilteredRowIterator toRowIterator(TableMetadata metadata,
                                                   ClusteringIndexFilter clusteringIndexFilter,
                                                   ColumnFilter columnFilter,
                                                   long now)
        {
            Iterator<Row> iterator = (clusteringIndexFilter.isReversed() ? rows.descendingMap() : rows).values().iterator();

            return new AbstractUnfilteredRowIterator(metadata,
                                                     key,
                                                     DeletionTime.LIVE,
                                                     columnFilter.queriedColumns(),
                                                     Rows.EMPTY_STATIC_ROW,
                                                     false,
                                                     EncodingStats.NO_STATS)
            {
                protected Unfiltered computeNext()
                {
                    while (iterator.hasNext())
                    {
                        Row row = iterator.next();
                        if (clusteringIndexFilter.selects(row.clustering))
                            return row.toTableRow(columns, now);
                    }
                    return endOfData();
                }
            };
        }
    }

    private static class Row
    {
        private final TableMetadata metadata;
        private final Clustering clustering;

        private final Map<ColumnMetadata, Object> values = new HashMap<>();

        private Row(TableMetadata metadata, Clustering clustering)
        {
            this.metadata = metadata;
            this.clustering = clustering;
        }

        private void add(String columnName, Object value)
        {
            ColumnMetadata column = metadata.getColumn(ByteBufferUtil.bytes(columnName));
            if (null == column || !column.isRegular())
                throw new IllegalArgumentException();
            values.put(column, value);
        }

        private org.apache.cassandra.db.rows.Row toTableRow(RegularAndStaticColumns columns, long now)
        {
            org.apache.cassandra.db.rows.Row.Builder builder = BTreeRow.unsortedBuilder();
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
