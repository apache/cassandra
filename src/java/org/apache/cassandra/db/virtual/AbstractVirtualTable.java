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

import java.util.Iterator;
import java.util.NavigableMap;
import java.util.function.Supplier;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * An abstract virtual table implementation that builds the resultset on demand.
 */
public abstract class AbstractVirtualTable implements VirtualTable
{
    protected final TableMetadata metadata;

    protected AbstractVirtualTable(TableMetadata metadata)
    {
        if (!metadata.isVirtual())
            throw new IllegalArgumentException("Cannot instantiate a non-virtual table");

        this.metadata = metadata;
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    /**
     * Provide a {@link DataSet} that is contains all of the virtual table's data.
     */
    public abstract DataSet data();

    /**
     * Provide a {@link DataSet} that is potentially restricted to the provided partition - but is allowed to contain
     * other partitions.
     */
    public DataSet data(DecoratedKey partitionKey)
    {
        return data();
    }

    @Override
    public final UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter)
    {
        Partition partition = data(partitionKey).getPartition(partitionKey);

        if (null == partition)
            return EmptyIterators.unfilteredPartition(metadata);

        long now = currentTimeMillis();
        UnfilteredRowIterator rowIterator = partition.toRowIterator(metadata(), clusteringIndexFilter, columnFilter, now);
        return new SingletonUnfilteredPartitionIterator(rowIterator);
    }

    @Override
    public final UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        DataSet data = data();

        if (data.isEmpty())
            return EmptyIterators.unfilteredPartition(metadata);

        Iterator<Partition> iterator = data.getPartitions(dataRange);

        long now = currentTimeMillis();

        return new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public UnfilteredRowIterator next()
            {
                Partition partition = iterator.next();
                return partition.toRowIterator(metadata, dataRange.clusteringIndexFilter(partition.key()), columnFilter, now);
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public TableMetadata metadata()
            {
                return metadata;
            }
        };
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

    public interface DataSet
    {
        boolean isEmpty();
        Partition getPartition(DecoratedKey partitionKey);
        Iterator<Partition> getPartitions(DataRange range);
    }

    public interface Partition
    {
        DecoratedKey key();
        UnfilteredRowIterator toRowIterator(TableMetadata metadata, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter, long now);
    }

    /**
     * An abstract, map-backed DataSet implementation. Can be backed by any {@link NavigableMap}, then either maintained
     * persistently, or built on demand and thrown away after use, depending on the implementing class.
     */
    public static abstract class AbstractDataSet implements DataSet
    {
        protected final NavigableMap<DecoratedKey, Partition> partitions;

        protected AbstractDataSet(NavigableMap<DecoratedKey, Partition> partitions)
        {
            this.partitions = partitions;
        }

        public boolean isEmpty()
        {
            return partitions.isEmpty();
        }

        public Partition getPartition(DecoratedKey key)
        {
            return partitions.get(key);
        }

        public Iterator<Partition> getPartitions(DataRange dataRange)
        {
            AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
            PartitionPosition startKey = keyRange.left;
            PartitionPosition endKey = keyRange.right;

            NavigableMap<DecoratedKey, Partition> selection = partitions;

            if (startKey.isMinimum() && endKey.isMinimum())
                return selection.values().iterator();

            if (startKey.isMinimum() && endKey instanceof DecoratedKey)
                return selection.headMap((DecoratedKey) endKey, keyRange.isEndInclusive()).values().iterator();

            if (startKey instanceof DecoratedKey && endKey instanceof DecoratedKey)
            {
                return selection.subMap((DecoratedKey) startKey, keyRange.isStartInclusive(), (DecoratedKey) endKey, keyRange.isEndInclusive())
                                .values()
                                .iterator();
            }

            if (startKey instanceof DecoratedKey)
                selection = selection.tailMap((DecoratedKey) startKey, keyRange.isStartInclusive());

            if (endKey instanceof DecoratedKey)
                selection = selection.headMap((DecoratedKey) endKey, keyRange.isEndInclusive());

            // If we have reach this point it means that one of the PartitionPosition is a KeyBound and we have
            // to use filtering for eliminating the unwanted partitions.
            Iterator<Partition> iterator = selection.values().iterator();

            return new AbstractIterator<Partition>()
            {
                private boolean encounteredPartitionsWithinRange;

                @Override
                protected Partition computeNext()
                {
                    while (iterator.hasNext())
                    {
                        Partition partition = iterator.next();
                        if (dataRange.contains(partition.key()))
                        {
                            encounteredPartitionsWithinRange = true;
                            return partition;
                        }

                        // we encountered some partitions within the range, but the last one is outside of the range: we are done
                        if (encounteredPartitionsWithinRange)
                            return endOfData();
                    }

                    return endOfData();
                }
            };
        }
    }

    public static class SimpleTable extends AbstractVirtualTable
    {
        private final Supplier<? extends AbstractVirtualTable.DataSet> supplier;
        public SimpleTable(TableMetadata metadata, Supplier<AbstractVirtualTable.DataSet> supplier)
        {
            super(metadata);
            this.supplier = supplier;
        }

        public AbstractVirtualTable.DataSet data()
        {
            return supplier.get();
        }
    }
}
