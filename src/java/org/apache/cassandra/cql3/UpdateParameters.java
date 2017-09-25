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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Groups the parameters of an update query, and make building updates easier.
 */
public class UpdateParameters
{
    public final CFMetaData metadata;
    public final PartitionColumns updatedColumns;
    public final QueryOptions options;

    private final int nowInSec;
    private final long timestamp;
    private final int ttl;

    private final DeletionTime deletionTime;

    // For lists operation that require a read-before-write. Will be null otherwise.
    private final Map<DecoratedKey, Partition> prefetchedRows;

    private Row.Builder staticBuilder;
    private Row.Builder regularBuilder;

    // The builder currently in use. Will alias either staticBuilder or regularBuilder, which are themselves built lazily.
    private Row.Builder builder;

    public UpdateParameters(CFMetaData metadata,
                            PartitionColumns updatedColumns,
                            QueryOptions options,
                            long timestamp,
                            int ttl,
                            Map<DecoratedKey, Partition> prefetchedRows)
    throws InvalidRequestException
    {
        this.metadata = metadata;
        this.updatedColumns = updatedColumns;
        this.options = options;

        this.nowInSec = FBUtilities.nowInSeconds();
        this.timestamp = timestamp;
        this.ttl = ttl;

        this.deletionTime = new DeletionTime(timestamp, nowInSec);

        this.prefetchedRows = prefetchedRows;

        // We use MIN_VALUE internally to mean the absence of of timestamp (in Selection, in sstable stats, ...), so exclude
        // it to avoid potential confusion.
        if (timestamp == Long.MIN_VALUE)
            throw new InvalidRequestException(String.format("Out of bound timestamp, must be in [%d, %d]", Long.MIN_VALUE + 1, Long.MAX_VALUE));
    }

    public void newRow(Clustering clustering) throws InvalidRequestException
    {
        if (metadata.isDense() && !metadata.isCompound())
        {
            // If it's a COMPACT STORAGE table with a single clustering column, the clustering value is
            // translated in Thrift to the full Thrift column name, and for backward compatibility we
            // don't want to allow that to be empty (even though this would be fine for the storage engine).
            assert clustering.size() == 1;
            ByteBuffer value = clustering.get(0);
            if (value == null || !value.hasRemaining())
                throw new InvalidRequestException("Invalid empty or null value for column " + metadata.clusteringColumns().get(0).name);
        }

        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            if (staticBuilder == null)
                staticBuilder = BTreeRow.unsortedBuilder(nowInSec);
            builder = staticBuilder;
        }
        else
        {
            if (regularBuilder == null)
                regularBuilder = BTreeRow.unsortedBuilder(nowInSec);
            builder = regularBuilder;
        }

        builder.newRow(clustering);
    }

    public Clustering currentClustering()
    {
        return builder.clustering();
    }

    public void addPrimaryKeyLivenessInfo()
    {
        builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp, ttl, nowInSec));
    }

    public void addRowDeletion()
    {
        // For compact tables, at the exclusion of the static row (of static compact tables), each row ever has a single column,
        // the "compact" one. As such, deleting the row or deleting that single cell is equivalent. We favor the later however
        // because that makes it easier when translating back to the old format layout (for thrift and pre-3.0 backward
        // compatibility) as we don't have to special case for the row deletion. This is also in line with what we used to do pre-3.0.
        if (metadata.isCompactTable() && builder.clustering() != Clustering.STATIC_CLUSTERING && !metadata.isSuper())
            addTombstone(metadata.compactValueColumn());
        else
            builder.addRowDeletion(Row.Deletion.regular(deletionTime));
    }

    public void addTombstone(ColumnDefinition column) throws InvalidRequestException
    {
        addTombstone(column, null);
    }

    public void addTombstone(ColumnDefinition column, CellPath path) throws InvalidRequestException
    {
        builder.addCell(BufferCell.tombstone(column, timestamp, nowInSec, path));
    }

    public void addCell(ColumnDefinition column, ByteBuffer value) throws InvalidRequestException
    {
        addCell(column, null, value);
    }

    public void addCell(ColumnDefinition column, CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        Cell cell = ttl == LivenessInfo.NO_TTL
                  ? BufferCell.live(column, timestamp, value, path)
                  : BufferCell.expiring(column, timestamp, ttl, nowInSec, value, path);
        builder.addCell(cell);
    }

    public void addCounter(ColumnDefinition column, long increment) throws InvalidRequestException
    {
        addCounter(column, increment, null);
    }

    public void addCounter(ColumnDefinition column, long increment, CellPath path) throws InvalidRequestException
    {
        assert ttl == LivenessInfo.NO_TTL;

        // Because column is a counter, we need the value to be a CounterContext. However, we're only creating a
        // "counter update", which is a temporary state until we run into 'CounterMutation.updateWithCurrentValue()'
        // which does the read-before-write and sets the proper CounterId, clock and updated value.
        //
        // We thus create a "fake" local shard here. The clock used doesn't matter as this is just a temporary
        // state that will be replaced when processing the mutation in CounterMutation, but the reason we use a 'local'
        // shard is due to the merging rules: if a user includes multiple updates to the same counter in a batch, those
        // multiple updates will be merged in the PartitionUpdate *before* they even reach CounterMutation. So we need
        // such update to be added together, and that's what a local shard gives us.
        //
        // We set counterid to a special value to differentiate between regular pre-2.0 local shards from pre-2.1 era
        // and "counter update" temporary state cells. Please see CounterContext.createUpdate() for further details.
        builder.addCell(BufferCell.live(column, timestamp, CounterContext.instance().createUpdate(increment), path));
    }

    public void setComplexDeletionTime(ColumnDefinition column)
    {
        builder.addComplexDeletion(column, deletionTime);
    }

    public void setComplexDeletionTimeForOverwrite(ColumnDefinition column)
    {
        builder.addComplexDeletion(column, new DeletionTime(deletionTime.markedForDeleteAt() - 1, deletionTime.localDeletionTime()));
    }

    public Row buildRow()
    {
        Row built = builder.build();
        builder = null; // Resetting to null just so we quickly bad usage where we forget to call newRow() after that.
        return built;
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public RangeTombstone makeRangeTombstone(ClusteringComparator comparator, Clustering clustering)
    {
        return makeRangeTombstone(Slice.make(comparator, clustering));
    }

    public RangeTombstone makeRangeTombstone(Slice slice)
    {
        return new RangeTombstone(slice, deletionTime);
    }

    /**
     * Returns the prefetched row with the already performed modifications.
     * <p>If no modification have yet been performed this method will return the fetched row or {@code null} if
     * the row does not exist. If some modifications (updates or deletions) have already been done the row returned
     * will be the result of the merge of the fetched row and of the pending mutations.</p>
     *
     * @param key the partition key
     * @param clustering the row clustering
     * @return the prefetched row with the already performed modifications
     */
    public Row getPrefetchedRow(DecoratedKey key, Clustering clustering)
    {
        if (prefetchedRows == null)
            return null;

        Partition partition = prefetchedRows.get(key);
        Row prefetchedRow = partition == null ? null : partition.searchIterator(ColumnFilter.selection(partition.columns()), false).next(clustering);

        // We need to apply the pending mutations to return the row in its current state
        Row pendingMutations = builder.copy().build();

        if (pendingMutations.isEmpty())
            return prefetchedRow;

        if (prefetchedRow == null)
            return pendingMutations;

        return Rows.merge(prefetchedRow, pendingMutations, nowInSec)
                   .purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
    }
}
