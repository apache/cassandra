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

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Groups the parameters of an update query, and make building updates easier.
 */
public class UpdateParameters
{
    public final TableMetadata metadata;
    public final RegularAndStaticColumns updatedColumns;
    public final ClientState clientState;
    public final QueryOptions options;

    private final long nowInSec;
    private final long timestamp;
    private final int ttl;

    private final DeletionTime deletionTime;

    // For lists operation that require a read-before-write. Will be null otherwise.
    private final Map<DecoratedKey, Partition> prefetchedRows;

    private Row.Builder staticBuilder;
    private Row.Builder regularBuilder;

    // The builder currently in use. Will alias either staticBuilder or regularBuilder, which are themselves built lazily.
    private Row.Builder builder;

    public UpdateParameters(TableMetadata metadata,
                            RegularAndStaticColumns updatedColumns,
                            ClientState clientState,
                            QueryOptions options,
                            long timestamp,
                            long nowInSec,
                            int ttl,
                            Map<DecoratedKey, Partition> prefetchedRows)
    throws InvalidRequestException
    {
        this.metadata = metadata;
        this.updatedColumns = updatedColumns;
        this.clientState = clientState;
        this.options = options;

        this.nowInSec = nowInSec;
        this.timestamp = timestamp;
        this.ttl = ttl;

        this.deletionTime = DeletionTime.build(timestamp, nowInSec);

        this.prefetchedRows = prefetchedRows;

        // We use MIN_VALUE internally to mean the absence of of timestamp (in Selection, in sstable stats, ...), so exclude
        // it to avoid potential confusion.
        if (timestamp == Long.MIN_VALUE)
            throw new InvalidRequestException(String.format("Out of bound timestamp, must be in [%d, %d]", Long.MIN_VALUE + 1, Long.MAX_VALUE));
    }

    public <V> void newRow(Clustering<V> clustering) throws InvalidRequestException
    {
        if (metadata.isCompactTable())
        {
            if (TableMetadata.Flag.isDense(metadata.flags) && !TableMetadata.Flag.isCompound(metadata.flags))
            {
                // If it's a COMPACT STORAGE table with a single clustering column and for backward compatibility we
                // don't want to allow that to be empty (even though this would be fine for the storage engine).
                assert clustering.size() == 1 : clustering.toString(metadata);
                V value = clustering.get(0);
                if (value == null || clustering.accessor().isEmpty(value))
                    throw new InvalidRequestException("Invalid empty or null value for column " + metadata.clusteringColumns().get(0).name);
            }
        }

        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            if (staticBuilder == null)
                staticBuilder = BTreeRow.unsortedBuilder();
            builder = staticBuilder;
        }
        else
        {
            if (regularBuilder == null)
                regularBuilder = BTreeRow.unsortedBuilder();
            builder = regularBuilder;
        }

        builder.newRow(clustering);
    }

    public Clustering<?> currentClustering()
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
        // the "compact" one. As such, deleting the row or deleting that single cell is equivalent. We favor the later
        // for backward compatibility (thought it doesn't truly matter anymore).
        if (metadata.isCompactTable() && builder.clustering() != Clustering.STATIC_CLUSTERING)
            addTombstone(((TableMetadata.CompactTableMetadata) metadata).compactValueColumn);
        else
            builder.addRowDeletion(Row.Deletion.regular(deletionTime));
    }

    public void addTombstone(ColumnMetadata column) throws InvalidRequestException
    {
        addTombstone(column, null);
    }

    public void addTombstone(ColumnMetadata column, CellPath path) throws InvalidRequestException
    {
        // Deleting individual elements of non-frozen sets and maps involves creating tombstones that contain the value
        // of the deleted element, independently on whether the element existed or not. That tombstone value is guarded
        // by the columnValueSize guardrail, to prevent the insertion of tombstones over the threshold. The downside is
        // that enabling or raising this threshold can prevent users from deleting set/map elements that were written
        // when the guardrail was disabled or with a lower value. Deleting the entire column, row or partition is always
        // allowed, since the tombstones created for those operations don't contain the CQL column values.
        if (path != null && column.type.isMultiCell())
            Guardrails.columnValueSize.guard(path.dataSize(), column.name.toString(), false, clientState);

        builder.addCell(BufferCell.tombstone(column, timestamp, nowInSec, path));
    }

    public Cell<?> addCell(ColumnMetadata column, ByteBuffer value) throws InvalidRequestException
    {
        return addCell(column, null, value);
    }

    public Cell<?> addCell(ColumnMetadata column, CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        Guardrails.columnValueSize.guard(value.remaining(), column.name.toString(), false, clientState);

        if (path != null && column.type.isMultiCell())
            Guardrails.columnValueSize.guard(path.dataSize(), column.name.toString(), false, clientState);

        Cell<?> cell = ttl == LivenessInfo.NO_TTL
                       ? BufferCell.live(column, timestamp, value, path)
                       : BufferCell.expiring(column, timestamp, ttl, nowInSec, value, path);
        builder.addCell(cell);
        return cell;
    }

    public void addCounter(ColumnMetadata column, long increment) throws InvalidRequestException
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
        builder.addCell(BufferCell.live(column, timestamp, CounterContext.instance().createUpdate(increment)));
    }

    public void setComplexDeletionTime(ColumnMetadata column)
    {
        builder.addComplexDeletion(column, deletionTime);
    }

    public void setComplexDeletionTimeForOverwrite(ColumnMetadata column)
    {
        builder.addComplexDeletion(column, DeletionTime.build(deletionTime.markedForDeleteAt() - 1, deletionTime.localDeletionTime()));
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

    public RangeTombstone makeRangeTombstone(ClusteringComparator comparator, Clustering<?> clustering)
    {
        return makeRangeTombstone(Slice.make(comparator, clustering));
    }

    public RangeTombstone makeRangeTombstone(Slice slice)
    {
        return new RangeTombstone(slice, deletionTime);
    }

    public byte[] nextTimeUUIDAsBytes()
    {
        return TimeUUID.Generator.nextTimeUUIDAsBytes();
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
    public Row getPrefetchedRow(DecoratedKey key, Clustering<?> clustering)
    {
        if (prefetchedRows == null)
            return null;

        Partition partition = prefetchedRows.get(key);
        Row prefetchedRow = partition == null ? null : partition.getRow(clustering);

        // We need to apply the pending mutations to return the row in its current state
        Row pendingMutations = builder.copy().build();

        if (pendingMutations.isEmpty())
            return prefetchedRow;

        if (prefetchedRow == null)
            return pendingMutations;

        return Rows.merge(prefetchedRow, pendingMutations)
                   .purge(DeletionPurger.PURGE_ALL, nowInSec, metadata.enforceStrictLiveness());
    }
}
