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
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A simple container that simplify passing parameters for collections methods.
 */
public class UpdateParameters
{
    public final CFMetaData metadata;
    public final QueryOptions options;

    private final LivenessInfo defaultLiveness;
    private final LivenessInfo deletionLiveness;
    private final DeletionTime deletionTime;

    private final SecondaryIndexManager indexManager;

    // For lists operation that require a read-before-write. Will be null otherwise.
    private final Map<DecoratedKey, Partition> prefetchedRows;

    public UpdateParameters(CFMetaData metadata, QueryOptions options, long timestamp, int ttl, Map<DecoratedKey, Partition> prefetchedRows, boolean validateIndexedColumns)
    throws InvalidRequestException
    {
        this.metadata = metadata;
        this.options = options;

        int nowInSec = FBUtilities.nowInSeconds();
        this.defaultLiveness = SimpleLivenessInfo.forUpdate(timestamp, ttl, nowInSec, metadata);
        this.deletionLiveness = SimpleLivenessInfo.forDeletion(timestamp, nowInSec);
        this.deletionTime = new SimpleDeletionTime(timestamp, nowInSec);

        this.prefetchedRows = prefetchedRows;

        // Index column validation triggers a call to Keyspace.open() which we want to be able to avoid in some case (e.g. when using CQLSSTableWriter)
        if (validateIndexedColumns)
        {
            SecondaryIndexManager manager = Keyspace.openAndGetStore(metadata).indexManager;
            indexManager = manager.hasIndexes() ? manager : null;
        }
        else
        {
            indexManager = null;
        }

        // We use MIN_VALUE internally to mean the absence of of timestamp (in Selection, in sstable stats, ...), so exclude
        // it to avoid potential confusion.
        if (timestamp == Long.MIN_VALUE)
            throw new InvalidRequestException(String.format("Out of bound timestamp, must be in [%d, %d]", Long.MIN_VALUE + 1, Long.MAX_VALUE));
    }

    public void newPartition(DecoratedKey partitionKey) throws InvalidRequestException
    {
        if (indexManager != null)
            indexManager.validate(partitionKey);
    }

    public void writeClustering(Clustering clustering, Row.Writer writer) throws InvalidRequestException
    {
        if (indexManager != null)
            indexManager.validate(clustering);

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

        Rows.writeClustering(clustering, writer);
    }

    public void writePartitionKeyLivenessInfo(Row.Writer writer)
    {
        writer.writePartitionKeyLivenessInfo(defaultLiveness);
    }

    public void writeRowDeletion(Row.Writer writer)
    {
        writer.writeRowDeletion(deletionTime);
    }

    public void addTombstone(ColumnDefinition column, Row.Writer writer) throws InvalidRequestException
    {
        addTombstone(column, writer, null);
    }

    public void addTombstone(ColumnDefinition column, Row.Writer writer, CellPath path) throws InvalidRequestException
    {
        writer.writeCell(column, false, ByteBufferUtil.EMPTY_BYTE_BUFFER, deletionLiveness, path);
    }

    public void addCell(Clustering clustering, ColumnDefinition column, Row.Writer writer, ByteBuffer value) throws InvalidRequestException
    {
        addCell(clustering, column, writer, null, value);
    }

    public void addCell(Clustering clustering, ColumnDefinition column, Row.Writer writer, CellPath path, ByteBuffer value) throws InvalidRequestException
    {
        if (indexManager != null)
            indexManager.validate(column, value, path);

        writer.writeCell(column, false, value, defaultLiveness, path);
    }

    public void addCounter(ColumnDefinition column, Row.Writer writer, long increment) throws InvalidRequestException
    {
        assert defaultLiveness.ttl() == LivenessInfo.NO_TTL;

        // In practice, the actual CounterId (and clock really) that we use doesn't matter, because we will
        // actually ignore it in CounterMutation when we do the read-before-write to create the actual value
        // that is applied. In other words, this is not the actual value that will be written to the memtable
        // because this will be replaced in CounterMutation.updateWithCurrentValue().
        // As an aside, since we don't care about the CounterId/clock, we used to only send the incremement,
        // but that makes things a bit more complex as this means we need to be able to distinguish inside
        // PartitionUpdate between counter updates that has been processed by CounterMutation and those that
        // haven't.
        ByteBuffer value = CounterContext.instance().createLocal(increment);
        writer.writeCell(column, true, value, defaultLiveness, null);
    }

    public void setComplexDeletionTime(ColumnDefinition column, Row.Writer writer)
    {
        writer.writeComplexDeletion(column, deletionTime);
    }

    public void setComplexDeletionTimeForOverwrite(ColumnDefinition column, Row.Writer writer)
    {
        writer.writeComplexDeletion(column, new SimpleDeletionTime(deletionTime.markedForDeleteAt() - 1, deletionTime.localDeletionTime()));
    }

    public DeletionTime deletionTime()
    {
        return deletionTime;
    }

    public RangeTombstone makeRangeTombstone(CBuilder cbuilder)
    {
        return new RangeTombstone(cbuilder.buildSlice(), deletionTime);
    }

    public Row getPrefetchedRow(DecoratedKey key, Clustering clustering)
    {
        if (prefetchedRows == null)
            return null;

        Partition partition = prefetchedRows.get(key);
        return partition == null ? null : partition.searchIterator(ColumnFilter.selection(partition.columns()), false).next(clustering);
    }
}
