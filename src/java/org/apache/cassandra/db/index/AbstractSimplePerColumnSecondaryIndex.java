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
package org.apache.cassandra.db.index;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Implements a secondary index for a column family using a second column family
 * in which the row keys are indexed values, and column names are base row keys.
 */
public abstract class AbstractSimplePerColumnSecondaryIndex extends PerColumnSecondaryIndex
{
    protected ColumnFamilyStore indexCfs;

    // SecondaryIndex "forces" a set of ColumnDefinition. However this class (and thus it's subclass)
    // only support one def per index. So inline it in a field for 1) convenience and 2) avoid creating
    // an iterator each time we need to access it.
    // TODO: we should fix SecondaryIndex API
    protected ColumnDefinition columnDef;

    public void init()
    {
        assert baseCfs != null && columnDefs != null && columnDefs.size() == 1;

        columnDef = columnDefs.iterator().next();

        CFMetaData indexedCfMetadata = SecondaryIndex.newIndexMetadata(baseCfs.metadata, columnDef);
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             indexedCfMetadata.cfName,
                                                             new LocalPartitioner(getIndexKeyComparator()),
                                                             indexedCfMetadata,
                                                             baseCfs.getTracker().loadsstables);
    }

    protected AbstractType<?> getIndexKeyComparator()
    {
        return columnDef.type;
    }

    public ColumnDefinition indexedColumn()
    {
        return columnDef;
    }

    @Override
    String indexTypeForGrouping()
    {
        return "_internal_";
    }

    protected Clustering makeIndexClustering(ByteBuffer rowKey, Clustering clustering, Cell cell)
    {
        return makeIndexClustering(rowKey, clustering, cell == null ? null : cell.path());
    }

    protected Clustering makeIndexClustering(ByteBuffer rowKey, Clustering clustering, CellPath path)
    {
        return buildIndexClusteringPrefix(rowKey, clustering, path).build();
    }

    protected Slice.Bound makeIndexBound(ByteBuffer rowKey, Slice.Bound bound)
    {
        return buildIndexClusteringPrefix(rowKey, bound, null).buildBound(bound.isStart(), bound.isInclusive());
    }

    protected abstract CBuilder buildIndexClusteringPrefix(ByteBuffer rowKey, ClusteringPrefix prefix, CellPath path);

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, Cell cell)
    {
        return cell == null
             ? getIndexedValue(rowKey, clustering, null, null)
             : getIndexedValue(rowKey, clustering, cell.value(), cell.path());
    }

    protected abstract ByteBuffer getIndexedValue(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath cellPath);

    public void delete(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec)
    {
        deleteForCleanup(rowKey, clustering, cell, opGroup, nowInSec);
    }

    public void deleteForCleanup(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup, int nowInSec)
    {
        delete(rowKey, clustering, cell.value(), cell.path(), new SimpleDeletionTime(cell.livenessInfo().timestamp(), nowInSec), opGroup);
    }

    public void delete(ByteBuffer rowKey, Clustering clustering, ByteBuffer cellValue, CellPath path, DeletionTime deletion, OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey, clustering, cellValue, path));
        PartitionUpdate upd = new PartitionUpdate(indexCfs.metadata, valueKey, PartitionColumns.NONE, 1);
        Row.Writer writer = upd.writer();
        Rows.writeClustering(makeIndexClustering(rowKey, clustering, path), writer);
        writer.writeRowDeletion(deletion);
        writer.endOfRow();
        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", valueKey, upd);
    }

    public void insert(ByteBuffer rowKey, Clustering clustering, Cell cell, OpOrder.Group opGroup)
    {
        insert(rowKey, clustering, cell, cell.livenessInfo(), opGroup);
    }

    public void insert(ByteBuffer rowKey, Clustering clustering, Cell cell, LivenessInfo info, OpOrder.Group opGroup)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey, clustering, cell));

        PartitionUpdate upd = new PartitionUpdate(indexCfs.metadata, valueKey, PartitionColumns.NONE, 1);
        Row.Writer writer = upd.writer();
        Rows.writeClustering(makeIndexClustering(rowKey, clustering, cell), writer);
        writer.writePartitionKeyLivenessInfo(info);
        writer.endOfRow();
        if (logger.isDebugEnabled())
            logger.debug("applying index row {} in {}", indexCfs.metadata.getKeyValidator().getString(valueKey.getKey()), upd);

        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);
    }

    public void update(ByteBuffer rowKey, Clustering clustering, Cell oldCell, Cell cell, OpOrder.Group opGroup, int nowInSec)
    {
        // insert the new value before removing the old one, so we never have a period
        // where the row is invisible to both queries (the opposite seems preferable); see CASSANDRA-5540
        insert(rowKey, clustering, cell, opGroup);
        if (SecondaryIndexManager.shouldCleanupOldValue(oldCell, cell))
            delete(rowKey, clustering, oldCell, opGroup, nowInSec);
    }

    public boolean indexes(ColumnDefinition column)
    {
        return column.name.equals(columnDef.name);
    }

    public void removeIndex(ByteBuffer columnName)
    {
        indexCfs.invalidate();
    }

    public void forceBlockingFlush()
    {
        Future<?> wait;
        // we synchronise on the baseCfs to make sure we are ordered correctly with other flushes to the base CFS
        synchronized (baseCfs.getTracker())
        {
            wait = indexCfs.forceFlush();
        }
        FBUtilities.waitOnFuture(wait);
    }

    public void invalidate()
    {
        indexCfs.invalidate();
    }

    public void truncateBlocking(long truncatedAt)
    {
        indexCfs.discardSSTables(truncatedAt);
    }

    public ColumnFamilyStore getIndexCfs()
    {
       return indexCfs;
    }

    protected ClusteringComparator getIndexComparator()
    {
        assert indexCfs != null;
        return indexCfs.metadata.comparator;
    }

    public String getIndexName()
    {
        return indexCfs.name;
    }

    public void reload()
    {
        indexCfs.metadata.reloadIndexMetadataProperties(baseCfs.metadata);
        indexCfs.reload();
    }

    public long estimateResultRows()
    {
        return getIndexCfs().getMeanColumns();
    }

    public void validate(DecoratedKey partitionKey) throws InvalidRequestException
    {
        if (columnDef.kind == ColumnDefinition.Kind.PARTITION_KEY)
            validateIndexedValue(getIndexedValue(partitionKey.getKey(), null, null, null));
    }

    public void validate(Clustering clustering) throws InvalidRequestException
    {
        if (columnDef.kind == ColumnDefinition.Kind.CLUSTERING_COLUMN)
            validateIndexedValue(getIndexedValue(null, clustering, null, null));
    }

    public void validate(ByteBuffer cellValue, CellPath path) throws InvalidRequestException
    {
        if (!columnDef.isPrimaryKeyColumn())
            validateIndexedValue(getIndexedValue(null, null, cellValue, path));
    }

    private void validateIndexedValue(ByteBuffer value)
    {
        if (value != null && value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Cannot index value of size %d for index %s on %s.%s(%s) (maximum allowed size=%d)",
                                                            value.remaining(), getIndexName(), baseKeyspace(), baseTable(), columnDef.name, FBUtilities.MAX_UNSIGNED_SHORT));
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", baseTable(), columnDef.name);
    }
}
