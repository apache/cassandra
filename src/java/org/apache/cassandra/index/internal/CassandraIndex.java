/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.internal;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.*;
import org.apache.cassandra.index.internal.composites.CompositesSearcher;
import org.apache.cassandra.index.internal.keys.KeysSearcher;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * Index implementation which indexes the values for a single column in the base
 * table and which stores its index data in a local, hidden table.
 */
public abstract class CassandraIndex implements Index
{
    public static final String NAME = "legacy_local_table";
    
    private static final Logger logger = LoggerFactory.getLogger(CassandraIndex.class);

    public final ColumnFamilyStore baseCfs;
    protected IndexMetadata metadata;
    protected ColumnFamilyStore indexCfs;
    protected ColumnMetadata indexedColumn;
    protected CassandraIndexFunctions functions;

    protected CassandraIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef)
    {
        this.baseCfs = baseCfs;
        setMetadata(indexDef);
    }

    /**
     * Returns true if an index of this type can support search predicates of the form [column] OPERATOR [value]
     * @param indexedColumn
     * @param operator
     * @return
     */
    protected boolean supportsOperator(ColumnMetadata indexedColumn, Operator operator)
    {
        return operator == Operator.EQ;
    }

    /**
     * Used to construct an the clustering for an entry in the index table based on values from the base data.
     * The clustering columns in the index table encode the values required to retrieve the correct data from the base
     * table and varies depending on the kind of the indexed column. See indexCfsMetadata for more details
     * Used whenever a row in the index table is written or deleted.
     * @param partitionKey from the base data being indexed
     * @param prefix from the base data being indexed
     * @param path from the base data being indexed
     * @return a clustering prefix to be used to insert into the index table
     */
    protected abstract <T> CBuilder buildIndexClusteringPrefix(ByteBuffer partitionKey,
                                                           ClusteringPrefix<T> prefix,
                                                           CellPath path);

    /**
     * Used at search time to convert a row in the index table into a simple struct containing the values required
     * to retrieve the corresponding row from the base table.
     * @param indexedValue the partition key of the indexed table (i.e. the value that was indexed)
     * @param indexEntry a row from the index table
     * @return
     */
    public abstract IndexEntry decodeEntry(DecoratedKey indexedValue,
                                           Row indexEntry);

    /**
     * Check whether a value retrieved from an index is still valid by comparing it to current row from the base table.
     * Used at read time to identify out of date index entries so that they can be excluded from search results and
     * repaired
     * @param row the current row from the primary data table
     * @param indexValue the value we retrieved from the index
     * @param nowInSec
     * @return true if the index is out of date and the entry should be dropped
     */
    public abstract boolean isStale(Row row, ByteBuffer indexValue, long nowInSec);

    /**
     * Extract the value to be inserted into the index from the components of the base data
     * @param partitionKey from the primary data
     * @param clustering from the primary data
     * @param path from the primary data
     * @param cellValue from the primary data
     * @return a ByteBuffer containing the value to be inserted in the index. This will be used to make the partition
     * key in the index table
     */
    protected abstract ByteBuffer getIndexedValue(ByteBuffer partitionKey,
                                                  Clustering<?> clustering,
                                                  CellPath path,
                                                  ByteBuffer cellValue);

    public ColumnMetadata getIndexedColumn()
    {
        return indexedColumn;
    }

    public ClusteringComparator getIndexComparator()
    {
        return indexCfs.metadata().comparator;
    }

    public ColumnFamilyStore getIndexCfs()
    {
        return indexCfs;
    }

    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
    }

    public Callable<?> getInitializationTask()
    {
        // if we're just linking in the index on an already-built index post-restart or if the base
        // table is empty we've nothing to do. Otherwise, submit for building via SecondaryIndexBuilder
        return isBuilt() || baseCfs.isEmpty() ? null : getBuildIndexTask();
    }

    public IndexMetadata getIndexMetadata()
    {
        return metadata;
    }

    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return indexCfs == null ? Optional.empty() : Optional.of(indexCfs);
    }

    public Callable<Void> getBlockingFlushTask()
    {
        return () -> {
            indexCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_TABLE_FLUSH);
            return null;
        };
    }

    public Callable<?> getInvalidateTask()
    {
        return () -> {
            invalidate();
            return null;
        };
    }

    public Callable<?> getMetadataReloadTask(IndexMetadata indexDef)
    {
        return () -> {
            indexCfs.reload();
            return null;
        };
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            ByteBuffer indexValue = target.get().getIndexValue();
            checkFalse(indexValue.remaining() > FBUtilities.MAX_UNSIGNED_SHORT,
                       "Index expression values may not be larger than 64K");
        }
    }

    private void setMetadata(IndexMetadata indexDef)
    {
        metadata = indexDef;
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), indexDef);
        functions = getFunctions(indexDef, target);
        TableMetadataRef tableRef = TableMetadataRef.forOfflineTools(indexCfsMetadata(baseCfs.metadata(), indexDef));
        indexCfs = ColumnFamilyStore.createColumnFamilyStore(baseCfs.keyspace,
                                                             tableRef.name,
                                                             tableRef,
                                                             baseCfs.getTracker().loadsstables);
        indexedColumn = target.left;
    }

    public Callable<?> getTruncateTask(final long truncatedAt)
    {
        return () -> {
            indexCfs.discardSSTables(truncatedAt);
            return null;
        };
    }

    public boolean shouldBuildBlocking()
    {
        // built-in indexes are always included in builds initiated from SecondaryIndexManager
        return true;
    }

    public boolean dependsOn(ColumnMetadata column)
    {
        return indexedColumn.name.equals(column.name);
    }

    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return indexedColumn.name.equals(column.name)
               && supportsOperator(indexedColumn, operator);
    }

    private boolean supportsExpression(RowFilter.Expression expression)
    {
        return supportsExpression(expression.column(), expression.operator());
    }

    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    public long getEstimatedResultRows()
    {
        return indexCfs.getMeanRowCount();
    }

    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return getTargetExpression(filter.getExpressions()).map(filter::without)
                                                           .orElse(filter);
    }

    private Optional<RowFilter.Expression> getTargetExpression(List<RowFilter.Expression> expressions)
    {
        return expressions.stream().filter(this::supportsExpression).findFirst();
    }

    public Index.Searcher searcherFor(ReadCommand command)
    {
        Optional<RowFilter.Expression> target = getTargetExpression(command.rowFilter().getExpressions());

        if (target.isPresent())
        {
            switch (getIndexMetadata().kind)
            {
                case COMPOSITES:
                    return new CompositesSearcher(command, target.get(), this);
                case KEYS:
                    return new KeysSearcher(command, target.get(), this);

                default:
                    throw new IllegalStateException(String.format("Unsupported index type %s for index %s on %s",
                                                                  metadata.kind,
                                                                  metadata.name,
                                                                  indexedColumn.name.toString()));
            }
        }

        return null;

    }

    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        switch (indexedColumn.kind)
        {
            case PARTITION_KEY:
                validatePartitionKey(update.partitionKey());
                break;
            case CLUSTERING:
                validateClusterings(update);
                break;
            case REGULAR:
                if (update.columns().regulars.contains(indexedColumn))
                    validateRows(update);
                break;
            case STATIC:
                if (update.columns().statics.contains(indexedColumn))
                    validateRows(Collections.singleton(update.staticRow()));
                break;
        }
    }

    public Indexer indexerFor(final DecoratedKey key,
                              final RegularAndStaticColumns columns,
                              final long nowInSec,
                              final WriteContext ctx,
                              final IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        /*
         * Indexes on regular and static columns (the non primary-key ones) only care about updates with live
         * data for the column they index. In particular, they don't care about having just row or range deletions
         * as they don't know how to update the index table unless they know exactly the value that is deleted.
         *
         * Note that in practice this means that those indexes are only purged of stale entries on compaction,
         * when we resolve both the deletion and the prior data it deletes. Of course, such stale entries are also
         * filtered on read.
         */
        if (!isPrimaryKeyIndex() && !columns.contains(indexedColumn))
            return null;

        return new Indexer()
        {
            public void begin()
            {
            }

            public void partitionDelete(DeletionTime deletionTime)
            {
            }

            public void rangeTombstone(RangeTombstone tombstone)
            {
            }

            public void insertRow(Row row)
            {
                if (row.isStatic() && !indexedColumn.isStatic() && !indexedColumn.isPartitionKey())
                    return;

                if (isPrimaryKeyIndex())
                {
                    indexPrimaryKey(row.clustering(),
                                    getPrimaryKeyIndexLiveness(row),
                                    row.deletion());
                }
                else
                {
                    if (indexedColumn.isComplex())
                        indexCells(row.clustering(), row.getComplexColumnData(indexedColumn));
                    else
                        indexCell(row.clustering(), row.getCell(indexedColumn));
                }
            }

            public void removeRow(Row row)
            {
                if (isPrimaryKeyIndex())
                    return;

                if (indexedColumn.isComplex())
                    removeCells(row.clustering(), row.getComplexColumnData(indexedColumn));
                else
                    removeCell(row.clustering(), row.getCell(indexedColumn));
            }

            public void updateRow(Row oldRow, Row newRow)
            {
                assert oldRow.isStatic() == newRow.isStatic();
                if (newRow.isStatic() != indexedColumn.isStatic())
                    return;

                if (isPrimaryKeyIndex())
                    indexPrimaryKey(newRow.clustering(),
                                    getPrimaryKeyIndexLiveness(newRow),
                                    newRow.deletion());

                if (indexedColumn.isComplex())
                {
                    indexCells(newRow.clustering(), newRow.getComplexColumnData(indexedColumn));
                    removeCells(oldRow.clustering(), oldRow.getComplexColumnData(indexedColumn));
                }
                else
                {
                    indexCell(newRow.clustering(), newRow.getCell(indexedColumn));
                    removeCell(oldRow.clustering(), oldRow.getCell(indexedColumn));
                }
            }

            public void finish()
            {
            }

            private void indexCells(Clustering<?> clustering, Iterable<Cell<?>> cells)
            {
                if (cells == null)
                    return;

                for (Cell<?> cell : cells)
                    indexCell(clustering, cell);
            }

            private void indexCell(Clustering<?> clustering, Cell<?> cell)
            {
                if (cell == null || !cell.isLive(nowInSec))
                    return;

                insert(key.getKey(),
                       clustering,
                       cell,
                       LivenessInfo.withExpirationTime(cell.timestamp(), cell.ttl(), cell.localDeletionTime()),
                       ctx);
            }

            private void removeCells(Clustering<?> clustering, Iterable<Cell<?>> cells)
            {
                if (cells == null)
                    return;

                for (Cell<?> cell : cells)
                    removeCell(clustering, cell);
            }

            private void removeCell(Clustering<?> clustering, Cell<?> cell)
            {
                if (cell == null || !cell.isLive(nowInSec))
                    return;

                delete(key.getKey(), clustering, cell, ctx, nowInSec);
            }

            private void indexPrimaryKey(final Clustering<?> clustering,
                                         final LivenessInfo liveness,
                                         final Row.Deletion deletion)
            {
                if (liveness.timestamp() != LivenessInfo.NO_TIMESTAMP)
                    insert(key.getKey(), clustering, null, liveness, ctx);

                if (!deletion.isLive())
                    delete(key.getKey(), clustering, deletion.time(), ctx);
            }

            private LivenessInfo getPrimaryKeyIndexLiveness(Row row)
            {
                long timestamp = row.primaryKeyLivenessInfo().timestamp();
                int ttl = row.primaryKeyLivenessInfo().ttl();
                for (Cell<?> cell : row.cells())
                {
                    long cellTimestamp = cell.timestamp();
                    if (cell.isLive(nowInSec))
                    {
                        if (cellTimestamp > timestamp)
                            timestamp = cellTimestamp;
                    }
                }
                return LivenessInfo.create(timestamp, ttl, nowInSec);
            }
        };
    }

    /**
     * Specific to internal indexes, this is called by a
     * searcher when it encounters a stale entry in the index
     * @param indexKey the partition key in the index table
     * @param indexClustering the clustering in the index table
     * @param deletion deletion timestamp etc
     * @param ctx the write context under which to perform the deletion
     */
    public void deleteStaleEntry(DecoratedKey indexKey,
                                 Clustering<?> indexClustering,
                                 DeletionTime deletion,
                                 WriteContext ctx)
    {
        doDelete(indexKey, indexClustering, deletion, ctx);
        logger.trace("Removed index entry for stale value {}", indexKey);
    }

    /**
     * Called when adding a new entry to the index
     */
    private void insert(ByteBuffer rowKey,
                        Clustering<?> clustering,
                        Cell<?> cell,
                        LivenessInfo info,
                        WriteContext ctx)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               cell));
        Row row = BTreeRow.noCellLiveRow(buildIndexClustering(rowKey, clustering, cell), info);
        PartitionUpdate upd = partitionUpdate(valueKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, false);
        logger.trace("Inserted entry into index for value {}", valueKey);
    }

    /**
     * Called when deleting entries on non-primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering<?> clustering,
                        Cell<?> cell,
                        WriteContext ctx,
                        long nowInSec)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               cell));
        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, cell),
                 DeletionTime.build(cell.timestamp(), nowInSec),
                 ctx);
    }

    /**
     * Called when deleting entries from indexes on primary key columns
     */
    private void delete(ByteBuffer rowKey,
                        Clustering<?> clustering,
                        DeletionTime deletion,
                        WriteContext ctx)
    {
        DecoratedKey valueKey = getIndexKeyFor(getIndexedValue(rowKey,
                                                               clustering,
                                                               null));
        doDelete(valueKey,
                 buildIndexClustering(rowKey, clustering, null),
                 deletion,
                 ctx);
    }

    private void doDelete(DecoratedKey indexKey,
                          Clustering<?> indexClustering,
                          DeletionTime deletion,
                          WriteContext ctx)
    {
        Row row = BTreeRow.emptyDeletedRow(indexClustering, Row.Deletion.regular(deletion));
        PartitionUpdate upd = partitionUpdate(indexKey, row);
        indexCfs.getWriteHandler().write(upd, ctx, false);
        logger.trace("Removed index entry for value {}", indexKey);
    }

    private void validatePartitionKey(DecoratedKey partitionKey) throws InvalidRequestException
    {
        assert indexedColumn.isPartitionKey();
        validateIndexedValue(getIndexedValue(partitionKey.getKey(), null, null));
    }

    private void validateClusterings(PartitionUpdate update) throws InvalidRequestException
    {
        assert indexedColumn.isClusteringColumn();
        for (Row row : update)
            validateIndexedValue(getIndexedValue(null, row.clustering(), null));
    }

    private void validateRows(Iterable<Row> rows)
    {
        assert !indexedColumn.isPrimaryKeyColumn();
        for (Row row : rows)
        {
            if (indexedColumn.isComplex())
            {
                ComplexColumnData data = row.getComplexColumnData(indexedColumn);
                if (data != null)
                {
                    for (Cell<?> cell : data)
                    {
                        validateIndexedValue(getIndexedValue(null, null, cell.path(), cell.buffer()));
                    }
                }
            }
            else
            {
                validateIndexedValue(getIndexedValue(null, null, row.getCell(indexedColumn)));
            }
        }
    }

    private void validateIndexedValue(ByteBuffer value)
    {
        if (value != null && value.remaining() >= FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format(
                                                           "Cannot index value of size %d for index %s on %s(%s) (maximum allowed size=%d)",
                                                           value.remaining(),
                                                           metadata.name,
                                                           baseCfs.metadata,
                                                           indexedColumn.name.toString(),
                                                           FBUtilities.MAX_UNSIGNED_SHORT));
    }

    private ByteBuffer getIndexedValue(ByteBuffer rowKey,
                                       Clustering<?> clustering,
                                       Cell<?> cell)
    {
        return getIndexedValue(rowKey,
                               clustering,
                               cell == null ? null : cell.path(),
                               cell == null ? null : cell.buffer()
        );
    }

    private Clustering<?> buildIndexClustering(ByteBuffer rowKey,
                                            Clustering<?> clustering,
                                            Cell<?> cell)
    {
        return buildIndexClusteringPrefix(rowKey,
                                          clustering,
                                          cell == null ? null : cell.path()).build();
    }

    private DecoratedKey getIndexKeyFor(ByteBuffer value)
    {
        return indexCfs.decorateKey(value);
    }

    private PartitionUpdate partitionUpdate(DecoratedKey valueKey, Row row)
    {
        return PartitionUpdate.singleRowUpdate(indexCfs.metadata(), valueKey, row);
    }

    private void invalidate()
    {
        // interrupt in-progress compactions
        Collection<ColumnFamilyStore> cfss = Collections.singleton(indexCfs);
        CompactionManager.instance.interruptCompactionForCFs(cfss, (sstable) -> true, true);
        CompactionManager.instance.waitForCessation(cfss, (sstable) -> true);
        Keyspace.writeOrder.awaitNewBarrier();
        indexCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_REMOVED);
        indexCfs.readOrdering.awaitNewBarrier();
        indexCfs.invalidate();
    }

    private boolean isBuilt()
    {
        return SystemKeyspace.isIndexBuilt(baseCfs.getKeyspaceName(), metadata.name);
    }

    private boolean isPrimaryKeyIndex()
    {
        return indexedColumn.isPrimaryKeyColumn();
    }

    private Callable<?> getBuildIndexTask()
    {
        return () -> {
            buildBlocking();
            return null;
        };
    }

    private void buildBlocking()
    {
        baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
             Refs<SSTableReader> sstables = viewFragment.refs)
        {
            if (sstables.isEmpty())
            {
                logger.info("No SSTable data for {}.{} to build index {} from, marking empty index as built",
                            baseCfs.metadata.keyspace,
                            baseCfs.metadata.name,
                            metadata.name);
                return;
            }

            logger.info("Submitting index build of {} for data in {}",
                        metadata.name,
                        getSSTableNames(sstables));

            SecondaryIndexBuilder builder = new CollatedViewIndexBuilder(baseCfs,
                                                                         Collections.singleton(this),
                                                                         new ReducingKeyIterator(sstables),
                                                                         ImmutableSet.copyOf(sstables));
            Future<?> future = CompactionManager.instance.submitIndexBuild(builder);
            FBUtilities.waitOnFuture(future);
            indexCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_COMPLETED);
        }
        logger.info("Index build of {} complete", metadata.name);
    }

    private static String getSSTableNames(Collection<SSTableReader> sstables)
    {
        return StreamSupport.stream(sstables.spliterator(), false)
                            .map(SSTableReader::toString)
                            .collect(Collectors.joining(", "));
    }

    /**
     * Construct the TableMetadata for an index table, the clustering columns in the index table
     * vary dependent on the kind of the indexed value.
     * @param baseCfsMetadata
     * @param indexMetadata
     * @return
     */
    public static TableMetadata indexCfsMetadata(TableMetadata baseCfsMetadata, IndexMetadata indexMetadata)
    {
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfsMetadata, indexMetadata);
        CassandraIndexFunctions utils = getFunctions(indexMetadata, target);
        ColumnMetadata indexedColumn = target.left;
        AbstractType<?> indexedValueType = utils.getIndexedValueType(indexedColumn);

        // if Cassandra's major version is before 5, use the old behaviour
        boolean isCompatible = DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5);
        AbstractType<?> indexedTablePartitionKeyType = baseCfsMetadata.partitioner.partitionOrdering(baseCfsMetadata.partitionKeyType);
        TableMetadata.Builder builder =
            TableMetadata.builder(baseCfsMetadata.keyspace, baseCfsMetadata.indexTableName(indexMetadata), baseCfsMetadata.id)
                         .kind(TableMetadata.Kind.INDEX)
                         .partitioner(new LocalPartitioner(indexedValueType))
                         .addPartitionKeyColumn(indexedColumn.name, isCompatible ? indexedColumn.type : utils.getIndexedPartitionKeyType(indexedColumn))
                         .addClusteringColumn("partition_key", isCompatible ? baseCfsMetadata.partitioner.partitionOrdering() : indexedTablePartitionKeyType);

        // Adding clustering columns, which depends on the index type.
        builder = utils.addIndexClusteringColumns(builder, baseCfsMetadata, indexedColumn);

        return builder.build().updateIndexTableMetadata(baseCfsMetadata.params);
    }

    /**
     * Factory method for new CassandraIndex instances
     * @param baseCfs
     * @param indexMetadata
     * @return
     */
    public static CassandraIndex newIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        return getFunctions(indexMetadata, TargetParser.parse(baseCfs.metadata(), indexMetadata)).newIndexInstance(baseCfs, indexMetadata);
    }

    static CassandraIndexFunctions getFunctions(IndexMetadata indexDef,
                                                Pair<ColumnMetadata, IndexTarget.Type> target)
    {
        if (indexDef.isKeys())
            return CassandraIndexFunctions.KEYS_INDEX_FUNCTIONS;

        ColumnMetadata indexedColumn = target.left;
        if (indexedColumn.type.isCollection() && indexedColumn.type.isMultiCell())
        {
            switch (((CollectionType)indexedColumn.type).kind)
            {
                case LIST:
                    return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
                case SET:
                    return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
                case MAP:
                    switch (target.right)
                    {
                        case KEYS:
                            return CassandraIndexFunctions.COLLECTION_KEY_INDEX_FUNCTIONS;
                        case KEYS_AND_VALUES:
                            return CassandraIndexFunctions.COLLECTION_ENTRY_INDEX_FUNCTIONS;
                        case VALUES:
                            return CassandraIndexFunctions.COLLECTION_VALUE_INDEX_FUNCTIONS;
                    }
                    throw new AssertionError();
            }
        }

        switch (indexedColumn.kind)
        {
            case CLUSTERING:
                return CassandraIndexFunctions.CLUSTERING_COLUMN_INDEX_FUNCTIONS;
            case REGULAR:
            case STATIC:
                return CassandraIndexFunctions.REGULAR_COLUMN_INDEX_FUNCTIONS;
            case PARTITION_KEY:
                return CassandraIndexFunctions.PARTITION_KEY_INDEX_FUNCTIONS;
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }
}
