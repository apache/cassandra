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

package org.apache.cassandra.index.sai;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class StorageAttachedIndex implements Index
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndex.class);

    private static class StorageAttachedIndexBuildingSupport implements IndexBuildingSupport
    {
        @Override
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild,
                                                       boolean isFullRebuild)
        {
            // Return an empty builder for in-memory implementation
            return new SecondaryIndexBuilder()
            {
                @Override
                public void build()
                {
                }

                @Override
                public CompactionInfo getCompactionInfo()
                {
                    return null;
                }
            };
        }
    }

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

    private static final Set<String> VALID_OPTIONS = ImmutableSet.of(IndexTarget.TARGET_OPTION_NAME,
                                                                     IndexTarget.CUSTOM_INDEX_OPTION_NAME);

    public static final Set<CQL3Type> SUPPORTED_TYPES = ImmutableSet.of(CQL3Type.Native.ASCII, CQL3Type.Native.BIGINT, CQL3Type.Native.DATE,
                                                                        CQL3Type.Native.DOUBLE, CQL3Type.Native.FLOAT, CQL3Type.Native.INT,
                                                                        CQL3Type.Native.SMALLINT, CQL3Type.Native.TEXT, CQL3Type.Native.TIME,
                                                                        CQL3Type.Native.TIMESTAMP, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TINYINT,
                                                                        CQL3Type.Native.UUID, CQL3Type.Native.VARCHAR, CQL3Type.Native.INET,
                                                                        CQL3Type.Native.VARINT, CQL3Type.Native.DECIMAL, CQL3Type.Native.BOOLEAN);

    private static final Set<Class<? extends IPartitioner>> ILLEGAL_PARTITIONERS =
            ImmutableSet.of(OrderPreservingPartitioner.class, LocalPartitioner.class, ByteOrderedPartitioner.class, RandomPartitioner.class);

    private final ColumnFamilyStore baseCfs;
    private final IndexContext indexContext;

    public StorageAttachedIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, config);
        this.indexContext = new IndexContext(tableMetadata.keyspace,
                                             tableMetadata.name,
                                             tableMetadata.partitionKeyType,
                                             tableMetadata.comparator,
                                             target.left,
                                             target.right,
                                             config);
    }

    /**
     * Used via reflection in {@link IndexMetadata}
     */
    @SuppressWarnings({ "unused" })
    public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata)
    {
        Map<String, String> unknown = new HashMap<>(2);

        for (Map.Entry<String, String> option : options.entrySet())
        {
            if (!VALID_OPTIONS.contains(option.getKey()))
            {
                unknown.put(option.getKey(), option.getValue());
            }
        }

        if (!unknown.isEmpty())
        {
            return unknown;
        }

        if (ILLEGAL_PARTITIONERS.contains(metadata.partitioner.getClass()))
        {
            throw new InvalidRequestException("Storage-attached index does not support the following IPartitioner implementations: " + ILLEGAL_PARTITIONERS);
        }

        String targetColumn = options.get(IndexTarget.TARGET_OPTION_NAME);

        if (targetColumn == null)
        {
            throw new InvalidRequestException("Missing target column");
        }

        if (targetColumn.split(",").length > 1)
        {
            throw new InvalidRequestException("A storage-attached index cannot be created over multiple columns: " + targetColumn);
        }

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(metadata, targetColumn);

        if (target == null)
        {
            throw new InvalidRequestException("Failed to retrieve target column for: " + targetColumn);
        }

        // In order to support different index targets on non-frozen map, ie. KEYS, VALUE, ENTRIES, we need to put index
        // name as part of index file name instead of column name. We only need to check that the target is different
        // between indexes. This will only allow indexes in the same column with a different IndexTarget.Type.
        //
        // Note that: "metadata.indexes" already includes current index
        if (metadata.indexes.stream().filter(index -> index.getIndexClassName().equals(StorageAttachedIndex.class.getName()))
                            .map(index -> TargetParser.parse(metadata, index.options.get(IndexTarget.TARGET_OPTION_NAME)))
                            .filter(Objects::nonNull).filter(t -> t.equals(target)).count() > 1)
        {
            throw new InvalidRequestException("Cannot create more than one storage-attached index on the same column: " + target.left);
        }

        AbstractType<?> type = TypeUtil.cellValueType(target.left, target.right);

        // If we are indexing map entries we need to validate the subtypes
        if (TypeUtil.isComposite(type))
        {
            for (AbstractType<?> subType : type.subTypes())
            {
                if (!SUPPORTED_TYPES.contains(subType.asCQL3Type()) && !TypeUtil.isFrozen(subType))
                    throw new InvalidRequestException("Unsupported type: " + subType.asCQL3Type());
            }
        }
        else if (!SUPPORTED_TYPES.contains(type.asCQL3Type()) && !TypeUtil.isFrozen(type))
        {
            throw new InvalidRequestException("Unsupported type: " + type.asCQL3Type());
        }

        AbstractAnalyzer.fromOptions(type, options);

        return Collections.emptyMap();
    }

    @Override
    public void register(IndexRegistry registry)
    {
        // index will be available for writes
        registry.registerIndex(this, StorageAttachedIndexGroup.class, () -> new StorageAttachedIndexGroup(baseCfs));
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return indexContext.getIndexMetadata();
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        // New storage-attached indexes will be available for queries after on disk index data are built.
        // Memtable data will be indexed via flushing triggered by schema change
        return () -> startInitialBuild(baseCfs).get();
    }

    private Future<?> startInitialBuild(ColumnFamilyStore baseCfs)
    {
        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexContext.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage attached index queryable..."));
        }
        baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null; // storage-attached indexes are flushed alongside memtable
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () ->
        {
            indexContext.invalidate();
            return null;
        };
    }

    @Override
    public Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        /*
         * During bootstrap, streamed SSTable are already built for existing indexes via {@link StorageAttachedIndexBuildingSupport}
         * from {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable}.
         *
         * For indexes created during bootstrapping, we don't have to block bootstrap for them.
         */

        return this::startPreJoinTask;
    }

    private Future<?> startPreJoinTask()
    {
        try
        {
            if (baseCfs.indexManager.isIndexQueryable(this))
                logger.debug(indexContext.logMessage("Skipping validation in pre-join task, as the initialization task has already made the index queryable..."));

            baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed in pre-join task!"), t);
        }

        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in
         * {@link LogTransaction#delete(java.io.File)} asynchronously.
         */
        return null;
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return indexContext.getDefinition().compareTo(column) == 0;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return dependsOn(column) && indexContext.supports(operator);
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        // it should be executed from the SAI query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public long getEstimatedResultRows()
    {
        throw new UnsupportedOperationException("Use StorageAttachedIndexQueryPlan#getEstimatedResultRows() instead.");
    }

    @Override
    public boolean isQueryable(Status status)
    {
        // consider unknown status as queryable, because gossip may not be up-to-date for newly joining nodes.
        return status == Status.BUILD_SUCCEEDED || status == Status.UNKNOWN;
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {}

    @Override
    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        // searchers should be created from the query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
    {
        // flush observers should be created from the index group, this is only used by the singleton index group
        throw new UnsupportedOperationException("Storage-attached index flush observers should never be created directly.");
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext writeContext,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        if (transactionType == IndexTransaction.Type.UPDATE)
        {
            return new UpdateIndexer(key, memtable, writeContext);
        }

        // we are only interested in the data from Memtable
        // everything else is going to be handled by SSTableWriter observers
        return null;
    }

    @Override
    public IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s.%s", baseCfs.keyspace.getName(), baseCfs.name, getIndexMetadata() == null ? "?" : getIndexMetadata());
    }

    /**
     * Removes this index from the {@code SecondaryIndexManager}'s set of queryable indexes.
     */
    public void makeIndexNonQueryable()
    {
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexContext.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }

    private class UpdateIndexer implements Index.Indexer
    {
        private final DecoratedKey key;
        private final Memtable memtable;
        private final WriteContext writeContext;

        UpdateIndexer(DecoratedKey key, Memtable memtable, WriteContext writeContext)
        {
            this.key = key;
            this.memtable = memtable;
            this.writeContext = writeContext;
        }

        @Override
        public void insertRow(Row row)
        {
            adjustMemtableSize(indexContext.index(key, row, memtable), CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            insertRow(newRow);
        }

        void adjustMemtableSize(long additionalSpace, OpOrder.Group opGroup)
        {
            memtable.markExtraOnHeapUsed(additionalSpace, opGroup);
        }
    }
}
