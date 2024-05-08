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

package org.apache.cassandra.index.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

public class RouteIndex implements Index, INotificationConsumer
{
    public enum RegisterStatus
    {PENDING, REGISTERED, UNREGISTERED}

    private static final Logger logger = LoggerFactory.getLogger(RouteIndex.class);

    private static final Component.Type type = Component.Type.createSingleton("AccordRoute", "AccordRoute.*.db", true, null);

    private final ColumnFamilyStore baseCfs;
    private final ColumnMetadata column;
    private final IndexMetadata indexMetadata;
    private final IndexMetrics indexMetrics;
    private final MemtableIndexManager memtableIndexManager;
    private final SSTableManager sstableManager;
    // Tracks whether we've started the index build on initialization.
    private volatile boolean initBuildStarted = false;
    private volatile RegisterStatus registerStatus = RegisterStatus.PENDING;

    public RouteIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        if (!SchemaConstants.ACCORD_KEYSPACE_NAME.equals(baseCfs.getKeyspaceName()))
            throw new IllegalArgumentException("Route index is only allowed for accord commands table; given " + baseCfs().metadata());
        if (!AccordKeyspace.COMMANDS.equals(baseCfs.name))
            throw new IllegalArgumentException("Route index is only allowed for accord commands table; given " + baseCfs().metadata());

        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, indexMetadata);
        if (!AccordKeyspace.CommandsColumns.route.name.equals(target.left.name))
            throw new IllegalArgumentException("Attempted to index the wrong column; needed " + AccordKeyspace.CommandsColumns.route.name + " but given " + target.left.name);

        if (target.right != IndexTarget.Type.VALUES)
            throw new IllegalArgumentException("Attempted to index " + AccordKeyspace.CommandsColumns.route.name + " with index type " + target.right + "; only " + IndexTarget.Type.VALUES + " is supported");

        this.baseCfs = baseCfs;
        this.indexMetadata = indexMetadata;
        this.memtableIndexManager = new RouteMemtableIndexManager(this);
        this.sstableManager = new RouteSSTableManager();
        this.indexMetrics = new IndexMetrics(this);
        this.column = target.left;

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    public ColumnMetadata column()
    {
        return column;
    }

    public IndexMetrics indexMetrics()
    {
        return indexMetrics;
    }

    public ColumnFamilyStore baseCfs()
    {
        return baseCfs;
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return indexMetadata;
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public boolean isSSTableAttached()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public Set<Component> getComponents()
    {
        return Collections.singleton(type.getSingleton());
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        //TODO (now): in SAI startup doesn't validate... what are the downstream issues this can face?  Corrupt indexes not being detected?
        boolean starting = StorageService.instance.isStarting();
        return () -> {
            if (baseCfs.indexManager.isIndexQueryable(this))
            {
                initBuildStarted = true;
                return null;
            }

            // stop in-progress compaction tasks to prevent compacted sstable not being indexed.
            CompactionManager.instance.interruptCompactionFor(Collections.singleton(baseCfs.metadata()),
                                                              ssTableReader -> true,
                                                              true);
            // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
            // In the case of offline scrub, there are no live memtables.
            if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
                baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);

            // It is now safe to flush indexes directly from flushing Memtables.
            initBuildStarted = true;

            List<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, sstableManager);

            if (nonIndexed.isEmpty())
                return null;

            // split sorted sstables into groups with similar size and build each group in separate compaction thread
            List<List<SSTableReader>> groups = StorageAttachedIndex.groupBySize(nonIndexed, DatabaseDescriptor.getConcurrentIndexBuilders());
            List<Future<?>> futures = new ArrayList<>();

            for (List<SSTableReader> group : groups)
            {
                futures.add(CompactionManager.instance.submitIndexBuild(new RouteSecondaryIndexBuilder(this, sstableManager, group, false, true)));
            }

            return FutureCombiner.allOf(futures).get();
        };
    }

    private List<SSTableReader> findNonIndexedSSTables(ColumnFamilyStore baseCfs, SSTableManager manager)
    {
        Set<SSTableReader> sstables = baseCfs.getLiveSSTables();

        // Initialize the SSTable indexes w/ valid existing components...
        manager.onSSTableChanged(Collections.emptyList(), sstables);

        // ...then identify and rebuild the SSTable indexes that are missing.
        List<SSTableReader> nonIndexed = new ArrayList<>();

        for (SSTableReader sstable : sstables)
        {
            if (!sstable.isMarkedCompacted() && !manager.isIndexComplete(sstable))
            {
                nonIndexed.add(sstable);
            }
        }

        return nonIndexed;
    }


    @Override
    public boolean isQueryable(Status status)
    {
        // consider unknown status as queryable, because gossip may not be up-to-date for newly joining nodes.
        return status == Status.BUILD_SUCCEEDED || status == Status.UNKNOWN;
    }

    public RegisterStatus registerStatus()
    {
        return registerStatus;
    }

    @Override
    public synchronized void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
        registerStatus = RegisterStatus.REGISTERED;
    }

    @Override
    public synchronized void unregister(IndexRegistry registry)
    {
        Index.super.unregister(registry);
        registerStatus = RegisterStatus.UNREGISTERED;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in {@link LogTransaction#delete(java.io.File)}
         * asynchronously, but we need to mark the index queryable because if the truncation is during the initial
         * build of the index it won't get marked queryable by the build.
         */
        return () -> {
            logger.info("Making index queryable during table truncation");
            baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            return null;
        };
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null; // storage-attached indexes are flushed alongside memtable
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () -> null;
    }

    @Override
    public void validate(PartitionUpdate update, ClientState state) throws InvalidRequestException
    {
        // only internal can write... so it must be valid no?
    }


    //TODO (now): flesh this stuff out...


    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor,
                                                 LifecycleNewTracker tracker)
    {
        // mimics org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat.newPerColumnIndexWriter
        IndexDescriptor id = IndexDescriptor.create(descriptor, baseCfs.getPartitioner(), baseCfs.metadata().comparator);
        if (tracker.opType() != OperationType.FLUSH || !initBuildStarted)
        {
            return new RouteIndexFormat.SSTableIndexWriter(this, id);
        }
        else
        {
            return new RouteIndexFormat.MemtableRouteIndexWriter(id, memtableIndexManager.getPendingMemtableIndex(tracker));
        }
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext ctx,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        // since we are attached we only care about update
        if (transactionType != IndexTransaction.Type.UPDATE)
            return null;
        return new Indexer()
        {
            @Override
            public void insertRow(Row row)
            {
                long size = memtableIndexManager.index(key, row, memtable);
                if (size > 0)
                    memtable.markExtraOnHeapUsed(size, CassandraWriteContext.fromContext(ctx).getGroup());
            }

            @Override
            public void updateRow(Row oldRowData, Row newRowData)
            {
                insertRow(newRowData);
            }
        };
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        // disallow all queries, in order to interact with this index you must bypass CQL
        return false;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return RowFilter.none();
    }

    @Override
    public Searcher searcherFor(ReadCommand command)
    {
        List<RowFilter.Expression> expressions = command.rowFilter().getExpressions().stream().collect(Collectors.toList());
        if (expressions.isEmpty())
            return null;
        ByteBuffer start = null;
        boolean startInclusive = true;
        ByteBuffer end = null;
        boolean endInclusive = true;
        Integer storeId = null;
        for (RowFilter.Expression e : expressions)
        {
            if (e.column() == AccordKeyspace.CommandsColumns.route)
            {
                switch (e.operator())
                {
                    case GT:
                        start = e.getIndexValue();
                        startInclusive = false;
                        break;
                    case GTE:
                        start = e.getIndexValue();
                        startInclusive = true;
                        break;
                    case LT:
                        end = e.getIndexValue();
                        endInclusive = false;
                        break;
                    case LTE:
                        end = e.getIndexValue();
                        endInclusive = true;
                        break;
                    default:
                        return null;
                }
            }
            else if (e.column() == AccordKeyspace.CommandsColumns.store_id && e.operator() == Operator.EQ)
            {
                storeId = Int32Type.instance.compose(e.getIndexValue());
            }
        }
        if (start == null || end == null || storeId == null)
            return null;
        int finalStoreId = storeId;
        ByteBuffer finalStart = start;
        boolean finalStartInclusive = startInclusive;
        ByteBuffer finalEnd = end;
        boolean finalEndInclusive = endInclusive;
        return new Searcher()
        {
            @Override
            public ReadCommand command()
            {
                return command;
            }

            @Override
            public UnfilteredPartitionIterator search(ReadExecutionController executionController)
            {
                // find all partitions from memtable / sstable
                NavigableSet<ByteBuffer> partitions = search(finalStoreId, finalStart, finalStartInclusive, finalEnd, finalEndInclusive);
                // do SinglePartitionReadCommand per partition
                return new SearchIterator(executionController, command, partitions);
            }

            NavigableSet<ByteBuffer> search(int storeId,
                                              ByteBuffer startTableWithToken, boolean startInclusive,
                                              ByteBuffer endTableWithToken, boolean endInclusive)
            {
                TableId tableId;
                byte[] start;
                {

                    AccordRoutingKey route = OrderedRouteSerializer.deserializeRoutingKey(startTableWithToken);
                    tableId = route.table();
                    start = OrderedRouteSerializer.serializeRoutingKeyNoTable(route);
                }
                byte[] end = OrderedRouteSerializer.serializeRoutingKeyNoTable(OrderedRouteSerializer.deserializeRoutingKey(endTableWithToken));
                NavigableSet<ByteBuffer> matches = sstableManager.search(storeId, tableId, start, startInclusive, end, endInclusive);
                matches.addAll(memtableIndexManager.search(storeId, tableId, start, startInclusive, end, endInclusive));
                return matches;
            }
        };
    }

    private class SearchIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final ReadExecutionController executionController;
        private final ReadCommand command;
        private final TableMetadata metadata;
        private final Iterator<ByteBuffer> partitions;

        private SearchIterator(ReadExecutionController executionController, ReadCommand command, NavigableSet<ByteBuffer> partitions)
        {
            this.executionController = executionController;
            this.command = command;
            this.metadata = command.metadata();
            this.partitions = partitions.iterator();
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        protected UnfilteredRowIterator computeNext()
        {
            if (!partitions.hasNext())
                return endOfData();
            DecoratedKey pk = metadata.partitioner.decorateKey(partitions.next());
            return new UnfilteredRowIterator()
            {
                @Override
                public DeletionTime partitionLevelDeletion()
                {
                    return DeletionTime.LIVE;
                }

                @Override
                public EncodingStats stats()
                {
                    return EncodingStats.NO_STATS;
                }

                @Override
                public TableMetadata metadata()
                {
                    return metadata;
                }

                @Override
                public boolean isReverseOrder()
                {
                    return false;
                }

                @Override
                public RegularAndStaticColumns columns()
                {
                    return RegularAndStaticColumns.NONE;
                }

                @Override
                public DecoratedKey partitionKey()
                {
                    return pk;
                }

                @Override
                public Row staticRow()
                {
                    return null;
                }

                @Override
                public void close()
                {

                }

                private Row row = BTreeRow.emptyRow(Clustering.EMPTY);

                @Override
                public boolean hasNext()
                {
                    return row != null;
                }

                @Override
                public Unfiltered next()
                {
                    Row row = this.row;
                    this.row = null;
                    return row;
                }
            };
        }

        @Override
        public void close()
        {

        }
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;
            sstableManager.onSSTableChanged(Collections.emptySet(), notice.added);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;
            sstableManager.onSSTableChanged(notice.removed, notice.added);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            memtableIndexManager.renewMemtable(((MemtableRenewedNotification) notification).renewed);
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            memtableIndexManager.discardMemtable(((MemtableDiscardedNotification) notification).memtable);
        }
    }

    //TODO (coverage): everything below here never triggered...

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getEstimatedResultRows()
    {
        throw new UnsupportedOperationException();
    }
}
