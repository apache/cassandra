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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.base.Splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.MaxDecidedRX.DecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Operator;
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
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
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
import org.apache.cassandra.service.accord.AccordJournalTable;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static accord.primitives.Routable.Domain.Range;

public class RouteJournalIndex implements Index, INotificationConsumer
{
    public enum RegisterStatus { PENDING, REGISTERED, UNREGISTERED }

    private static final Logger logger = LoggerFactory.getLogger(RouteJournalIndex.class);

    private static final Component.Type type = Component.Type.createSingleton("AccordRoute", "AccordRoute.*.db", true, null);

    private final ColumnFamilyStore baseCfs;
    private final IndexMetadata indexMetadata;
    private final IndexMetrics indexMetrics;
    private final MemtableIndexManager memtableIndexManager;
    private final SSTableManager sstableManager;
    // Tracks whether we've started the index build on initialization.
    private volatile boolean initBuildStarted = false;
    private volatile RegisterStatus registerStatus = RegisterStatus.PENDING;

    public RouteJournalIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        validateTargets(baseCfs, indexMetadata);

        this.baseCfs = baseCfs;
        // type is only IndexTarget.Type.VALUES
        this.indexMetadata = indexMetadata;

        this.memtableIndexManager = new RouteMemtableIndexManager(this);
        this.sstableManager = new RouteSSTableManager();
        this.indexMetrics = new IndexMetrics(this);

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    public static boolean allowed(JournalKey id)
    {
        return id.type == JournalKey.Type.COMMAND_DIFF && allowed(id.id);
    }

    public static boolean allowed(TxnId id)
    {
        return id.is(Range);
    }

    private static void validateTargets(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        // this contains 2 columns....
        if (!SchemaConstants.ACCORD_KEYSPACE_NAME.equals(baseCfs.getKeyspaceName()))
            throw new IllegalArgumentException("Route index is only allowed for accord journal table; given " + baseCfs.metadata());
        if (!AccordKeyspace.JOURNAL.equals(baseCfs.name))
            throw new IllegalArgumentException("Route index is only allowed for accord journal table; given " + baseCfs.metadata());
        Set<String> columns = Splitter.on(',').trimResults().omitEmptyStrings().splitToStream(indexMetadata.options.get("target")).collect(Collectors.toSet());
        Set<String> expected = Set.of("record", "user_version");
        if (!expected.equals(columns))
            throw new IllegalArgumentException("Route index is only allowed for accord journal table, and on the record/user_value columns; given " + baseCfs.metadata() + " and columns " + columns);
    }

    public IndexMetrics indexMetrics()
    {
        return indexMetrics;
    }

    public RegisterStatus registerStatus()
    {
        return registerStatus;
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

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor,
                                                 ILifecycleTransaction txn)
    {
        // mimics org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat.newPerColumnIndexWriter
        IndexDescriptor id = IndexDescriptor.create(descriptor, baseCfs.getPartitioner(), baseCfs.metadata().comparator);
        if (txn.opType() != OperationType.FLUSH || !initBuildStarted)
        {
            return new RouteIndexFormat.SSTableIndexWriter(this, id);
        }
        else
        {
            return new RouteIndexFormat.MemtableRouteIndexWriter(id, memtableIndexManager.getPendingMemtableIndex(txn));
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
        List<RowFilter.Expression> expressions = new ArrayList<>(command.rowFilter().getExpressions());
        if (expressions.isEmpty())
            return null;
        ByteBuffer start = null;
        ByteBuffer end = null;
        Integer storeId = null;
        TxnId minTxnId = TxnId.NONE;
        Timestamp maxTxnId = TxnId.MAX;
        for (RowFilter.Expression e : expressions)
        {
            if (e.column() == AccordJournalTable.SyntheticColumn.participants.metadata)
            {
                switch (e.operator())
                {
                    case GT:
                    case GTE:
                        start = e.getIndexValue();
                        break;
                    case LT:
                    case LTE:
                        end = e.getIndexValue();
                        break;
                    default:
                        return null;
                }
            }
            else if (e.column() == AccordJournalTable.SyntheticColumn.store_id.metadata && e.operator() == Operator.EQ)
            {
                storeId = Int32Type.instance.compose(e.getIndexValue());
            }
            else if (e.column() == AccordJournalTable.SyntheticColumn.txn_id.metadata)
            {
                switch (e.operator())
                {
                    case GT:
                    case GTE:
                        minTxnId = CommandSerializers.txnId.deserialize(e.getIndexValue());
                        break;
                    case LT:
                    case LTE:
                        maxTxnId = CommandSerializers.timestamp.deserialize(e.getIndexValue());
                        break;
                    default:
                        return null;
                }
            }
            else
            {
                String cqlString;
                try
                {
                    cqlString = e.toCQLString();
                }
                catch (Exception ex)
                {
                    cqlString = "Unable to convert RowFilter to CQL; " + e.column() + ' ' + e.operator();
                }
                throw new IllegalArgumentException("Unexpected expression: " + cqlString);
            }
        }
        if (start == null || end == null || storeId == null)
            return null;
        if (start.equals(end))
            return keySearcher(command, storeId, start, minTxnId, maxTxnId, null);
        return rangeSearcher(command, storeId, start, end, minTxnId, maxTxnId, null);
    }

    private Searcher keySearcher(ReadCommand command, Integer storeId, ByteBuffer key,
                                 TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
    {
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
                NavigableSet<ByteBuffer> partitions = search(storeId, key,
                                                             minTxnId, maxTxnId, decidedRX);
                // do SinglePartitionReadCommand per partition
                return new SearchIterator(command, partitions);
            }

            NavigableSet<ByteBuffer> search(int storeId, ByteBuffer key,
                                            TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
            {
                TableId tableId;
                byte[] start;
                {
                    TokenKey route = OrderedRouteSerializer.deserialize(key);
                    tableId = route.table();
                    start = OrderedRouteSerializer.serializeTokenOnly(route);
                }
                // store matches in a hash set so add is O(1), and the sorting is done after collecting all matches
                Set<ByteBuffer> matches = new HashSet<>();
                sstableManager.search(storeId, tableId, start, minTxnId, maxTxnId, decidedRX, matches::add);
                memtableIndexManager.search(storeId, tableId, start,
                                            minTxnId, maxTxnId, decidedRX,
                                            matches::add);
                return new TreeSet<>(matches);
            }
        };
    }

    private Searcher rangeSearcher(ReadCommand command, int storeId,
                                   ByteBuffer start, ByteBuffer end,
                                   TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
    {
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
                NavigableSet<ByteBuffer> partitions = search(storeId,
                                                             start, end,
                                                             minTxnId, maxTxnId, decidedRX);
                // do SinglePartitionReadCommand per partition
                return new SearchIterator(command, partitions);
            }

            NavigableSet<ByteBuffer> search(int storeId,
                                            ByteBuffer startTableWithToken, ByteBuffer endTableWithToken,
                                            TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX)
            {
                TableId tableId;
                byte[] start;
                {

                    TokenKey route = OrderedRouteSerializer.deserialize(startTableWithToken);
                    tableId = route.table();
                    start = OrderedRouteSerializer.serializeTokenOnly(route);
                }
                byte[] end = OrderedRouteSerializer.serializeTokenOnly(OrderedRouteSerializer.deserialize(endTableWithToken));
                // store matches in a hash set so add is O(1), and the sorting is done after collecting all matches
                Set<ByteBuffer> matches = new HashSet<>();
                sstableManager.search(storeId, tableId, start, end, minTxnId, maxTxnId, decidedRX, matches::add);
                memtableIndexManager.search(storeId, tableId, start, end, minTxnId, maxTxnId, decidedRX, matches::add);
                return new TreeSet<>(matches);
            }
        };
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

    private static class SearchIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final Iterator<ByteBuffer> partitions;

        private SearchIterator(ReadCommand command, NavigableSet<ByteBuffer> partitions)
        {
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
}
