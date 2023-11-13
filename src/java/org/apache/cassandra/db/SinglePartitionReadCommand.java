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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import org.apache.cassandra.cache.IRowCacheEntry;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.cache.RowCacheSentinel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorWithLowerBound;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * A read command that selects a (part of a) single partition.
 */
public class SinglePartitionReadCommand extends ReadCommand implements SinglePartitionReadQuery
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    protected final DecoratedKey partitionKey;
    protected final ClusteringIndexFilter clusteringIndexFilter;

    @VisibleForTesting
    protected SinglePartitionReadCommand(boolean isDigest,
                                         int digestVersion,
                                         boolean acceptsTransient,
                                         TableMetadata metadata,
                                         long nowInSec,
                                         ColumnFilter columnFilter,
                                         RowFilter rowFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         ClusteringIndexFilter clusteringIndexFilter,
                                         Index.QueryPlan indexQueryPlan,
                                         boolean trackWarnings)
    {
        super(Kind.SINGLE_PARTITION, isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, indexQueryPlan, trackWarnings);
        assert partitionKey.getPartitioner() == metadata.partitioner;
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;
    }

    private static SinglePartitionReadCommand create(boolean isDigest,
                                                    int digestVersion,
                                                    boolean acceptsTransient,
                                                    TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    Index.QueryPlan indexQueryPlan,
                                                    boolean trackWarnings)
    {
        if (metadata.isVirtual())
        {
            return new VirtualTableSinglePartitionReadCommand(isDigest,
                                                              digestVersion,
                                                              acceptsTransient,
                                                              metadata,
                                                              nowInSec,
                                                              columnFilter,
                                                              rowFilter,
                                                              limits,
                                                              partitionKey,
                                                              clusteringIndexFilter,
                                                              indexQueryPlan,
                                                              trackWarnings);
        }
        return new SinglePartitionReadCommand(isDigest,
                                              digestVersion,
                                              acceptsTransient,
                                              metadata,
                                              nowInSec,
                                              columnFilter,
                                              rowFilter,
                                              limits,
                                              partitionKey,
                                              clusteringIndexFilter,
                                              indexQueryPlan,
                                              trackWarnings);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     * @param indexQueryPlan explicitly specified index to use for the query
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter,
                                                    Index.QueryPlan indexQueryPlan)
    {
        return create(false,
                      0,
                      false,
                      metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      indexQueryPlan,
                      false);
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     *
     * @return a newly created read command.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DecoratedKey partitionKey,
                                                    ClusteringIndexFilter clusteringIndexFilter)
    {
        return create(metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      partitionKey,
                      clusteringIndexFilter,
                      findIndexQueryPlan(metadata, rowFilter));
    }

    /**
     * Creates a new read command on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param columnFilter the column filter to use for the query.
     * @param filter the clustering index filter to use for the query.
     *
     * @return a newly created read command. The returned command will use no row filter and have no limits.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata,
                                                    long nowInSec,
                                                    DecoratedKey key,
                                                    ColumnFilter columnFilter,
                                                    ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, long nowInSec, DecoratedKey key)
    {
        return create(metadata, nowInSec, key, Slices.ALL);
    }

    /**
     * Creates a new read command that queries a single partition in its entirety.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     *
     * @return a newly created read command that queries all the rows of {@code key}.
     */
    public static SinglePartitionReadCommand fullPartitionRead(TableMetadata metadata, long nowInSec, ByteBuffer key)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), Slices.ALL);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, ByteBuffer key, Slices slices)
    {
        return create(metadata, nowInSec, metadata.partitioner.decorateKey(key), slices);
    }

    /**
     * Creates a new single partition name command for the provided rows.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param names the clustering for the rows to query.
     *
     * @return a newly created read command that queries the {@code names} in {@code key}. The returned query will
     * query every columns (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, NavigableSet<Clustering<?>> names)
    {
        ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(names, false);
        return create(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new single partition name command for the provided row.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param name the clustering for the row to query.
     *
     * @return a newly created read command that queries {@code name} in {@code key}. The returned query will
     * query every columns (without limit or row filtering).
     */
    public static SinglePartitionReadCommand create(TableMetadata metadata, long nowInSec, DecoratedKey key, Clustering<?> name)
    {
        return create(metadata, nowInSec, key, FBUtilities.singleton(name, metadata.comparator));
    }

    public SinglePartitionReadCommand copy()
    {
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected SinglePartitionReadCommand copyAsDigestQuery()
    {
        return create(true,
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected SinglePartitionReadCommand copyAsTransientQuery()
    {
        return create(false,
                      0,
                      true,
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public SinglePartitionReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      newLimits,
                      partitionKey(),
                      clusteringIndexFilter(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public ClusteringIndexFilter clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return clusteringIndexFilter;
    }

    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getReadRpcTimeout(unit);
    }

    public boolean isReversed()
    {
        return clusteringIndexFilter.isReversed();
    }

    @Override
    public SinglePartitionReadCommand forPaging(Clustering<?> lastReturned, DataLimits limits)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        SinglePartitionReadCommand cmd = create(metadata(),
                                                nowInSec(),
                                                columnFilter(),
                                                rowFilter(),
                                                limits,
                                                partitionKey(),
                                                lastReturned == null ? clusteringIndexFilter() : clusteringIndexFilter.forPaging(metadata().comparator, lastReturned, false));
        if (isTrackingWarnings())
            cmd.trackWarnings();
        return cmd;
    }

    @Override
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
    {
        if (clusteringIndexFilter.isEmpty(metadata().comparator))
            return EmptyIterators.partition();

        return StorageProxy.read(Group.one(this), consistency, queryStartNanoTime);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    protected UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        // skip the row cache and go directly to sstables/memtable if repaired status of
        // data is being tracked. This is only requested after an initial digest mismatch
        UnfilteredRowIterator partition = cfs.isRowCacheEnabled() && !executionController.isTrackingRepairedStatus()
                                        ? getThroughCache(cfs, executionController)
                                        : queryMemtableAndDisk(cfs, executionController);
        return new SingletonUnfilteredPartitionIterator(partition);
    }

    /**
     * Fetch the rows requested if in cache; if not, read it from disk and cache it.
     * <p>
     * If the partition is cached, and the filter given is within its bounds, we return
     * from cache, otherwise from disk.
     * <p>
     * If the partition is is not cached, we figure out what filter is "biggest", read
     * that from disk, then filter the result and either cache that or return it.
     */
    private UnfilteredRowIterator getThroughCache(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        assert !cfs.isIndex(); // CASSANDRA-5732
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", cfs.name);

        RowCacheKey key = new RowCacheKey(metadata(), partitionKey());

        // Attempt a sentinel-read-cache sequence.  if a write invalidates our sentinel, we'll return our
        // (now potentially obsolete) data, but won't cache it. see CASSANDRA-3862
        // TODO: don't evict entire partitions on writes (#2864)
        IRowCacheEntry cached = CacheService.instance.rowCache.get(key);
        if (cached != null)
        {
            if (cached instanceof RowCacheSentinel)
            {
                // Some other read is trying to cache the value, just do a normal non-caching read
                Tracing.trace("Row cache miss (race)");
                cfs.metric.rowCacheMiss.inc();
                return queryMemtableAndDisk(cfs, executionController);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(clusteringIndexFilter(), limits(), cachedPartition, nowInSec(), metadata().enforceStrictLiveness()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                UnfilteredRowIterator unfilteredRowIterator = clusteringIndexFilter().getUnfilteredRowIterator(columnFilter(), cachedPartition);
                cfs.metric.updateSSTableIterated(0);
                return unfilteredRowIterator;
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return queryMemtableAndDisk(cfs, executionController);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        // Note that on tables with no clustering keys, any positive value of
        // rowsToCache implies caching the full partition
        boolean cacheFullPartitions = metadata().clusteringColumns().size() > 0 ?
                                      metadata().params.caching.cacheAllRows() :
                                      metadata().params.caching.cacheRows();

        // To be able to cache what we read, what we read must at least covers what the cache holds, that
        // is the 'rowsToCache' first rows of the partition. We could read those 'rowsToCache' first rows
        // systematically, but we'd have to "extend" that to whatever is needed for the user query that the
        // 'rowsToCache' first rows don't cover and it's not trivial with our existing filters. So currently
        // we settle for caching what we read only if the user query does query the head of the partition since
        // that's the common case of when we'll be able to use the cache anyway. One exception is if we cache
        // full partitions, in which case we just always read it all and cache.
        if (cacheFullPartitions || clusteringIndexFilter().isHeadFilter())
        {
            RowCacheSentinel sentinel = new RowCacheSentinel();
            boolean sentinelSuccess = CacheService.instance.rowCache.putIfAbsent(key, sentinel);
            boolean sentinelReplaced = false;

            try
            {
                final int rowsToCache = metadata().params.caching.rowsPerPartitionToCache();
                final boolean enforceStrictLiveness = metadata().enforceStrictLiveness();

                UnfilteredRowIterator iter = fullPartitionRead(metadata(), nowInSec(), partitionKey()).queryMemtableAndDisk(cfs, executionController);
                try
                {
                    // Use a custom iterator instead of DataLimits to avoid stopping the original iterator
                    UnfilteredRowIterator toCacheIterator = new WrappingUnfilteredRowIterator()
                    {
                        private int rowsCounted = 0;

                        @Override
                        public UnfilteredRowIterator wrapped()
                        {
                            return iter;
                        }

                        @Override
                        public boolean hasNext()
                        {
                            return rowsCounted < rowsToCache && iter.hasNext();
                        }

                        @Override
                        public Unfiltered next()
                        {
                            Unfiltered unfiltered = iter.next();
                            if (unfiltered.isRow())
                            {
                                Row row = (Row) unfiltered;
                                if (row.hasLiveData(nowInSec(), enforceStrictLiveness))
                                    rowsCounted++;
                            }
                            return unfiltered;
                        }
                    };

                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = CachedBTreePartition.create(toCacheIterator, nowInSec());
                    if (sentinelSuccess && !toCache.isEmpty())
                    {
                        Tracing.trace("Caching {} rows", toCache.rowCount());
                        CacheService.instance.rowCache.replace(key, sentinel, toCache);
                        // Whether or not the previous replace has worked, our sentinel is not in the cache anymore
                        sentinelReplaced = true;
                    }

                    // We then re-filter out what this query wants.
                    // Note that in the case where we don't cache full partitions, it's possible that the current query is interested in more
                    // than what we've cached, so we can't just use toCache.
                    UnfilteredRowIterator cacheIterator = clusteringIndexFilter().getUnfilteredRowIterator(columnFilter(), toCache);
                    if (cacheFullPartitions)
                    {
                        // Everything is guaranteed to be in 'toCache', we're done with 'iter'
                        assert !iter.hasNext();
                        iter.close();
                        return cacheIterator;
                    }
                    return UnfilteredRowIterators.concat(cacheIterator, clusteringIndexFilter().filterNotIndexed(columnFilter(), iter));
                }
                catch (RuntimeException | Error e)
                {
                    iter.close();
                    throw e;
                }
            }
            finally
            {
                if (sentinelSuccess && !sentinelReplaced)
                    cfs.invalidateCachedPartition(key);
            }
        }

        Tracing.trace("Fetching data but not populating cache as query does not query from the start of the partition");
        return queryMemtableAndDisk(cfs, executionController);
    }

    /**
     * Queries both memtable and sstables to fetch the result of this query.
     * <p>
     * Please note that this method:
     *   1) does not check the row cache.
     *   2) does not apply the query limit, nor the row filter (and so ignore 2ndary indexes).
     *      Those are applied in {@link ReadCommand#executeLocally}.
     *   3) does not record some of the read metrics (latency, scanned cells histograms) nor
     *      throws TombstoneOverwhelmingException.
     * It is publicly exposed because there is a few places where that is exactly what we want,
     * but it should be used only where you know you don't need thoses things.
     * <p>
     * Also note that one must have created a {@code ReadExecutionController} on the queried table and we require it as
     * a parameter to enforce that fact, even though it's not explicitlly used by the method.
     */
    public UnfilteredRowIterator queryMemtableAndDisk(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        assert executionController != null && executionController.validForReadOn(cfs);
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        return queryMemtableAndDiskInternal(cfs, executionController);
    }

    private UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, ReadExecutionController controller)
    {
        /*
         * We have 2 main strategies:
         *   1) We query memtables and sstables simulateneously. This is our most generic strategy and the one we use
         *      unless we have a names filter that we know we can optimize futher.
         *   2) If we have a name filter (so we query specific rows), we can make a bet: that all column for all queried row
         *      will have data in the most recent sstable(s), thus saving us from reading older ones. This does imply we
         *      have a way to guarantee we have all the data for what is queried, which is only possible for name queries
         *      and if we have neither non-frozen collections/UDTs nor counters.
         *      If a non-frozen collection or UDT is queried we can't guarantee that an older sstable won't have some
         *      elements that weren't in the most recent sstables.
         *      Counters are intrinsically a collection of shards and so have the same problem.
         *      Counter tables are also special in the sense that their rows do not have primary key liveness
         *      as INSERT statements are not supported on counter tables. Due to that even if only the primary key
         *      columns where queried, querying SSTables in timestamp order will always be less efficient for counter tables.
         *      Also, if tracking repaired data then we skip this optimization so we can collate the repaired sstables
         *      and generate a digest over their merge, which procludes an early return.
         */
        if (clusteringIndexFilter() instanceof ClusteringIndexNamesFilter
            && !metadata().isCounter()
            && !queriesMulticellType()
            && !controller.isTrackingRepairedStatus())
        {
            return queryMemtableAndSSTablesInTimestampOrder(cfs, (ClusteringIndexNamesFilter)clusteringIndexFilter(), controller);
        }

        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));
        view.sstables.sort(SSTableReader.maxTimestampDescending);
        ClusteringIndexFilter filter = clusteringIndexFilter();
        long minTimestamp = Long.MAX_VALUE;
        long mostRecentPartitionTombstone = Long.MIN_VALUE;
        InputCollector<UnfilteredRowIterator> inputCollector = iteratorsForPartition(view, controller);
        try
        {
            SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

            for (Memtable memtable : view.memtables)
            {
                UnfilteredRowIterator iter = memtable.rowIterator(partitionKey(), filter.getSlices(metadata()), columnFilter(), filter.isReversed(), metricsCollector);
                if (iter == null)
                    continue;

                if (memtable.getMinTimestamp() != Memtable.NO_MIN_TIMESTAMP)
                    minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

                // Memtable data is always considered unrepaired
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                inputCollector.addMemtableIterator(RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false));

                mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                        iter.partitionLevelDeletion().markedForDeleteAt());
            }

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 < maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in descending maxTimestamp order allow to do our mostRecentPartitionTombstone
             * elimination in one pass, and minimize the number of sstables for which we read a partition tombstone.
            */
            view.sstables.sort(SSTableReader.maxTimestampDescending);
            int nonIntersectingSSTables = 0;
            int includedDueToTombstones = 0;

            if (controller.isTrackingRepairedStatus())
                Tracing.trace("Collecting data from sstables and tracking repaired status");

            for (SSTableReader sstable : view.sstables)
            {
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                // if we're tracking repaired status, we mark the repaired digest inconclusive
                // as other replicas may not have seen this partition delete and so could include
                // data from this sstable (or others) in their digests
                if (sstable.getMaxTimestamp() < mostRecentPartitionTombstone)
                {
                    inputCollector.markInconclusive();
                    break;
                }

                boolean intersects = intersects(sstable);
                boolean hasRequiredStatics = hasRequiredStatics(sstable);
                boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

                if (!intersects && !hasRequiredStatics && !hasPartitionLevelDeletions)
                {
                    nonIntersectingSSTables++;
                    continue;
                }

                if (intersects || hasRequiredStatics)
                {
                    if (!sstable.isRepaired())
                        controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                    // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                    UnfilteredRowIterator iter = intersects ? makeRowIteratorWithLowerBound(cfs, sstable, metricsCollector)
                                                            : makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector);

                    inputCollector.addSSTableIterator(sstable, iter);
                    mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                            iter.partitionLevelDeletion().markedForDeleteAt());
                }
                else
                {
                    nonIntersectingSSTables++;

                    // if the sstable contained range or cell tombstones, it would intersect; since we are here, it means
                    // that there are no cell or range tombstones we are interested in (due to the filter)
                    // however, we know that there are partition level deletions in this sstable and we need to make
                    // an iterator figure out that (see `StatsMetadata.hasPartitionLevelDeletions`)

                    // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                    UnfilteredRowIterator iter = makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector);

                    // if the sstable contains a partition delete, then we must include it regardless of whether it
                    // shadows any other data seen locally as we can't guarantee that other replicas have seen it
                    if (!iter.partitionLevelDeletion().isLive())
                    {
                        if (!sstable.isRepaired())
                            controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());
                        inputCollector.addSSTableIterator(sstable, iter);
                        includedDueToTombstones++;
                        mostRecentPartitionTombstone = Math.max(mostRecentPartitionTombstone,
                                                                iter.partitionLevelDeletion().markedForDeleteAt());
                    }
                    else
                    {
                        iter.close();
                    }
                }
            }

            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                               nonIntersectingSSTables, view.sstables.size(), includedDueToTombstones);

            if (inputCollector.isEmpty())
                return EmptyIterators.unfilteredRow(cfs.metadata(), partitionKey(), filter.isReversed());

            StorageHook.instance.reportRead(cfs.metadata().id, partitionKey());

            List<UnfilteredRowIterator> iterators = inputCollector.finalizeIterators(cfs, nowInSec(), controller.oldestUnrepairedTombstone());
            return withSSTablesIterated(iterators, cfs.metric, metricsCollector);
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                inputCollector.close();
            }
            catch (Exception e1)
            {
                e.addSuppressed(e1);
            }
            throw e;
        }
    }

    @Override
    protected boolean intersects(SSTableReader sstable)
    {
        return clusteringIndexFilter().intersects(sstable.metadata().comparator, sstable.getSSTableMetadata().coveredClustering);
    }

    private UnfilteredRowIteratorWithLowerBound makeRowIteratorWithLowerBound(ColumnFamilyStore cfs,
                                                                              SSTableReader sstable,
                                                                              SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIteratorWithLowerBound(cfs,
                                                                  sstable,
                                                                  partitionKey(),
                                                                  clusteringIndexFilter(),
                                                                  columnFilter(),
                                                                  listener);

    }

    private UnfilteredRowIterator makeRowIterator(ColumnFamilyStore cfs,
                                                  SSTableReader sstable,
                                                  ClusteringIndexNamesFilter clusteringIndexFilter,
                                                  SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIterator(cfs,
                                                    sstable,
                                                    partitionKey(),
                                                    clusteringIndexFilter.getSlices(cfs.metadata()),
                                                    columnFilter(),
                                                    clusteringIndexFilter.isReversed(),
                                                    listener);
    }

    private UnfilteredRowIterator makeRowIteratorWithSkippedNonStaticContent(ColumnFamilyStore cfs,
                                                                             SSTableReader sstable,
                                                                             SSTableReadsListener listener)
    {
        return StorageHook.instance.makeRowIterator(cfs,
                                                    sstable,
                                                    partitionKey(),
                                                    Slices.NONE,
                                                    columnFilter(),
                                                    clusteringIndexFilter().isReversed(),
                                                    listener);
    }

    /**
     * Return a wrapped iterator that when closed will update the sstables iterated and READ sample metrics.
     * Note that we cannot use the Transformations framework because they greedily get the static row, which
     * would cause all iterators to be initialized and hence all sstables to be accessed.
     */
    private UnfilteredRowIterator withSSTablesIterated(List<UnfilteredRowIterator> iterators,
                                                       TableMetrics metrics,
                                                       SSTableReadMetricsCollector metricsCollector)
    {
        UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators);

        if (!merged.isEmpty())
        {
            DecoratedKey key = merged.partitionKey();
            metrics.topReadPartitionFrequency.addSample(key.getKey(), 1);
            metrics.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
        }

        class UpdateSstablesIterated extends Transformation<UnfilteredRowIterator>
        {
           public void onPartitionClose()
           {
               int mergedSSTablesIterated = metricsCollector.getMergedSSTables();
               metrics.updateSSTableIterated(mergedSSTablesIterated);
               Tracing.trace("Merged data from memtables and {} sstables", mergedSSTablesIterated);
           }
        }
        return Transformation.apply(merged, new UpdateSstablesIterated());
    }

    private boolean queriesMulticellType()
    {
        for (ColumnMetadata column : columnFilter().queriedColumns())
        {
            if (column.type.isMultiCell())
                return true;
        }
        return false;
    }

    /**
     * Do a read by querying the memtable(s) first, and then each relevant sstables sequentially by order of the sstable
     * max timestamp.
     *
     * This is used for names query in the hope of only having to query the 1 or 2 most recent query and then knowing nothing
     * more recent could be in the older sstables (which we can only guarantee if we know exactly which row we queries, and if
     * no collection or counters are included).
     * This method assumes the filter is a {@code ClusteringIndexNamesFilter}.
     */
    private UnfilteredRowIterator queryMemtableAndSSTablesInTimestampOrder(ColumnFamilyStore cfs, ClusteringIndexNamesFilter filter, ReadExecutionController controller)
    {
        Tracing.trace("Acquiring sstable references");
        ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, partitionKey()));

        ImmutableBTreePartition result = null;
        SSTableReadMetricsCollector metricsCollector = new SSTableReadMetricsCollector();

        Tracing.trace("Merging memtable contents");
        for (Memtable memtable : view.memtables)
        {
            try (UnfilteredRowIterator iter = memtable.rowIterator(partitionKey, filter.getSlices(metadata()), columnFilter(), isReversed(), metricsCollector))
            {
                if (iter == null)
                    continue;

                result = add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false),
                             result,
                             filter,
                             false,
                             controller);
            }
        }

        /* add the SSTables on disk */
        view.sstables.sort(SSTableReader.maxTimestampDescending);
        // read sorted sstables
        for (SSTableReader sstable : view.sstables)
        {
            // if we've already seen a partition tombstone with a timestamp greater
            // than the most recent update to this sstable, we're done, since the rest of the sstables
            // will also be older
            if (result != null && sstable.getMaxTimestamp() < result.partitionLevelDeletion().markedForDeleteAt())
                break;

            long currentMaxTs = sstable.getMaxTimestamp();
            filter = reduceFilter(filter, result, currentMaxTs);

            if (filter == null)
                break;

            boolean intersects = intersects(sstable);
            boolean hasRequiredStatics = hasRequiredStatics(sstable);
            boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);

            if (!intersects && !hasRequiredStatics)
            {
                // This mean that nothing queried by the filter can be in the sstable. One exception is the top-level partition deletion
                // however: if it is set, it impacts everything and must be included. Getting that top-level partition deletion costs us
                // some seek in general however (unless the partition is indexed and is in the key cache), so we first check if the sstable
                // has any tombstone at all as a shortcut.
                if (!hasPartitionLevelDeletions)
                    continue; // no tombstone at all, we can skip that sstable

                // We need to get the partition deletion and include it if it's live. In any case though, we're done with that sstable.
                try (UnfilteredRowIterator iter = makeRowIteratorWithSkippedNonStaticContent(cfs, sstable, metricsCollector))
                {
                    if (!iter.partitionLevelDeletion().isLive())
                    {
                        result = add(UnfilteredRowIterators.noRowsIterator(iter.metadata(),
                                                                           iter.partitionKey(),
                                                                           Rows.EMPTY_STATIC_ROW,
                                                                           iter.partitionLevelDeletion(),
                                                                           filter.isReversed()),
                                     result,
                                     filter,
                                     sstable.isRepaired(),
                                     controller);
                    }
                    else
                    {
                        result = add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.SSTABLE, false),
                                     result,
                                     filter,
                                     sstable.isRepaired(),
                                     controller);
                    }
                }

                continue;
            }

            try (UnfilteredRowIterator iter = makeRowIterator(cfs, sstable, filter, metricsCollector))
            {
                if (iter.isEmpty())
                    continue;

                result = add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.SSTABLE, false),
                             result,
                             filter,
                             sstable.isRepaired(),
                             controller);
            }
        }

        cfs.metric.updateSSTableIterated(metricsCollector.getMergedSSTables());

        if (result == null || result.isEmpty())
            return EmptyIterators.unfilteredRow(metadata(), partitionKey(), false);

        DecoratedKey key = result.partitionKey();
        cfs.metric.topReadPartitionFrequency.addSample(key.getKey(), 1);
        cfs.metric.topReadPartitionSSTableCount.addSample(key.getKey(), metricsCollector.getMergedSSTables());
        StorageHook.instance.reportRead(cfs.metadata.id, partitionKey());

        return result.unfilteredIterator(columnFilter(), Slices.ALL, clusteringIndexFilter().isReversed());
    }

    private ImmutableBTreePartition add(UnfilteredRowIterator iter, ImmutableBTreePartition result, ClusteringIndexNamesFilter filter, boolean isRepaired, ReadExecutionController controller)
    {
        if (!isRepaired)
            controller.updateMinOldestUnrepairedTombstone(iter.stats().minLocalDeletionTime);

        int maxRows = Math.max(filter.requestedRows().size(), 1);
        if (result == null)
            return ImmutableBTreePartition.create(iter, maxRows);

        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(Arrays.asList(iter, result.unfilteredIterator(columnFilter(), Slices.ALL, filter.isReversed()))))
        {
            return ImmutableBTreePartition.create(merged, maxRows);
        }
    }

    private ClusteringIndexNamesFilter reduceFilter(ClusteringIndexNamesFilter filter, ImmutableBTreePartition result, long sstableTimestamp)
    {
        if (result == null)
            return filter;

        // According to the CQL semantics a row exists if at least one of its columns is not null (including the primary key columns).
        // Having the queried columns not null is unfortunately not enough to prove that a row exists as some column deletion
        // for the queried columns can exist on another node.
        // For CQL tables it is enough to have the primary key liveness and the queried columns as the primary key liveness prove that
        // the row exists even if all the other columns are deleted.
        // COMPACT tables do not have primary key liveness and by consequence we are forced to get  all the fetched columns to ensure that
        // we can return the correct result if the queried columns are deleted on another node but one of the non-queried columns is not.
        RegularAndStaticColumns columns = metadata().isCompactTable() ? columnFilter().fetchedColumns() : columnFilter().queriedColumns();

        NavigableSet<Clustering<?>> clusterings = filter.requestedRows();

        // We want to remove rows for which we have values for all requested columns. We have to deal with both static and regular rows.

        boolean removeStatic = false;
        if (!columns.statics.isEmpty())
        {
            Row staticRow = result.getRow(Clustering.STATIC_CLUSTERING);
            removeStatic = staticRow != null && isRowComplete(staticRow, columns.statics, sstableTimestamp);
        }

        NavigableSet<Clustering<?>> toRemove = null;

        DeletionInfo deletionInfo = result.deletionInfo();

        if (deletionInfo.hasRanges())
        {
            for (Clustering<?> clustering : clusterings)
            {
                RangeTombstone rt = deletionInfo.rangeCovering(clustering);
                if (rt != null && rt.deletionTime().deletes(sstableTimestamp))
                {
                    if (toRemove == null)
                        toRemove = new TreeSet<>(result.metadata().comparator);
                    toRemove.add(clustering);
                }
            }
        }

        try (UnfilteredRowIterator iterator = result.unfilteredIterator(columnFilter(), clusterings, false))
        {
            while (iterator.hasNext())
            {
                Unfiltered unfiltered = iterator.next();
                if (unfiltered == null || !unfiltered.isRow())
                    continue;

                Row row = (Row) unfiltered;
                if (!isRowComplete(row, columns.regulars, sstableTimestamp))
                    continue;

                if (toRemove == null)
                    toRemove = new TreeSet<>(result.metadata().comparator);
                toRemove.add(row.clustering());
            }
        }

        if (!removeStatic && toRemove == null)
            return filter;

        // Check if we have everything we need
        boolean hasNoMoreStatic = columns.statics.isEmpty() || removeStatic;
        boolean hasNoMoreClusterings = clusterings.isEmpty() || (toRemove != null && toRemove.size() == clusterings.size());
        if (hasNoMoreStatic && hasNoMoreClusterings)
            return null;

        if (toRemove != null)
        {
            BTreeSet.Builder<Clustering<?>> newClusterings = BTreeSet.builder(result.metadata().comparator);
            newClusterings.addAll(Sets.difference(clusterings, toRemove));
            clusterings = newClusterings.build();
        }
        return new ClusteringIndexNamesFilter(clusterings, filter.isReversed());
    }

    /**
     * We can stop reading row data from disk if what we've already read is more recent than the max timestamp
     * of the next newest SSTable that might have data for the query. We care about 1.) the row timestamp (since
     * every query cares if the row exists or not), 2.) the timestamps of the requested cells, and 3.) whether or
     * not any of the cells we've read have actual data.
     *
     * @param row a potentially incomplete {@link Row}
     * @param requestedColumns the columns requested by the query
     * @param sstableTimestamp the max timestamp of the next newest SSTable to read
     *
     * @return true if the supplied {@link Row} is complete and its data more recent than the supplied timestamp
     */
    private boolean isRowComplete(Row row, Columns requestedColumns, long sstableTimestamp)
    {
        // Static rows do not have row deletion or primary key liveness info
        if (!row.isStatic())
        {
            // If the row has been deleted or is part of a range deletion we know that we have enough information and can
            // stop at this point.
            // Note that deleted rows in compact tables (non static) do not have a row deletion. Single column
            // cells are deleted instead. By consequence this check will not work for those, but the row will appear as complete later on
            // in the method.
            if (!row.deletion().isLive() && row.deletion().time().deletes(sstableTimestamp))
                return true;

            // Note that compact tables will always have an empty primary key liveness info.
            if (!metadata().isCompactTable() && (row.primaryKeyLivenessInfo().isEmpty() || row.primaryKeyLivenessInfo().timestamp() <= sstableTimestamp))
                return false;
        }

        for (ColumnMetadata column : requestedColumns)
        {
            Cell<?> cell = row.getCell(column);

            if (cell == null || cell.timestamp() <= sstableTimestamp)
                return false;
        }

        return true;
    }

    @Override
    public boolean selectsFullPartition()
    {
        if (metadata().isStaticCompactTable())
            return true;

        return clusteringIndexFilter.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns();
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)",
                             metadata().toString(),
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             metadata().partitionKeyType.getString(partitionKey().getKey()),
                             clusteringIndexFilter.toString(metadata()),
                             nowInSec());
    }

    @Override
    public Verb verb()
    {
        return Verb.READ_REQ;
    }

    @Override
    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ").append(partitionKey().toCQLString(metadata()));

        String filterString = clusteringIndexFilter().toCQLString(metadata(), rowFilter());
        if (!filterString.isEmpty())
        {
            if (!clusteringIndexFilter().selectsAllPartition() || !rowFilter().isEmpty())
                sb.append(" AND ");
            sb.append(filterString);
        }
    }

    @Override
    public String loggableTokens()
    {
        return "token=" + partitionKey.getToken().toString();
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        metadata().partitionKeyType.writeValue(partitionKey().getKey(), out);
        ClusteringIndexFilter.serializer.serialize(clusteringIndexFilter(), out, version);
    }

    protected long selectionSerializedSize(int version)
    {
        return metadata().partitionKeyType.writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializer.serializedSize(clusteringIndexFilter(), version);
    }

    public boolean isLimitedToOnePartition()
    {
        return true;
    }

    public boolean isRangeRequest()
    {
        return false;
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group extends SinglePartitionReadQuery.Group<SinglePartitionReadCommand>
    {
        public static Group create(TableMetadata metadata,
                                   long nowInSec,
                                   ColumnFilter columnFilter,
                                   RowFilter rowFilter,
                                   DataLimits limits,
                                   List<DecoratedKey> partitionKeys,
                                   ClusteringIndexFilter clusteringIndexFilter)
        {
            List<SinglePartitionReadCommand> commands = new ArrayList<>(partitionKeys.size());
            for (DecoratedKey partitionKey : partitionKeys)
            {
                commands.add(SinglePartitionReadCommand.create(metadata,
                                                               nowInSec,
                                                               columnFilter,
                                                               rowFilter,
                                                               limits,
                                                               partitionKey,
                                                               clusteringIndexFilter));
            }

            return create(commands, limits);
        }

        private Group(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            super(commands, limits);
        }

        public static Group one(SinglePartitionReadCommand command)
        {
            return create(Collections.singletonList(command), command.limits());
        }

        public static Group create(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            return commands.get(0).metadata().isVirtual() ?
                   new VirtualTableGroup(commands, limits) :
                   new Group(commands, limits);
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
        {
            return StorageProxy.read(this, consistency, queryStartNanoTime);
        }
    }

    public static class VirtualTableGroup extends Group
    {
        public VirtualTableGroup(List<SinglePartitionReadCommand> commands, DataLimits limits)
        {
            super(commands, limits);
        }

        @Override
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
        {
            if (queries.size() == 1)
                return queries.get(0).execute(consistency, state, queryStartNanoTime);

            return PartitionIterators.concat(queries.stream()
                                                    .map(q -> q.execute(consistency, state, queryStartNanoTime))
                                                    .collect(Collectors.toList()));
        }
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       boolean isDigest,
                                       int digestVersion,
                                       boolean acceptsTransient,
                                       TableMetadata metadata,
                                       long nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       Index.QueryPlan indexQueryPlan)
        throws IOException
        {
            DecoratedKey key = metadata.partitioner.decorateKey(metadata.partitionKeyType.readBuffer(in, DatabaseDescriptor.getMaxValueSize()));
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializer.deserialize(in, version, metadata);
            return SinglePartitionReadCommand.create(isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, key, filter, indexQueryPlan, false);
        }
    }

    /**
     * {@code SSTableReaderListener} used to collect metrics about SSTable read access.
     */
    private static final class SSTableReadMetricsCollector implements SSTableReadsListener
    {
        /**
         * The number of SSTables that need to be merged. This counter is only updated for single partition queries
         * since this has been the behavior so far.
         */
        private int mergedSSTables;

        @Override
        public void onSSTableSelected(SSTableReader sstable, SelectionReason reason)
        {
            sstable.incrementReadCount();
            mergedSSTables++;
        }

        /**
         * Returns the number of SSTables that need to be merged.
         * @return the number of SSTables that need to be merged.
         */
        public int getMergedSSTables()
        {
            return mergedSSTables;
        }
    }

    public static class VirtualTableSinglePartitionReadCommand extends SinglePartitionReadCommand
    {
        protected VirtualTableSinglePartitionReadCommand(boolean isDigest,
                                                         int digestVersion,
                                                         boolean acceptsTransient,
                                                         TableMetadata metadata,
                                                         long nowInSec,
                                                         ColumnFilter columnFilter,
                                                         RowFilter rowFilter,
                                                         DataLimits limits,
                                                         DecoratedKey partitionKey,
                                                         ClusteringIndexFilter clusteringIndexFilter,
                                                         Index.QueryPlan indexQueryPlan,
                                                         boolean trackWarnings)
        {
            super(isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter, indexQueryPlan, trackWarnings);
        }

        @Override
        public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
        {
            return executeInternal(executionController());
        }

        @Override
        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            VirtualTable view = VirtualKeyspaceRegistry.instance.getTableNullable(metadata().id);
            UnfilteredPartitionIterator resultIterator = view.select(partitionKey, clusteringIndexFilter, columnFilter());
            return limits().filter(rowFilter().filter(resultIterator, nowInSec()), nowInSec(), selectsFullPartition());
        }

        @Override
        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        @Override
        public ReadExecutionController executionController(boolean trackRepairedStatus)
        {
            return executionController();
        }
    }
}