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
import java.util.*;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.*;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * A read command that selects a (part of a) single partition.
 */
public abstract class SinglePartitionReadCommand<F extends ClusteringIndexFilter> extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DecoratedKey partitionKey;
    private final F clusteringIndexFilter;

    protected SinglePartitionReadCommand(boolean isDigest,
                                         int digestVersion,
                                         boolean isForThrift,
                                         CFMetaData metadata,
                                         int nowInSec,
                                         ColumnFilter columnFilter,
                                         RowFilter rowFilter,
                                         DataLimits limits,
                                         DecoratedKey partitionKey,
                                         F clusteringIndexFilter)
    {
        super(Kind.SINGLE_PARTITION, isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits);
        assert partitionKey.getPartitioner() == metadata.partitioner;
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;
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
    public static SinglePartitionReadCommand<?> create(CFMetaData metadata,
                                                       int nowInSec,
                                                       ColumnFilter columnFilter,
                                                       RowFilter rowFilter,
                                                       DataLimits limits,
                                                       DecoratedKey partitionKey,
                                                       ClusteringIndexFilter clusteringIndexFilter)
    {
        return create(false, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    /**
     * Creates a new read command on a single partition for thrift.
     *
     * @param isForThrift whether the query is for thrift or not.
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
    public static SinglePartitionReadCommand<?> create(boolean isForThrift,
                                                       CFMetaData metadata,
                                                       int nowInSec,
                                                       ColumnFilter columnFilter,
                                                       RowFilter rowFilter,
                                                       DataLimits limits,
                                                       DecoratedKey partitionKey,
                                                       ClusteringIndexFilter clusteringIndexFilter)
    {
        if (clusteringIndexFilter instanceof ClusteringIndexSliceFilter)
            return new SinglePartitionSliceCommand(false, 0, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, (ClusteringIndexSliceFilter) clusteringIndexFilter);

        assert clusteringIndexFilter instanceof ClusteringIndexNamesFilter;
        return new SinglePartitionNamesCommand(false, 0, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, (ClusteringIndexNamesFilter) clusteringIndexFilter);
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
    public static SinglePartitionReadCommand<?> create(CFMetaData metadata, int nowInSec, DecoratedKey key, ColumnFilter columnFilter, ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.NONE, DataLimits.NONE, key, filter);
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
    public static SinglePartitionReadCommand fullPartitionRead(CFMetaData metadata, int nowInSec, DecoratedKey key)
    {
        return SinglePartitionSliceCommand.create(metadata, nowInSec, key, Slices.ALL);
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
    public static SinglePartitionReadCommand fullPartitionRead(CFMetaData metadata, int nowInSec, ByteBuffer key)
    {
        return SinglePartitionSliceCommand.create(metadata, nowInSec, metadata.decorateKey(key), Slices.ALL);
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public F clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return clusteringIndexFilter;
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getReadRpcTimeout();
    }

    public boolean selects(DecoratedKey partitionKey, Clustering clustering)
    {
        if (!partitionKey().equals(partitionKey))
            return false;

        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        return clusteringIndexFilter().selects(clustering);
    }

    /**
     * Returns a new command suitable to paging from the last returned row.
     *
     * @param lastReturned the last row returned by the previous page. The newly created command
     * will only query row that comes after this (in query order). This can be {@code null} if this
     * is the first page.
     * @param pageSize the size to use for the page to query.
     *
     * @return the newly create command.
     */
    public SinglePartitionReadCommand forPaging(Clustering lastReturned, int pageSize)
    {
        // We shouldn't have set digest yet when reaching that point
        assert !isDigestQuery();
        return create(isForThrift(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits().forPaging(pageSize),
                      partitionKey(),
                      lastReturned == null ? clusteringIndexFilter() : clusteringIndexFilter.forPaging(metadata().comparator, lastReturned, false));
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
    {
        return StorageProxy.read(Group.one(this), consistency, clientState);
    }

    public SinglePartitionPager getPager(PagingState pagingState)
    {
        return getPager(this, pagingState);
    }

    private static SinglePartitionPager getPager(SinglePartitionReadCommand command, PagingState pagingState)
    {
        return new SinglePartitionPager(command, pagingState);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.readLatency.addNano(latencyNanos);
    }

    protected UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadOrderGroup orderGroup)
    {
        @SuppressWarnings("resource") // we close the created iterator through closing the result of this method (and SingletonUnfilteredPartitionIterator ctor cannot fail)
        UnfilteredRowIterator partition = cfs.isRowCacheEnabled()
                                        ? getThroughCache(cfs, orderGroup.baseReadOpOrderGroup())
                                        : queryMemtableAndDisk(cfs, orderGroup.baseReadOpOrderGroup());
        return new SingletonUnfilteredPartitionIterator(partition, isForThrift());
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
    private UnfilteredRowIterator getThroughCache(ColumnFamilyStore cfs, OpOrder.Group readOp)
    {
        assert !cfs.isIndex(); // CASSANDRA-5732
        assert cfs.isRowCacheEnabled() : String.format("Row cache is not enabled on table [%s]", cfs.name);

        UUID cfId = metadata().cfId;
        RowCacheKey key = new RowCacheKey(cfId, partitionKey());

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
                return queryMemtableAndDisk(cfs, readOp);
            }

            CachedPartition cachedPartition = (CachedPartition)cached;
            if (cfs.isFilterFullyCoveredBy(clusteringIndexFilter(), limits(), cachedPartition, nowInSec()))
            {
                cfs.metric.rowCacheHit.inc();
                Tracing.trace("Row cache hit");
                return clusteringIndexFilter().getUnfilteredRowIterator(columnFilter(), cachedPartition);
            }

            cfs.metric.rowCacheHitOutOfRange.inc();
            Tracing.trace("Ignoring row cache as cached value could not satisfy query");
            return queryMemtableAndDisk(cfs, readOp);
        }

        cfs.metric.rowCacheMiss.inc();
        Tracing.trace("Row cache miss");

        boolean cacheFullPartitions = metadata().params.caching.cacheAllRows();

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
                int rowsToCache = metadata().params.caching.rowsPerPartitionToCache();
                @SuppressWarnings("resource") // we close on exception or upon closing the result of this method
                UnfilteredRowIterator iter = SinglePartitionReadCommand.fullPartitionRead(metadata(), nowInSec(), partitionKey()).queryMemtableAndDisk(cfs, readOp);
                try
                {
                    // We want to cache only rowsToCache rows
                    CachedPartition toCache = CachedBTreePartition.create(DataLimits.cqlLimits(rowsToCache).filter(iter, nowInSec()), nowInSec());
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
        return queryMemtableAndDisk(cfs, readOp);
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
     * Also note that one must have "started" a {@code OpOrder.Group} on the queried table, and that is
     * to enforce that that it is required as parameter, even though it's not explicitlly used by the method.
     */
    public UnfilteredRowIterator queryMemtableAndDisk(ColumnFamilyStore cfs, OpOrder.Group readOp)
    {
        Tracing.trace("Executing single-partition query on {}", cfs.name);

        boolean copyOnHeap = Memtable.MEMORY_POOL.needToCopyOnHeap();
        return queryMemtableAndDiskInternal(cfs, copyOnHeap);
    }

    protected abstract UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap);

    @Override
    public String toString()
    {
        return String.format("Read(%s.%s columns=%s rowFilter=%s limits=%s key=%s filter=%s, nowInSec=%d)",
                             metadata().ksName,
                             metadata().cfName,
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             metadata().getKeyValidator().getString(partitionKey().getKey()),
                             clusteringIndexFilter.toString(metadata()),
                             nowInSec());
    }

    public MessageOut<ReadCommand> createMessage(int version)
    {
        return new MessageOut<>(MessagingService.Verb.READ, this, version < MessagingService.VERSION_30 ? legacyReadCommandSerializer : serializer);
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ");

        sb.append(ColumnDefinition.toCQLString(metadata().partitionKeyColumns())).append(" = ");
        DataRange.appendKeyString(sb, metadata().getKeyValidator(), partitionKey().getKey());

        // We put the row filter first because the clustering index filter can end by "ORDER BY"
        if (!rowFilter().isEmpty())
            sb.append(" AND ").append(rowFilter());

        String filterString = clusteringIndexFilter().toCQLString(metadata());
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        metadata().getKeyValidator().writeValue(partitionKey().getKey(), out);
        ClusteringIndexFilter.serializer.serialize(clusteringIndexFilter(), out, version);
    }

    protected long selectionSerializedSize(int version)
    {
        return metadata().getKeyValidator().writtenLength(partitionKey().getKey())
             + ClusteringIndexFilter.serializer.serializedSize(clusteringIndexFilter(), version);
    }

    /**
     * Groups multiple single partition read commands.
     */
    public static class Group implements ReadQuery
    {
        public final List<SinglePartitionReadCommand<?>> commands;
        private final DataLimits limits;
        private final int nowInSec;

        public Group(List<SinglePartitionReadCommand<?>> commands, DataLimits limits)
        {
            assert !commands.isEmpty();
            this.commands = commands;
            this.limits = limits;
            this.nowInSec = commands.get(0).nowInSec();
            for (int i = 1; i < commands.size(); i++)
                assert commands.get(i).nowInSec() == nowInSec;
        }

        public static Group one(SinglePartitionReadCommand<?> command)
        {
            return new Group(Collections.<SinglePartitionReadCommand<?>>singletonList(command), command.limits());
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState) throws RequestExecutionException
        {
            return StorageProxy.read(this, consistency, clientState);
        }

        public int nowInSec()
        {
            return nowInSec;
        }

        public DataLimits limits()
        {
            return limits;
        }

        public CFMetaData metadata()
        {
            return commands.get(0).metadata();
        }

        public ReadOrderGroup startOrderGroup()
        {
            // Note that the only difference between the command in a group must be the partition key on which
            // they applied. So as far as ReadOrderGroup is concerned, we can use any of the commands to start one.
            return commands.get(0).startOrderGroup();
        }

        public PartitionIterator executeInternal(ReadOrderGroup orderGroup)
        {
            List<PartitionIterator> partitions = new ArrayList<>(commands.size());
            for (SinglePartitionReadCommand cmd : commands)
                partitions.add(cmd.executeInternal(orderGroup));

            // Because we only have enforce the limit per command, we need to enforce it globally.
            return limits.filter(PartitionIterators.concat(partitions), nowInSec);
        }

        public QueryPager getPager(PagingState pagingState)
        {
            if (commands.size() == 1)
                return SinglePartitionReadCommand.getPager(commands.get(0), pagingState);

            return new MultiPartitionPager(this, pagingState);
        }

        @Override
        public String toString()
        {
            return commands.toString();
        }
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in, int version, boolean isDigest, int digestVersion, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
        throws IOException
        {
            DecoratedKey key = metadata.decorateKey(metadata.getKeyValidator().readValue(in));
            ClusteringIndexFilter filter = ClusteringIndexFilter.serializer.deserialize(in, version, metadata);
            if (filter instanceof ClusteringIndexNamesFilter)
                return new SinglePartitionNamesCommand(isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, key, (ClusteringIndexNamesFilter)filter);
            else
                return new SinglePartitionSliceCommand(isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, key, (ClusteringIndexSliceFilter)filter);
        }
    }
}
