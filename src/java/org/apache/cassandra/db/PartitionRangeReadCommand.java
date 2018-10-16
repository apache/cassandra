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
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.*;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A read command that selects a (part of a) range of partitions.
 */
public class PartitionRangeReadCommand extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final DataRange dataRange;
    private int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    private PartitionRangeReadCommand(boolean isDigest,
                                      int digestVersion,
                                      boolean isForThrift,
                                      CFMetaData metadata,
                                      int nowInSec,
                                      ColumnFilter columnFilter,
                                      RowFilter rowFilter,
                                      DataLimits limits,
                                      DataRange dataRange,
                                      IndexMetadata index)
    {
        super(Kind.PARTITION_RANGE, isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        this.dataRange = dataRange;
    }

    public static PartitionRangeReadCommand create(boolean isForThrift,
                                                   CFMetaData metadata,
                                                   int nowInSec,
                                                   ColumnFilter columnFilter,
                                                   RowFilter rowFilter,
                                                   DataLimits limits,
                                                   DataRange dataRange)
    {
        return new PartitionRangeReadCommand(false,
                                             0,
                                             isForThrift,
                                             metadata,
                                             nowInSec,
                                             columnFilter,
                                             rowFilter,
                                             limits,
                                             dataRange,
                                             findIndex(metadata, rowFilter));
    }

    /**
     * Creates a new read command that query all the data in the table.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     *
     * @return a newly created read command that queries everything in the table.
     */
    public static PartitionRangeReadCommand allDataRead(CFMetaData metadata, int nowInSec)
    {
        return new PartitionRangeReadCommand(false, 0, false,
                                             metadata,
                                             nowInSec,
                                             ColumnFilter.all(metadata),
                                             RowFilter.NONE,
                                             DataLimits.NONE,
                                             DataRange.allData(metadata.partitioner),
                                             null);
    }

    public DataRange dataRange()
    {
        return dataRange;
    }

    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        return dataRange.clusteringIndexFilter(key);
    }

    public boolean isNamesQuery()
    {
        return dataRange.isNamesQuery();
    }

    /**
     * Returns an equivalent command but that only queries data within the provided range.
     *
     * @param range the sub-range to restrict the command to. This method <b>assumes</b> that this is a proper sub-range
     * of the command this is applied to.
     * @param isRangeContinuation whether {@code range} is a direct continuation of whatever previous range we have
     * queried. This matters for the {@code DataLimits} that may contain states when we do paging and in the context of
     * parallel queries: that state only make sense if the range queried is indeed the follow-up of whatever range we've
     * previously query (that yield said state). In practice this means that ranges for which {@code isRangeContinuation}
     * is false may have to be slightly pessimistic when counting data and may include a little bit than necessary, and
     * this should be dealt with post-query (in the case of {@code StorageProxy.getRangeSlice()}, which uses this method
     * for replica queries, this is dealt with by re-counting results on the coordinator). Note that if this is the
     * first range we queried, then the {@code DataLimits} will have not state and the value of this parameter doesn't
     * matter.
     */
    public PartitionRangeReadCommand forSubRange(AbstractBounds<PartitionPosition> range, boolean isRangeContinuation)
    {
        // If we're not a continuation of whatever range we've previously queried, we should ignore the states of the
        // DataLimits as it's either useless, or misleading. This is particularly important for GROUP BY queries, where
        // DataLimits.CQLGroupByLimits.GroupByAwareCounter assumes that if GroupingState.hasClustering(), then we're in
        // the middle of a group, but we can't make that assumption if we query and range "in advance" of where we are
        // on the ring.
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             isRangeContinuation ? limits() : limits().withoutState(),
                                             dataRange().forSubRange(range),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand copy()
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand copyAsDigestQuery()
    {
        return new PartitionRangeReadCommand(true,
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             dataRange(),
                                             indexMetadata());
    }

    public ReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             dataRange(),
                                             indexMetadata());
    }

    public PartitionRangeReadCommand withUpdatedDataRange(DataRange newDataRange)
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             limits(),
                                             newDataRange,
                                             indexMetadata());
    }

    public PartitionRangeReadCommand withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange)
    {
        return new PartitionRangeReadCommand(isDigestQuery(),
                                             digestVersion(),
                                             isForThrift(),
                                             metadata(),
                                             nowInSec(),
                                             columnFilter(),
                                             rowFilter(),
                                             newLimits,
                                             newDataRange,
                                             indexMetadata());
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getRangeRpcTimeout();
    }

    public boolean selectsKey(DecoratedKey key)
    {
        if (!dataRange().contains(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().getKeyValidator());
    }

    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!dataRange().clusteringIndexFilter(key).selects(clustering))
            return false;
        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(this, consistency, queryStartNanoTime);
    }

    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
            return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    @VisibleForTesting
    public UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(dataRange().keyRange()));
        Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), dataRange().keyRange().getString(metadata().getKeyValidator()));

        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        final List<UnfilteredPartitionIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());

        try
        {
            for (Memtable memtable : view.memtables)
            {
                @SuppressWarnings("resource") // We close on exception and on closing the result returned by this method
                Memtable.MemtableUnfilteredPartitionIterator iter = memtable.makePartitionIterator(columnFilter(), dataRange(), isForThrift());

                @SuppressWarnings("resource") // We close on exception and on closing the result returned by this method
                UnfilteredPartitionIterator iterator = isForThrift() ? ThriftResultsMerger.maybeWrap(iter, metadata(), nowInSec()) : iter;
                iterators.add(RTBoundValidator.validate(iterator, RTBoundValidator.Stage.MEMTABLE, false));

                oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, iter.getMinLocalDeletionTime());
            }

            SSTableReadsListener readCountUpdater = newReadCountUpdater();
            for (SSTableReader sstable : view.sstables)
            {
                @SuppressWarnings("resource") // We close on exception and on closing the result returned by this method
                UnfilteredPartitionIterator iter = sstable.getScanner(columnFilter(), dataRange(), isForThrift(), readCountUpdater);

                if (isForThrift())
                    iter = ThriftResultsMerger.maybeWrap(iter, metadata(), nowInSec());

                iterators.add(RTBoundValidator.validate(iter, RTBoundValidator.Stage.SSTABLE, false));

                if (!sstable.isRepaired())
                    oldestUnrepairedTombstone = Math.min(oldestUnrepairedTombstone, sstable.getMinLocalDeletionTime());
            }
            // iterators can be empty for offline tools
            return iterators.isEmpty() ? EmptyIterators.unfilteredPartition(metadata(), isForThrift())
                                       : checkCacheFilter(UnfilteredPartitionIterators.mergeLazily(iterators, nowInSec()), cfs);
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                FBUtilities.closeAll(iterators);
            }
            catch (Exception suppressed)
            {
                e.addSuppressed(suppressed);
            }

            throw e;
        }
    }

    /**
     * Creates a new {@code SSTableReadsListener} to update the SSTables read counts.
     * @return a new {@code SSTableReadsListener} to update the SSTables read counts.
     */
    private static SSTableReadsListener newReadCountUpdater()
    {
        return new SSTableReadsListener()
                {
                    @Override
                    public void onScanningStarted(SSTableReader sstable)
                    {
                        sstable.incrementReadCount();
                    }
                };
    }

    @Override
    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }

    private UnfilteredPartitionIterator checkCacheFilter(UnfilteredPartitionIterator iter, final ColumnFamilyStore cfs)
    {
        class CacheFilter extends Transformation
        {
            @Override
            public BaseRowIterator applyToPartition(BaseRowIterator iter)
            {
                // Note that we rely on the fact that until we actually advance 'iter', no really costly operation is actually done
                // (except for reading the partition key from the index file) due to the call to mergeLazily in queryStorage.
                DecoratedKey dk = iter.partitionKey();

                // Check if this partition is in the rowCache and if it is, if  it covers our filter
                CachedPartition cached = cfs.getRawCachedPartition(dk);
                ClusteringIndexFilter filter = dataRange().clusteringIndexFilter(dk);

                if (cached != null && cfs.isFilterFullyCoveredBy(filter, limits(), cached, nowInSec()))
                {
                    // We won't use 'iter' so close it now.
                    iter.close();

                    return filter.getUnfilteredRowIterator(columnFilter(), cached);
                }

                return iter;
            }
        }
        return Transformation.apply(iter, new CacheFilter());
    }

    public MessageOut<ReadCommand> createMessage(int version)
    {
        return dataRange().isPaging()
             ? new MessageOut<>(MessagingService.Verb.PAGED_RANGE, this, pagedRangeSerializer)
             : new MessageOut<>(MessagingService.Verb.RANGE_SLICE, this, rangeSliceSerializer);
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (dataRange.isUnrestricted() && rowFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the row filter first because the data range can end by "ORDER BY"
        if (!rowFilter().isEmpty())
        {
            sb.append(rowFilter());
            if (!dataRange.isUnrestricted())
                sb.append(" AND ");
        }
        if (!dataRange.isUnrestricted())
            sb.append(dataRange.toCQLString(metadata()));
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public PartitionIterator postReconciliationProcessing(PartitionIterator result)
    {
        ColumnFamilyStore cfs = Keyspace.open(metadata().ksName).getColumnFamilyStore(metadata().cfName);
        Index index = getIndex(cfs);
        return index == null ? result : index.postProcessorFor(this).apply(result, this);
    }

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (dataRange.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s.%s columns=%s rowfilter=%s limits=%s %s)",
                             metadata().ksName,
                             metadata().cfName,
                             columnFilter(),
                             rowFilter(),
                             limits(),
                             dataRange().toString(metadata()));
    }

    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        DataRange.serializer.serialize(dataRange(), out, version, metadata());
    }

    protected long selectionSerializedSize(int version)
    {
        return DataRange.serializer.serializedSize(dataRange(), version, metadata());
    }

    /*
     * We are currently using PartitionRangeReadCommand for most index queries, even if they are explicitly restricted
     * to a single partition key. Return true if that is the case.
     *
     * See CASSANDRA-11617 and CASSANDRA-11872 for details.
     */
    public boolean isLimitedToOnePartition()
    {
        return dataRange.keyRange instanceof Bounds
            && dataRange.startKey().kind() == PartitionPosition.Kind.ROW_KEY
            && dataRange.startKey().equals(dataRange.stopKey());
    }

    private static class Deserializer extends SelectionDeserializer
    {
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       boolean isDigest,
                                       int digestVersion,
                                       boolean isForThrift,
                                       CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       IndexMetadata index)
        throws IOException
        {
            DataRange range = DataRange.serializer.deserialize(in, version, metadata);
            return new PartitionRangeReadCommand(isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, range, index);
        }
    }
}
