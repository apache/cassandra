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
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tracing.Tracing;

/**
 * A read command that selects a (part of a) range of partitions.
 */
public class PartitionRangeReadCommand extends ReadCommand implements PartitionRangeReadQuery
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    protected final DataRange dataRange;
    protected final Slices requestedSlices;

    private PartitionRangeReadCommand(boolean isDigest,
                                      int digestVersion,
                                      boolean acceptsTransient,
                                      TableMetadata metadata,
                                      long nowInSec,
                                      ColumnFilter columnFilter,
                                      RowFilter rowFilter,
                                      DataLimits limits,
                                      DataRange dataRange,
                                      Index.QueryPlan indexQueryPlan,
                                      boolean trackWarnings)
    {
        super(Kind.PARTITION_RANGE, isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, indexQueryPlan, trackWarnings);
        this.dataRange = dataRange;
        this.requestedSlices = dataRange.clusteringIndexFilter.getSlices(metadata());

    }

    private static PartitionRangeReadCommand create(boolean isDigest,
                                                    int digestVersion,
                                                    boolean acceptsTransient,
                                                    TableMetadata metadata,
                                                    long nowInSec,
                                                    ColumnFilter columnFilter,
                                                    RowFilter rowFilter,
                                                    DataLimits limits,
                                                    DataRange dataRange,
                                                    Index.QueryPlan indexQueryPlan,
                                                    boolean trackWarnings)
    {
        if (metadata.isVirtual())
        {
            return new VirtualTablePartitionRangeReadCommand(isDigest,
                                                             digestVersion,
                                                             acceptsTransient,
                                                             metadata,
                                                             nowInSec,
                                                             columnFilter,
                                                             rowFilter,
                                                             limits,
                                                             dataRange,
                                                             indexQueryPlan,
                                                             trackWarnings);
        }
        return new PartitionRangeReadCommand(isDigest,
                                             digestVersion,
                                             acceptsTransient,
                                             metadata,
                                             nowInSec,
                                             columnFilter,
                                             rowFilter,
                                             limits,
                                             dataRange,
                                             indexQueryPlan,
                                             trackWarnings);
    }

    public static PartitionRangeReadCommand create(TableMetadata metadata,
                                                   long nowInSec,
                                                   ColumnFilter columnFilter,
                                                   RowFilter rowFilter,
                                                   DataLimits limits,
                                                   DataRange dataRange)
    {
        return create(false,
                      0,
                      false,
                      metadata,
                      nowInSec,
                      columnFilter,
                      rowFilter,
                      limits,
                      dataRange,
                      findIndexQueryPlan(metadata, rowFilter),
                      false);
    }

    /**
     * Creates a new read command that query all the data in the table.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     *
     * @return a newly created read command that queries everything in the table.
     */
    public static PartitionRangeReadCommand allDataRead(TableMetadata metadata, long nowInSec)
    {
        return create(false,
                      0,
                      false,
                      metadata,
                      nowInSec,
                      ColumnFilter.all(metadata),
                      RowFilter.none(),
                      DataLimits.NONE,
                      DataRange.allData(metadata.partitioner),
                      null,
                      false);
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
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      isRangeContinuation ? limits() : limits().withoutState(),
                      dataRange().forSubRange(range),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    public PartitionRangeReadCommand copy()
    {
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      dataRange(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected PartitionRangeReadCommand copyAsDigestQuery()
    {
        return create(true,
                      digestVersion(),
                      false,
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      dataRange(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    protected PartitionRangeReadCommand copyAsTransientQuery()
    {
        return create(false,
                      0,
                      true,
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      limits(),
                      dataRange(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public PartitionRangeReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      newLimits,
                      dataRange(),
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    @Override
    public PartitionRangeReadCommand withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange)
    {
        return create(isDigestQuery(),
                      digestVersion(),
                      acceptsTransient(),
                      metadata(),
                      nowInSec(),
                      columnFilter(),
                      rowFilter(),
                      newLimits,
                      newDataRange,
                      indexQueryPlan(),
                      isTrackingWarnings());
    }

    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getRangeRpcTimeout(unit);
    }

    public boolean isReversed()
    {
        return dataRange.isReversed();
    }

    public PartitionIterator execute(ConsistencyLevel consistency, ClientState state, long queryStartNanoTime) throws RequestExecutionException
    {
        return StorageProxy.getRangeSlice(this, consistency, queryStartNanoTime);
    }

    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    @VisibleForTesting
    public UnfilteredPartitionIterator queryStorage(final ColumnFamilyStore cfs, ReadExecutionController controller)
    {
        ColumnFamilyStore.ViewFragment view = cfs.select(View.selectLive(dataRange().keyRange()));
        Tracing.trace("Executing seq scan across {} sstables for {}", view.sstables.size(), dataRange().keyRange().getString(metadata().partitionKeyType));

        // fetch data from current memtable, historical memtables, and SSTables in the correct order.
        InputCollector<UnfilteredPartitionIterator> inputCollector = iteratorsForRange(view, controller);
        try
        {
            SSTableReadsListener readCountUpdater = newReadCountUpdater();
            for (Memtable memtable : view.memtables)
            {
                UnfilteredPartitionIterator iter = memtable.partitionIterator(columnFilter(), dataRange(), readCountUpdater);
                controller.updateMinOldestUnrepairedTombstone(memtable.getMinLocalDeletionTime());
                inputCollector.addMemtableIterator(RTBoundValidator.validate(iter, RTBoundValidator.Stage.MEMTABLE, false));
            }

            int selectedSSTablesCnt = 0;
            for (SSTableReader sstable : view.sstables)
            {
                boolean intersects = intersects(sstable);
                boolean hasPartitionLevelDeletions = hasPartitionLevelDeletions(sstable);
                boolean hasRequiredStatics = hasRequiredStatics(sstable);

                if (!intersects && !hasPartitionLevelDeletions && !hasRequiredStatics)
                    continue;

                UnfilteredPartitionIterator iter = sstable.partitionIterator(columnFilter(), dataRange(), readCountUpdater);
                inputCollector.addSSTableIterator(sstable, RTBoundValidator.validate(iter, RTBoundValidator.Stage.SSTABLE, false));

                if (!sstable.isRepaired())
                    controller.updateMinOldestUnrepairedTombstone(sstable.getMinLocalDeletionTime());

                selectedSSTablesCnt++;
            }

            final int finalSelectedSSTables = selectedSSTablesCnt;

            // iterators can be empty for offline tools
            if (inputCollector.isEmpty())
                return EmptyIterators.unfilteredPartition(metadata());

            List<UnfilteredPartitionIterator> finalizedIterators = inputCollector.finalizeIterators(cfs, nowInSec(), controller.oldestUnrepairedTombstone());
            UnfilteredPartitionIterator merged = UnfilteredPartitionIterators.mergeLazily(finalizedIterators);
            return checkCacheFilter(Transformation.apply(merged, new Transformation<UnfilteredRowIterator>()
            {
                @Override
                protected void onClose()
                {
                    super.onClose();
                    cfs.metric.updateSSTableIteratedInRangeRead(finalSelectedSSTables);
                }
            }), cfs);
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
        return requestedSlices.intersects(sstable.getSSTableMetadata().coveredClustering);
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

    private UnfilteredPartitionIterator checkCacheFilter(UnfilteredPartitionIterator iter, final ColumnFamilyStore cfs)
    {
        class CacheFilter extends Transformation<BaseRowIterator<?>>
        {
            @Override
            public BaseRowIterator<?> applyToPartition(BaseRowIterator<?> iter)
            {
                // Note that we rely on the fact that until we actually advance 'iter', no really costly operation is actually done
                // (except for reading the partition key from the index file) due to the call to mergeLazily in queryStorage.
                DecoratedKey dk = iter.partitionKey();

                // Check if this partition is in the rowCache and if it is, if  it covers our filter
                CachedPartition cached = cfs.getRawCachedPartition(dk);
                ClusteringIndexFilter filter = dataRange().clusteringIndexFilter(dk);

                if (cached != null && cfs.isFilterFullyCoveredBy(filter,
                                                                 limits(),
                                                                 cached,
                                                                 nowInSec(),
                                                                 iter.metadata().enforceStrictLiveness()))
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

    @Override
    public Verb verb()
    {
        return Verb.RANGE_REQ;
    }

    protected void appendCQLWhereClause(StringBuilder sb)
    {
        String filterString = dataRange().toCQLString(metadata(), rowFilter());
        if (!filterString.isEmpty())
            sb.append(" WHERE ").append(filterString);
    }

    @Override
    public String loggableTokens()
    {
        return "token range: " + (dataRange.keyRange.inclusiveLeft() ? '[' : '(') +
               dataRange.keyRange.left.getToken().toString() + ", " +
               dataRange.keyRange.right.getToken().toString() +
               (dataRange.keyRange.inclusiveRight() ? ']' : ')');
    }

    /**
     * Allow to post-process the result of the query after it has been reconciled on the coordinator
     * but before it is passed to the CQL layer to return the ResultSet.
     *
     * See CASSANDRA-8717 for why this exists.
     */
    public PartitionIterator postReconciliationProcessing(PartitionIterator result)
    {
        Index.QueryPlan queryPlan = indexQueryPlan();
        return queryPlan == null ? result : queryPlan.postProcessor(this).apply(result);
    }

    @Override
    public String toString()
    {
        return String.format("Read(%s columns=%s rowfilter=%s limits=%s %s)",
                             metadata().toString(),
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

    public boolean isRangeRequest()
    {
        return true;
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
            DataRange range = DataRange.serializer.deserialize(in, version, metadata);
            return PartitionRangeReadCommand.create(isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, range, indexQueryPlan, false);
        }
    }

    public static class VirtualTablePartitionRangeReadCommand extends PartitionRangeReadCommand
    {
        private VirtualTablePartitionRangeReadCommand(boolean isDigest,
                                                      int digestVersion,
                                                      boolean acceptsTransient,
                                                      TableMetadata metadata,
                                                      long nowInSec,
                                                      ColumnFilter columnFilter,
                                                      RowFilter rowFilter,
                                                      DataLimits limits,
                                                      DataRange dataRange,
                                                      Index.QueryPlan indexQueryPlan,
                                                      boolean trackWarnings)
        {
            super(isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, dataRange, indexQueryPlan, trackWarnings);
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
            UnfilteredPartitionIterator resultIterator = view.select(dataRange, columnFilter());
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