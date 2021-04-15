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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Used by {@code EndpointGroupingCoordinator} to query all involved ranges on a given replica at once.
 *
 * Note: digest is not supported because each replica is responsible for different token ranges, there is no point on
 * sending digest.
 */
public class MultiRangeReadCommand extends ReadCommand
{
    protected static final SelectionDeserializer selectionDeserializer = new Deserializer();

    private final List<DataRange> dataRanges;

    private MultiRangeReadCommand(boolean isDigest,
                                  int digestVersion,
                                  boolean acceptsTransient,
                                  TableMetadata metadata,
                                  int nowInSec,
                                  ColumnFilter columnFilter,
                                  RowFilter rowFilter,
                                  DataLimits limits,
                                  List<DataRange> dataRanges,
                                  Index.QueryPlan indexQueryPlan)
    {
        super(Kind.MULTI_RANGE, isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, indexQueryPlan);

        assert dataRanges.size() > 0;
        this.dataRanges = dataRanges;
    }

    /**
     *
     * @param command current partition range command
     * @param ranges token ranges to be queried on specific endpoint
     * @param isRangeContinuation whether it's querying the first range in the batch
     * @return multi-range read command for specific endpoint
     */
    @VisibleForTesting
    public static MultiRangeReadCommand create(PartitionRangeReadCommand command, List<AbstractBounds<PartitionPosition>> ranges, boolean isRangeContinuation)
    {
        List<DataRange> dataRanges = new ArrayList<>(ranges.size());
        for (AbstractBounds<PartitionPosition> range : ranges)
            dataRanges.add(command.dataRange().forSubRange(range));

        return new MultiRangeReadCommand(command.isDigestQuery(),
                                         command.digestVersion(),
                                         command.acceptsTransient(),
                                         command.metadata(),
                                         command.nowInSec(),
                                         command.columnFilter(),
                                         command.rowFilter(),
                                         isRangeContinuation ? command.limits() : command.limits().withoutState(),
                                         dataRanges,
                                         command.indexQueryPlan());
    }

    /**
     * @param subrangeHandlers handlers for all vnode ranges replicated in current endpoint.
     * @return multi-range read command for specific endpoint
     */
    public static MultiRangeReadCommand create(List<ReadCallback<?, ?>> subrangeHandlers)
    {
        assert !subrangeHandlers.isEmpty();

        PartitionRangeReadCommand command = (PartitionRangeReadCommand) subrangeHandlers.get(0).command();
        List<DataRange> dataRanges = new ArrayList<>(subrangeHandlers.size());
        for(ReadCallback<?, ?> handler : subrangeHandlers)
        {
            dataRanges.add(((PartitionRangeReadCommand) handler.command()).dataRange());
        }


        return new MultiRangeReadCommand(command.isDigestQuery(),
                                         command.digestVersion(),
                                         command.acceptsTransient(),
                                         command.metadata(),
                                         command.nowInSec(),
                                         command.columnFilter(),
                                         command.rowFilter(),
                                         command.limits(),
                                         dataRanges,
                                         command.indexQueryPlan());
    }

    /**
     * @return all token ranges to be queried
     */
    public List<DataRange> ranges()
    {
        return dataRanges;
    }

    @Override
    protected void serializeSelection(DataOutputPlus out, int version) throws IOException
    {
        int rangeCount = dataRanges.size();
        out.writeInt(rangeCount);

        for (DataRange range : dataRanges)
            DataRange.serializer.serialize(range, out, version, metadata());
    }

    @Override
    protected long selectionSerializedSize(int version)
    {
        int rangeCount = dataRanges.size();
        long size = TypeSizes.sizeof(rangeCount);

        for (DataRange range : dataRanges)
            size += DataRange.serializer.serializedSize(range, version, metadata());

        return size;
    }

    @Override
    public boolean isLimitedToOnePartition()
    {
        if (dataRanges.size() != 1)
            return false;

        DataRange dataRange = dataRanges.get(0);
        return dataRange.keyRange() instanceof Bounds
               && dataRange.startKey().kind() == PartitionPosition.Kind.ROW_KEY
               && dataRange.startKey().equals(dataRange.stopKey());
    }

    @Override
    public boolean isRangeRequest()
    {
        return false;
    }

    @Override
    public ReadCommand withUpdatedLimit(DataLimits newLimits)
    {
        return new MultiRangeReadCommand(isDigestQuery(),
                                         digestVersion(),
                                         acceptsTransient(),
                                         metadata(),
                                         nowInSec(),
                                         columnFilter(),
                                         rowFilter(),
                                         newLimits,
                                         dataRanges,
                                         indexQueryPlan());
    }

    @Override
    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getRangeRpcTimeout(unit);
    }

    @Override
    public ReadResponse createResponse(UnfilteredPartitionIterator iterator, RepairedDataInfo rdi)
    {
        assert !isDigestQuery();
        return MultiRangeReadResponse.createDataResponse(iterator, this);
    }

    @Override
    public ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key)
    {
        for (DataRange dataRange : ranges())
        {
            if (dataRange.keyRange().contains(key))
                return dataRange.clusteringIndexFilter(key);
        }

        throw new IllegalArgumentException(key + " is not in data ranges " + dataRanges.stream().map(r -> r.toString(metadata())).collect(Collectors.toList()));
    }

    @Override
    public ReadCommand copy()
    {
        return new MultiRangeReadCommand(isDigestQuery(),
                                         digestVersion(),
                                         acceptsTransient(),
                                         metadata(),
                                         nowInSec(),
                                         columnFilter(),
                                         rowFilter(),
                                         limits(),
                                         dataRanges,
                                         indexQueryPlan());
    }

    @Override
    protected ReadCommand copyAsTransientQuery()
    {
        return new MultiRangeReadCommand(false,
                                          0,
                                          true,
                                          metadata(),
                                          nowInSec(),
                                          columnFilter(),
                                          rowFilter(),
                                          limits(),
                                          dataRanges,
                                          indexQueryPlan());
    }

    @Override
    protected ReadCommand copyAsDigestQuery()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController)
    {
        return UnfilteredPartitionIterators.concat(dataRanges.stream()
                                                             .map(this::toPartitionRangeReadCommand)
                                                             .map(command -> command.queryStorage(cfs, executionController))
                                                             .collect(Collectors.toList()));
    }

    @Override
    public UnfilteredPartitionIterator searchStorage(Index.Searcher searcher, ReadExecutionController controller)
    {
        if (indexQueryPlan.supportsMultiRangeReadCommand())
        {
            // SAI supports fetching multiple ranges at once
            return super.searchStorage(searcher, controller);
        }
        else
        {
            // search each subrange separately as they don't support MultiRangeReadCommand
            return UnfilteredPartitionIterators.concat(dataRanges.stream()
                                                                 .map(this::toPartitionRangeReadCommand)
                                                                 .map(command -> command.searchStorage(searcher, controller))
                                                                 .collect(Collectors.toList()));
        }
    }

    private PartitionRangeReadCommand toPartitionRangeReadCommand(DataRange dataRange)
    {
        return PartitionRangeReadCommand.create(metadata(), nowInSec(), columnFilter(), rowFilter(), limits(), dataRange, indexQueryPlan());
    }

    @Override
    public boolean isReversed()
    {
        return ranges().get(0).isReversed();
    }

    @Override
    protected void recordLatency(TableMetrics metric, long latencyNanos)
    {
        metric.rangeLatency.addNano(latencyNanos);
    }

    @Override
    public Verb verb()
    {
        return Verb.MULTI_RANGE_REQ;
    }

    @Override
    protected void appendCQLWhereClause(StringBuilder sb)
    {
        if (ranges().size() == 1 && ranges().get(0).isUnrestricted() && rowFilter().isEmpty())
            return;

        sb.append(" WHERE ");
        // We put the row filter first because the data range can end by "ORDER BY"
        if (!rowFilter().isEmpty())
        {
            sb.append(rowFilter());
            sb.append(" AND ");
        }

        boolean isFirst = true;
        for (int i = 0; i < ranges().size(); i++)
        {
            DataRange dataRange = ranges().get(i);
            if (!dataRange.isUnrestricted())
            {
                if (!isFirst)
                    sb.append(" AND ");
                isFirst = false;
                sb.append(dataRange.toCQLString(metadata()));
            }
        }
    }

    @Override
    public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
    {
        // MultiRangeReadCommand should only be executed on the replica side
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        // MultiRangeReadCommand should only be executed at replica side"
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean selectsKey(DecoratedKey key)
    {
        for (DataRange dataRange : ranges())
        {
            if (!dataRange.contains(key))
                continue;

            return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
        }

        return false;
    }

    @Override
    public boolean selectsClustering(DecoratedKey key, Clustering clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        for (DataRange dataRange : ranges())
        {
            if (!dataRange.keyRange().contains(key) || !dataRange.clusteringIndexFilter(key).selects(clustering))
                continue;

            if (rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering))
                return true;
        }

        return false;
    }

    @Override
    public boolean selectsFullPartition()
    {
        return metadata().isStaticCompactTable() ||
               (ranges().stream().allMatch(DataRange::selectsAllPartition) && !rowFilter().hasExpressionOnClusteringOrRegularColumns());
    }

    private static class Deserializer extends SelectionDeserializer
    {
        @Override
        public ReadCommand deserialize(DataInputPlus in,
                                       int version,
                                       boolean isDigest,
                                       int digestVersion,
                                       boolean acceptsTransient,
                                       TableMetadata metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       Index.QueryPlan indexQueryPlan)
        throws IOException
        {
            int rangeCount = in.readInt();

            List<DataRange> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add(DataRange.serializer.deserialize(in, version, metadata));

            return new MultiRangeReadCommand(isDigest,
                                             digestVersion,
                                             acceptsTransient,
                                             metadata,
                                             nowInSec,
                                             columnFilter,
                                             rowFilter,
                                             limits,
                                             ranges,
                                             indexQueryPlan);
        }
    }
}
