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

import java.io.DataInput;
import java.io.IOException;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand implements ReadQuery
{
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    private final Kind kind;
    private final CFMetaData metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    private boolean isDigestQuery;
    private final boolean isForThrift;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInput in, int version, boolean isDigest, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private SelectionDeserializer selectionDeserializer;

        private Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          boolean isDigestQuery,
                          boolean isForThrift,
                          CFMetaData metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits)
    {
        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.isForThrift = isForThrift;
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.rowFilter = rowFilter;
        this.limits = limits;
    }

    protected abstract void serializeSelection(DataOutputPlus out, int version) throws IOException;
    protected abstract long selectionSerializedSize(int version);

    /**
     * The metadata for the table queried.
     *
     * @return the metadata for the table queried.
     */
    public CFMetaData metadata()
    {
        return metadata;
    }

    /**
     * The time in seconds to use as "now" for this query.
     * <p>
     * We use the same time as "now" for the whole query to avoid considering different
     * values as expired during the query, which would be buggy (would throw of counting amongst other
     * things).
     *
     * @return the time (in seconds) to use as "now".
     */
    public int nowInSec()
    {
        return nowInSec;
    }

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout();

    /**
     * A filter on which (non-PK) columns must be returned by the query.
     *
     * @return which columns must be fetched by this query.
     */
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    /**
     * Filters/Resrictions on CQL rows.
     * <p>
     * This contains the restrictions that are not directly handled by the
     * {@code ClusteringIndexFilter}. More specifically, this includes any non-PK column
     * restrictions and can include some PK columns restrictions when those can't be
     * satisfied entirely by the clustering index filter (because not all clustering columns
     * have been restricted for instance). If there is 2ndary indexes on the table,
     * one of this restriction might be handled by a 2ndary index.
     *
     * @return the filter holding the expression that rows must satisfy.
     */
    public RowFilter rowFilter()
    {
        return rowFilter;
    }

    /**
     * The limits set on this query.
     *
     * @return the limits set on this query.
     */
    public DataLimits limits()
    {
        return limits;
    }

    /**
     * Whether this query is a digest one or not.
     *
     * @return Whether this query is a digest query.
     */
    public boolean isDigestQuery()
    {
        return isDigestQuery;
    }

    /**
     * Sets whether this command should be a digest one or not.
     *
     * @param isDigestQuery whether the command should be set as a digest one or not.
     * @return this read command.
     */
    public ReadCommand setIsDigestQuery(boolean isDigestQuery)
    {
        this.isDigestQuery = isDigestQuery;
        return this;
    }

    /**
     * Whether this query is for thrift or not.
     *
     * @return whether this query is for thrift.
     */
    public boolean isForThrift()
    {
        return isForThrift;
    }

    /**
     * The clustering index filter this command to use for the provided key.
     * <p>
     * Note that that method should only be called on a key actually queried by this command
     * and in practice, this will almost always return the same filter, but for the sake of
     * paging, the filter on the first key of a range command might be slightly different.
     *
     * @param key a partition key queried by this command.
     *
     * @return the {@code ClusteringIndexFilter} to use for the partition of key {@code key}.
     */
    public abstract ClusteringIndexFilter clusteringIndexFilter(DecoratedKey key);

    /**
     * Returns a copy of this command.
     *
     * @return a copy of this command.
     */
    public abstract ReadCommand copy();

    /**
     * Whether the provided row, identified by its primary key components, is selected by
     * this read command.
     *
     * @param partitionKey the partition key for the row to test.
     * @param clustering the clustering for the row to test.
     *
     * @return whether the row of partition key {@code partitionKey} and clustering
     * {@code clustering} is selected by this command.
     */
    public abstract boolean selects(DecoratedKey partitionKey, Clustering clustering);

    protected abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadOrderGroup orderGroup);

    public ReadResponse createResponse(UnfilteredPartitionIterator iterator)
    {
        return isDigestQuery()
             ? ReadResponse.createDigestResponse(iterator)
             : ReadResponse.createDataResponse(iterator);
    }

    protected SecondaryIndexSearcher getIndexSearcher(ColumnFamilyStore cfs)
    {
        return cfs.indexManager.getBestIndexSearcherFor(this);
    }

    /**
     * Executes this command on the local host.
     *
     * @param cfs the store for the table queried by this command.
     *
     * @return an iterator over the result of executing this command locally.
     */
    @SuppressWarnings("resource") // The result iterator is closed upon exceptions (we know it's fine to potentially not close the intermediary
                                  // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    public UnfilteredPartitionIterator executeLocally(ReadOrderGroup orderGroup)
    {
        long startTimeNanos = System.nanoTime();

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
        SecondaryIndexSearcher searcher = getIndexSearcher(cfs);
        UnfilteredPartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs, orderGroup)
                                         : searcher.search(this, orderGroup);

        try
        {
            resultIterator = UnfilteredPartitionIterators.convertExpiredCellsToTombstones(resultIterator, nowInSec);
            resultIterator = withMetricsRecording(withoutExpiredTombstones(resultIterator, cfs), cfs.metric, startTimeNanos);

            // TODO: we should push the dropping of columns down the layers because
            // 1) it'll be more efficient
            // 2) it could help us solve #6276
            // But there is not reason not to do this as a followup so keeping it here for now (we'll have
            // to be wary of cached row if we move this down the layers)
            if (!metadata().getDroppedColumns().isEmpty())
                resultIterator = UnfilteredPartitionIterators.removeDroppedColumns(resultIterator, metadata().getDroppedColumns());

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            RowFilter updatedFilter = searcher == null
                                       ? rowFilter()
                                       : rowFilter().without(searcher.primaryClause(this));

            // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return limits().filter(rowFilter().filter(resultIterator, nowInSec()), nowInSec());
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    protected abstract void recordLatency(ColumnFamilyMetrics metric, long latencyNanos);

    public PartitionIterator executeInternal(ReadOrderGroup orderGroup)
    {
        return UnfilteredPartitionIterators.filter(executeLocally(orderGroup), nowInSec());
    }

    public ReadOrderGroup startOrderGroup()
    {
        return ReadOrderGroup.forCommand(this);
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final ColumnFamilyMetrics metric, final long startTimeNanos)
    {
        return new WrappingUnfilteredPartitionIterator(iter)
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !ReadCommand.this.metadata().ksName.equals(SystemKeyspace.NAME);

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public UnfilteredRowIterator computeNext(UnfilteredRowIterator iter)
            {
                currentKey = iter.partitionKey();

                return new WrappingUnfilteredRowIterator(iter)
                {
                    public Unfiltered next()
                    {
                        Unfiltered unfiltered = super.next();
                        if (unfiltered.kind() == Unfiltered.Kind.ROW)
                        {
                            Row row = (Row) unfiltered;
                            if (row.hasLiveData(ReadCommand.this.nowInSec()))
                                ++liveRows;
                            for (Cell cell : row)
                                if (!cell.isLive(ReadCommand.this.nowInSec()))
                                    countTombstone(row.clustering());
                        }
                        else
                        {
                            countTombstone(unfiltered.clustering());
                        }

                        return unfiltered;
                    }

                    private void countTombstone(ClusteringPrefix clustering)
                    {
                        ++tombstones;
                        if (tombstones > failureThreshold && respectTombstoneThresholds)
                        {
                            String query = ReadCommand.this.toCQLString();
                            Tracing.trace("Scanned over {} tombstones for query {}; query aborted (see tombstone_failure_threshold)", failureThreshold, query);
                            throw new TombstoneOverwhelmingException(tombstones, query, ReadCommand.this.metadata(), currentKey, clustering);
                        }
                    }
                };
            }

            @Override
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    recordLatency(metric, System.nanoTime() - startTimeNanos);

                    metric.tombstoneScannedHistogram.update(tombstones);
                    metric.liveScannedHistogram.update(liveRows);

                    boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                    if (warnTombstones)
                    {
                        String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toCQLString());
                        ClientWarn.warn(msg);
                        logger.warn(msg);
                    }

                    Tracing.trace("Read {} live and {} tombstone cells{}", new Object[]{ liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : "") });
                }
            }
        };
    }

    /**
     * Creates a message for this command.
     */
    public MessageOut<ReadCommand> createMessage()
    {
        // TODO: we should use different verbs for old message (RANGE_SLICE, PAGED_RANGE)
        return new MessageOut<>(MessagingService.Verb.READ, this, serializer);
    }

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip expired tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for expired tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutExpiredTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        return new TombstonePurgingPartitionIterator(iterator, cfs.gcBefore(nowInSec()))
        {
            protected long getMaxPurgeableTimestamp()
            {
                return Long.MAX_VALUE;
            }
        };
    }

    /**
     * Recreate the CQL string corresponding to this query.
     * <p>
     * Note that in general the returned string will not be exactly the original user string, first
     * because there isn't always a single syntax for a given query,  but also because we don't have
     * all the information needed (we know the non-PK columns queried but not the PK ones as internally
     * we query them all). So this shouldn't be relied too strongly, but this should be good enough for
     * debugging purpose which is what this is for.
     */
    public String toCQLString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(columnFilter());
        sb.append(" FROM ").append(metadata().ksName).append(".").append(metadata.cfName);
        appendCQLWhereClause(sb);

        if (limits() != DataLimits.NONE)
            sb.append(" ").append(limits());
        return sb.toString();
    }

    private static class Serializer implements IVersionedSerializer<ReadCommand>
    {
        private static int digestFlag(boolean isDigest)
        {
            return isDigest ? 0x01 : 0;
        }

        private static boolean isDigest(int flags)
        {
            return (flags & 0x01) != 0;
        }

        private static int thriftFlag(boolean isForThrift)
        {
            return isForThrift ? 0x02 : 0;
        }

        private static boolean isForThrift(int flags)
        {
            return (flags & 0x02) != 0;
        }

        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            out.writeByte(command.kind.ordinal());
            out.writeByte(digestFlag(command.isDigestQuery()) | thriftFlag(command.isForThrift()));
            CFMetaData.serializer.serialize(command.metadata(), out, version);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(), out, version);
            RowFilter.serializer.serialize(command.rowFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInput in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            Kind kind = Kind.values()[in.readByte()];
            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            boolean isForThrift = isForThrift(flags);
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);
            RowFilter rowFilter = RowFilter.serializer.deserialize(in, version, metadata);
            DataLimits limits = DataLimits.serializer.deserialize(in, version);

            return kind.selectionDeserializer.deserialize(in, version, isDigest, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            if (version < MessagingService.VERSION_30)
                throw new UnsupportedOperationException();

            TypeSizes sizes = TypeSizes.NATIVE;

            return 2 // kind + flags
                 + CFMetaData.serializer.serializedSize(command.metadata(), version, sizes)
                 + sizes.sizeof(command.nowInSec())
                 + ColumnFilter.serializer.serializedSize(command.columnFilter(), version, sizes)
                 + RowFilter.serializer.serializedSize(command.rowFilter(), version)
                 + DataLimits.serializer.serializedSize(command.limits(), version)
                 + command.selectionSerializedSize(version);
        }
    }
}
