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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.monitoring.MonitorableImpl;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.UnknownIndexException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand extends MonitorableImpl implements ReadQuery
{
    private static final int TEST_ITERATION_DELAY_MILLIS = Integer.valueOf(System.getProperty("cassandra.test.read_iteration_delay_ms", "0"));
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);
    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();
    // For RANGE_SLICE verb: will either dispatch on 'serializer' for 3.0 or 'legacyRangeSliceCommandSerializer' for earlier version.
    // Can be removed (and replaced by 'serializer') once we drop pre-3.0 backward compatibility.
    public static final IVersionedSerializer<ReadCommand> rangeSliceSerializer = new RangeSliceSerializer();

    public static final IVersionedSerializer<ReadCommand> legacyRangeSliceCommandSerializer = new LegacyRangeSliceCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyPagedRangeCommandSerializer = new LegacyPagedRangeCommandSerializer();
    public static final IVersionedSerializer<ReadCommand> legacyReadCommandSerializer = new LegacyReadCommandSerializer();

    private final Kind kind;
    private final CFMetaData metadata;
    private final int nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    // SecondaryIndexManager will attempt to provide the most selective of any available indexes
    // during execution. Here we also store an the results of that lookup to repeating it over
    // the lifetime of the command.
    protected Optional<IndexMetadata> index = Optional.empty();

    // Flag to indicate whether the index manager has been queried to select an index for this
    // command. This is necessary as the result of that lookup may be null, in which case we
    // still don't want to repeat it.
    private boolean indexManagerQueried = false;

    private boolean isDigestQuery;
    // if a digest query, the version for which the digest is expected. Ignored if not a digest.
    private int digestVersion;
    private final boolean isForThrift;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInputPlus in, int version, boolean isDigest, int digestVersion, boolean isForThrift, CFMetaData metadata, int nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits, Optional<IndexMetadata> index) throws IOException;
    }

    protected enum Kind
    {
        SINGLE_PARTITION (SinglePartitionReadCommand.selectionDeserializer),
        PARTITION_RANGE  (PartitionRangeReadCommand.selectionDeserializer);

        private final SelectionDeserializer selectionDeserializer;

        Kind(SelectionDeserializer selectionDeserializer)
        {
            this.selectionDeserializer = selectionDeserializer;
        }
    }

    protected ReadCommand(Kind kind,
                          boolean isDigestQuery,
                          int digestVersion,
                          boolean isForThrift,
                          CFMetaData metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits)
    {
        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.digestVersion = digestVersion;
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
     * If the query is a digest one, the requested digest version.
     *
     * @return the requested digest version if the query is a digest. Otherwise, this can return
     * anything.
     */
    public int digestVersion()
    {
        return digestVersion;
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
     * Sets the digest version, for when digest for that command is requested.
     * <p>
     * Note that we allow setting this independently of setting the command as a digest query as
     * this allows us to use the command as a carrier of the digest version even if we only call
     * setIsDigestQuery on some copy of it.
     *
     * @param digestVersion the version for the digest is this command is used for digest query..
     * @return this read command.
     */
    public ReadCommand setDigestVersion(int digestVersion)
    {
        this.digestVersion = digestVersion;
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

    protected abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController);

    protected abstract int oldestUnrepairedTombstone();

    public ReadResponse createResponse(UnfilteredPartitionIterator iterator)
    {
        return isDigestQuery()
             ? ReadResponse.createDigestResponse(iterator, this)
             : ReadResponse.createDataResponse(iterator, this);
    }

    public long indexSerializedSize(int version)
    {
        if (index.isPresent())
            return IndexMetadata.serializer.serializedSize(index.get(), version);
        else
            return 0;
    }

    public Index getIndex(ColumnFamilyStore cfs)
    {
        // if we've already consulted the index manager, and it returned a valid index
        // the result should be cached here.
        if(index.isPresent())
            return cfs.indexManager.getIndex(index.get());

        // if no cached index is present, but we've already consulted the index manager
        // then no registered index is suitable for this command, so just return null.
        if (indexManagerQueried)
            return null;

        // do the lookup, set the flag to indicate so and cache the result if not null
        Index selected = cfs.indexManager.getBestIndexFor(this);
        indexManagerQueried = true;

        if (selected == null)
            return null;

        index = Optional.of(selected.getIndexMetadata());
        return selected;
    }

    /**
     * If the index manager for the CFS determines that there's an applicable
     * 2i that can be used to execute this command, call its (optional)
     * validation method to check that nothing in this command's parameters
     * violates the implementation specific validation rules.
     */
    public void maybeValidateIndex()
    {
        Index index = getIndex(Keyspace.openAndGetStore(metadata));
        if (null != index)
            index.validate(this);
    }

    /**
     * Executes this command on the local host.
     *
     * @param executionController the execution controller spanning this command
     *
     * @return an iterator over the result of executing this command locally.
     */
    @SuppressWarnings("resource") // The result iterator is closed upon exceptions (we know it's fine to potentially not close the intermediary
                                  // iterators created inside the try as long as we do close the original resultIterator), or by closing the result.
    public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
    {
        long startTimeNanos = System.nanoTime();

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata());
        Index index = getIndex(cfs);

        Index.Searcher searcher = null;
        if (index != null)
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                throw new IndexNotAvailableException(index);

            searcher = index.searcherFor(this);
            Tracing.trace("Executing read on {}.{} using index {}", cfs.metadata.ksName, cfs.metadata.cfName, index.getIndexMetadata().name);
        }

        UnfilteredPartitionIterator resultIterator = searcher == null
                                         ? queryStorage(cfs, executionController)
                                         : searcher.search(executionController);

        try
        {
            resultIterator = withStateTracking(resultIterator);
            resultIterator = withMetricsRecording(withoutPurgeableTombstones(resultIterator, cfs), cfs.metric, startTimeNanos);

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            RowFilter updatedFilter = searcher == null
                                    ? rowFilter()
                                    : index.getPostIndexQueryFilter(rowFilter());

            // TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
            // we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
            // would be more efficient (the sooner we discard stuff we know we don't care, the less useless
            // processing we do on it).
            return limits().filter(updatedFilter.filter(resultIterator, nowInSec()), nowInSec());
        }
        catch (RuntimeException | Error e)
        {
            resultIterator.close();
            throw e;
        }
    }

    protected abstract void recordLatency(TableMetrics metric, long latencyNanos);

    public PartitionIterator executeInternal(ReadExecutionController controller)
    {
        return UnfilteredPartitionIterators.filter(executeLocally(controller), nowInSec());
    }

    public ReadExecutionController executionController()
    {
        return ReadExecutionController.forCommand(this);
    }

    /**
     * Wraps the provided iterator so that metrics on what is scanned by the command are recorded.
     * This also log warning/trow TombstoneOverwhelmingException if appropriate.
     */
    private UnfilteredPartitionIterator withMetricsRecording(UnfilteredPartitionIterator iter, final TableMetrics metric, final long startTimeNanos)
    {
        class MetricRecording extends Transformation<UnfilteredRowIterator>
        {
            private final int failureThreshold = DatabaseDescriptor.getTombstoneFailureThreshold();
            private final int warningThreshold = DatabaseDescriptor.getTombstoneWarnThreshold();

            private final boolean respectTombstoneThresholds = !Schema.isSystemKeyspace(ReadCommand.this.metadata().ksName);

            private int liveRows = 0;
            private int tombstones = 0;

            private DecoratedKey currentKey;

            @Override
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator iter)
            {
                currentKey = iter.partitionKey();
                return Transformation.apply(iter, this);
            }

            @Override
            public Row applyToStatic(Row row)
            {
                return applyToRow(row);
            }

            @Override
            public Row applyToRow(Row row)
            {
                if (row.hasLiveData(ReadCommand.this.nowInSec()))
                    ++liveRows;

                for (Cell cell : row.cells())
                {
                    if (!cell.isLive(ReadCommand.this.nowInSec()))
                        countTombstone(row.clustering());
                }
                return row;
            }

            @Override
            public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                countTombstone(marker.clustering());
                return marker;
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

            @Override
            public void onClose()
            {
                recordLatency(metric, System.nanoTime() - startTimeNanos);

                metric.tombstoneScannedHistogram.update(tombstones);
                metric.liveScannedHistogram.update(liveRows);

                boolean warnTombstones = tombstones > warningThreshold && respectTombstoneThresholds;
                if (warnTombstones)
                {
                    String msg = String.format("Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)", liveRows, tombstones, ReadCommand.this.toCQLString());
                    ClientWarn.instance.warn(msg);
                    logger.warn(msg);
                }

                Tracing.trace("Read {} live and {} tombstone cells{}", liveRows, tombstones, (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
            }
        };

        return Transformation.apply(iter, new MetricRecording());
    }

    protected class CheckForAbort extends StoppingTransformation<BaseRowIterator<?>>
    {
        protected BaseRowIterator<?> applyToPartition(BaseRowIterator partition)
        {
            if (maybeAbort())
            {
                partition.close();
                return null;
            }

            return partition;
        }

        protected Row applyToRow(Row row)
        {
            return maybeAbort() ? null : row;
        }

        private boolean maybeAbort()
        {
            if (TEST_ITERATION_DELAY_MILLIS > 0)
                maybeDelayForTesting();

            if (isAborted())
            {
                stop();
                return true;
            }

            return false;
        }
    }

    protected UnfilteredPartitionIterator withStateTracking(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new CheckForAbort());
    }

    protected UnfilteredRowIterator withStateTracking(UnfilteredRowIterator iter)
    {
        return Transformation.apply(iter, new CheckForAbort());
    }

    private void maybeDelayForTesting()
    {
        if (!metadata.ksName.startsWith("system"))
            FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);
    }

    /**
     * Creates a message for this command.
     */
    public abstract MessageOut<ReadCommand> createMessage(int version);

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        final boolean isForThrift = iterator.isForThrift();
        class WithoutPurgeableTombstones extends PurgeFunction
        {
            public WithoutPurgeableTombstones()
            {
                super(isForThrift, nowInSec(), cfs.gcBefore(nowInSec()), oldestUnrepairedTombstone(), cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones());
            }

            protected long getMaxPurgeableTimestamp()
            {
                return Long.MAX_VALUE;
            }
        }
        return Transformation.apply(iterator, new WithoutPurgeableTombstones());
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
        sb.append(" FROM ").append(metadata().ksName).append('.').append(metadata.cfName);
        appendCQLWhereClause(sb);

        if (limits() != DataLimits.NONE)
            sb.append(' ').append(limits());
        return sb.toString();
    }

    // Monitorable interface
    public String name()
    {
        return toCQLString();
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

        private static int indexFlag(boolean hasIndex)
        {
            return hasIndex ? 0x04 : 0;
        }

        private static boolean hasIndex(int flags)
        {
            return (flags & 0x04) != 0;
        }

        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            // for serialization, createLegacyMessage() should cause legacyReadCommandSerializer to be used directly
            assert version >= MessagingService.VERSION_30;

            out.writeByte(command.kind.ordinal());
            out.writeByte(digestFlag(command.isDigestQuery()) | thriftFlag(command.isForThrift()) | indexFlag(command.index.isPresent()));
            if (command.isDigestQuery())
                out.writeUnsignedVInt(command.digestVersion());
            CFMetaData.serializer.serialize(command.metadata(), out, version);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(), out, version);
            RowFilter.serializer.serialize(command.rowFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version);
            if (command.index.isPresent())
                IndexMetadata.serializer.serialize(command.index.get(), out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                return legacyReadCommandSerializer.deserialize(in, version);

            Kind kind = Kind.values()[in.readByte()];
            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            boolean isForThrift = isForThrift(flags);
            boolean hasIndex = hasIndex(flags);
            int digestVersion = isDigest ? (int)in.readUnsignedVInt() : 0;
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);
            RowFilter rowFilter = RowFilter.serializer.deserialize(in, version, metadata);
            DataLimits limits = DataLimits.serializer.deserialize(in, version);
            Optional<IndexMetadata> index = hasIndex
                                            ? deserializeIndexMetadata(in, version, metadata)
                                            : Optional.empty();

            return kind.selectionDeserializer.deserialize(in, version, isDigest, digestVersion, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        }

        private Optional<IndexMetadata> deserializeIndexMetadata(DataInputPlus in, int version, CFMetaData cfm) throws IOException
        {
            try
            {
                return Optional.of(IndexMetadata.serializer.deserialize(in, version, cfm));
            }
            catch (UnknownIndexException e)
            {
                String message = String.format("Couldn't find a defined index on %s.%s with the id %s. " +
                                               "If an index was just created, this is likely due to the schema not " +
                                               "being fully propagated. Local read will proceed without using the " +
                                               "index. Please wait for schema agreement after index creation.",
                                               cfm.ksName, cfm.cfName, e.indexId.toString());
                logger.info(message);
                return Optional.empty();
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            // for serialization, createLegacyMessage() should cause legacyReadCommandSerializer to be used directly
            assert version >= MessagingService.VERSION_30;

            return 2 // kind + flags
                 + (command.isDigestQuery() ? TypeSizes.sizeofUnsignedVInt(command.digestVersion()) : 0)
                 + CFMetaData.serializer.serializedSize(command.metadata(), version)
                 + TypeSizes.sizeof(command.nowInSec())
                 + ColumnFilter.serializer.serializedSize(command.columnFilter(), version)
                 + RowFilter.serializer.serializedSize(command.rowFilter(), version)
                 + DataLimits.serializer.serializedSize(command.limits(), version)
                 + command.selectionSerializedSize(version)
                 + command.indexSerializedSize(version);
        }
    }

    // Dispatch to either Serializer or LegacyRangeSliceCommandSerializer. Only useful as long as we maintain pre-3.0
    // compatibility
    private static class RangeSliceSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                legacyRangeSliceCommandSerializer.serialize(command, out, version);
            else
                serializer.serialize(command, out, version);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            return version < MessagingService.VERSION_30
                 ? legacyRangeSliceCommandSerializer.deserialize(in, version)
                 : serializer.deserialize(in, version);
        }

        public long serializedSize(ReadCommand command, int version)
        {
            return version < MessagingService.VERSION_30
                 ? legacyRangeSliceCommandSerializer.serializedSize(command, version)
                 : serializer.serializedSize(command, version);
        }
    }

    private enum LegacyType
    {
        GET_BY_NAMES((byte)1),
        GET_SLICES((byte)2);

        public final byte serializedValue;

        LegacyType(byte b)
        {
            this.serializedValue = b;
        }

        public static LegacyType fromPartitionFilterKind(ClusteringIndexFilter.Kind kind)
        {
            return kind == ClusteringIndexFilter.Kind.SLICE
                   ? GET_SLICES
                   : GET_BY_NAMES;
        }

        public static LegacyType fromSerializedValue(byte b)
        {
            return b == 1 ? GET_BY_NAMES : GET_SLICES;
        }
    }

    /**
     * Serializer for pre-3.0 RangeSliceCommands.
     */
    private static class LegacyRangeSliceCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            assert !rangeCommand.dataRange().isPaging();

            // convert pre-3.0 incompatible names filters to slice filters
            rangeCommand = maybeConvertNamesToSlice(rangeCommand);

            CFMetaData metadata = rangeCommand.metadata();

            out.writeUTF(metadata.ksName);
            out.writeUTF(metadata.cfName);
            out.writeLong(rangeCommand.nowInSec() * 1000L);  // convert from seconds to millis

            // begin DiskAtomFilterSerializer.serialize()
            if (rangeCommand.isNamesQuery())
            {
                out.writeByte(1);  // 0 for slices, 1 for names
                ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter;
                LegacyReadCommandSerializer.serializeNamesFilter(rangeCommand, filter, out);
            }
            else
            {
                out.writeByte(0);  // 0 for slices, 1 for names

                // slice filter serialization
                ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

                boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                LegacyReadCommandSerializer.serializeSlices(out, filter.requestedSlices(), filter.isReversed(), makeStaticSlice, metadata);

                out.writeBoolean(filter.isReversed());

                // limit
                DataLimits limits = rangeCommand.limits();
                if (limits.isDistinct())
                    out.writeInt(1);
                else
                    out.writeInt(LegacyReadCommandSerializer.updateLimitForQuery(rangeCommand.limits().count(), filter.requestedSlices()));

                int compositesToGroup;
                boolean selectsStatics = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() || filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                if (limits.kind() == DataLimits.Kind.THRIFT_LIMIT)
                    compositesToGroup = -1;
                else if (limits.isDistinct() && !selectsStatics)
                    compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
                else
                    compositesToGroup = metadata.isDense() ? -1 : metadata.clusteringColumns().size();

                out.writeInt(compositesToGroup);
            }

            serializeRowFilter(out, rangeCommand.rowFilter());
            AbstractBounds.rowPositionSerializer.serialize(rangeCommand.dataRange().keyRange(), out, version);

            // maxResults
            out.writeInt(rangeCommand.limits().count());

            // countCQL3Rows
            if (rangeCommand.isForThrift() || rangeCommand.limits().perPartitionCount() == 1)  // if for Thrift or DISTINCT
                out.writeBoolean(false);
            else
                out.writeBoolean(true);

            // isPaging
            out.writeBoolean(false);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            if (metadata == null)
            {
                String message = String.format("Got legacy range command for nonexistent table %s.%s.", keyspace, columnFamily);
                throw new UnknownColumnFamilyException(message, null);
            }

            int nowInSec = (int) (in.readLong() / 1000);  // convert from millis to seconds

            ClusteringIndexFilter filter;
            ColumnFilter selection;
            int compositesToGroup = 0;
            int perPartitionLimit = -1;
            byte readType = in.readByte();  // 0 for slices, 1 for names
            if (readType == 1)
            {
                Pair<ColumnFilter, ClusteringIndexNamesFilter> selectionAndFilter = LegacyReadCommandSerializer.deserializeNamesSelectionAndFilter(in, metadata);
                selection = selectionAndFilter.left;
                filter = selectionAndFilter.right;
            }
            else
            {
                Pair<ClusteringIndexSliceFilter, Boolean> p = LegacyReadCommandSerializer.deserializeSlicePartitionFilter(in, metadata);
                filter = p.left;
                perPartitionLimit = in.readInt();
                compositesToGroup = in.readInt();
                selection = getColumnSelectionForSlice(p.right, compositesToGroup, metadata);
            }

            RowFilter rowFilter = deserializeRowFilter(in, metadata);

            AbstractBounds<PartitionPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, metadata.partitioner, version);
            int maxResults = in.readInt();

            boolean countCQL3Rows = in.readBoolean();  // countCQL3Rows (not needed)
            in.readBoolean();  // isPaging (not needed)

            boolean selectsStatics = (!selection.fetchedColumns().statics.isEmpty() || filter.selects(Clustering.STATIC_CLUSTERING));
            // We have 2 types of DISTINCT queries: ones on only the partition key, and ones on the partition key and static columns. For the former,
            // we can easily detect the case because compositeToGroup is -2 and that's the only case it can be that. The latter one is slightly less
            // direct, but we know that on 2.1/2.2 queries, DISTINCT queries are the only CQL queries that have countCQL3Rows to false so we use
            // that fact.
            boolean isDistinct = compositesToGroup == -2 || (compositesToGroup != -1 && !countCQL3Rows);
            DataLimits limits;
            if (isDistinct)
                limits = DataLimits.distinctLimits(maxResults);
            else if (compositesToGroup == -1)
                limits = DataLimits.thriftLimits(maxResults, perPartitionLimit);
            else
                limits = DataLimits.cqlLimits(maxResults);

            return new PartitionRangeReadCommand(false, 0, true, metadata, nowInSec, selection, rowFilter, limits, new DataRange(keyRange, filter), Optional.empty());
        }

        static void serializeRowFilter(DataOutputPlus out, RowFilter rowFilter) throws IOException
        {
            ArrayList<RowFilter.Expression> indexExpressions = Lists.newArrayList(rowFilter.iterator());
            out.writeInt(indexExpressions.size());
            for (RowFilter.Expression expression : indexExpressions)
            {
                ByteBufferUtil.writeWithShortLength(expression.column().name.bytes, out);
                expression.operator().writeTo(out);
                ByteBufferUtil.writeWithShortLength(expression.getIndexValue(), out);
            }
        }

        static RowFilter deserializeRowFilter(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int numRowFilters = in.readInt();
            if (numRowFilters == 0)
                return RowFilter.NONE;

            RowFilter rowFilter = RowFilter.create(numRowFilters);
            for (int i = 0; i < numRowFilters; i++)
            {
                ByteBuffer columnName = ByteBufferUtil.readWithShortLength(in);
                ColumnDefinition column = metadata.getColumnDefinition(columnName);
                Operator op = Operator.readFrom(in);
                ByteBuffer indexValue = ByteBufferUtil.readWithShortLength(in);
                rowFilter.add(column, op, indexValue);
            }
            return rowFilter;
        }

        static long serializedRowFilterSize(RowFilter rowFilter)
        {
            long size = TypeSizes.sizeof(0);  // rowFilterCount
            for (RowFilter.Expression expression : rowFilter)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes);
                size += TypeSizes.sizeof(0);  // operator int value
                size += ByteBufferUtil.serializedSizeWithShortLength(expression.getIndexValue());
            }
            return size;
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.PARTITION_RANGE;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            rangeCommand = maybeConvertNamesToSlice(rangeCommand);
            CFMetaData metadata = rangeCommand.metadata();

            long size = TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof(metadata.cfName);
            size += TypeSizes.sizeof((long) rangeCommand.nowInSec());

            size += 1;  // single byte flag: 0 for slices, 1 for names
            if (rangeCommand.isNamesQuery())
            {
                PartitionColumns columns = rangeCommand.columnFilter().fetchedColumns();
                ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter;
                size += LegacyReadCommandSerializer.serializedNamesFilterSize(filter, metadata, columns);
            }
            else
            {
                ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;
                boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
                size += LegacyReadCommandSerializer.serializedSlicesSize(filter.requestedSlices(), makeStaticSlice, metadata);
                size += TypeSizes.sizeof(filter.isReversed());
                size += TypeSizes.sizeof(rangeCommand.limits().perPartitionCount());
                size += TypeSizes.sizeof(0); // compositesToGroup
            }

            if (rangeCommand.rowFilter().equals(RowFilter.NONE))
            {
                size += TypeSizes.sizeof(0);
            }
            else
            {
                ArrayList<RowFilter.Expression> indexExpressions = Lists.newArrayList(rangeCommand.rowFilter().iterator());
                size += TypeSizes.sizeof(indexExpressions.size());
                for (RowFilter.Expression expression : indexExpressions)
                {
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.column().name.bytes);
                    size += TypeSizes.sizeof(expression.operator().ordinal());
                    size += ByteBufferUtil.serializedSizeWithShortLength(expression.getIndexValue());
                }
            }

            size += AbstractBounds.rowPositionSerializer.serializedSize(rangeCommand.dataRange().keyRange(), version);
            size += TypeSizes.sizeof(rangeCommand.limits().count());
            size += TypeSizes.sizeof(!rangeCommand.isForThrift());
            return size + TypeSizes.sizeof(rangeCommand.dataRange().isPaging());
        }

        static PartitionRangeReadCommand maybeConvertNamesToSlice(PartitionRangeReadCommand command)
        {
            if (!command.dataRange().isNamesQuery())
                return command;

            CFMetaData metadata = command.metadata();
            if (!LegacyReadCommandSerializer.shouldConvertNamesToSlice(metadata, command.columnFilter().fetchedColumns()))
                return command;

            ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter) command.dataRange().clusteringIndexFilter;
            ClusteringIndexSliceFilter sliceFilter = LegacyReadCommandSerializer.convertNamesFilterToSliceFilter(filter, metadata);
            DataRange newRange = new DataRange(command.dataRange().keyRange(), sliceFilter);
            return new PartitionRangeReadCommand(
                    command.isDigestQuery(), command.digestVersion(), command.isForThrift(), metadata, command.nowInSec(),
                    command.columnFilter(), command.rowFilter(), command.limits(), newRange, Optional.empty());
        }

        static ColumnFilter getColumnSelectionForSlice(boolean selectsStatics, int compositesToGroup, CFMetaData metadata)
        {
            // A value of -2 indicates this is a DISTINCT query that doesn't select static columns, only partition keys.
            // In that case, we'll basically be querying the first row of the partition, but we must make sure we include
            // all columns so we get at least one cell if there is a live row as it would confuse pre-3.0 nodes otherwise.
            if (compositesToGroup == -2)
                return ColumnFilter.all(metadata);

            // if a slice query from a pre-3.0 node doesn't cover statics, we shouldn't select them at all
            PartitionColumns columns = selectsStatics
                                     ? metadata.partitionColumns()
                                     : metadata.partitionColumns().withoutStatics();
            return ColumnFilter.selectionBuilder().addAll(columns).build();
        }
    }

    /**
     * Serializer for pre-3.0 PagedRangeCommands.
     */
    private static class LegacyPagedRangeCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            assert rangeCommand.dataRange().isPaging();

            CFMetaData metadata = rangeCommand.metadata();

            out.writeUTF(metadata.ksName);
            out.writeUTF(metadata.cfName);
            out.writeLong(rangeCommand.nowInSec() * 1000L);  // convert from seconds to millis

            AbstractBounds.rowPositionSerializer.serialize(rangeCommand.dataRange().keyRange(), out, version);

            // pre-3.0 nodes don't accept names filters for paged range commands
            ClusteringIndexSliceFilter filter;
            if (rangeCommand.dataRange().clusteringIndexFilter.kind() == ClusteringIndexFilter.Kind.NAMES)
                filter = LegacyReadCommandSerializer.convertNamesFilterToSliceFilter((ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter, metadata);
            else
                filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

            // slice filter
            boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            LegacyReadCommandSerializer.serializeSlices(out, filter.requestedSlices(), filter.isReversed(), makeStaticSlice, metadata);
            out.writeBoolean(filter.isReversed());

            // slice filter's count
            DataLimits.Kind kind = rangeCommand.limits().kind();
            boolean isDistinct = (kind == DataLimits.Kind.CQL_LIMIT || kind == DataLimits.Kind.CQL_PAGING_LIMIT) && rangeCommand.limits().perPartitionCount() == 1;
            if (isDistinct)
                out.writeInt(1);
            else
                out.writeInt(LegacyReadCommandSerializer.updateLimitForQuery(rangeCommand.limits().perPartitionCount(), filter.requestedSlices()));

            // compositesToGroup
            boolean selectsStatics = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() || filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            int compositesToGroup;
            if (kind == DataLimits.Kind.THRIFT_LIMIT)
                compositesToGroup = -1;
            else if (isDistinct && !selectsStatics)
                compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
            else
                compositesToGroup = metadata.isDense() ? -1 : metadata.clusteringColumns().size();

            out.writeInt(compositesToGroup);

            // command-level "start" and "stop" composites.  The start is the last-returned cell name if there is one,
            // otherwise it's the same as the slice filter's start.  The stop appears to always be the same as the
            // slice filter's stop.
            DataRange.Paging pagingRange = (DataRange.Paging) rangeCommand.dataRange();
            Clustering lastReturned = pagingRange.getLastReturned();
            Slice.Bound newStart = Slice.Bound.exclusiveStartOf(lastReturned);
            Slice lastSlice = filter.requestedSlices().get(filter.requestedSlices().size() - 1);
            ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeBound(metadata, newStart, true), out);
            ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeClustering(metadata, lastSlice.end().clustering()), out);

            LegacyRangeSliceCommandSerializer.serializeRowFilter(out, rangeCommand.rowFilter());

            // command-level limit
            // Pre-3.0 we would always request one more row than we actually needed and the command-level "start" would
            // be the last-returned cell name, so the response would always include it.  When dealing with compound comparators,
            // we can pass an exclusive start and use the normal limit.  However, when dealing with non-compound comparators,
            // pre-3.0 nodes cannot perform exclusive slices, so we need to request one extra row.
            int maxResults = rangeCommand.limits().count() + (metadata.isCompound() ? 0 : 1);
            out.writeInt(maxResults);

            // countCQL3Rows
            if (rangeCommand.isForThrift() || rangeCommand.limits().perPartitionCount() == 1)  // for Thrift or DISTINCT
                out.writeBoolean(false);
            else
                out.writeBoolean(true);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;

            String keyspace = in.readUTF();
            String columnFamily = in.readUTF();

            CFMetaData metadata = Schema.instance.getCFMetaData(keyspace, columnFamily);
            if (metadata == null)
            {
                String message = String.format("Got legacy paged range command for nonexistent table %s.%s.", keyspace, columnFamily);
                throw new UnknownColumnFamilyException(message, null);
            }

            int nowInSec = (int) (in.readLong() / 1000);  // convert from millis to seconds
            AbstractBounds<PartitionPosition> keyRange = AbstractBounds.rowPositionSerializer.deserialize(in, metadata.partitioner, version);

            Pair<ClusteringIndexSliceFilter, Boolean> p = LegacyReadCommandSerializer.deserializeSlicePartitionFilter(in, metadata);
            ClusteringIndexSliceFilter filter = p.left;
            boolean selectsStatics = p.right;

            int perPartitionLimit = in.readInt();
            int compositesToGroup = in.readInt();

            // command-level Composite "start" and "stop"
            LegacyLayout.LegacyBound startBound = LegacyLayout.decodeBound(metadata, ByteBufferUtil.readWithShortLength(in), true);

            ByteBufferUtil.readWithShortLength(in);  // the composite "stop", which isn't actually needed

            ColumnFilter selection = LegacyRangeSliceCommandSerializer.getColumnSelectionForSlice(selectsStatics, compositesToGroup, metadata);

            RowFilter rowFilter = LegacyRangeSliceCommandSerializer.deserializeRowFilter(in, metadata);
            int maxResults = in.readInt();
            boolean countCQL3Rows = in.readBoolean();

            // We have 2 types of DISTINCT queries: ones on only the partition key, and ones on the partition key and static columns. For the former,
            // we can easily detect the case because compositeToGroup is -2 and that's the only case it can be that. The latter one is slightly less
            // direct, but we know that on 2.1/2.2 queries, DISTINCT queries are the only CQL queries that have countCQL3Rows to false so we use
            // that fact.
            boolean isDistinct = compositesToGroup == -2 || (compositesToGroup != -1 && !countCQL3Rows);
            DataLimits limits;
            if (isDistinct)
                limits = DataLimits.distinctLimits(maxResults);
            else
                limits = DataLimits.cqlLimits(maxResults);

            limits = limits.forPaging(maxResults);

            // The pagedRangeCommand is used in pre-3.0 for both the first page and the following ones. On the first page, the startBound will be
            // the start of the overall slice and will not be a proper Clustering. So detect that case and just return a non-paging DataRange, which
            // is what 3.0 does.
            DataRange dataRange = new DataRange(keyRange, filter);
            Slices slices = filter.requestedSlices();
            if (!isDistinct && startBound != LegacyLayout.LegacyBound.BOTTOM && !startBound.bound.equals(slices.get(0).start()))
            {
                // pre-3.0 nodes normally expect pages to include the last cell from the previous page, but they handle it
                // missing without any problems, so we can safely always set "inclusive" to false in the data range
                dataRange = dataRange.forPaging(keyRange, metadata.comparator, startBound.getAsClustering(metadata), false);
            }
            return new PartitionRangeReadCommand(false, 0, true, metadata, nowInSec, selection, rowFilter, limits, dataRange, Optional.empty());
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.PARTITION_RANGE;

            PartitionRangeReadCommand rangeCommand = (PartitionRangeReadCommand) command;
            CFMetaData metadata = rangeCommand.metadata();
            assert rangeCommand.dataRange().isPaging();

            long size = TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof(metadata.cfName);
            size += TypeSizes.sizeof((long) rangeCommand.nowInSec());

            size += AbstractBounds.rowPositionSerializer.serializedSize(rangeCommand.dataRange().keyRange(), version);

            // pre-3.0 nodes only accept slice filters for paged range commands
            ClusteringIndexSliceFilter filter;
            if (rangeCommand.dataRange().clusteringIndexFilter.kind() == ClusteringIndexFilter.Kind.NAMES)
                filter = LegacyReadCommandSerializer.convertNamesFilterToSliceFilter((ClusteringIndexNamesFilter) rangeCommand.dataRange().clusteringIndexFilter, metadata);
            else
                filter = (ClusteringIndexSliceFilter) rangeCommand.dataRange().clusteringIndexFilter;

            // slice filter
            boolean makeStaticSlice = !rangeCommand.columnFilter().fetchedColumns().statics.isEmpty() && !filter.requestedSlices().selects(Clustering.STATIC_CLUSTERING);
            size += LegacyReadCommandSerializer.serializedSlicesSize(filter.requestedSlices(), makeStaticSlice, metadata);
            size += TypeSizes.sizeof(filter.isReversed());

            // slice filter's count
            size += TypeSizes.sizeof(rangeCommand.limits().perPartitionCount());

            // compositesToGroup
            size += TypeSizes.sizeof(0);

            // command-level Composite "start" and "stop"
            DataRange.Paging pagingRange = (DataRange.Paging) rangeCommand.dataRange();
            Clustering lastReturned = pagingRange.getLastReturned();
            Slice lastSlice = filter.requestedSlices().get(filter.requestedSlices().size() - 1);
            size += ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeClustering(metadata, lastReturned));
            size += ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeClustering(metadata, lastSlice.end().clustering()));

            size += LegacyRangeSliceCommandSerializer.serializedRowFilterSize(rangeCommand.rowFilter());

            // command-level limit
            size += TypeSizes.sizeof(rangeCommand.limits().count());

            // countCQL3Rows
            return size + TypeSizes.sizeof(true);
        }
    }

    /**
     * Serializer for pre-3.0 ReadCommands.
     */
    static class LegacyReadCommandSerializer implements IVersionedSerializer<ReadCommand>
    {
        public void serialize(ReadCommand command, DataOutputPlus out, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.SINGLE_PARTITION;

            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;
            singleReadCommand = maybeConvertNamesToSlice(singleReadCommand);

            CFMetaData metadata = singleReadCommand.metadata();

            out.writeByte(LegacyType.fromPartitionFilterKind(singleReadCommand.clusteringIndexFilter().kind()).serializedValue);

            out.writeBoolean(singleReadCommand.isDigestQuery());
            out.writeUTF(metadata.ksName);
            ByteBufferUtil.writeWithShortLength(singleReadCommand.partitionKey().getKey(), out);
            out.writeUTF(metadata.cfName);
            out.writeLong(singleReadCommand.nowInSec() * 1000L);  // convert from seconds to millis

            if (singleReadCommand.clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.SLICE)
                serializeSliceCommand(singleReadCommand, out);
            else
                serializeNamesCommand(singleReadCommand, out);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version < MessagingService.VERSION_30;
            LegacyType msgType = LegacyType.fromSerializedValue(in.readByte());

            boolean isDigest = in.readBoolean();
            String keyspaceName = in.readUTF();
            ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
            String cfName = in.readUTF();
            long nowInMillis = in.readLong();
            int nowInSeconds = (int) (nowInMillis / 1000);  // convert from millis to seconds
            CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
            DecoratedKey dk = metadata.partitioner.decorateKey(key);

            switch (msgType)
            {
                case GET_BY_NAMES:
                    return deserializeNamesCommand(in, isDigest, metadata, dk, nowInSeconds, version);
                case GET_SLICES:
                    return deserializeSliceCommand(in, isDigest, metadata, dk, nowInSeconds, version);
                default:
                    throw new AssertionError();
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            assert version < MessagingService.VERSION_30;
            assert command.kind == Kind.SINGLE_PARTITION;
            SinglePartitionReadCommand singleReadCommand = (SinglePartitionReadCommand) command;
            singleReadCommand = maybeConvertNamesToSlice(singleReadCommand);

            int keySize = singleReadCommand.partitionKey().getKey().remaining();

            CFMetaData metadata = singleReadCommand.metadata();

            long size = 1;  // message type (single byte)
            size += TypeSizes.sizeof(command.isDigestQuery());
            size += TypeSizes.sizeof(metadata.ksName);
            size += TypeSizes.sizeof((short) keySize) + keySize;
            size += TypeSizes.sizeof((long) command.nowInSec());

            if (singleReadCommand.clusteringIndexFilter().kind() == ClusteringIndexFilter.Kind.SLICE)
                return size + serializedSliceCommandSize(singleReadCommand);
            else
                return size + serializedNamesCommandSize(singleReadCommand);
        }

        private void serializeNamesCommand(SinglePartitionReadCommand command, DataOutputPlus out) throws IOException
        {
            serializeNamesFilter(command, (ClusteringIndexNamesFilter)command.clusteringIndexFilter(), out);
        }

        private static void serializeNamesFilter(ReadCommand command, ClusteringIndexNamesFilter filter, DataOutputPlus out) throws IOException
        {
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            CFMetaData metadata = command.metadata();
            SortedSet<Clustering> requestedRows = filter.requestedRows();

            if (requestedRows.isEmpty())
            {
                // only static columns are requested
                out.writeInt(columns.size());
                for (ColumnDefinition column : columns)
                    ByteBufferUtil.writeWithShortLength(column.name.bytes, out);
            }
            else
            {
                out.writeInt(requestedRows.size() * columns.size());
                for (Clustering clustering : requestedRows)
                {
                    for (ColumnDefinition column : columns)
                        ByteBufferUtil.writeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null), out);
                }
            }

            // countCql3Rows should be true if it's not for Thrift or a DISTINCT query
            if (command.isForThrift() || (command.limits().kind() == DataLimits.Kind.CQL_LIMIT && command.limits().perPartitionCount() == 1))
                out.writeBoolean(false);  // it's compact and not a DISTINCT query
            else
                out.writeBoolean(true);
        }

        static long serializedNamesFilterSize(ClusteringIndexNamesFilter filter, CFMetaData metadata, PartitionColumns fetchedColumns)
        {
            SortedSet<Clustering> requestedRows = filter.requestedRows();

            long size = 0;
            if (requestedRows.isEmpty())
            {
                // only static columns are requested
                size += TypeSizes.sizeof(fetchedColumns.size());
                for (ColumnDefinition column : fetchedColumns)
                    size += ByteBufferUtil.serializedSizeWithShortLength(column.name.bytes);
            }
            else
            {
                size += TypeSizes.sizeof(requestedRows.size() * fetchedColumns.size());
                for (Clustering clustering : requestedRows)
                {
                    for (ColumnDefinition column : fetchedColumns)
                        size += ByteBufferUtil.serializedSizeWithShortLength(LegacyLayout.encodeCellName(metadata, clustering, column.name.bytes, null));
                }
            }

            return size + TypeSizes.sizeof(true);  // countCql3Rows
        }

        private SinglePartitionReadCommand deserializeNamesCommand(DataInputPlus in, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds, int version) throws IOException
        {
            Pair<ColumnFilter, ClusteringIndexNamesFilter> selectionAndFilter = deserializeNamesSelectionAndFilter(in, metadata);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionReadCommand(
                    isDigest, version, true, metadata, nowInSeconds, selectionAndFilter.left, RowFilter.NONE, DataLimits.NONE,
                    key, selectionAndFilter.right);
        }

        static Pair<ColumnFilter, ClusteringIndexNamesFilter> deserializeNamesSelectionAndFilter(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int numCellNames = in.readInt();

            // The names filter could include either a) static columns or b) normal columns with the clustering columns
            // fully specified.  We need to handle those cases differently in 3.0.
            NavigableSet<Clustering> clusterings = new TreeSet<>(metadata.comparator);

            ColumnFilter.Builder selectionBuilder = ColumnFilter.selectionBuilder();
            for (int i = 0; i < numCellNames; i++)
            {
                ByteBuffer buffer = ByteBufferUtil.readWithShortLength(in);
                LegacyLayout.LegacyCellName cellName;
                try
                {
                    cellName = LegacyLayout.decodeCellName(metadata, buffer);
                }
                catch (UnknownColumnException exc)
                {
                    // TODO this probably needs a new exception class that shares a parent with UnknownColumnFamilyException
                    throw new UnknownColumnFamilyException(
                            "Received legacy range read command with names filter for unrecognized column name. " +
                                    "Fill name in filter (hex): " + ByteBufferUtil.bytesToHex(buffer), metadata.cfId);
                }

                // If we're querying for a static column, we may also need to read it
                // as if it were a thrift dynamic column (because the column metadata,
                // which makes it a static column in 3.0+, may have been added *after*
                // some values were written). Note that all cql queries on non-compact
                // tables used slice & not name filters prior to 3.0 so this path is
                // not taken for non-compact tables. It is theoretically possible to
                // get here via thrift, hence the check on metadata.isStaticCompactTable.
                // See CASSANDRA-11087.
                if (metadata.isStaticCompactTable() && cellName.clustering.equals(Clustering.STATIC_CLUSTERING))
                {
                    clusterings.add(Clustering.make(cellName.column.name.bytes));
                    selectionBuilder.add(metadata.compactValueColumn());
                }
                else
                {
                    clusterings.add(cellName.clustering);
                }

                selectionBuilder.add(cellName.column);
            }

            // for compact storage tables without clustering keys, the column holding the selected value is named
            // 'value' internally we add it to the selection here to prevent errors due to unexpected column names
            // when serializing the initial local data response
            if (metadata.isStaticCompactTable() && clusterings.isEmpty())
                selectionBuilder.addAll(metadata.partitionColumns());

            in.readBoolean();  // countCql3Rows

            // clusterings cannot include STATIC_CLUSTERING, so if the names filter is for static columns, clusterings
            // will be empty.  However, by requesting the static columns in our ColumnFilter, this will still work.
            ClusteringIndexNamesFilter filter = new ClusteringIndexNamesFilter(clusterings, false);
            return Pair.create(selectionBuilder.build(), filter);
        }

        private long serializedNamesCommandSize(SinglePartitionReadCommand command)
        {
            ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter)command.clusteringIndexFilter();
            PartitionColumns columns = command.columnFilter().fetchedColumns();
            return serializedNamesFilterSize(filter, command.metadata(), columns);
        }

        private void serializeSliceCommand(SinglePartitionReadCommand command, DataOutputPlus out) throws IOException
        {
            CFMetaData metadata = command.metadata();
            ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter)command.clusteringIndexFilter();

            Slices slices = filter.requestedSlices();
            boolean makeStaticSlice = !command.columnFilter().fetchedColumns().statics.isEmpty() && !slices.selects(Clustering.STATIC_CLUSTERING);
            serializeSlices(out, slices, filter.isReversed(), makeStaticSlice, metadata);

            out.writeBoolean(filter.isReversed());

            boolean selectsStatics = !command.columnFilter().fetchedColumns().statics.isEmpty() || slices.selects(Clustering.STATIC_CLUSTERING);
            DataLimits limits = command.limits();
            if (limits.isDistinct())
                out.writeInt(1);  // the limit is always 1 for DISTINCT queries
            else
                out.writeInt(updateLimitForQuery(command.limits().count(), filter.requestedSlices()));

            int compositesToGroup;
            if (limits.kind() == DataLimits.Kind.THRIFT_LIMIT || metadata.isDense())
                compositesToGroup = -1;
            else if (limits.isDistinct() && !selectsStatics)
                compositesToGroup = -2;  // for DISTINCT queries (CASSANDRA-8490)
            else
                compositesToGroup = metadata.clusteringColumns().size();

            out.writeInt(compositesToGroup);
        }

        private SinglePartitionReadCommand deserializeSliceCommand(DataInputPlus in, boolean isDigest, CFMetaData metadata, DecoratedKey key, int nowInSeconds, int version) throws IOException
        {
            Pair<ClusteringIndexSliceFilter, Boolean> p = deserializeSlicePartitionFilter(in, metadata);
            ClusteringIndexSliceFilter filter = p.left;
            boolean selectsStatics = p.right;
            int count = in.readInt();
            int compositesToGroup = in.readInt();

            // if a slice query from a pre-3.0 node doesn't cover statics, we shouldn't select them at all
            ColumnFilter columnFilter = LegacyRangeSliceCommandSerializer.getColumnSelectionForSlice(selectsStatics, compositesToGroup, metadata);

            // We have 2 types of DISTINCT queries: ones on only the partition key, and ones on the partition key and static columns. For the former,
            // we can easily detect the case because compositeToGroup is -2 and that's the only case it can be that. The latter is probablematic
            // however as we have no way to distinguish it from a normal select with a limit of 1 (and this, contrarily to the range query case
            // were the countCQL3Rows boolean allows us to decide).
            // So we consider this case not distinct here. This is ok because even if it is a distinct (with static), the count will be 1 and
            // we'll still just query one row (a distinct DataLimits currently behave exactly like a CQL limit with a count of 1). The only
            // drawback is that we'll send back the first row entirely while a 2.1/2.2 node would return only the first cell in that same
            // situation. This isn't a problem for 2.1/2.2 code however (it would be for a range query, as it would throw off the count for
            // reasons similar to CASSANDRA-10762, but it's ok for single partition queries).
            // We do _not_ want to do the reverse however and consider a 'SELECT * FROM foo LIMIT 1' as a DISTINCT query as that would make
            // us only return the 1st cell rather then 1st row.
            DataLimits limits;
            if (compositesToGroup == -2)
                limits = DataLimits.distinctLimits(count);  // See CASSANDRA-8490 for the explanation of this value
            else if (compositesToGroup == -1)
                limits = DataLimits.thriftLimits(1, count);
            else
                limits = DataLimits.cqlLimits(count);

            // messages from old nodes will expect the thrift format, so always use 'true' for isForThrift
            return new SinglePartitionReadCommand(isDigest, version, true, metadata, nowInSeconds, columnFilter, RowFilter.NONE, limits, key, filter);
        }

        private long serializedSliceCommandSize(SinglePartitionReadCommand command)
        {
            CFMetaData metadata = command.metadata();
            ClusteringIndexSliceFilter filter = (ClusteringIndexSliceFilter)command.clusteringIndexFilter();

            Slices slices = filter.requestedSlices();
            boolean makeStaticSlice = !command.columnFilter().fetchedColumns().statics.isEmpty() && !slices.selects(Clustering.STATIC_CLUSTERING);

            long size = serializedSlicesSize(slices, makeStaticSlice, metadata);
            size += TypeSizes.sizeof(command.clusteringIndexFilter().isReversed());
            size += TypeSizes.sizeof(command.limits().count());
            return size + TypeSizes.sizeof(0);  // compositesToGroup
        }

        static void serializeSlices(DataOutputPlus out, Slices slices, boolean isReversed, boolean makeStaticSlice, CFMetaData metadata) throws IOException
        {
            out.writeInt(slices.size() + (makeStaticSlice ? 1 : 0));

            // In 3.0 we always store the slices in normal comparator order.  Pre-3.0 nodes expect the slices to
            // be in reversed order if the query is reversed, so we handle that here.
            if (isReversed)
            {
                for (int i = slices.size() - 1; i >= 0; i--)
                    serializeSlice(out, slices.get(i), true, metadata);
                if (makeStaticSlice)
                    serializeStaticSlice(out, true, metadata);
            }
            else
            {
                if (makeStaticSlice)
                    serializeStaticSlice(out, false, metadata);
                for (Slice slice : slices)
                    serializeSlice(out, slice, false, metadata);
            }
        }

        static long serializedSlicesSize(Slices slices, boolean makeStaticSlice, CFMetaData metadata)
        {
            long size = TypeSizes.sizeof(slices.size());

            for (Slice slice : slices)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, slice.start(), true);
                size += ByteBufferUtil.serializedSizeWithShortLength(sliceStart);
                ByteBuffer sliceEnd = LegacyLayout.encodeBound(metadata, slice.end(), false);
                size += ByteBufferUtil.serializedSizeWithShortLength(sliceEnd);
            }

            if (makeStaticSlice)
                size += serializedStaticSliceSize(metadata);

            return size;
        }

        static long serializedStaticSliceSize(CFMetaData metadata)
        {
            // unlike serializeStaticSlice(), but we don't care about reversal for size calculations
            ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
            long size = ByteBufferUtil.serializedSizeWithShortLength(sliceStart);

            size += TypeSizes.sizeof((short) (metadata.comparator.size() * 3 + 2));
            size += TypeSizes.sizeof((short) LegacyLayout.STATIC_PREFIX);
            for (int i = 0; i < metadata.comparator.size(); i++)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(ByteBufferUtil.EMPTY_BYTE_BUFFER);
                size += 1;  // EOC
            }
            return size;
        }

        private static void serializeSlice(DataOutputPlus out, Slice slice, boolean isReversed, CFMetaData metadata) throws IOException
        {
            ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, isReversed ? slice.end() : slice.start(), !isReversed);
            ByteBufferUtil.writeWithShortLength(sliceStart, out);

            ByteBuffer sliceEnd = LegacyLayout.encodeBound(metadata, isReversed ? slice.start() : slice.end(), isReversed);
            ByteBufferUtil.writeWithShortLength(sliceEnd, out);
        }

        private static void serializeStaticSlice(DataOutputPlus out, boolean isReversed, CFMetaData metadata) throws IOException
        {
            // if reversed, write an empty bound for the slice start; if reversed, write out an empty bound for the
            // slice finish after we've written the static slice start
            if (!isReversed)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
                ByteBufferUtil.writeWithShortLength(sliceStart, out);
            }

            // write out the length of the composite
            out.writeShort(2 + metadata.comparator.size() * 3);  // two bytes + EOC for each component, plus static prefix
            out.writeShort(LegacyLayout.STATIC_PREFIX);
            for (int i = 0; i < metadata.comparator.size(); i++)
            {
                ByteBufferUtil.writeWithShortLength(ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
                // write the EOC, using an inclusive end if we're on the final component
                out.writeByte(i == metadata.comparator.size() - 1 ? 1 : 0);
            }

            if (isReversed)
            {
                ByteBuffer sliceStart = LegacyLayout.encodeBound(metadata, Slice.Bound.BOTTOM, false);
                ByteBufferUtil.writeWithShortLength(sliceStart, out);
            }
        }

        // Returns the deserialized filter, and whether static columns are queried (in pre-3.0, both info are determined by the slices,
        // but in 3.0 they are separated: whether static columns are queried or not depends on the ColumnFilter).
        static Pair<ClusteringIndexSliceFilter, Boolean> deserializeSlicePartitionFilter(DataInputPlus in, CFMetaData metadata) throws IOException
        {
            int numSlices = in.readInt();
            ByteBuffer[] startBuffers = new ByteBuffer[numSlices];
            ByteBuffer[] finishBuffers = new ByteBuffer[numSlices];
            for (int i = 0; i < numSlices; i++)
            {
                startBuffers[i] = ByteBufferUtil.readWithShortLength(in);
                finishBuffers[i] = ByteBufferUtil.readWithShortLength(in);
            }

            boolean reversed = in.readBoolean();

            if (reversed)
            {
                // pre-3.0, reversed query slices put the greater element at the start of the slice
                ByteBuffer[] tmp = finishBuffers;
                finishBuffers = startBuffers;
                startBuffers = tmp;
            }

            boolean selectsStatics = false;
            Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
            for (int i = 0; i < numSlices; i++)
            {
                LegacyLayout.LegacyBound start = LegacyLayout.decodeBound(metadata, startBuffers[i], true);
                LegacyLayout.LegacyBound finish = LegacyLayout.decodeBound(metadata, finishBuffers[i], false);

                if (start.isStatic)
                {
                    // If we start at the static block, this means we start at the beginning of the partition in 3.0
                    // terms (since 3.0 handles static outside of the slice).
                    start = LegacyLayout.LegacyBound.BOTTOM;

                    // Then if we include the static, records it
                    if (start.bound.isInclusive())
                        selectsStatics = true;
                }
                else if (start == LegacyLayout.LegacyBound.BOTTOM)
                {
                    selectsStatics = true;
                }

                // If the end of the slice is the end of the statics, then that mean this slice was just selecting static
                // columns. We have already recorded that in selectsStatics, so we can ignore the slice (which doesn't make
                // sense for 3.0).
                if (finish.isStatic)
                {
                    assert finish.bound.isInclusive(); // it would make no sense for a pre-3.0 node to have a slice that stops
                                                     // before the static columns (since there is nothing before that)
                    continue;
                }

                slicesBuilder.add(Slice.make(start.bound, finish.bound));
            }

            return Pair.create(new ClusteringIndexSliceFilter(slicesBuilder.build(), reversed), selectsStatics);
        }

        private static SinglePartitionReadCommand maybeConvertNamesToSlice(SinglePartitionReadCommand command)
        {
            if (command.clusteringIndexFilter().kind() != ClusteringIndexFilter.Kind.NAMES)
                return command;

            CFMetaData metadata = command.metadata();

            if (!shouldConvertNamesToSlice(metadata, command.columnFilter().fetchedColumns()))
                return command;

            ClusteringIndexNamesFilter filter = (ClusteringIndexNamesFilter)command.clusteringIndexFilter();
            ClusteringIndexSliceFilter sliceFilter = convertNamesFilterToSliceFilter(filter, metadata);
            return new SinglePartitionReadCommand(
                    command.isDigestQuery(), command.digestVersion(), command.isForThrift(), metadata, command.nowInSec(),
                    command.columnFilter(), command.rowFilter(), command.limits(), command.partitionKey(), sliceFilter);
        }

        /**
         * Returns true if a names filter on the given table and column selection should be converted to a slice
         * filter for compatibility with pre-3.0 nodes, false otherwise.
         */
        static boolean shouldConvertNamesToSlice(CFMetaData metadata, PartitionColumns columns)
        {
            // On pre-3.0 nodes, due to CASSANDRA-5762, we always do a slice for CQL3 tables (not dense, composite).
            if (!metadata.isDense() && metadata.isCompound())
                return true;

            // pre-3.0 nodes don't support names filters for reading collections, so if we're requesting any of those,
            // we need to convert this to a slice filter
            for (ColumnDefinition column : columns)
            {
                if (column.type.isMultiCell())
                    return true;
            }
            return false;
        }

        /**
         * Converts a names filter that is incompatible with pre-3.0 nodes to a slice filter that is compatible.
         */
        private static ClusteringIndexSliceFilter convertNamesFilterToSliceFilter(ClusteringIndexNamesFilter filter, CFMetaData metadata)
        {
            SortedSet<Clustering> requestedRows = filter.requestedRows();
            Slices slices;
            if (requestedRows.isEmpty())
            {
                slices = Slices.NONE;
            }
            else if (requestedRows.size() == 1 && requestedRows.first().size() == 0)
            {
                slices = Slices.ALL;
            }
            else
            {
                Slices.Builder slicesBuilder = new Slices.Builder(metadata.comparator);
                for (Clustering clustering : requestedRows)
                    slicesBuilder.add(Slice.Bound.inclusiveStartOf(clustering), Slice.Bound.inclusiveEndOf(clustering));
                slices = slicesBuilder.build();
            }

            return new ClusteringIndexSliceFilter(slices, filter.isReversed());
        }

        /**
         * Potentially increases the existing query limit to account for the lack of exclusive bounds in pre-3.0 nodes.
         * @param limit the existing query limit
         * @param slices the requested slices
         * @return the updated limit
         */
        static int updateLimitForQuery(int limit, Slices slices)
        {
            // Pre-3.0 nodes don't support exclusive bounds for slices. Instead, we query one more element if necessary
            // and filter it later (in LegacyRemoteDataResponse)
            if (!slices.hasLowerBound() && ! slices.hasUpperBound())
                return limit;

            for (Slice slice : slices)
            {
                if (limit == Integer.MAX_VALUE)
                    return limit;

                if (!slice.start().isInclusive())
                    limit++;
                if (!slice.end().isInclusive())
                    limit++;
            }
            return limit;
        }
    }
}
