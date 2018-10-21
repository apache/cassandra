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
import java.util.function.BiFunction;
import java.util.function.LongPredicate;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.RTBoundCloser;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.db.transform.RTBoundValidator.Stage;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HashingUtils;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.filter;

/**
 * General interface for storage-engine read commands (common to both range and
 * single partition commands).
 * <p>
 * This contains all the informations needed to do a local read.
 */
public abstract class ReadCommand extends AbstractReadQuery
{
    private static final int TEST_ITERATION_DELAY_MILLIS = Integer.parseInt(System.getProperty("cassandra.test.read_iteration_delay_ms", "0"));
    protected static final Logger logger = LoggerFactory.getLogger(ReadCommand.class);
    public static final IVersionedSerializer<ReadCommand> serializer = new Serializer();

    private final Kind kind;

    private final boolean isDigestQuery;
    private final boolean acceptsTransient;
    // if a digest query, the version for which the digest is expected. Ignored if not a digest.
    private int digestVersion;

    // for data queries, coordinators may request information on the repaired data used in constructing the response
    private boolean trackRepairedStatus = false;
    // tracker for repaired data, initialized to singelton null object
    private static final RepairedDataInfo NULL_REPAIRED_DATA_INFO = new RepairedDataInfo()
    {
        void trackPartitionKey(DecoratedKey key){}
        void trackDeletion(DeletionTime deletion){}
        void trackRangeTombstoneMarker(RangeTombstoneMarker marker){}
        void trackRow(Row row){}
        boolean isConclusive(){ return true; }
        ByteBuffer getDigest(){ return ByteBufferUtil.EMPTY_BYTE_BUFFER; }
    };

    private RepairedDataInfo repairedDataInfo = NULL_REPAIRED_DATA_INFO;

    int oldestUnrepairedTombstone = Integer.MAX_VALUE;

    @Nullable
    private final IndexMetadata index;

    protected static abstract class SelectionDeserializer
    {
        public abstract ReadCommand deserialize(DataInputPlus in,
                                                int version,
                                                boolean isDigest,
                                                int digestVersion,
                                                boolean acceptsTransient,
                                                TableMetadata metadata,
                                                int nowInSec,
                                                ColumnFilter columnFilter,
                                                RowFilter rowFilter,
                                                DataLimits limits,
                                                IndexMetadata index) throws IOException;
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
                          boolean acceptsTransient,
                          TableMetadata metadata,
                          int nowInSec,
                          ColumnFilter columnFilter,
                          RowFilter rowFilter,
                          DataLimits limits,
                          IndexMetadata index)
    {
        super(metadata, nowInSec, columnFilter, rowFilter, limits);
        if (acceptsTransient && isDigestQuery)
            throw new IllegalArgumentException("Attempted to issue a digest response to transient replica");

        this.kind = kind;
        this.isDigestQuery = isDigestQuery;
        this.digestVersion = digestVersion;
        this.acceptsTransient = acceptsTransient;
        this.index = index;
    }

    protected abstract void serializeSelection(DataOutputPlus out, int version) throws IOException;
    protected abstract long selectionSerializedSize(int version);

    public abstract boolean isLimitedToOnePartition();

    /**
     * Creates a new <code>ReadCommand</code> instance with new limits.
     *
     * @param newLimits the new limits
     * @return a new <code>ReadCommand</code> with the updated limits
     */
    public abstract ReadCommand withUpdatedLimit(DataLimits newLimits);

    /**
     * The configured timeout for this command.
     *
     * @return the configured timeout for this command.
     */
    public abstract long getTimeout();

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
     * @return Whether this query expects only a transient data response, or a full response
     */
    public boolean acceptsTransient()
    {
        return acceptsTransient;
    }

    /**
     * Activates repaired data tracking for this command.
     *
     * When active, a digest will be created from data read from repaired SSTables. The digests
     * from each replica can then be compared on the coordinator to detect any divergence in their
     * repaired datasets. In this context, an sstable is considered repaired if it is marked
     * repaired or has a pending repair session which has been committed.
     * In addition to the digest, a set of ids for any pending but as yet uncommitted repair sessions
     * is recorded and returned to the coordinator. This is to help reduce false positives caused
     * by compaction lagging which can leave sstables from committed sessions in the pending state
     * for a time.
     */
    public void trackRepairedStatus()
    {
        trackRepairedStatus = true;
    }

    /**
     * Whether or not repaired status of any data read is being tracked or not
     *
     * @return Whether repaired status tracking is active for this command
     */
    public boolean isTrackingRepairedStatus()
    {
        return trackRepairedStatus;
    }

    /**
     * Returns a digest of the repaired data read in the execution of this command.
     *
     * If either repaired status tracking is not active or the command has not yet been
     * executed, then this digest will be an empty buffer.
     * Otherwise, it will contain a digest* of the repaired data read, or empty buffer
     * if no repaired data was read.
     * @return digest of the repaired data read in the execution of the command
     */
    public ByteBuffer getRepairedDataDigest()
    {
        return repairedDataInfo.getDigest();
    }

    /**
     * Returns a boolean indicating whether any relevant sstables were skipped during the read
     * that produced the repaired data digest.
     *
     * If true, then no pending repair sessions or partition deletes have influenced the extent
     * of the repaired sstables that went into generating the digest.
     * This indicates whether or not the digest can reliably be used to infer consistency
     * issues between the repaired sets across replicas.
     *
     * If either repaired status tracking is not active or the command has not yet been
     * executed, then this will always return true.
     *
     * @return boolean to indicate confidence in the dwhether or not the digest of the repaired data can be
     * reliably be used to infer inconsistency issues between the repaired sets across
     * replicas.
     */
    public boolean isRepairedDataDigestConclusive()
    {
        return repairedDataInfo.isConclusive();
    }

    /**
     * Index (metadata) chosen for this query. Can be null.
     *
     * @return index (metadata) chosen for this query
     */
    @Nullable
    public IndexMetadata indexMetadata()
    {
        return index;
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
     * Returns a copy of this command with acceptsTransient set to true.
     */
    public ReadCommand copyAsTransientQuery(Replica replica)
    {
        Preconditions.checkArgument(replica.isTransient(),
                                    "Can't make a transient request on a full replica: " + replica);
        return copyAsTransientQuery();
    }

    /**
     * Returns a copy of this command with acceptsTransient set to true.
     */
    public ReadCommand copyAsTransientQuery(Iterable<Replica> replicas)
    {
        if (any(replicas, Replica::isFull))
            throw new IllegalArgumentException("Can't make a transient request on full replicas: " + Iterables.toString(filter(replicas, Replica::isFull)));
        return copyAsTransientQuery();
    }

    protected abstract ReadCommand copyAsTransientQuery();

    /**
     * Returns a copy of this command with isDigestQuery set to true.
     */
    public ReadCommand copyAsDigestQuery(Replica replica)
    {
        Preconditions.checkArgument(replica.isFull(),
                                    "Can't make a digest request on a transient replica " + replica);
        return copyAsDigestQuery();
    }

    /**
     * Returns a copy of this command with isDigestQuery set to true.
     */
    public ReadCommand copyAsDigestQuery(Iterable<Replica> replicas)
    {
        if (any(replicas, Replica::isTransient))
            throw new IllegalArgumentException("Can't make a digest request on a transient replica " + Iterables.toString(filter(replicas, Replica::isTransient)));

        return copyAsDigestQuery();
    }

    protected abstract ReadCommand copyAsDigestQuery();

    protected abstract UnfilteredPartitionIterator queryStorage(ColumnFamilyStore cfs, ReadExecutionController executionController);

    protected int oldestUnrepairedTombstone()
    {
        return oldestUnrepairedTombstone;
    }


    @SuppressWarnings("resource")
    public ReadResponse createResponse(UnfilteredPartitionIterator iterator)
    {
        // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
        // ends equal, and there are no dangling RT bound in any partition.
        iterator = RTBoundValidator.validate(iterator, Stage.PROCESSED, true);

        return isDigestQuery()
             ? ReadResponse.createDigestResponse(iterator, this)
             : ReadResponse.createDataResponse(iterator, this);
    }

    long indexSerializedSize(int version)
    {
        return null != index
             ? IndexMetadata.serializer.serializedSize(index, version)
             : 0;
    }

    public Index getIndex(ColumnFamilyStore cfs)
    {
        return null != index
             ? cfs.indexManager.getIndex(index)
             : null;
    }

    static IndexMetadata findIndex(TableMetadata table, RowFilter rowFilter)
    {
        if (table.indexes.isEmpty() || rowFilter.isEmpty())
            return null;

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(table);

        Index index = cfs.indexManager.getBestIndexFor(rowFilter);

        return null != index
             ? index.getIndexMetadata()
             : null;
    }

    /**
     * If the index manager for the CFS determines that there's an applicable
     * 2i that can be used to execute this command, call its (optional)
     * validation method to check that nothing in this command's parameters
     * violates the implementation specific validation rules.
     */
    public void maybeValidateIndex()
    {
        if (null != index)
            IndexRegistry.obtain(metadata()).getIndex(index).validate(this);
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
            Tracing.trace("Executing read on {}.{} using index {}", cfs.metadata.keyspace, cfs.metadata.name, index.getIndexMetadata().name);
        }

        if (isTrackingRepairedStatus())
            repairedDataInfo = new RepairedDataInfo();

        UnfilteredPartitionIterator iterator = (null == searcher) ? queryStorage(cfs, executionController) : searcher.search(executionController);
        iterator = RTBoundValidator.validate(iterator, Stage.MERGED, false);

        try
        {
            iterator = withStateTracking(iterator);
            iterator = RTBoundValidator.validate(withoutPurgeableTombstones(iterator, cfs), Stage.PURGED, false);
            iterator = withMetricsRecording(iterator, cfs.metric, startTimeNanos);

            // If we've used a 2ndary index, we know the result already satisfy the primary expression used, so
            // no point in checking it again.
            RowFilter filter = (null == searcher) ? rowFilter() : index.getPostIndexQueryFilter(rowFilter());

            /*
             * TODO: We'll currently do filtering by the rowFilter here because it's convenient. However,
             * we'll probably want to optimize by pushing it down the layer (like for dropped columns) as it
             * would be more efficient (the sooner we discard stuff we know we don't care, the less useless
             * processing we do on it).
             */
            iterator = filter.filter(iterator, nowInSec());

            // apply the limits/row counter; this transformation is stopping and would close the iterator as soon
            // as the count is observed; if that happens in the middle of an open RT, its end bound will not be included.
            iterator = limits().filter(iterator, nowInSec(), selectsFullPartition());

            // because of the above, we need to append an aritifical end bound if the source iterator was stopped short by a counter.
            return RTBoundCloser.close(iterator);
        }
        catch (RuntimeException | Error e)
        {
            iterator.close();
            throw e;
        }
    }

    protected abstract void recordLatency(TableMetrics metric, long latencyNanos);

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

            private final boolean respectTombstoneThresholds = !SchemaConstants.isLocalSystemKeyspace(ReadCommand.this.metadata().keyspace);
            private final boolean enforceStrictLiveness = metadata().enforceStrictLiveness();

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
                boolean hasTombstones = false;
                for (Cell cell : row.cells())
                {
                    if (!cell.isLive(ReadCommand.this.nowInSec()))
                    {
                        countTombstone(row.clustering());
                        hasTombstones = true; // allows to avoid counting an extra tombstone if the whole row expired
                    }
                }

                if (row.hasLiveData(ReadCommand.this.nowInSec(), enforceStrictLiveness))
                    ++liveRows;
                else if (!row.primaryKeyLivenessInfo().isLive(ReadCommand.this.nowInSec())
                        && row.hasDeletion(ReadCommand.this.nowInSec())
                        && !hasTombstones)
                {
                    // We're counting primary key deletions only here.
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
                    metric.tombstoneFailures.inc();
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
                    String msg = String.format(
                            "Read %d live rows and %d tombstone cells for query %1.512s (see tombstone_warn_threshold)",
                            liveRows, tombstones, ReadCommand.this.toCQLString());
                    ClientWarn.instance.warn(msg);
                    if (tombstones < failureThreshold)
                    {
                        metric.tombstoneWarnings.inc();
                    }

                    logger.warn(msg);
                }

                Tracing.trace("Read {} live rows and {} tombstone cells{}",
                        liveRows, tombstones,
                        (warnTombstones ? " (see tombstone_warn_threshold)" : ""));
            }
        };

        return Transformation.apply(iter, new MetricRecording());
    }

    protected class CheckForAbort extends StoppingTransformation<UnfilteredRowIterator>
    {
        long lastChecked = 0;

        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            if (maybeAbort())
            {
                partition.close();
                return null;
            }

            return Transformation.apply(partition, this);
        }

        protected Row applyToRow(Row row)
        {
            if (TEST_ITERATION_DELAY_MILLIS > 0)
                maybeDelayForTesting();

            return maybeAbort() ? null : row;
        }

        private boolean maybeAbort()
        {
            /**
             * The value returned by ApproximateTime.currentTimeMillis() is updated only every
             * {@link ApproximateTime.CHECK_INTERVAL_MS}, by default 10 millis. Since MonitorableImpl
             * relies on ApproximateTime, we don't need to check unless the approximate time has elapsed.
             */
            if (lastChecked == ApproximateTime.currentTimeMillis())
                return false;

            lastChecked = ApproximateTime.currentTimeMillis();

            if (isAborted())
            {
                stop();
                return true;
            }

            return false;
        }

        private void maybeDelayForTesting()
        {
            if (!metadata().keyspace.startsWith("system"))
                FBUtilities.sleepQuietly(TEST_ITERATION_DELAY_MILLIS);
        }
    }

    protected UnfilteredPartitionIterator withStateTracking(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new CheckForAbort());
    }

    /**
     * Creates a message for this command.
     */
    public abstract MessageOut<ReadCommand> createMessage();

    protected abstract void appendCQLWhereClause(StringBuilder sb);

    // Skip purgeable tombstones. We do this because it's safe to do (post-merge of the memtable and sstable at least), it
    // can save us some bandwith, and avoid making us throw a TombstoneOverwhelmingException for purgeable tombstones (which
    // are to some extend an artefact of compaction lagging behind and hence counting them is somewhat unintuitive).
    protected UnfilteredPartitionIterator withoutPurgeableTombstones(UnfilteredPartitionIterator iterator, ColumnFamilyStore cfs)
    {
        class WithoutPurgeableTombstones extends PurgeFunction
        {
            public WithoutPurgeableTombstones()
            {
                super(nowInSec(), cfs.gcBefore(nowInSec()), oldestUnrepairedTombstone(),
                      cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                      iterator.metadata().enforceStrictLiveness());
            }

            protected LongPredicate getPurgeEvaluator()
            {
                return time -> true;
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
        sb.append(" FROM ").append(metadata().keyspace).append('.').append(metadata().name);
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

    private static UnfilteredPartitionIterator withRepairedDataInfo(final UnfilteredPartitionIterator iterator,
                                                               final RepairedDataInfo repairedDataInfo)
    {
        class WithRepairedDataTracking extends Transformation<UnfilteredRowIterator>
        {
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return withRepairedDataInfo(partition, repairedDataInfo);
            }
        }

        return Transformation.apply(iterator, new WithRepairedDataTracking());
    }

    private static UnfilteredRowIterator withRepairedDataInfo(final UnfilteredRowIterator iterator,
                                                              final RepairedDataInfo repairedDataInfo)
    {
        class WithTracking extends Transformation
        {
            protected DecoratedKey applyToPartitionKey(DecoratedKey key)
            {
                repairedDataInfo.trackPartitionKey(key);
                return key;
            }

            protected DeletionTime applyToDeletion(DeletionTime deletionTime)
            {
                repairedDataInfo.trackDeletion(deletionTime);
                return deletionTime;
            }

            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                repairedDataInfo.trackRangeTombstoneMarker(marker);
                return marker;
            }

            protected Row applyToStatic(Row row)
            {
                repairedDataInfo.trackRow(row);
                return row;
            }

            protected Row applyToRow(Row row)
            {
                repairedDataInfo.trackRow(row);
                return row;
            }
        }

        return Transformation.apply(iterator, new WithTracking());
    }

    private static class RepairedDataInfo
    {
        private Hasher hasher;
        private boolean isConclusive = true;

        ByteBuffer getDigest()
        {
            return hasher == null
                   ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                   : ByteBuffer.wrap(getHasher().hash().asBytes());
        }

        boolean isConclusive()
        {
            return isConclusive;
        }

        void markInconclusive()
        {
            isConclusive = false;
        }

        void trackPartitionKey(DecoratedKey key)
        {
            HashingUtils.updateBytes(getHasher(), key.getKey().duplicate());
        }

        void trackDeletion(DeletionTime deletion)
        {
            deletion.digest(getHasher());
        }

        void trackRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            marker.digest(getHasher());
        }

        void trackRow(Row row)
        {
            row.digest(getHasher());
        }

        private Hasher getHasher()
        {
            if (hasher == null)
                hasher = Hashing.crc32c().newHasher();

            return hasher;
        }
    }

    @SuppressWarnings("resource") // resultant iterators are closed by their callers
    InputCollector<UnfilteredRowIterator> iteratorsForPartition(ColumnFamilyStore.ViewFragment view)
    {
        BiFunction<List<UnfilteredRowIterator>, RepairedDataInfo, UnfilteredRowIterator> merge =
            (unfilteredRowIterators, repairedDataInfo) ->
                withRepairedDataInfo(UnfilteredRowIterators.merge(unfilteredRowIterators), repairedDataInfo);

        return new InputCollector<>(view, repairedDataInfo, merge, isTrackingRepairedStatus());
    }

    @SuppressWarnings("resource") // resultant iterators are closed by their callers
    InputCollector<UnfilteredPartitionIterator> iteratorsForRange(ColumnFamilyStore.ViewFragment view)
    {
        BiFunction<List<UnfilteredPartitionIterator>, RepairedDataInfo, UnfilteredPartitionIterator> merge =
            (unfilteredPartitionIterators, repairedDataInfo) ->
                withRepairedDataInfo(UnfilteredPartitionIterators.merge(unfilteredPartitionIterators, UnfilteredPartitionIterators.MergeListener.NOOP), repairedDataInfo);

        return new InputCollector<>(view, repairedDataInfo, merge, isTrackingRepairedStatus());
    }

    /**
     * Handles the collation of unfiltered row or partition iterators that comprise the
     * input for a query. Separates them according to repaired status and of repaired
     * status is being tracked, handles the merge and wrapping in a digest generator of
     * the repaired iterators.
     *
     * Intentionally not AutoCloseable so we don't mistakenly use this in ARM blocks
     * as this prematurely closes the underlying iterators
     */
    static class InputCollector<T extends AutoCloseable>
    {
        final RepairedDataInfo repairedDataInfo;
        private final boolean isTrackingRepairedStatus;
        Set<SSTableReader> repairedSSTables;
        BiFunction<List<T>, RepairedDataInfo, T> repairedMerger;
        List<T> repairedIters;
        List<T> unrepairedIters;

        InputCollector(ColumnFamilyStore.ViewFragment view,
                       RepairedDataInfo repairedDataInfo,
                       BiFunction<List<T>, RepairedDataInfo, T> repairedMerger,
                       boolean isTrackingRepairedStatus)
        {
            this.repairedDataInfo = repairedDataInfo;
            this.isTrackingRepairedStatus = isTrackingRepairedStatus;
            if (isTrackingRepairedStatus)
            {
                for (SSTableReader sstable : view.sstables)
                {
                    if (considerRepairedForTracking(sstable))
                    {
                        if (repairedSSTables == null)
                            repairedSSTables = Sets.newHashSetWithExpectedSize(view.sstables.size());
                        repairedSSTables.add(sstable);
                    }
                }
            }
            if (repairedSSTables == null)
            {
                repairedIters = Collections.emptyList();
                unrepairedIters = new ArrayList<>(view.sstables.size());
            }
            else
            {
                repairedIters = new ArrayList<>(repairedSSTables.size());
                // when we're done collating, we'll merge the repaired iters and add the
                // result to the unrepaired list, so size that list accordingly
                unrepairedIters = new ArrayList<>((view.sstables.size() - repairedSSTables.size()) + Iterables.size(view.memtables) + 1);
            }
            this.repairedMerger = repairedMerger;
        }

        void addMemtableIterator(T iter)
        {
            unrepairedIters.add(iter);
        }

        void addSSTableIterator(SSTableReader sstable, T iter)
        {
            if (repairedSSTables != null && repairedSSTables.contains(sstable))
                repairedIters.add(iter);
            else
                unrepairedIters.add(iter);
        }

        List<T> finalizeIterators()
        {
            if (repairedIters.isEmpty())
                return unrepairedIters;

            // merge the repaired data before returning, wrapping in a digest generator
            unrepairedIters.add(repairedMerger.apply(repairedIters, repairedDataInfo));
            return unrepairedIters;
        }

        boolean isEmpty()
        {
            return repairedIters.isEmpty() && unrepairedIters.isEmpty();
        }

        // For tracking purposes we consider data repaired if the sstable is either:
        // * marked repaired
        // * marked pending, but the local session has been committed. This reduces the window
        //   whereby the tracking is affected by compaction backlog causing repaired sstables to
        //   remain in the pending state
        // If an sstable is involved in a pending repair which is not yet committed, we mark the
        // repaired data info inconclusive, as the same data on other replicas may be in a
        // slightly different state.
        private boolean considerRepairedForTracking(SSTableReader sstable)
        {
            if (!isTrackingRepairedStatus)
                return false;

            UUID pendingRepair = sstable.getPendingRepair();
            if (pendingRepair != ActiveRepairService.NO_PENDING_REPAIR)
            {
                if (ActiveRepairService.instance.consistent.local.isSessionFinalized(pendingRepair))
                    return true;

                // In the edge case where compaction is backed up long enough for the session to
                // timeout and be purged by LocalSessions::cleanup, consider the sstable unrepaired
                // as it will be marked unrepaired when compaction catches up
                if (!ActiveRepairService.instance.consistent.local.sessionExists(pendingRepair))
                    return false;

                repairedDataInfo.markInconclusive();
            }

            return sstable.isRepaired();
        }

        void markInconclusive()
        {
            repairedDataInfo.markInconclusive();
        }

        public void close() throws Exception
        {
            FBUtilities.closeAll(unrepairedIters);
            FBUtilities.closeAll(repairedIters);
        }
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

        private static boolean acceptsTransient(int flags)
        {
            return (flags & 0x08) != 0;
        }

        private static int acceptsTransientFlag(boolean acceptsTransient)
        {
            return acceptsTransient ? 0x08 : 0;
        }

        // We don't set this flag anymore, but still look if we receive a
        // command with it set in case someone is using thrift a mixed 3.0/4.0+
        // cluster (which is unsupported). This is also a reminder for not
        // re-using this flag until we drop 3.0/3.X compatibility (since it's
        // used by these release for thrift and would thus confuse things)
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
            out.writeByte(command.kind.ordinal());
            out.writeByte(
                    digestFlag(command.isDigestQuery())
                    | indexFlag(null != command.indexMetadata())
                    | acceptsTransientFlag(command.acceptsTransient())
            );
            if (command.isDigestQuery())
                out.writeUnsignedVInt(command.digestVersion());
            command.metadata().id.serialize(out);
            out.writeInt(command.nowInSec());
            ColumnFilter.serializer.serialize(command.columnFilter(), out, version);
            RowFilter.serializer.serialize(command.rowFilter(), out, version);
            DataLimits.serializer.serialize(command.limits(), out, version, command.metadata().comparator);
            if (null != command.index)
                IndexMetadata.serializer.serialize(command.index, out, version);

            command.serializeSelection(out, version);
        }

        public ReadCommand deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            int flags = in.readByte();
            boolean isDigest = isDigest(flags);
            boolean acceptsTransient = acceptsTransient(flags);
            // Shouldn't happen or it's a user error (see comment above) but
            // better complain loudly than doing the wrong thing.
            if (isForThrift(flags))
                throw new IllegalStateException("Received a command with the thrift flag set. "
                                              + "This means thrift is in use in a mixed 3.0/3.X and 4.0+ cluster, "
                                              + "which is unsupported. Make sure to stop using thrift before "
                                              + "upgrading to 4.0");

            boolean hasIndex = hasIndex(flags);
            int digestVersion = isDigest ? (int)in.readUnsignedVInt() : 0;
            TableMetadata metadata = Schema.instance.getExistingTableMetadata(TableId.deserialize(in));
            int nowInSec = in.readInt();
            ColumnFilter columnFilter = ColumnFilter.serializer.deserialize(in, version, metadata);
            RowFilter rowFilter = RowFilter.serializer.deserialize(in, version, metadata);
            DataLimits limits = DataLimits.serializer.deserialize(in, version,  metadata.comparator);
            IndexMetadata index = hasIndex ? deserializeIndexMetadata(in, version, metadata) : null;

            return kind.selectionDeserializer.deserialize(in, version, isDigest, digestVersion, acceptsTransient, metadata, nowInSec, columnFilter, rowFilter, limits, index);
        }

        private IndexMetadata deserializeIndexMetadata(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            try
            {
                return IndexMetadata.serializer.deserialize(in, version, metadata);
            }
            catch (UnknownIndexException e)
            {
                logger.info("Couldn't find a defined index on {}.{} with the id {}. " +
                            "If an index was just created, this is likely due to the schema not " +
                            "being fully propagated. Local read will proceed without using the " +
                            "index. Please wait for schema agreement after index creation.",
                            metadata.keyspace, metadata.name, e.indexId);
                return null;
            }
        }

        public long serializedSize(ReadCommand command, int version)
        {
            return 2 // kind + flags
                   + (command.isDigestQuery() ? TypeSizes.sizeofUnsignedVInt(command.digestVersion()) : 0)
                   + command.metadata().id.serializedSize()
                   + TypeSizes.sizeof(command.nowInSec())
                   + ColumnFilter.serializer.serializedSize(command.columnFilter(), version)
                   + RowFilter.serializer.serializedSize(command.rowFilter(), version)
                   + DataLimits.serializer.serializedSize(command.limits(), version, command.metadata().comparator)
                   + command.selectionSerializedSize(version)
                   + command.indexSerializedSize(version);
        }
    }
}
