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

import java.nio.ByteBuffer;
import java.util.function.LongPredicate;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.MoreRows;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

@NotThreadSafe
class RepairedDataInfo
{
    public static final RepairedDataInfo NO_OP_REPAIRED_DATA_INFO = new RepairedDataInfo(null)
    {
        @Override
        public UnfilteredPartitionIterator withRepairedDataInfo(UnfilteredPartitionIterator iterator)
        {
            return iterator;
        }

        @Override
        public UnfilteredRowIterator withRepairedDataInfo(UnfilteredRowIterator iterator)
        {
            return iterator;
        }
            
        @Override
        public UnfilteredPartitionIterator extend(UnfilteredPartitionIterator partitions, DataLimits.Counter limit)
        {
           return partitions;
        }
    };

    // Keeps a digest of the partition currently being processed. Since we won't know
    // whether a partition will be fully purged from a read result until it's been
    // consumed, we buffer this per-partition digest and add it to the final digest
    // when the partition is closed (if it wasn't fully purged).
    private Digest perPartitionDigest;
    private Digest perCommandDigest;
    private boolean isConclusive = true;
    private ByteBuffer calculatedDigest = null;

    // Doesn't actually purge from the underlying iterators, but excludes from the digest
    // the purger can't be initialized until we've iterated all the sstables for the query
    // as it requires the oldest repaired tombstone
    private RepairedDataPurger purger;
    private boolean isFullyPurged = true;

    // Supplies additional partitions from the repaired data set to be consumed when the limit of
    // executing ReadCommand has been reached. This is to ensure that each replica attempts to
    // read the same amount of repaired data, otherwise comparisons of the repaired data digests
    // may be invalidated by varying amounts of repaired data being present on each replica.
    // This can't be initialized until after the underlying repaired iterators have been merged.
    private UnfilteredPartitionIterator postLimitPartitions = null;
    private final DataLimits.Counter repairedCounter;
    private UnfilteredRowIterator currentPartition;
    private TableMetrics metrics;

    public RepairedDataInfo(DataLimits.Counter repairedCounter)
    {
        this.repairedCounter = repairedCounter;
    }

    /**
     * If either repaired status tracking is not active or the command has not yet been
     * executed, then this digest will be an empty buffer.
     * Otherwise, it will contain a digest of the repaired data read, or an empty buffer
     * if no repaired data was read.
     *
     * @return a digest of the repaired data read during local execution of a command
     */
    ByteBuffer getDigest()
    {
        if (calculatedDigest != null)
            return calculatedDigest;

        calculatedDigest = perCommandDigest == null
                           ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                           : ByteBuffer.wrap(perCommandDigest.digest());

        return calculatedDigest;
    }

    void prepare(ColumnFamilyStore cfs, long nowInSec, long oldestUnrepairedTombstone)
    {
        this.purger = new RepairedDataPurger(cfs, nowInSec, oldestUnrepairedTombstone);
        this.metrics = cfs.metric;
    }

    void finalize(UnfilteredPartitionIterator postLimitPartitions)
    {
        this.postLimitPartitions = postLimitPartitions;
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
     * @return boolean to indicate confidence in the whether or not the digest of the repaired data can be
     *         reliably be used to infer inconsistency issues between the repaired sets across replicas
     */
    boolean isConclusive()
    {
        return isConclusive;
    }

    void markInconclusive()
    {
        isConclusive = false;
    }

    private void onNewPartition(UnfilteredRowIterator partition)
    {
        assert purger != null;
        purger.setCurrentKey(partition.partitionKey());
        purger.setIsReverseOrder(partition.isReverseOrder());
        this.currentPartition = partition;
    }

    private Digest getPerPartitionDigest()
    {
        if (perPartitionDigest == null)
            perPartitionDigest = Digest.forRepairedDataTracking();

        return perPartitionDigest;
    }

    public UnfilteredPartitionIterator withRepairedDataInfo(final UnfilteredPartitionIterator iterator)
    {
        class WithTracking extends Transformation<UnfilteredRowIterator>
        {
            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return withRepairedDataInfo(partition);
            }
        }
        return Transformation.apply(iterator, new WithTracking());
    }

    public UnfilteredRowIterator withRepairedDataInfo(final UnfilteredRowIterator iterator)
    {
        class WithTracking extends Transformation<UnfilteredRowIterator>
        {
            protected DecoratedKey applyToPartitionKey(DecoratedKey key)
            {
                getPerPartitionDigest().update(key.getKey());
                return key;
            }

            protected DeletionTime applyToDeletion(DeletionTime deletionTime)
            {
                if (repairedCounter.isDone())
                    return deletionTime;

                assert purger != null;
                DeletionTime purged = purger.applyToDeletion(deletionTime);
                if (!purged.isLive())
                    isFullyPurged = false;
                purged.digest(getPerPartitionDigest());
                return deletionTime;
            }

            protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
            {
                if (repairedCounter.isDone())
                    return marker;

                assert purger != null;
                RangeTombstoneMarker purged = purger.applyToMarker(marker);
                if (purged != null)
                {
                    isFullyPurged = false;
                    purged.digest(getPerPartitionDigest());
                }
                return marker;
            }

            protected Row applyToStatic(Row row)
            {
                return applyToRow(row);
            }

            protected Row applyToRow(Row row)
            {
                if (repairedCounter.isDone())
                    return row;

                assert purger != null;
                Row purged = purger.applyToRow(row);
                if (purged != null && !purged.isEmpty())
                {
                    isFullyPurged = false;
                    purged.digest(getPerPartitionDigest());
                }
                return row;
            }

            protected void onPartitionClose()
            {
                if (perPartitionDigest != null)
                {
                    // If the partition wasn't completely emptied by the purger,
                    // calculate the digest for the partition and use it to
                    // update the overall digest
                    if (!isFullyPurged)
                    {
                        if (perCommandDigest == null)
                            perCommandDigest = Digest.forRepairedDataTracking();

                        byte[] partitionDigest = perPartitionDigest.digest();
                        perCommandDigest.update(partitionDigest, 0, partitionDigest.length);
                    }

                    perPartitionDigest = null;
                }
                isFullyPurged = true;
            }
        }

        if (repairedCounter.isDone())
            return iterator;

        UnfilteredRowIterator tracked = repairedCounter.applyTo(Transformation.apply(iterator, new WithTracking()));
        onNewPartition(tracked);
        return tracked;
    }

    public UnfilteredPartitionIterator extend(final UnfilteredPartitionIterator partitions,
                                              final DataLimits.Counter limit)
    {
        class OverreadRepairedData extends Transformation<UnfilteredRowIterator> implements MoreRows<UnfilteredRowIterator>
        {

            protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return MoreRows.extend(partition, this, partition.columns());
            }

            public UnfilteredRowIterator moreContents()
            {
                // We don't need to do anything until the DataLimits of the
                // of the read have been reached
                if (!limit.isDone() || repairedCounter.isDone())
                    return null;

                long countBeforeOverreads = repairedCounter.counted();
                long overreadStartTime = nanoTime();
                if (currentPartition != null)
                    consumePartition(currentPartition, repairedCounter);

                if (postLimitPartitions != null)
                    while (postLimitPartitions.hasNext() && !repairedCounter.isDone())
                        consumePartition(postLimitPartitions.next(), repairedCounter);

                // we're not actually providing any more rows, just consuming the repaired data
                long rows = repairedCounter.counted() - countBeforeOverreads;
                long nanos = nanoTime() - overreadStartTime;
                metrics.repairedDataTrackingOverreadRows.update(rows);
                metrics.repairedDataTrackingOverreadTime.update(nanos, TimeUnit.NANOSECONDS);
                Tracing.trace("Read {} additional rows of repaired data for tracking in {}ps", rows, TimeUnit.NANOSECONDS.toMicros(nanos));
                return null;
            }

            private void consumePartition(UnfilteredRowIterator partition, DataLimits.Counter counter)
            {
                if (partition == null)
                    return;

                while (!counter.isDone() && partition.hasNext())
                    partition.next();

                partition.close();
            }
        }
        // If the read didn't touch any sstables prepare() hasn't been called and
        // we can skip this transformation
        if (metrics == null || repairedCounter.isDone())
            return partitions;
        return Transformation.apply(partitions, new OverreadRepairedData());
    }

    /**
     * Although PurgeFunction extends Transformation, this is never applied to an iterator.
     * Instead, it is used by RepairedDataInfo during the generation of a repaired data
     * digest to exclude data which will actually be purged later on in the read pipeline.
     */
    private static class RepairedDataPurger extends PurgeFunction
    {
        RepairedDataPurger(ColumnFamilyStore cfs,
                           long nowInSec,
                           long oldestUnrepairedTombstone)
        {
            super(nowInSec,
                  cfs.gcBefore(nowInSec),
                  oldestUnrepairedTombstone,
                  cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones(),
                  cfs.metadata.get().enforceStrictLiveness());
        }

        protected LongPredicate getPurgeEvaluator()
        {
            return (time) -> true;
        }

        void setCurrentKey(DecoratedKey key)
        {
            super.onNewPartition(key);
        }

        void setIsReverseOrder(boolean isReverseOrder)
        {
            super.setReverseOrder(isReverseOrder);
        }

        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            return super.applyToDeletion(deletionTime);
        }

        public Row applyToRow(Row row)
        {
            return super.applyToRow(row);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return super.applyToMarker(marker);
        }
    }
}
