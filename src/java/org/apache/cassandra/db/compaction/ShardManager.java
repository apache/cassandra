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

package org.apache.cassandra.db.compaction;

import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.EstimatedHistogram;

public interface ShardManager
{
    /**
     * Single-partition, and generally sstables with very few partitions, can cover very small sections of the token
     * space, resulting in very high densities.
     * When the number of partitions in an sstable is smaller than this threshold, we will use a per-partition minimum
     * span, calculated from the total number of partitions in this table.
     */
    static final long PER_PARTITION_SPAN_THRESHOLD = 100;

    /**
     * Additionally, sstables that have completely fallen outside the local token ranges will end up with a zero
     * coverage.
     * To avoid problems with this we check if coverage is below the minimum, and replace it using the per-partition
     * calculation.
     */
    static final double MINIMUM_TOKEN_COVERAGE = Math.scalb(1.0, -48);

    static ShardManager create(ColumnFamilyStore cfs)
    {
        return create(cfs, estimatedPartitionCount(cfs));
    }

    static ShardManager create(ColumnFamilyStore cfs, long estimatedPartitionCount)
    {
        final ImmutableList<PartitionPosition> diskPositions = cfs.getDiskBoundaries().positions;
        ColumnFamilyStore.VersionedLocalRanges localRanges = cfs.localRangesWeighted();
        IPartitioner partitioner = cfs.getPartitioner();

        if (diskPositions != null && diskPositions.size() > 1)
            return new ShardManagerDiskAware(localRanges, diskPositions.stream()
                                                                       .map(PartitionPosition::getToken)
                                                                       .collect(Collectors.toList()),
                                             estimatedPartitionCount);
        else if (partitioner.splitter().isPresent())
            return new ShardManagerNoDisks(localRanges, estimatedPartitionCount);
        else
            return new ShardManagerTrivial(partitioner);
    }

    /**
     * Return the estimated partition count, used when the number of partitions in an sstable is not sufficient to give
     * a sensible range estimation.
     */
    static long estimatedPartitionCount(ColumnFamilyStore cfs)
    {
        final long INITIAL_ESTIMATED_PARTITION_COUNT = 1 << 16; // If we don't yet have a count, use a sensible default.
        if (cfs.metric == null)
            return INITIAL_ESTIMATED_PARTITION_COUNT;
        final Long estimation = cfs.metric.estimatedPartitionCount.getValue();
        if (estimation == null || estimation <= 0)
            return INITIAL_ESTIMATED_PARTITION_COUNT;
        return estimation;
    }

    boolean isOutOfDate(long ringVersion);

    /**
     * As {@link #isOutOfDate(long)}. The default ignores the estimated partition count. Implementations whose span
     * calculations depend on it must override this and also report out of date when the count has changed from the
     * value the manager was created with.
     */
    default boolean isOutOfDate(long ringVersion, long estimatedPartitionCount)
    {
        return isOutOfDate(ringVersion);
    }

    /**
     * The token range fraction spanned by the given range, adjusted for the local range ownership.
     */
    double rangeSpanned(Range<Token> tableRange);

    /**
     * The total fraction of the token space covered by the local ranges.
     */
    double localSpaceCoverage();

    /**
     * The fraction of the token space covered by a shard set, i.e. the space that is split in the requested number of
     * shards.
     * If no disks are defined, this is the same as localSpaceCoverage(). Otherwise, it is the token coverage of a disk.
     */
    double shardSetCoverage();

    /**
     * The minimum token space share per partition that should be assigned to sstables with small numbers of partitions
     * or which have fallen outside the local token ranges.
     */
    double minimumPerPartitionSpan();

    /**
     * Construct a boundary/shard iterator for the given number of shards.
     *
     * Note: This does not offer a method of listing the shard boundaries it generates, just to advance to the
     * corresponding one for a given token.  The only usage for listing is currently in tests. Should a need for this
     * arise, see {@link CompactionSimulationTest} for a possible implementation.
     */
    ShardTracker boundaries(int shardCount);

    static Range<Token> coveringRange(SSTableReader sstable)
    {
        return coveringRange(sstable.getFirst(), sstable.getLast());
    }

    static Range<Token> coveringRange(PartitionPosition first, PartitionPosition last)
    {
        // To include the token of last, the range's upper bound must be increased.
        return new Range<>(first.getToken(), last.getToken().nextValidToken());
    }


    /**
     * Return the token space share that the given SSTable spans, excluding any non-locally owned space.
     * Returns a positive floating-point number between 0 and 1.
     */
    default double rangeSpanned(SSTableReader rdr)
    {
        double reported = rdr.tokenSpaceCoverage();

        double span;
        if (reported > 0)   // also false for NaN
            span = reported;
        else
            span = rangeSpanned(rdr.getFirst(), rdr.getLast());

        long partitionCount = partitionCount(rdr);
        if (partitionCount >= PER_PARTITION_SPAN_THRESHOLD && span >= MINIMUM_TOKEN_COVERAGE)
            return span;

        // Too small ranges are expected to be the result of either an sstable with a very small number of partitions,
        // or falling outside the local token ranges. In these cases we apply a per-partition minimum calculated from
        // the number of partitions in the table.
        double perPartitionMinimum = Math.min(partitionCount * minimumPerPartitionSpan(), 1.0);
        return span > perPartitionMinimum ? span : perPartitionMinimum; // The latter will be chosen if span is NaN too.
    }

    /**
     * The number of partitions in the given sstable, read from its stats metadata, which stores the exact
     * count for every sstable format.
     */
    static long partitionCount(SSTableReader rdr)
    {
        EstimatedHistogram partitionSizes = rdr.getEstimatedPartitionSize();
        if (partitionSizes != null && partitionSizes.count() > 0)
            return partitionSizes.count();
        // Fall back to estimatedKeys() if the histogram is absent or empty, clamped to 1 so rangeSpanned
        // never computes a zero floor, which would give a zero span and an infinite density.
        return Math.max(1, rdr.estimatedKeys());
    }

    default double rangeSpanned(PartitionPosition first, PartitionPosition last)
    {
        return rangeSpanned(ShardManager.coveringRange(first, last));
    }

    /**
     * Return the density of an SSTable, i.e. its size divided by the covered token space share.
     * This is an improved measure of the compaction age of an SSTable that grows both with STCS-like full-SSTable
     * compactions (where size grows, share is constant), LCS-like size-threshold splitting (where size is constant
     * but share shrinks), UCS-like compactions (where size may grow and covered shards i.e. share may decrease)
     * and can reproduce levelling structure that corresponds to all, including their mixtures.
     */
    default double density(SSTableReader rdr)
    {
        return rdr.onDiskLength() / rangeSpanned(rdr);
    }

    default int compareByDensity(SSTableReader a, SSTableReader b)
    {
        return Double.compare(density(a), density(b));
    }

    /**
     * Estimate the density of the sstable that will be the result of compacting the given sources.
     */
    default double calculateCombinedDensity(Set<? extends SSTableReader> sstables)
    {
        if (sstables.isEmpty())
            return 0;
        long onDiskLength = 0;
        long partitionCount = 0;
        PartitionPosition min = null;
        PartitionPosition max = null;
        for (SSTableReader sstable : sstables)
        {
            onDiskLength += sstable.onDiskLength();
            partitionCount += partitionCount(sstable);
            min = min == null || min.compareTo(sstable.getFirst()) > 0 ? sstable.getFirst() : min;
            max = max == null || max.compareTo(sstable.getLast()) < 0 ? sstable.getLast() : max;
        }
        double span = rangeSpanned(min, max);
        if (partitionCount >= PER_PARTITION_SPAN_THRESHOLD && span >= MINIMUM_TOKEN_COVERAGE)
            return onDiskLength / span;

        // Apply the same per-partition minimum as rangeSpanned(SSTableReader), so that the output of compacting
        // small sstables is sized by the floored span its inputs were selected with.
        double perPartitionMinimum = Math.min(partitionCount * minimumPerPartitionSpan(), 1.0);
        span = span > perPartitionMinimum ? span : perPartitionMinimum;
        if (span >= MINIMUM_TOKEN_COVERAGE)
            return onDiskLength / span;
        else
            return onDiskLength;
    }
}
