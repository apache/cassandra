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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * A compaction aggregate is either a level in {@link LeveledCompactionStrategy} or a tier (bucket) in other
 * compaction strategies.
 * <p/>
 * It contains a list of {@link CompactionPick}, which are the compactions either in progress or pending.
 * It also contains a selected {@link CompactionPick}, which is a compaction about to be submitted. The submitted
 * compaction is also part of the compactions. Lastly, it contains a set of all the sstables in this aggregate,
 * regardless of whether they need compaction.
 */
public abstract class CompactionAggregate
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionAggregate.class);

    /** The sstables in this aggregate, whether they are compaction candidates or not */
    final Set<SSTableReader> sstables;

    /** The compaction that was selected for this aggregate when it was created. It is also part of {@link this#compactions}. */
    final CompactionPick selected;

    /** The compactions that are part of this aggregate, they could be pending or in progress. */
    final LinkedHashSet<CompactionPick> compactions;

    CompactionAggregate(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> pending)
    {
        if (sstables == null || selected == null || pending == null)
            throw new IllegalArgumentException("Arguments cannot be null");

        this.sstables = new HashSet<>(); sstables.forEach(this.sstables::add);
        this.selected = selected;

        // Here we want to keep the iteration order since normally pending compactions are ordered by a strategy
        // and the selected compaction should be the first one
        this.compactions = new LinkedHashSet<>();
        if (!selected.isEmpty())
            compactions.add(selected);

        for (CompactionPick p : pending)
        {
            if (p == null || p.isEmpty())
                throw new IllegalArgumentException("Pending compactions should be valid compactions");

            compactions.add(p);
        }
    }

    public CompactionPick getSelected()
    {
        return selected;
    }

    /**
     * @return compactions that have not yet been submitted (no compaction id).
     */
    public List<CompactionPick> getPending()
    {
        List<CompactionPick> ret = new ArrayList<>(compactions.size());
        for (CompactionPick comp : compactions)
        {
            if (comp.id == null)
                ret.add(comp);
        }

        return ret;
    }

    /**
     * @return compactions that have already been submitted (compaction id is available) and haven't completed yet
     */
    public List<CompactionPick> getInProgress()
    {
        List<CompactionPick> ret = new ArrayList<>(compactions.size());
        for (CompactionPick comp : compactions)
        {
            if (comp.id != null && !comp.completed)
                ret.add(comp);
        }

        return ret;
    }

    /**
     * @return all the compactions we have
     */
    public List<CompactionPick> getActive()
    {
        return new ArrayList<>(compactions);
    }

    /**
     * @return true if this aggregate has no compactions
     */
    public boolean isEmpty()
    {
        return compactions.isEmpty();
    }

    /**
     * Merge the pending compactions and the compactions in progress to create some aggregated statistics.
     *
     * @return the statistics for this compaction aggregate, see {@link CompactionAggregateStatistics}.
     */
    public abstract CompactionAggregateStatistics getStatistics();

    /**
     * @return the number of estimated compactions that are still pending.
     */
    public int numEstimatedCompactions()
    {
        return getPending().size();
    }

    /**
     * @return a key that is specific to the concrete implementation, used for grouping compacting aggregates
     */
    abstract long getKey();

    /**
     * Return a matching aggregate from the map passed in or null. Normally this is just a matter of finding
     * the key in the map but for STCS we need to look at the possible min and maximum average sizes and so
     * {@link SizeTiered} overrides this method.
     *
     * @param others a map of other aggregates
     *
     * @return an aggregate with the same key or null
     */
    @Nullable CompactionAggregate getMatching(TreeMap<Long, CompactionAggregate> others)
    {
        return others.get(getKey());
    }

    /**
     * Create a copy of this aggregate with the new parameters
     *
     * @return a deep copy of this aggregate
     */
    protected abstract CompactionAggregate clone(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> compactions);


    /**
     * Add expired sstables to the selected compaction pick and return a new compaction aggregate.
     */
    CompactionAggregate withExpired(Collection<SSTableReader> expired)
    {
        return clone(Iterables.concat(sstables, expired), selected.withAddedSSTables(expired), compactions);
    }

    /**
     * Add existing compactions to our own compactions and return a new compaction aggregate
     */
    public CompactionAggregate withAdditionalCompactions(Collection<CompactionPick> comps)
    {
        List<SSTableReader> sstables = comps.stream().flatMap(comp -> comp.sstables.stream()).collect(Collectors.toList());
        return clone(Iterables.concat(this.sstables, sstables), selected, Iterables.concat(compactions, comps));
    }

    /**
     * Only keep the compactions passed in, strip everything else.
     */
    public CompactionAggregate withOnlyTheseCompactions(Collection<CompactionPick> comps)
    {
        List<SSTableReader> sstables = comps.stream().flatMap(comp -> comp.sstables.stream()).collect(Collectors.toList());
        return clone(sstables, CompactionPick.EMPTY, comps);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sstables, selected, compactions);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof CompactionAggregate))
            return false;

        CompactionAggregate that = (CompactionAggregate) obj;
        return sstables.equals(that.sstables) &&
               selected.equals(that.selected) &&
               compactions.equals(that.compactions);
    }

    /**
     * Contains information about a levelled compaction aggregate, this is equivalent to a level in {@link LeveledCompactionStrategy}.
     */
    public static final class Leveled extends CompactionAggregate
    {
        /** The current level number */
        final int level;

        /** The next level number */
        final int nextLevel;

        /** The score of this level as defined in {@link LeveledCompactionStrategy}. */
        final double score;

        /** The maximum size of each output sstable that will be produced by compaction, Long.MAX_VALUE if no maximum exists */
        final long maxSSTableBytes;

        /** How many more compactions this level is expected to perform. This is required because for LCS we cannot easily identify candidate
         * sstables to put into the pending picks.
         */
        final int pendingCompactions;

        Leveled(Iterable<SSTableReader> sstables,
                CompactionPick selected,
                Iterable<CompactionPick> compactions,
                int level,
                int nextLevel,
                double score,
                long maxSSTableBytes,
                int pendingCompactions)
        {
            super(sstables, selected, compactions);

            this.level = level;
            this.nextLevel = nextLevel;
            this.score = score;
            this.maxSSTableBytes = maxSSTableBytes;
            this.pendingCompactions = pendingCompactions;
        }

        @Override
        protected CompactionAggregate clone(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new Leveled(sstables, selected, compactions, level, nextLevel, score, maxSSTableBytes, pendingCompactions);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            int numCompactions = pendingCompactions;
            int numCompactionsInProgress = 0;
            int numCandidateSSTables = 0;
            int numCompactingSSTables = 0;
            long tot = 0;
            long read = 0;
            long readLevel = 0;
            long written = 0;
            long durationNanos = 0;

            for (CompactionPick compaction : compactions)
            {
                if (compaction.completed)
                    continue;

                numCompactions++;
                numCandidateSSTables += compaction.sstables.size();
                tot += compaction.sstables.stream().mapToLong(SSTableReader::uncompressedLength).reduce(0L, Long::sum);

                if (compaction.id != null)
                {
                    numCompactionsInProgress++;
                    numCompactingSSTables += compaction.sstables.size();
                }

                if (compaction.progress != null)
                {
                    read += compaction.progress.uncompressedBytesRead();
                    readLevel += compaction.progress.uncompressedBytesRead(level);
                    written += compaction.progress.uncompressedBytesWritten();
                    durationNanos += compaction.progress.durationInNanos();
                }
            }

            double readThroughput = durationNanos == 0 ? 0 : ((double) read / durationNanos) * TimeUnit.SECONDS.toNanos(1);
            double writeThroughput = durationNanos == 0 ? 0 : ((double) written / durationNanos) * TimeUnit.SECONDS.toNanos(1);

            return new LeveledCompactionStatistics(level,
                                                   score,
                                                   numCompactions,
                                                   numCompactionsInProgress,
                                                   sstables.size(),
                                                   numCandidateSSTables,
                                                   numCompactingSSTables,
                                                   getTotSizeBytes(sstables),
                                                   readThroughput,
                                                   writeThroughput,
                                                   tot,
                                                   read,
                                                   readLevel,
                                                   written);
        }

        @Override
        public int numEstimatedCompactions()
        {
            return pendingCompactions;
        }

        @Override
        public boolean isEmpty()
        {
            return super.isEmpty() && pendingCompactions == 0;
        }

        @Override
        long getKey()
        {
            return level;
        }

        @Override
        public String toString()
        {
            return String.format("Level %d with %d sstables, %d compactions and %d pending", level, sstables.size(), compactions.size(), pendingCompactions);
        }
    }

    /**
     * Create a level where we have a compaction candidate.
     */
    static CompactionAggregate.Leveled createLeveled(Collection<SSTableReader> all,
                                                     Collection<SSTableReader> candidates,
                                                     int pendingCompactions,
                                                     long maxSSTableBytes,
                                                     int level,
                                                     int nextLevel,
                                                     double score)
    {
        return new Leveled(all,
                           CompactionPick.create(level, candidates),
                           ImmutableList.of(),
                           level,
                           nextLevel,
                           score,
                           maxSSTableBytes,
                           pendingCompactions);
    }

    /**
     * Create a level when we only have estimated tasks.
     */
    static CompactionAggregate.Leveled createLeveled(Collection<SSTableReader> all,
                                                     int pendingCompactions,
                                                     long maxSSTableBytes,
                                                     int level,
                                                     double score)
    {
        return new Leveled(all,
                           CompactionPick.EMPTY,
                           ImmutableList.of(),
                           level,
                           level + 1,
                           score,
                           maxSSTableBytes,
                           pendingCompactions);
    }

    /**
     * Create a leveled aggregate when LCS is doing STCS on level 0
     */
    static CompactionAggregate.Leveled createLeveledForSTCS(Collection<SSTableReader> all,
                                                            CompactionPick pick,
                                                            int pendingCompactions,
                                                            double score)
    {
        return new Leveled(all,
                           pick,
                           ImmutableList.of(),
                           0,
                           0,
                           score,
                           Long.MAX_VALUE,
                           pendingCompactions);
    }

    /**
     * Contains information about a size-tiered compaction aggregate, this is equivalent to a bucket in {@link SizeTieredCompactionStrategy}.
     */
    public static final class SizeTiered extends CompactionAggregate
    {
        /** The total read hotness of the sstables in this tier, as defined by {@link SSTableReader#hotness()} */
        final double hotness;

        /** The average on disk size in bytes of the sstables in this tier */
        final long avgSizeBytes;

        /** The minimum on disk size in bytes for this tier, this is normally the avg size times the STCS bucket low and it is
         * used to find compacting aggregates that are on the same tier. */
        final long minSizeBytes;

        /** The maximum on disk size in bytes for this tier, this is normally the avg size times the STCS bucket high and it is
         * used to find compacting aggregates that are on the same tier. */
        final long maxSizeBytes;

        SizeTiered(Iterable<SSTableReader> sstables,
                   CompactionPick selected,
                   Iterable<CompactionPick> pending,
                   double hotness,
                   long avgSizeBytes,
                   long minSizeBytes,
                   long maxSizeBytes)
        {
            super(sstables, selected, pending);

            this.hotness = hotness;
            this.avgSizeBytes = avgSizeBytes;
            this.minSizeBytes = minSizeBytes;
            this.maxSizeBytes = maxSizeBytes;
        }

        @Override
        protected CompactionAggregate clone(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new SizeTiered(sstables, selected, compactions, getTotHotness(sstables), getAvgSizeBytes(sstables), minSizeBytes, maxSizeBytes);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            int numCompactions = 0;
            int numCompactionsInProgress = 0;
            int numCandidateSSTables = 0;
            int numCompactingSSTables = 0;
            long tot = 0;
            long read = 0;
            long written = 0;
            double hotness = 0;
            long durationNanos = 0;

            for (CompactionPick compaction : compactions)
            {
                if (compaction.completed)
                    continue;

                numCompactions++;
                numCandidateSSTables += compaction.sstables.size();
                tot += compaction.sstables.stream().mapToLong(SSTableReader::uncompressedLength).reduce(0L, Long::sum);
                hotness += compaction.hotness;

                if (compaction.id != null)
                {
                    numCompactionsInProgress++;
                    numCompactingSSTables += compaction.sstables.size();
                }

                if (compaction.progress != null)
                {
                    read += compaction.progress.uncompressedBytesRead();
                    written += compaction.progress.uncompressedBytesWritten();
                    durationNanos += compaction.progress.durationInNanos();
                }
            }

            double readThroughput = durationNanos == 0 ? 0 : ((double) read / durationNanos) * TimeUnit.SECONDS.toNanos(1);
            double writeThroughput = durationNanos == 0 ? 0 : ((double) written / durationNanos) * TimeUnit.SECONDS.toNanos(1);

            return new SizeTieredCompactionStatistics(avgSizeBytes,
                                                      hotness,
                                                      numCompactions,
                                                      numCompactionsInProgress,
                                                      sstables.size(),
                                                      numCandidateSSTables,
                                                      numCompactingSSTables,
                                                      getTotSizeBytes(sstables),
                                                      readThroughput,
                                                      writeThroughput,
                                                      tot,
                                                      read,
                                                      written);
        }

        @Override
        long getKey()
        {
            return avgSizeBytes;
        }

        @Override
        @Nullable CompactionAggregate getMatching(TreeMap<Long, CompactionAggregate> others)
        {
            SortedMap<Long, CompactionAggregate> subMap = others.subMap(minSizeBytes, maxSizeBytes);
            if (subMap.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("Found no matching aggregate for {}",
                                 FBUtilities.prettyPrintMemory(avgSizeBytes));

                return null;
            }

            if (logger.isTraceEnabled())
                logger.trace("Found {} matching aggregates for {}",
                             subMap.size(),
                             FBUtilities.prettyPrintMemory(avgSizeBytes));

            Long closest = null;
            long minDiff = 0;
            for (Long m : subMap.keySet())
            {
                long diff = Math.abs(m - avgSizeBytes);
                if (closest == null || diff < minDiff)
                {
                    closest = m;
                    minDiff = diff;
                }
            }

            if (logger.isTraceEnabled())
                logger.trace("Using closest matching aggregate for {}: {}",
                             FBUtilities.prettyPrintMemory(avgSizeBytes),
                             FBUtilities.prettyPrintMemory(closest));

            return others.get(closest);
        }

        @Override
        public String toString()
        {
            return String.format("Size tiered %s/%s/%s with %d sstables, %d compactions",
                                 FBUtilities.prettyPrintMemory(minSizeBytes),
                                 FBUtilities.prettyPrintMemory(avgSizeBytes),
                                 FBUtilities.prettyPrintMemory(maxSizeBytes),
                                 sstables.size(),
                                 compactions.size());
        }
    }

    static CompactionAggregate createSizeTiered(Collection<SSTableReader> all,
                                                CompactionPick selected,
                                                List<CompactionPick> pending,
                                                double hotness,
                                                long avgSizeBytes,
                                                long minSizeBytes,
                                                long maxSizeBytes)
    {
        return new SizeTiered(all, selected, pending, hotness, avgSizeBytes, minSizeBytes, maxSizeBytes);
    }

    /**
     * Contains information about a size-tiered compaction aggregate, this is equivalent to a bucket in {@link SizeTieredCompactionStrategy}.
     */
    public static final class TimeTiered extends CompactionAggregate
    {
        /** The timestamp of this aggregate */
        final long timestamp;

        TimeTiered(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> pending, long timestamp)
        {
            super(sstables, selected, pending);
            this.timestamp = timestamp;
        }

        @Override
        protected CompactionAggregate clone(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new TimeTiered(sstables, selected, compactions, timestamp);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            int numCompactions = 0;
            int numCompactionsInProgress = 0;
            int numCandidateSSTables = 0;
            int numCompactingSSTables = 0;
            long tot = 0;
            long read = 0;
            long written = 0;
            double hotness = 0;
            long durationNanos = 0;

            for (CompactionPick compaction : compactions)
            {
                if (compaction.completed)
                    continue;

                numCompactions++;
                numCandidateSSTables += compaction.sstables.size();
                tot += compaction.sstables.stream().mapToLong(SSTableReader::uncompressedLength).reduce(0L, Long::sum);
                hotness += compaction.hotness;

                if (compaction.id != null)
                {
                    numCompactionsInProgress++;
                    numCompactingSSTables += compaction.sstables.size();
                }

                if (compaction.progress != null)
                {
                    read += compaction.progress.uncompressedBytesRead();
                    written += compaction.progress.uncompressedBytesWritten();
                    durationNanos += compaction.progress.durationInNanos();
                }
            }

            double readThroughput = durationNanos == 0 ? 0 : ((double) read / durationNanos) * TimeUnit.SECONDS.toNanos(1);
            double writeThroughput = durationNanos == 0 ? 0 : ((double) written / durationNanos) * TimeUnit.SECONDS.toNanos(1);

            return new TimeTieredCompactionStatistics(timestamp,
                                                      hotness,
                                                      numCompactions,
                                                      numCompactionsInProgress,
                                                      sstables.size(),
                                                      numCandidateSSTables,
                                                      numCompactingSSTables,
                                                      getTotSizeBytes(sstables),
                                                      readThroughput,
                                                      writeThroughput,
                                                      tot,
                                                      read,
                                                      written);
        }

        @Override
        long getKey()
        {
            return timestamp;
        }

        @Override
        public String toString()
        {
            return String.format("Time tiered %d with %d sstables, %d compactions", timestamp, sstables.size(), compactions.size());
        }
    }

    static CompactionAggregate createTimeTiered(Collection<SSTableReader> sstables, long timestamp)
    {
        return new TimeTiered(sstables, CompactionPick.create(timestamp, sstables), ImmutableList.of(), timestamp);
    }

    static CompactionAggregate createTimeTiered(Collection<SSTableReader> sstables, CompactionPick selected, List<CompactionPick> pending, long timestamp)
    {
        return new TimeTiered(sstables, selected, pending, timestamp);
    }

    /** An aggregate that is created for a compaction issued only to drop tombstones */
    public static final class TombstoneAggregate extends CompactionAggregate
    {
        TombstoneAggregate(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> pending)
        {
            super(sstables, selected, pending);
        }

        @Override
        protected CompactionAggregate clone(Iterable<SSTableReader> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new TombstoneAggregate(sstables, selected, compactions);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            int numCompactions = 0;
            int numCompactionsInProgress = 0;
            int numCandidateSSTables = 0;
            int numCompactingSSTables = 0;
            long read = 0;
            long written = 0;
            long durationNanos = 0;

            for (CompactionPick compaction : compactions)
            {
                if (compaction.completed)
                    continue;

                numCompactions++;
                numCandidateSSTables += compaction.sstables.size();

                if (compaction.id  != null)
                {
                    numCompactionsInProgress++;
                    numCompactingSSTables += compaction.sstables.size();
                }

                if (compaction.progress != null)
                {
                    read += compaction.progress.uncompressedBytesRead();
                    written += compaction.progress.uncompressedBytesWritten();
                    durationNanos += compaction.progress.durationInNanos();
                }
            }

            double readThroughput = durationNanos == 0 ? 0 : ((double) read / durationNanos) * TimeUnit.SECONDS.toNanos(1);
            double writeThroughput = durationNanos == 0 ? 0 : ((double) written / durationNanos) * TimeUnit.SECONDS.toNanos(1);

            return new CompactionAggregateStatistics(numCompactions,
                                                     numCompactionsInProgress,
                                                     sstables.size(),
                                                     numCandidateSSTables,
                                                     numCompactingSSTables,
                                                     getTotSizeBytes(sstables),
                                                     readThroughput,
                                                     writeThroughput);
        }

        @Override
        long getKey()
        {
            return -1; // Tombstone compactions are the only ones with negative keys so they will be matched by a unique aggregate
        }

        @Override
        public String toString()
        {
            return String.format("Tombstones with %d sstables, %d compactions", sstables.size(), compactions.size());
        }
    }

    static CompactionAggregate createForTombstones(SSTableReader sstable)
    {
        List<SSTableReader> sstables = ImmutableList.of(sstable);
        CompactionPick comp = CompactionPick.create(-1, sstables);
        return new TombstoneAggregate(sstables, comp, ImmutableList.of());
    }

    /**
     * Return the compaction statistics for this strategy and list of compactions that are either pending or in progress.
     *
     * @param aggregates the compaction aggregates
     *
     * @return the statistics about this compactions
     */
    static CompactionStrategyStatistics getStatistics(TableMetadata metadata,
                                                      AbstractCompactionStrategy strategy,
                                                      Map<Long, CompactionAggregate> aggregates)
    {
        List<Pair<Long, CompactionAggregateStatistics>> statistcs = new ArrayList<>(aggregates.size());

        for (CompactionAggregate comp : aggregates.values())
            statistcs.add(Pair.create(comp.getKey(), comp.getStatistics()));

        return new CompactionStrategyStatistics(metadata,
                                                strategy.getClass().getSimpleName(),
                                                statistcs.stream().map(p -> p.right).collect(Collectors.toList()));
    }

    /**
     * Return the number of compactions that are still pending;
     * @param aggregates the compaction aggregates
     *
     * @return the number of compactions that are still pending (net yet submitted)
     */
    static int numEstimatedCompactions(Map<Long, CompactionAggregate> aggregates)
    {
        int ret = 0;
        for (CompactionAggregate aggregate : aggregates.values())
            ret += aggregate.numEstimatedCompactions();

        return ret;
    }

    /**
     * Given a sorted list of compactions, return the first selected pick.
     *
     * @param aggregates a sorted list of compaction aggregates from most interesting to least interesting, some may be empty
     *
     * @return the compaction pick of the first aggregate
     */
    static CompactionPick getSelected(List<CompactionAggregate> aggregates)
    {
        return aggregates.isEmpty() ? CompactionPick.EMPTY : aggregates.get(0).getSelected();
    }

    /**
     * Given a list of sstables, return their average size on disk.
     *
     * @param sstables the sstables
     * @return average sstable size on disk or zero.
     */
    static long getAvgSizeBytes(Iterable<SSTableReader> sstables)
    {
        long ret = 0;
        long num = 0;
        for (SSTableReader sstable : sstables)
        {
            ret += sstable.onDiskLength();
            num++;
        }

        return num > 0 ? ret / num : 0;
    }

    /**
     * Given a list of sstables, return their total size on disk.
     *
     * @param sstables the sstables
     * @return total sstable size on disk or zero.
     */
    static long getTotSizeBytes(Iterable<SSTableReader> sstables)
    {
        long ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.onDiskLength();

        return ret;
    }

    /**
     * Given a list of sstables, return their total read hotness.
     *
     * @param sstables the sstables
     * @return total read hotness or zero.
     */
    static double getTotHotness(Iterable<SSTableReader> sstables)
    {
        double ret = 0;
        for (SSTableReader sstable : sstables)
            ret += sstable.hotness();

        return ret;
    }
}
