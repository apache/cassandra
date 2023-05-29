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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /** The unique key that identifies this aggregate. */
    final Key key;

    /** The sstables in this aggregate, whether they are compaction candidates or not */
    final Set<CompactionSSTable> sstables;

    /** The compaction that was selected for this aggregate when it was created. It is also part of {@link this#compactions}. */
    final CompactionPick selected;

    /** The compactions that are part of this aggregate, they could be pending or in progress. */
    final LinkedHashSet<CompactionPick> compactions;

    CompactionAggregate(Key key, Iterable<? extends CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> pending)
    {
        if (sstables == null || selected == null || pending == null)
            throw new IllegalArgumentException("Arguments cannot be null");

        this.key = key;
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
     * @return the total sstable size for all the compaction picks that are either pending or still in progress
     */
    public long getPendingBytes()
    {
        long ret = 0;
        for (CompactionPick comp : compactions)
        {
            if (!comp.submitted())
                ret += comp.totSizeInBytes();
        }
        return ret;
    }

    /**
     * @return compactions that have not yet been submitted (no compaction id).
     */
    public List<CompactionPick> getPending()
    {
        List<CompactionPick> ret = new ArrayList<>(compactions.size());
        for (CompactionPick comp : compactions)
        {
            if (!comp.submitted())
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
            if (comp.submitted() && !comp.completed())
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
     * Calculates basic compaction statistics, common for all types of {@link CompactionAggregate}s.
     *
     * @param trackHotness Indicates whether aggregate (tier/bucket) hotness is relevant and should be calculated.
     *                     If this is {@code false}, a default value of {@link Double#NaN} will be used to indicate
     *                     that hotness hasn't been calculated.
     *
     * @return a new {@link CompactionAggregateStatistics} instance, containing all the common statistics for the
     *         different types of {@link CompactionAggregate}s (see above for the caveat about hotness).
     */
    CompactionAggregateStatistics getCommonStatistics(boolean trackHotness)
    {
        int numCompactions = 0;
        int numCompactionsInProgress = 0;
        int numCandidateSSTables = 0;
        int numCompactingSSTables = 0;
        int numExpiredSSTables = 0;
        long tot = 0;
        long expiredTot = 0;
        double hotness = trackHotness ? 0.0 : Double.NaN;
        long read = 0;
        long written = 0;
        double readThroughput = 0;
        double writeThroughput = 0;

        for (CompactionPick compaction : compactions)
        {
            if (compaction.completed())
                continue;

            numCompactions++;
            numCandidateSSTables += compaction.sstables().size();
            numExpiredSSTables += compaction.expired().size();
            tot += compaction.sstables().stream().mapToLong(CompactionSSTable::uncompressedLength).reduce(0L, Long::sum);
            expiredTot += compaction.expired().stream().mapToLong(CompactionSSTable::uncompressedLength).reduce(0L, Long::sum);
            if (trackHotness)
                hotness += compaction.hotness();

            if (compaction.submitted())
            {
                numCompactionsInProgress++;
                numCompactingSSTables += compaction.sstables().size();
            }

            if (compaction.inProgress())
            {
                final CompactionProgress progress = compaction.progress();
                read += progress.uncompressedBytesRead();
                written += progress.uncompressedBytesWritten();
                readThroughput += progress.readThroughput();
                writeThroughput += progress.writeThroughput();
            }
        }

        return new CompactionAggregateStatistics(numCompactions,
                                                 numCompactionsInProgress,
                                                 sstables.size(),
                                                 numExpiredSSTables,
                                                 numCandidateSSTables,
                                                 numCompactingSSTables,
                                                 getTotSizeBytes(sstables),
                                                 tot,
                                                 expiredTot,
                                                 read,
                                                 written,
                                                 readThroughput,
                                                 writeThroughput,
                                                 hotness);
    }

    /**
     * @return the number of estimated compactions that are still pending.
     */
    public int numEstimatedCompactions()
    {
        return getPending().size();
    }

    /**
     * @return a key that ensures the uniqueness of an aggregate but also that allows identify future identical aggregates,
     *         e.g. when an aggregate is merged with an older aggregate that has still ongoing compactions like a level
     *         in LCS or a bucket in the unified strategy or STCS or a time window in TWCS
     */
    public Key getKey()
    {
        return key;
    }

    /**
     * Return a matching aggregate from the map passed in or null. Normally this is just a matter of finding
     * the key in the map but for STCS we need to look at the possible min and maximum average sizes and so
     * {@link SizeTiered} overrides this method.
     *
     * @param others a map of other aggregates
     *
     * @return an aggregate with the same key or null
     */
    @Nullable CompactionAggregate getMatching(NavigableMap<Key, CompactionAggregate> others)
    {
        return others.get(getKey());
    }

    /**
     * Create a copy of this aggregate with the new parameters
     *
     * @return a deep copy of this aggregate
     */
    protected abstract CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions);

    /**
     * Add expired sstables to the selected compaction pick and return a new compaction aggregate.
     */
    CompactionAggregate withExpired(Collection<CompactionSSTable> expired)
    {
       return clone(Iterables.concat(sstables, expired), selected.withExpiredSSTables(expired), compactions);
    }

    /**
     * Check if this aggregate compactions contain the compaction passed in. Here we're looking for
     * the exact same instance, not just a compaction that is equal to it.
     *
     * @param compaction the compaction to check if it can be found
     *
     * @return a pair containing the result on the left (true if the compaction is found, false otherwise), and
     * a matching compaction on the right (any compaction that is equal, including the same instance).
     */
    public Pair<Boolean, CompactionPick> containsSameInstance(CompactionPick compaction)
    {
        List<CompactionPick> activeCompactions = getActive();
        int existingCompactionIdx = activeCompactions.indexOf(compaction);
        CompactionPick existingCompaction = existingCompactionIdx == -1 ? null : activeCompactions.get(existingCompactionIdx);
        boolean containsSameInstance = existingCompaction != null && existingCompaction == compaction;
        return Pair.create(containsSameInstance, existingCompaction);
    }
    
    /**
     * Replace an existing compaction pick with a new one, this is used by CNDB because it creates new
     * compactions from etcd. If the existing compaction is null, simply add the replacement.
     */
    public CompactionAggregate withReplacedCompaction(CompactionPick replacement, @Nullable CompactionPick existing)
    {
        Preconditions.checkArgument(existing == null || this.compactions.contains(existing), "Expected existing to be part of compactions");
        if (existing == null)
            return withAdditionalCompactions(ImmutableList.of(replacement));

        List<CompactionSSTable> sstables = new ArrayList<>(this.sstables.size());
        LinkedHashSet<CompactionPick> compactions = new LinkedHashSet<>(this.compactions.size());
        for (CompactionPick comp : this.compactions)
        {
            if (comp == existing)
            {
                compactions.add(replacement);
                sstables.addAll(replacement.sstables());
            }
            else
            {
                compactions.add(comp);
                sstables.addAll(comp.sstables());
            }
        }

        return clone(sstables, existing == selected ? replacement : selected, compactions);
    }

    /**
     * Add existing compactions to our own compactions and return a new compaction aggregate
     */
    public CompactionAggregate withAdditionalCompactions(Collection<CompactionPick> comps)
    {
        List<CompactionSSTable> added = comps.stream().flatMap(comp -> comp.sstables().stream()).collect(Collectors.toList());
        return clone(Iterables.concat(sstables, added), selected, Iterables.concat(compactions, comps));
    }

    /**
     * Only keep the compactions passed in, strip everything else.
     */
    public CompactionAggregate withOnlyTheseCompactions(Collection<CompactionPick> comps)
    {
        List<CompactionSSTable> retained = comps.stream().flatMap(comp -> comp.sstables().stream()).collect(Collectors.toList());
        return clone(retained, CompactionPick.EMPTY, comps);
    }

    /**
     * Merge an aggregate with another one with the same key.
     */
    protected CompactionAggregate mergeWith(CompactionAggregate other)
    {
        return withAdditionalCompactions(other.compactions);
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

        /**
         * How many more compactions this level is expected to perform. This is required because for LCS we cannot
         * easily identify candidate sstables to put into the pending picks.
         */
        final int pendingCompactions;

        /** The fanout size */
        final int fanout;

        Leveled(Iterable<? extends CompactionSSTable> sstables,
                CompactionPick selected,
                Iterable<CompactionPick> compactions,
                int level,
                int nextLevel,
                double score,
                long maxSSTableBytes,
                int pendingCompactions,
                int fanout)
        {
            super(new Key(level), sstables, selected, compactions);

            this.level = level;
            this.nextLevel = nextLevel;
            this.score = score;
            this.maxSSTableBytes = maxSSTableBytes;
            this.pendingCompactions = pendingCompactions;
            this.fanout = fanout;
        }

        @Override
        protected CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new Leveled(sstables, selected, compactions, level, nextLevel, score, maxSSTableBytes, pendingCompactions, fanout);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            CompactionAggregateStatistics stats = getCommonStatistics(false);

            long readLevel = 0L;

            for (CompactionPick compaction : compactions)
                if (!compaction.completed() && compaction.inProgress())
                    readLevel += compaction.progress().uncompressedBytesRead(level);

            return new LeveledCompactionStatistics(stats, level, score, pendingCompactions, readLevel);
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
        public String toString()
        {
            return String.format("Level %d with %d sstables, %d compactions and %d pending", level, sstables.size(), compactions.size(), pendingCompactions);
        }
    }

    /**
     * Create a level where we have a compaction candidate.
     */
    static CompactionAggregate.Leveled createLeveled(Collection<? extends CompactionSSTable> all,
                                                     Collection<? extends CompactionSSTable> candidates,
                                                     int pendingCompactions,
                                                     long maxSSTableBytes,
                                                     int level,
                                                     int nextLevel,
                                                     double score,
                                                     int fanout)
    {
        return new Leveled(all,
                           CompactionPick.create(level, candidates),
                           ImmutableList.of(),
                           level,
                           nextLevel,
                           score,
                           maxSSTableBytes,
                           pendingCompactions,
                           fanout);
    }

    /**
     * Create a level when we only have estimated tasks.
     */
    static CompactionAggregate.Leveled createLeveled(Collection<? extends CompactionSSTable> all,
                                                     int pendingCompactions,
                                                     long maxSSTableBytes,
                                                     int level,
                                                     double score,
                                                     int fanout)
    {
        return new Leveled(all,
                           CompactionPick.EMPTY,
                           ImmutableList.of(),
                           level,
                           level + 1,
                           score,
                           maxSSTableBytes,
                           pendingCompactions,
                           fanout);
    }

    /**
     * Create a leveled aggregate when LCS is doing STCS on level 0
     */
    static CompactionAggregate.Leveled createLeveledForSTCS(Collection<? extends CompactionSSTable> all,
                                                            CompactionPick pick,
                                                            int pendingCompactions,
                                                            double score,
                                                            int fanout)
    {
        return new Leveled(all,
                           pick,
                           ImmutableList.of(),
                           0,
                           0,
                           score,
                           Long.MAX_VALUE,
                           pendingCompactions,
                           fanout);
    }

    /**
     * Contains information about a size-tiered compaction aggregate, this is equivalent to a bucket in {@link SizeTieredCompactionStrategy}.
     */
    public static final class SizeTiered extends CompactionAggregate
    {
        /** The total read hotness of the sstables in this tier, as defined by {@link CompactionSSTable#hotness()} */
        final double hotness;

        /** The average on disk size in bytes of the sstables in this tier */
        final long avgSizeBytes;

        /** The minimum on disk size in bytes for this tier, this is normally the avg size times the STCS bucket low and it is
         * used to find compacting aggregates that are on the same tier. */
        final long minSizeBytes;

        /** The maximum on disk size in bytes for this tier, this is normally the avg size times the STCS bucket high and it is
         * used to find compacting aggregates that are on the same tier. */
        final long maxSizeBytes;

        SizeTiered(Iterable<? extends CompactionSSTable> sstables,
                   CompactionPick selected,
                   Iterable<CompactionPick> pending,
                   double hotness,
                   long avgSizeBytes,
                   long minSizeBytes,
                   long maxSizeBytes)
        {
            super(new Key(avgSizeBytes), sstables, selected, pending);

            this.hotness = hotness;
            this.avgSizeBytes = avgSizeBytes;
            this.minSizeBytes = minSizeBytes;
            this.maxSizeBytes = maxSizeBytes;
        }

        @Override
        protected CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new SizeTiered(sstables, selected, compactions, getTotHotness(sstables), getAvgSizeBytes(sstables), minSizeBytes, maxSizeBytes);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            CompactionAggregateStatistics stats = getCommonStatistics(true);

            return new SizeTieredCompactionStatistics(stats, avgSizeBytes);
        }

        @Override
        @Nullable CompactionAggregate getMatching(NavigableMap<Key, CompactionAggregate> others)
        {
            SortedMap<Key, CompactionAggregate> subMap = others.subMap(new Key(minSizeBytes), new Key(maxSizeBytes));
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

            Key closest = null;
            long minDiff = 0;
            for (Key m : subMap.keySet())
            {
                long diff = Math.abs(m.index - avgSizeBytes);
                if (closest == null || diff < minDiff)
                {
                    closest = m;
                    minDiff = diff;
                }
            }

            if (logger.isTraceEnabled())
                logger.trace("Using closest matching aggregate for {}: {}",
                             FBUtilities.prettyPrintMemory(avgSizeBytes),
                             FBUtilities.prettyPrintMemory(closest != null ? closest.index : -1));

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

    static CompactionAggregate createSizeTiered(Collection<? extends CompactionSSTable> all,
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

        TimeTiered(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> pending, long timestamp)
        {
            super(new Key(timestamp), sstables, selected, pending);
            this.timestamp = timestamp;
        }

        @Override
        protected CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new TimeTiered(sstables, selected, compactions, timestamp);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            CompactionAggregateStatistics stats = getCommonStatistics(true);
            return new TimeTieredCompactionStatistics(stats, timestamp);
        }

        @Override
        public String toString()
        {
            return String.format("Time tiered %d with %d sstables, %d compactions", timestamp, sstables.size(), compactions.size());
        }
    }

    static CompactionAggregate createTimeTiered(Collection<CompactionSSTable> sstables, long timestamp)
    {
        return new TimeTiered(sstables, CompactionPick.create(timestamp, sstables), ImmutableList.of(), timestamp);
    }

    static CompactionAggregate createTimeTiered(Collection<CompactionSSTable> sstables, CompactionPick selected, List<CompactionPick> pending, long timestamp)
    {
        return new TimeTiered(sstables, selected, pending, timestamp);
    }

    public static class UnifiedAggregate extends CompactionAggregate
    {
        /** The arena to which this level belongs */
        private final UnifiedCompactionStrategy.Arena arena;

        /** The level generated by the compaction strategy */
        private final UnifiedCompactionStrategy.Level level;

        /** The maximum number of overlapping tables in the level. */
        private final int maxOverlap;

        UnifiedAggregate(Iterable<? extends CompactionSSTable> sstables,
                         int maxOverlap,
                         CompactionPick selected,
                         Iterable<CompactionPick> pending,
                         UnifiedCompactionStrategy.Arena arena,
                         UnifiedCompactionStrategy.Level level)
        {
            super(new ArenaedKey(arena, level.index), sstables, selected, pending);
            this.maxOverlap = maxOverlap;
            this.arena = arena;
            this.level = level;
        }

        public UnifiedCompactionStrategy.Arena getArena()
        {
            return arena;
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            CompactionAggregateStatistics stats = getCommonStatistics(false);

            return new UnifiedCompactionStatistics(stats,
                                                   level.index,
                                                   level.survivalFactor,
                                                   level.scalingParameter,
                                                   level.min,
                                                   level.max,
                                                   maxOverlap,
                                                   arena.name());
        }

        @Override
        protected CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new UnifiedAggregate(sstables, maxOverlap, selected, compactions, arena, level);
        }

        @Override
        protected CompactionAggregate mergeWith(CompactionAggregate other)
        {
            return new UnifiedAggregate(Iterables.concat(sstables, other.sstables),
                                        Math.max(maxOverlap, ((UnifiedAggregate) other).maxOverlap),
                                        selected,
                                        Iterables.concat(compactions, other.compactions),
                                        arena,
                                        level);
        }

        public int bucketIndex()
        {
            return level.index;
        }

        public double bucketMin()
        {
            return level.min;
        }

        public int maxOverlap()
        {
            return maxOverlap;
        }

        @Override
        public String toString()
        {
            return String.format("Unified arena %s level %d with %d sstables (max overlap %d) and %d compactions",
                                 arena.name(),
                                 level.index,
                                 sstables.size(),
                                 maxOverlap,
                                 compactions.size());
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this)
                return true;

            if (!(obj instanceof UnifiedAggregate))
                return false;

            UnifiedAggregate that = (UnifiedAggregate) obj;
            return sstables.equals(that.sstables) &&
                   selected.equals(that.selected) &&
                   compactions.equals(that.compactions) &&
                   level.equals(that.level) &&
                   arena.equals(that.arena);
            // no need to compare maxOverlap, that's a feature of sstables
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sstables, selected, compactions, level, arena);
        }
    }

    public static UnifiedAggregate createUnified(Collection<? extends CompactionSSTable> sstables,
                                                 int maxOverlap,
                                                 CompactionPick selected,
                                                 Iterable<CompactionPick> pending,
                                                 UnifiedCompactionStrategy.Arena arena,
                                                 UnifiedCompactionStrategy.Level level)
    {
        return new UnifiedAggregate(sstables, maxOverlap, selected, pending, arena, level);
    }



    /** An aggregate that is created for a compaction issued only to drop tombstones */
    public static final class TombstoneAggregate extends CompactionAggregate
    {
        TombstoneAggregate(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> pending)
        {
            super(new Key(-1), sstables, selected, pending);
        }

        @Override
        protected CompactionAggregate clone(Iterable<CompactionSSTable> sstables, CompactionPick selected, Iterable<CompactionPick> compactions)
        {
            return new TombstoneAggregate(sstables, selected, compactions);
        }

        @Override
        public CompactionAggregateStatistics getStatistics()
        {
            return getCommonStatistics(false);
        }

        @Override
        public String toString()
        {
            return String.format("Tombstones with %d sstables, %d compactions", sstables.size(), compactions.size());
        }
    }

    static CompactionAggregate createForTombstones(CompactionSSTable sstable)
    {
        List<CompactionSSTable> sstables = ImmutableList.of(sstable);
        CompactionPick comp = CompactionPick.create(-1, sstables);
        return new TombstoneAggregate(sstables, comp, ImmutableList.of());
    }

    /**
     * A key suitable for a strategy that has no arenas, that is a legacy strategy that is
     * managed by CompactionStrategyManager.
     */
    public static class Key implements Comparable<Key>
    {
        protected final long index;

        Key(long index)
        {
            this.index = index;
        }

        @Override
        public int compareTo(Key key)
        {
            return Long.compare(index, key.index);
        }

        @Override
        public String toString()
        {
            return Long.toString(index);
        }
    }

    /**
     * A key suitable for a strategy using arenas, first it compares by arena, and then by level index.
     */
    private static final class ArenaedKey extends Key
    {
        private final UnifiedCompactionStrategy.Arena arena;

        ArenaedKey(UnifiedCompactionStrategy.Arena arena, long index)
        {
            super(index);
            this.arena = arena;
        }

        @Override
        public int compareTo(Key key)
        {
            if (key instanceof ArenaedKey)
            {
                ArenaedKey arenaedKey = (ArenaedKey) key;

                int ret = arena.compareTo(arenaedKey.arena);
                if (ret != 0)
                    return ret;
            }

            // either not arenaed or same arena
            return Long.compare(index, key.index);
        }

        @Override
        public String toString()
        {
            return index + "-" + arena;
        }
    }

    /**
     * Return the compaction statistics for this strategy and list of compactions that are either pending or in progress.
     *
     * @param aggregates the compaction aggregates
     *
     * @return the statistics about this compactions
     */
    static CompactionStrategyStatistics getStatistics(TableMetadata metadata,
                                                      CompactionStrategy strategy,
                                                      Collection<CompactionAggregate> aggregates)
    {
        List<CompactionAggregateStatistics> statistics = new ArrayList<>(aggregates.size());

        for (CompactionAggregate aggregate : aggregates)
            statistics.add(aggregate.getStatistics());

        return new CompactionStrategyStatistics(metadata, strategy.getClass().getSimpleName(), statistics);
    }

    /**
     * Return the number of compactions that are still pending;
     * @param aggregates the compaction aggregates
     *
     * @return the number of compactions that are still pending (net yet submitted)
     */
    static int numEstimatedCompactions(Collection<CompactionAggregate> aggregates)
    {
        int ret = 0;
        for (CompactionAggregate aggregate : aggregates)
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
    static long getAvgSizeBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long ret = 0;
        long num = 0;
        for (CompactionSSTable sstable : sstables)
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
    static long getTotSizeBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long ret = 0;
        for (CompactionSSTable sstable : sstables)
            ret += sstable.onDiskLength();

        return ret;
    }

    /**
     * Given a list of sstables, return their total read hotness.
     *
     * @param sstables the sstables
     * @return total read hotness or zero.
     */
    static double getTotHotness(Iterable<? extends CompactionSSTable> sstables)
    {
        double ret = 0;
        for (CompactionSSTable sstable : sstables)
            ret += sstable.hotness();

        return ret;
    }
}
