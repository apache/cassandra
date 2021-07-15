/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

/**
 * Pluggable compaction strategy determines how SSTables get merged.
 *
 * There are two main goals:
 *  - perform background compaction constantly as needed; this typically makes a tradeoff between
 *    i/o done by compaction, and merging done at read time.
 *  - perform a full (maximum possible) compaction if requested by the user
 */
abstract class LegacyAbstractCompactionStrategy extends AbstractCompactionStrategy
{
    protected LegacyAbstractCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options)
    {
        super(factory, new BackgroundCompactions(factory.getCfs()), options);
        assert factory != null;
    }

    /**
     * Helper base class for strategies that provide CompactionAggregates, implementing the typical
     * getNextBackgroundTasks logic based on a getNextBackgroundAggregate method.
     */
    protected static abstract class WithAggregates extends LegacyAbstractCompactionStrategy
    {
        protected WithAggregates(CompactionStrategyFactory factory, Map<String, String> options)
        {
            super(factory, options);
        }

        @Override
        @SuppressWarnings("resource")
        public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
        {
            CompactionPick previous = null;
            while (true)
            {
                CompactionAggregate compaction = getNextBackgroundAggregate(gcBefore);
                if (compaction == null || compaction.isEmpty())
                    return ImmutableList.of();

                // Already tried acquiring references without success. It means there is a race with
                // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
                if (compaction.getSelected().equals(previous))
                {
                    logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                                "unless it happens frequently, in which case it must be reported. Will retry later.",
                                compaction.getSelected());
                    return ImmutableList.of();
                }

                LifecycleTransaction transaction = dataTracker.tryModify(compaction.getSelected().sstables, OperationType.COMPACTION);
                if (transaction != null)
                {
                    backgroundCompactions.setSubmitted(this, transaction.opId(), compaction);
                    return ImmutableList.of(createCompactionTask(gcBefore, transaction, compaction));
                }

                // Getting references to the sstables failed. This may be because we tried to compact sstables that are
                // no longer present (due to races in getting the notification), or because we still haven't
                // received any replace notifications. Remove any non-live sstables we track and try again.
                removeDeadSSTables();

                previous = compaction.getSelected();
            }
        }

        /**
         * Select the next compaction to perform. This method is typically synchronized.
         */
        protected abstract CompactionAggregate getNextBackgroundAggregate(int gcBefore);

        protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, CompactionAggregate compaction)
        {
            return new CompactionTask(cfs, txn, gcBefore, false, this);
        }

        /**
         * Get the estimated remaining compactions. Strategies that implement {@link WithAggregates} can delegate this
         * to {@link BackgroundCompactions} because they set the pending aggregates as background compactions but legacy
         * strategies that do not support aggregates must implement this method.
         * <p/>
         * @return the number of background tasks estimated to still be needed for this strategy
         */
        @Override
        public int getEstimatedRemainingTasks()
        {
            return backgroundCompactions.getEstimatedRemainingTasks();
        }
    }

    /**
     * Helper base class for (older, deprecated) strategies that provide a list of tables to compact, implementing the
     * typical getNextBackgroundTask logic based on a getNextBackgroundSSTables method.
     */
    protected static abstract class WithSSTableList extends LegacyAbstractCompactionStrategy
    {
        protected WithSSTableList(CompactionStrategyFactory factory, Map<String, String> options)
        {
            super(factory, options);
        }

        @Override
        @SuppressWarnings("resource")
        public Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
        {
            List<SSTableReader> previousCandidate = null;
            while (true)
            {
                List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

                if (latestBucket.isEmpty())
                    return ImmutableList.of();

                // Already tried acquiring references without success. It means there is a race with
                // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
                if (latestBucket.equals(previousCandidate))
                {
                    logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                                "unless it happens frequently, in which case it must be reported. Will retry later.",
                                latestBucket);
                    return ImmutableList.of();
                }

                LifecycleTransaction modifier = dataTracker.tryModify(latestBucket, OperationType.COMPACTION);
                if (modifier != null)
                    return ImmutableList.of(createCompactionTask(gcBefore, modifier, false, false));

                // Getting references to the sstables failed. This may be because we tried to compact sstables that are
                // no longer present (due to races in getting the notification), or because we still haven't
                // received any replace notifications. Remove any non-live sstables we track and try again.
                removeDeadSSTables();

                previousCandidate = latestBucket;
            }
        }

        /**
         * Select the next tables to compact. This method is typically synchronized.
         */
        protected abstract List<SSTableReader> getNextBackgroundSSTables(final int gcBefore);
    }

    /**
     * Replaces sstables in the compaction strategy
     *
     * Note that implementations must be able to handle duplicate notifications here (that removed are already gone and
     * added have already been added)
     * */
    public abstract void replaceSSTables(Collection<SSTableReader> removed, Collection<SSTableReader> added);

    /**
     * Adds sstable, note that implementations must handle duplicate notifications here (added already being in the compaction strategy)
     */
    abstract void addSSTable(SSTableReader added);

    /**
     * Adds sstables, note that implementations must handle duplicate notifications here (added already being in the compaction strategy)
     */
    public synchronized void addSSTables(Iterable<SSTableReader> added)
    {
        for (SSTableReader sstable : added)
            addSSTable(sstable);
    }

    /**
     * Removes sstable from the strategy, implementations must be able to handle the sstable having already been removed.
     */
    abstract void removeSSTable(SSTableReader sstable);

    /**
     * Removes sstables from the strategy, implementations must be able to handle the sstables having already been removed.
     */
    public void removeSSTables(Iterable<SSTableReader> removed)
    {
        for (SSTableReader sstable : removed)
            removeSSTable(sstable);
    }

    /**
     * Remove any tracked sstable that is no longer in the live set. Note that because we get notifications after the
     * tracker is modified, anything we know of must be already in the live set. If it is not, it has been removed
     * from there, and we either haven't received the removal notification yet, or we did and we messed it up (i.e.
     * we got it before the addition). The former is transient, but the latter can cause persistent problems, including
     * fully stopping compaction. In any case, we should remove any such sstables.
     * There is a special-case implementation of this in LeveledManifest.
     */
    abstract void removeDeadSSTables();

    void removeDeadSSTables(Iterable<SSTableReader> sstables)
    {
        synchronized (sstables)
        {
            int removed = 0;
            Set<SSTableReader> liveSet = cfs.getLiveSSTables();
            for (Iterator<SSTableReader> it = sstables.iterator(); it.hasNext(); )
            {
                SSTableReader sstable = it.next();
                if (!liveSet.contains(sstable))
                {
                    it.remove();
                    ++removed;
                }
            }

            if (removed > 0)
                logger.debug("Removed {} dead sstables from the compactions tracked list.", removed);
        }
    }

    public synchronized CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        removeDeadSSTables();
        return super.getMaximalTasks(gcBefore, splitOutput);
    }

    /**
     * Called when the metadata has changed for an sstable - for example if the level changed
     *
     * Not called when repair status changes (which is also metadata), because this results in the
     * sstable getting removed from the compaction strategy instance.
     *
     * This is only needed by the LCS manifest from what I could see.
     */
    void metadataChanged(StatsMetadata oldMetadata, SSTableReader sstable)
    {
    }

    /**
     * Select a table for tombstone-removing compaction from the given set. Returns null if no table is suitable.
     */
    @Nullable
    CompactionAggregate makeTombstoneCompaction(int gcBefore,
                                                Iterable<SSTableReader> candidates,
                                                Function<Collection<SSTableReader>, SSTableReader> selector)
    {
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : candidates)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return null;

        final SSTableReader sstable = selector.apply(sstablesWithTombstones);
        return CompactionAggregate.createForTombstones(sstable);
    }

    /**
     * Check if given sstable is worth dropping tombstones at gcBefore.
     * Check is skipped if tombstone_compaction_interval time does not elapse since sstable creation and returns false.
     *
     * @param sstable SSTable to check
     * @param gcBefore time to drop tombstones
     * @return true if given sstable's tombstones are expected to be removed
     */
    protected boolean worthDroppingTombstones(SSTableReader sstable, int gcBefore)
    {
        if (options.isDisableTombstoneCompactions() || CompactionController.NEVER_PURGE_TOMBSTONES || cfs.getNeverPurgeTombstones())
            return false;
        // since we use estimations to calculate, there is a chance that compaction will not drop tombstones actually.
        // if that happens we will end up in infinite compaction loop, so first we check enough if enough time has
        // elapsed since SSTable created.
        if (System.currentTimeMillis() < sstable.getCreationTimeFor(Component.DATA) + options.getTombstoneCompactionInterval() * 1000)
            return false;

        double droppableRatio = sstable.getEstimatedDroppableTombstoneRatio(gcBefore);
        if (droppableRatio <= options.getTombstoneThreshold())
            return false;

        //sstable range overlap check is disabled. See CASSANDRA-6563.
        if (options.isUncheckedTombstoneCompaction())
            return true;

        Collection<SSTableReader> overlaps = cfs.getOverlappingLiveSSTables(Collections.singleton(sstable));
        if (overlaps.isEmpty())
        {
            // there is no overlap, tombstones are safely droppable
            return true;
        }
        else if (CompactionController.getFullyExpiredSSTables(cfs, Collections.singleton(sstable), overlaps, gcBefore).size() > 0)
        {
            return true;
        }
        else
        {
            // what percentage of columns do we expect to compact outside of overlap?
            if (sstable.getIndexSummarySize() < 2)
            {
                // we have too few samples to estimate correct percentage
                return false;
            }
            // first, calculate estimated keys that do not overlap
            long keys = sstable.estimatedKeys();
            Set<Range<Token>> ranges = new HashSet<Range<Token>>(overlaps.size());
            for (SSTableReader overlap : overlaps)
                ranges.add(new Range<>(overlap.first.getToken(), overlap.last.getToken()));
            long remainingKeys = keys - sstable.estimatedKeysForRanges(ranges);
            // next, calculate what percentage of columns we have within those keys
            long columns = sstable.getEstimatedCellPerPartitionCount().mean() * remainingKeys;
            double remainingColumnsRatio = ((double) columns) / (sstable.getEstimatedCellPerPartitionCount().count() * sstable.getEstimatedCellPerPartitionCount().mean());

            // return if we still expect to have droppable tombstones in rest of columns
            return remainingColumnsRatio * droppableRatio > options.getTombstoneThreshold();
        }
    }
}
