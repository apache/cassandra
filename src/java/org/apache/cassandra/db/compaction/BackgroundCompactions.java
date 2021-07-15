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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.MovingAverage;

/**
 * A class for grouping the background compactions picked by a strategy, either pending or in progress.
 *
 * A compaction strategy has a {@link BackgroundCompactions} object as part of its state. Each
 * {@link LegacyAbstractCompactionStrategy} instance has its {@link BackgroundCompactions}, and their lifespans are the
 * same. In the case of {@link UnifiedCompactionStrategy} the new strategy instance inherits
 * {@link BackgroundCompactions} from its predecessor.
 */
public class BackgroundCompactions
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactions.class);

    /** The table metadata */
    private final TableMetadata metadata;

    /** The compaction aggregates with either pending or ongoing compactions, or both. This is a private map
     * whose access needs to be synchronized. */
    private final TreeMap<CompactionAggregate.Key, CompactionAggregate> aggregatesMap;

    /**
     * The current list of compaction aggregates, this list must be recreated every time the aggregates
     * map is changed.
     *
     * We publish aggregates to a separate variable instead of calling {@code aggregatesMap.values()} so that reads
     * that race with updates always observe a consistent snapshot.
     */
    private volatile List<CompactionAggregate> aggregates;

    /**  The ongoing compactions grouped by unique operation ID. */
    private final ConcurrentHashMap<UUID, CompactionPick> compactions = new ConcurrentHashMap<>();

    /**
     * Rate of progress (per thread) of recent compactions for the CFS. Used by the UnifiedCompactionStrategy to
     * limit the number of running compactions to no more than what is sufficient to saturate the throughput limit.
     * This needs to be a longer-running average to ensure that the rate limiter stalling a new thread can't cause
     * the compaction rate to temporarily drop to levels that permit an extra thread.
     */
    MovingAverage compactionRate = ExpMovingAverage.decayBy1000();

    BackgroundCompactions(ColumnFamilyStore cfs)
    {
        this.metadata = cfs.metadata();
        this.aggregatesMap = new TreeMap<>();
        this.aggregates = ImmutableList.of();
    }

    /**
     * Updates the list of pending compactions, while preserving the set of running ones. This is done
     * by creating new aggregates with the pending aggregates but adding any existing aggregates with
     * compactions in progress. If there is a matching pending aggregate then the existing compactions
     * are transferred to it, otherwise the old aggregate is stripped of its pending compactios and then
     * it is kept with the compactions in progress only.
     *
     * @param pending compaction aggregates with pending compactions
     */
    synchronized void setPending(CompactionStrategy strategy, Collection<? extends CompactionAggregate> pending)
    {
        if (pending == null)
            throw new IllegalArgumentException("argument cannot be null");

        if (logger.isTraceEnabled())
            logger.trace("Resetting pending aggregates for strategy {}/{}, received {} new aggregates",
                         strategy.getName(), strategy.hashCode(), pending.size());

        // First remove the existing aggregates
        aggregatesMap.clear();

        // Then add all the pending aggregates
        for (CompactionAggregate aggregate : pending)
        {
            CompactionAggregate prev = aggregatesMap.put(aggregate.getKey(), aggregate);
            if (logger.isTraceEnabled())
                logger.trace("Adding new pending aggregate: {}", aggregate);

            if (prev != null)
                throw new IllegalArgumentException("Received pending aggregates with non unique keys: " + prev.getKey());
        }

        // Then add the old aggregates but only if they have ongoing compactions
        for (CompactionAggregate oldAggregate : this.aggregates)
        {
            Collection<CompactionPick> compacting = oldAggregate.getInProgress();
            if (compacting.isEmpty())
            {
                if (logger.isTraceEnabled())
                    logger.trace("Existing aggregate {} has no in progress compactions, removing it", oldAggregate);

                continue;
            }

            // See if we have a matching aggregate in the pending aggregates, if so add all the existing compactions to it
            // otherwise strip the pending and selected compactions from the old one and keep it only with the compactions in progress
            CompactionAggregate newAggregate;
            CompactionAggregate matchingAggregate = oldAggregate.getMatching(aggregatesMap);
            if (matchingAggregate != null)
            {
                // add the old compactions to the new aggregate
                // the key will change slightly for STCS so remove it before adding it again
                aggregatesMap.remove(matchingAggregate.getKey());
                newAggregate = matchingAggregate.withAdditionalCompactions(compacting);

                if (logger.isTraceEnabled())
                    logger.trace("Removed matching aggregate {}", matchingAggregate);
            }
            else
            {
                // keep the old aggregate but only with the compactions already in progress and not yet completed
                newAggregate = oldAggregate.withOnlyTheseCompactions(compacting);

                if (logger.isTraceEnabled())
                    logger.trace("Keeping old aggregate but only with compactions {}", oldAggregate);
            }

            if (logger.isTraceEnabled())
                logger.trace("Adding new aggregate with previous compactions {}", newAggregate);

            aggregatesMap.put(newAggregate.getKey(), newAggregate);
        }

        // Publish the new aggregates
        this.aggregates = ImmutableList.copyOf(aggregatesMap.values());

        CompactionLogger compactionLogger = strategy.getCompactionLogger();
        if (compactionLogger != null && compactionLogger.enabled())
        {
            // compactionLogger.statistics(strategy, "pending", getStatistics()); // too much noise
            compactionLogger.pending(strategy, getEstimatedRemainingTasks());
        }
    }

    void setSubmitted(CompactionStrategy strategy, UUID id, CompactionAggregate aggregate)
    {
        if (id == null || aggregate == null)
            throw new IllegalArgumentException("arguments cannot be null");

        logger.debug("Submitting background compaction {}", id);
        CompactionPick compaction = aggregate.getSelected();

        CompactionPick prev = compactions.put(id, compaction);
        if (prev != null)
            throw new IllegalArgumentException("Found existing compaction with same id: " + id);

        compaction.setSubmitted(id);

        synchronized (this)
        {
            CompactionAggregate existingAggregate = aggregate.getMatching(aggregatesMap);
            boolean aggregatesMapChanged = false;

            if (existingAggregate == null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Could not find aggregate for compaction using the one passed in: {}", aggregate);

                aggregatesMapChanged = true;
                aggregatesMap.put(aggregate.getKey(), aggregate);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Found aggregate for compaction: {}", existingAggregate);

                if (!existingAggregate.getActive().contains(compaction))
                {
                    // add the compaction just submitted to the aggregate that was found but because for STCS its
                    // key may change slightly, first remove it
                    aggregatesMapChanged = true;
                    aggregatesMap.remove(existingAggregate.getKey());
                    CompactionAggregate newAggregate = existingAggregate.withAdditionalCompactions(ImmutableList.of(compaction));
                    aggregatesMap.put(newAggregate.getKey(), newAggregate);

                    if (logger.isTraceEnabled())
                        logger.trace("Added compaction to existing aggregate: {} -> {}", existingAggregate, newAggregate);
                }
                else
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Existing aggregate {} already had compaction", existingAggregate);
                }
            }

            // Publish the new aggregates if needed
            if (aggregatesMapChanged)
                this.aggregates = ImmutableList.copyOf(aggregatesMap.values());
        }

        CompactionLogger compactionLogger = strategy.getCompactionLogger();
        if (compactionLogger != null && compactionLogger.enabled())
            compactionLogger.statistics(strategy, "submitted", getStatistics(strategy));
    }

    public void onInProgress(CompactionProgress progress)
    {
        if (progress == null)
            throw new IllegalArgumentException("argument cannot be null");

        updateCompactionRate(progress);

        CompactionPick compaction = compactions.computeIfAbsent(progress.operationId(),
                                                                uuid -> CompactionPick.create(-1, progress.inSSTables()));

        logger.debug("Setting background compaction {} as in progress", progress.operationId());
        compaction.setProgress(progress);
    }

    public void onCompleted(CompactionStrategy strategy, UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("argument cannot be null");

        logger.debug("Removing compaction {}", id);

        // log the statistics before completing the compaction so that we see the stats for the
        // compaction that just completed
        CompactionLogger compactionLogger = strategy.getCompactionLogger();
        if (compactionLogger != null && compactionLogger.enabled())
            compactionLogger.statistics(strategy, "completed", getStatistics(strategy));

        CompactionPick completed = compactions.remove(id);
        CompactionProgress progress = completed.progress;
        updateCompactionRate(progress);

        if (completed != null)
            completed.setCompleted();

        // We rely on setPending() to refresh the aggregates again even though in some cases it may not be
        // called immediately (e.g. compactions disabled)
    }

    private void updateCompactionRate(CompactionProgress progress)
    {
        if (progress != null && progress.durationInNanos() > 0 && progress.outputDiskSize() > 0)
            compactionRate.update(progress.outputDiskSize() * 1.e9 / progress.durationInNanos());
    }

    public Collection<CompactionAggregate> getAggregates()
    {
        return aggregates;
    }

    /**
     * @return the number of background compactions estimated to still be needed
     */
    public int getEstimatedRemainingTasks()
    {
        return CompactionAggregate.numEstimatedCompactions(aggregates);
    }

    /**
     * @return the compactions currently in progress
     */
    public Collection<CompactionPick> getCompactionsInProgress()
    {
        return Collections.unmodifiableCollection(compactions.values());
    }

    /**
     * @return the total number of background compactions, pending or in progress
     */
    public int getTotalCompactions()
    {
        return compactions.size() + getEstimatedRemainingTasks();
    }

    /**
     * Return the compaction statistics for this strategy.
     *
     * @return statistics about this compaction strategy.
     */
    public CompactionStrategyStatistics getStatistics(CompactionStrategy strategy)
    {
        return CompactionAggregate.getStatistics(metadata, strategy, aggregates);
    }
}
