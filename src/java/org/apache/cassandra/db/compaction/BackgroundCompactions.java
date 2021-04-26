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
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A class for grouping the background compactions picked by a strategy, either pending or in progress.
 */
class BackgroundCompactions implements CompactionObserver
{
    private static final Logger logger = LoggerFactory.getLogger(BackgroundCompactions.class);

    /** The parent strategy */
    private final AbstractCompactionStrategy strategy;

    /** The table metadata */
    private final TableMetadata metadata;

    /** The compaction logger */
    private final CompactionLogger compactionLogger;

    /** The compaction aggregates with either pending or ongoing compactions, or both. */
    private volatile TreeMap<Long, CompactionAggregate> aggregates = new TreeMap<>();

    /**  The ongoing compactions grouped by unique operation ID. */
    private ConcurrentHashMap<UUID, CompactionPick> compactions = new ConcurrentHashMap<>();

    BackgroundCompactions(AbstractCompactionStrategy strategy, ColumnFamilyStore cfs)
    {
        if (cfs.getCompactionStrategyManager() == null)
            throw new IllegalStateException("Compaction strategy manager should be set in the CFS first");

        this.strategy = strategy;
        this.metadata = cfs.metadata();
        this.compactionLogger = cfs.getCompactionStrategyManager().compactionLogger();
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
    synchronized void setPending(List<CompactionAggregate> pending)
    {
        if (pending == null)
            throw new IllegalArgumentException("argument cannot be null");

        if (logger.isTraceEnabled())
            logger.trace("Resetting pending aggregates for strategy {}/{}, received {} new aggregates",
                         strategy.getName(), strategy.hashCode(), pending.size());

        // First create a new map with all the pending aggregates
        TreeMap<Long, CompactionAggregate> aggregates = new TreeMap();
        for (CompactionAggregate aggregate : pending)
        {
            CompactionAggregate prev = aggregates.put(aggregate.getKey(), aggregate);
            if (logger.isTraceEnabled())
                logger.trace("Adding new pending aggregate: {}", aggregate);

            if (prev != null)
                throw new IllegalArgumentException("Received pending aggregates with non unique keys: " + prev.getKey());
        }

        // Then add the current aggregates with ongoing compactions
        for (CompactionAggregate oldAggregate : this.aggregates.values())
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
            CompactionAggregate matchingAggregate = oldAggregate.getMatching(aggregates);
            if (matchingAggregate != null)
            {
                // add the old compactions to the new aggregate
                // the key will change slightly for STCS so remove it before adding it again
                aggregates.remove(matchingAggregate.getKey());
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

            aggregates.put(newAggregate.getKey(), newAggregate);
        }

        // Finally publish the new aggregates
        this.aggregates = aggregates;

        if (compactionLogger != null && compactionLogger.enabled())
        {
            compactionLogger.pending(strategy, getEstimatedRemainingTasks());
            compactionLogger.statistics(strategy, "pending", getStatistics());
        }
    }

    @Override
    public void setSubmitted(UUID id, CompactionAggregate aggregate)
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
            CompactionAggregate existingAggregate = aggregate.getMatching(aggregates);

            if (existingAggregate == null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Could not find aggregate for compaction using the one passed in: {}", aggregate);

                aggregates.put(aggregate.getKey(), aggregate);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Found aggregate for compaction: {}", existingAggregate);

                if (!existingAggregate.getActive().contains(compaction))
                {
                    // add the compaction just submitted to the aggregate that was found but because for STCS its
                    // key may change slightly, first remove it
                    aggregates.remove(existingAggregate.getKey());
                    CompactionAggregate newAggregate = existingAggregate.withAdditionalCompactions(ImmutableList.of(compaction));
                    aggregates.put(newAggregate.getKey(), newAggregate);

                    if (logger.isTraceEnabled())
                        logger.trace("Added compaction to existing aggregate: {} -> {}", existingAggregate, newAggregate);
                }
                else
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Existing aggregate {} already had compaction", existingAggregate);
                }
            }
        }

        if (compactionLogger != null && compactionLogger.enabled())
            compactionLogger.statistics(strategy, "submitted", getStatistics());
    }

    @Override
    public void setInProgress(CompactionProgress progress)
    {
        if (progress == null)
            throw new IllegalArgumentException("argument cannot be null");

        CompactionPick compaction = compactions.computeIfAbsent(progress.operationId(),
                                                                uuid -> CompactionPick.create(-1, progress.inSSTables()));

        logger.debug("Setting background compaction {} as in progress", progress.operationId());
        compaction.setProgress(progress);
    }

    @Override
    public void setCompleted(UUID id)
    {
        if (id == null)
            throw new IllegalArgumentException("argument cannot be null");

        logger.debug("Removing compaction {}", id);

        // log the statistics before completing the compaction so that we see the stats for the
        // compaction that just completed
        if (compactionLogger != null && compactionLogger.enabled())
            compactionLogger.statistics(strategy, "completed", getStatistics());

        CompactionPick completed = compactions.remove(id);
        if (completed != null)
            completed.setCompleted();

        // We rely on setPending() to refresh the aggregates again even though in some cases it may not be
        // called immediately (e.g. compactions disabled)
    }

    /**
     * @return the number of background compactions estimated to still be needed
     */
    public int getEstimatedRemainingTasks()
    {
        return CompactionAggregate.numEstimatedCompactions(aggregates);
    }

    /**
     * @return the number of compactions currently in progress
     */
    public int getCompactionsInProgress()
    {
        return compactions.size();
    }

    /**
     * @return the total number of background compactions, pending or in progress
     */
    public int getTotalCompactions()
    {
        return getCompactionsInProgress() + getEstimatedRemainingTasks();
    }

    /**
     * Return the compaction statistics for this strategy.
     *
     * @return statistics about this compaction strategy.
     */
    public CompactionStrategyStatistics getStatistics()
    {
        return CompactionAggregate.getStatistics(metadata, strategy, aggregates);
    }
}
