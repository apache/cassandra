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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.SimpleSSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import static org.apache.cassandra.db.ColumnFamilyStore.nonSuspectAndNotInPredicate;

abstract class AbstractCompactionStrategy implements CompactionStrategy
{
    public static final Class<? extends CompactionStrategyContainer> CONTAINER_CLASS = CompactionStrategyManager.class;

    protected static final Logger logger = LoggerFactory.getLogger(AbstractCompactionStrategy.class);

    protected final CompactionStrategyOptions options;
    protected final ColumnFamilyStore cfs;
    protected final Tracker dataTracker;

    protected final CompactionLogger compactionLogger;
    protected final Directories directories;
    /**
     * This class groups all the compaction tasks that are pending, submitted, in progress and completed.
     */
    protected final BackgroundCompactions backgroundCompactions;

    /**
     * pause/resume/getNextBackgroundTask must synchronize.  This guarantees that after pause completes,
     * no new tasks will be generated; or put another way, pause can't run until in-progress tasks are
     * done being created.
     *
     * This allows runWithCompactionsDisabled to be confident that after pausing, once in-progress
     * tasks abort, it's safe to proceed with truncate/cleanup/etc.
     *
     * See CASSANDRA-3430
     */
    protected volatile boolean isActive = false;

    protected AbstractCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options)
    {
        assert factory != null;
        this.cfs = factory.getCfs();
        this.dataTracker = cfs.getTracker();
        this.compactionLogger = factory.getCompactionLogger();
        this.options = new CompactionStrategyOptions(getClass(), options, false);
        this.directories = cfs.getDirectories();
        this.backgroundCompactions = backgroundCompactions;
    }

    CompactionStrategyOptions getOptions()
    {
        return options;
    }

    public CompactionLogger getCompactionLogger()
    {
        return compactionLogger;
    }

    //
    // Compaction Observer
    //

    @Override
    public void onInProgress(CompactionProgress progress)
    {
        backgroundCompactions.onInProgress(progress);
    }

    @Override
    public void onCompleted(UUID id)
    {
        backgroundCompactions.onCompleted(this, id);
    }

    //
    // CompactionStrategy
    //

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    @Override
    public synchronized void pause()
    {
        isActive = false;
    }

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    @Override
    public synchronized void resume()
    {
        isActive = true;
    }

    /**
     * Performs any extra initialization required
     */
    @Override
    public void startup()
    {
        isActive = true;
    }

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     */
    @Override
    public void shutdown()
    {
        isActive = false;
    }

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @Override
    @SuppressWarnings("resource")
    public synchronized CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = Iterables.filter(getSSTables(), sstable -> !sstable.isMarkedSuspect());
        if (Iterables.isEmpty(filteredSSTables))
            return CompactionTasks.empty();
        LifecycleTransaction txn = dataTracker.tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return CompactionTasks.empty();
        return CompactionTasks.create(Collections.singleton(createCompactionTask(gcBefore, txn, true, splitOutput)));
    }

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @Override
    @SuppressWarnings("resource")
    public synchronized CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = dataTracker.tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return CompactionTasks.empty();
        }

        return CompactionTasks.create(ImmutableList.of(createCompactionTask(gcBefore, modifier, false, false).setUserDefined(true)));
    }

    /**
     * Create a compaction task for a maximal, user defined or background compaction without aggregates (legacy strategies).
     * Background compactions for strategies that extend {@link LegacyAbstractCompactionStrategy.WithAggregates} will use
     * {@link LegacyAbstractCompactionStrategy.WithAggregates#createCompactionTask(int, LifecycleTransaction, boolean, boolean)} instead.
     *
     * @param gcBefore tombstone threshold, older tombstones can be discarded
     * @param txn the transaction containing the files to be compacted
     * @param isMaximal set to true only when it's a maximal compaction
     * @param splitOutput false except for maximal compactions and passed in by the user to indicate to SizeTieredCompactionStrategy to split the out,
     *                    ignored otherwise
     *
     * @return a compaction task, see {@link AbstractCompactionTask} and sub-classes
     */
    protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, boolean isMaximal, boolean splitOutput)
    {
        return new CompactionTask(cfs, txn, gcBefore, false, this);
    }

    /**
     * Create a compaction task for operations that are not driven by the strategies.
     *
     * @param txn the transaction containing the files to be compacted
     * @param gcBefore tombstone threshold, older tombstones can be discarded
     * @param maxSSTableBytes the maximum size in bytes for an output sstables
     *
     * @return a compaction task, see {@link AbstractCompactionTask} and sub-classes
     */
    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, final int gcBefore, long maxSSTableBytes)
    {
        return new CompactionTask(cfs, txn, gcBefore, false, this);
    }

    /**
     * @return a list of the compaction aggregates, e.g. the levels or buckets. Note that legacy strategies that derive from
     * {@link LeveledCompactionStrategy.WithSSTableList} will return an empty list.
     */
    public Collection<CompactionAggregate> getAggregates()
    {
        return backgroundCompactions.getAggregates();
    }

    /**
     * @return the total number of background compactions, pending or in progress
     */
    @Override
    public int getTotalCompactions()
    {
        return getEstimatedRemainingTasks() + backgroundCompactions.getCompactionsInProgress().size();
    }

    /**
     * Return the statistics. Only strategies that implement {@link LegacyAbstractCompactionStrategy.WithAggregates} will provide non-empty statistics,
     * the legacy strategies will always have empty statistics.
     * <p/>
     * @return statistics about this compaction picks.
     */
    @Override
    public List<CompactionStrategyStatistics> getStatistics()
    {
        return ImmutableList.of(backgroundCompactions.getStatistics(this));
    }

    public static Iterable<SSTableReader> nonSuspectAndNotIn(Iterable<SSTableReader> sstables, Set<SSTableReader> compacting)
    {
        return Iterables.filter(sstables, nonSuspectAndNotInPredicate(compacting));
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return new int[0];
    }

    @Override
    public int getLevelFanoutSize()
    {
        return LeveledCompactionStrategy.DEFAULT_LEVEL_FANOUT_SIZE; // this makes no sense but it's the existing behaviour
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    @SuppressWarnings("resource")
    @Override
    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        ArrayList<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>();
        try
        {
            for (SSTableReader sstable : sstables)
                scanners.add(sstable.getScanner(ranges));
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }
        return new ScannerList(scanners);
    }

    @Override
    public String getName()
    {
        return getClass().getSimpleName();
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return CompactionStrategyOptions.validateOptions(options);
    }

    /**
     * Method for grouping similar SSTables together, This will be used by
     * anti-compaction to determine which SSTables should be anitcompacted
     * as a group. If a given compaction strategy creates sstables which
     * cannot be merged due to some constraint it must override this method.
     */
    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        int groupSize = 2;
        List<SSTableReader> sortedSSTablesToGroup = new ArrayList<>(sstablesToGroup);
        Collections.sort(sortedSSTablesToGroup, SSTableReader.firstKeyComparator);

        Collection<Collection<SSTableReader>> groupedSSTables = new ArrayList<>();
        Collection<SSTableReader> currGroup = new ArrayList<>(groupSize);

        for (SSTableReader sstable : sortedSSTablesToGroup)
        {
            currGroup.add(sstable);
            if (currGroup.size() == groupSize)
            {
                groupedSSTables.add(currGroup);
                currGroup = new ArrayList<>(groupSize);
            }
        }

        if (currGroup.size() != 0)
            groupedSSTables.add(currGroup);
        return groupedSSTables;
    }

    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       MetadataCollector meta,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return SimpleSSTableMultiWriter.create(descriptor, keyCount, repairedAt, pendingRepair, isTransient, cfs.metadata, meta, header, indexGroups, lifecycleNewTracker);
    }

    @Override
    public boolean supportsEarlyOpen()
    {
        return true;
    }
}
