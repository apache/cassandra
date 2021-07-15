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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

/**
 * The common interface between legacy compaction strategies (those that extend {@link LegacyAbstractCompactionStrategy}
 * and the new compaction strategy, {@link UnifiedCompactionStrategy}.
 */
public interface CompactionStrategy extends CompactionObserver
{
    /**
     * @return the compaction logger optionally logs events in a csv file.
     */
    CompactionLogger getCompactionLogger();

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    void pause();

    /**
     * For internal, temporary suspension of background compactions so that we can do exceptional
     * things like truncate or major compaction
     */
    void resume();

    /**
     * Performs any extra initialization required
     */
    void startup();

    /**
     * Releases any resources if this strategy is shutdown (when the CFS is reloaded after a schema change).
     */
    void shutdown();

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return the next background/minor compaction tasks to run; empty if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore);

    /**
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task that should be run to compact this columnfamilystore
     * as much as possible.  Null if nothing to do.
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @SuppressWarnings("resource")
    CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput);

    /**
     * @param sstables SSTables to compact. Must be marked as compacting.
     * @param gcBefore throw away tombstones older than this
     *
     * @return a compaction task corresponding to the requested sstables.
     * Will not be null. (Will throw if user requests an invalid compaction.)
     *
     * Is responsible for marking its sstables as compaction-pending.
     */
    @SuppressWarnings("resource")
    CompactionTasks getUserDefinedTasks(Collection<SSTableReader> sstables, int gcBefore);

    /**
     * Get the estimated remaining compactions.
     *
     * @return the number of background tasks estimated to still be needed for this strategy
     */
    int getEstimatedRemainingTasks();

    /**
     * Create a compaction task for the sstables in the transaction.
     *
     * @return a valid compaction task that can be executed.
     */
    AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes);

    /**
     * @return the total number of background compactions, pending or in progress
     */
    int getTotalCompactions();

    /**
     * Return the statistics. Not all strategies will provide non-empty statistics,
     * the legacy strategies that do not support aggregates will return empty statistics.
     * <p/>
     * @return statistics about this compaction picks.
     */
    List<CompactionStrategyStatistics> getStatistics();

    /**
     * @return size in bytes of the largest sstables for this strategy
     */
    long getMaxSSTableBytes();

    /**
     * @return the number of sstables for each level, if this strategy supports levels. Otherwise return an empty array.
     */
    int[] getSSTableCountPerLevel();

    /**
     * @return the level fanout size if applicable to this strategy. Otherwise return the default LCS fanout size.
     */
    int getLevelFanoutSize();

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges);

    default ScannerList getScanners(Collection<SSTableReader> toCompact)
    {
        return getScanners(toCompact, null);
    }

    /**
     * @return the name of the strategy
     */
    String getName();

    /**
     * Returns the sstables managed by the strategy
     */
    Set<SSTableReader> getSSTables();

    /**
     * Group sstables that can be anti-compacted togetehr.
     */
    Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup);

    /**
     * Create an sstable writer that is suitable for the strategy.
     */
    SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                long keyCount,
                                                long repairedAt,
                                                UUID pendingRepair,
                                                boolean isTransient,
                                                MetadataCollector collector,
                                                SerializationHeader header,
                                                Collection<Index.Group> indexGroups,
                                                LifecycleNewTracker lifecycleNewTracker);

    /**
     * @return true if the strategy supports early open
     */
    boolean supportsEarlyOpen();
}