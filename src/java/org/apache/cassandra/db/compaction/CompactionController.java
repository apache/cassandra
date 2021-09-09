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

import java.util.*;
import java.util.function.LongPredicate;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;

/**
 * Manage compaction options.
 */
public class CompactionController extends AbstractCompactionController
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
    private static final String NEVER_PURGE_TOMBSTONES_PROPERTY = Config.PROPERTY_PREFIX + "never_purge_tombstones";
    static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean(NEVER_PURGE_TOMBSTONES_PROPERTY);

    private final boolean compactingRepaired;
    // note that overlapTracker will be null if NEVER_PURGE_TOMBSTONES is set - this is a
    // good thing so that noone starts using them and thinks that if overlappingSSTables is empty, there
    // is no overlap.
    @Nullable
    private final CompactionRealm.OverlapTracker overlapTracker;
    @Nullable
    private final Iterable<SSTableReader> compacting;
    @Nullable
    private final RateLimiter limiter;
    private final long minTimestamp;
    private final Map<SSTableReader, FileDataInput> openDataFiles = new HashMap<>();

    protected CompactionController(CompactionRealm realm, int maxValue)
    {
        this(realm, null, maxValue);
    }

    public CompactionController(CompactionRealm realm, Set<SSTableReader> compacting, int gcBefore)
    {
        this(realm, compacting, gcBefore, null, realm.getCompactionParams().tombstoneOption());
    }

    public CompactionController(CompactionRealm realm, Set<SSTableReader> compacting, int gcBefore, RateLimiter limiter, TombstoneOption tombstoneOption)
    {
        super(realm, gcBefore, tombstoneOption);
        this.compacting = compacting;
        this.limiter = limiter;
        compactingRepaired = compacting != null && compacting.stream().allMatch(SSTableReader::isRepaired);
        this.minTimestamp = compacting != null && !compacting.isEmpty()       // check needed for test
                          ? compacting.stream().mapToLong(SSTableReader::getMinTimestamp).min().getAsLong()
                          : 0;

        if (NEVER_PURGE_TOMBSTONES || realm.getNeverPurgeTombstones())
        {
            overlapTracker = null;
            if (NEVER_PURGE_TOMBSTONES)
                logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
            else
                logger.debug("Not using overlaps for {}.{} - neverPurgeTombstones is enabled", realm.getKeyspaceName(), realm.getTableName());
        }
        else if (ignoreOverlaps())
        {
            overlapTracker = realm.getOverlapTracker(null);
            logger.debug("Ignoring overlapping sstables for {}.{}", realm.getKeyspaceName(), realm.getTableName());
        }
        else
            overlapTracker = realm.getOverlapTracker(compacting);
    }

    public void maybeRefreshOverlaps()
    {
        if (overlapTracker != null && overlapTracker.maybeRefresh())
            closeDataFiles();
    }

    void closeDataFiles()
    {
        FileUtils.closeQuietly(openDataFiles.values());
        openDataFiles.clear();
    }

    public Set<CompactionSSTable> getFullyExpiredSSTables()
    {
        if (overlapTracker == null)
            return Collections.emptySet();
        return getFullyExpiredSSTables(realm, compacting, overlapTracker.overlaps(), gcBefore, ignoreOverlaps());
    }

    /**
     * Finds expired sstables
     *
     * works something like this;
     * 1. find "global" minTimestamp of overlapping sstables, compacting sstables and memtables containing any non-expired data
     * 2. build a list of fully expired candidates
     * 3. check if the candidates to be dropped actually can be dropped {@code (maxTimestamp < global minTimestamp)}
     *    - if not droppable, remove from candidates
     * 4. return candidates.
     *
     * @param realm
     * @param compacting we take the drop-candidates from this set, it is usually the sstables included in the compaction
     * @param overlapping the sstables that overlap the ones in compacting.
     * @param gcBefore
     * @param ignoreOverlaps don't check if data shadows/overlaps any data in other sstables
     * @return
     */
    public static
    Set<CompactionSSTable> getFullyExpiredSSTables(CompactionRealm realm,
                                                   Iterable<? extends CompactionSSTable> compacting,
                                                   Iterable<? extends CompactionSSTable> overlapping,
                                                   int gcBefore,
                                                   boolean ignoreOverlaps)
    {
        logger.trace("Checking droppable sstables in {}", realm);

        if (NEVER_PURGE_TOMBSTONES || compacting == null || realm.getNeverPurgeTombstones())
            return Collections.emptySet();

        if (realm.onlyPurgeRepairedTombstones() && !Iterables.all(compacting, CompactionSSTable::isRepaired))
            return Collections.emptySet();

        long minTimestamp;
        if (!ignoreOverlaps)
        {
            minTimestamp = Math.min(Math.min(minSurvivingTimestamp(overlapping, gcBefore),
                                             minSurvivingTimestamp(compacting, gcBefore)),
                                    minTimestamp(realm.getAllMemtables()));
        }
        else
        {
            minTimestamp = Long.MAX_VALUE;
        }

        // At this point, minTimestamp denotes the lowest timestamp of any relevant
        // SSTable or Memtable that contains a constructive value. Any compacting sstable with only expired content that
        // also has (getMaxTimestamp() < minTimestamp) serves no purpose anymore.

        Set<CompactionSSTable> expired = new HashSet<>();
        for (CompactionSSTable candidate : compacting)
        {
            if (candidate.getMaxLocalDeletionTime() < gcBefore &&
                candidate.getMaxTimestamp() < minTimestamp)
            {
                logger.trace("Dropping {}expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                             ignoreOverlaps ? "overlap ignored " : "",
                             candidate, candidate.getMaxLocalDeletionTime(), gcBefore);
                expired.add(candidate);
            }
        }
        return expired;
    }

    private static long minTimestamp(Iterable<Memtable> memtables)
    {
        long minTimestamp = Long.MAX_VALUE;
        for (Memtable memtable : memtables)
            minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());
        return minTimestamp;
    }

    private static long minSurvivingTimestamp(Iterable<? extends CompactionSSTable> ssTables,
                                              int gcBefore)
    {
        long minTimestamp = Long.MAX_VALUE;
        for (CompactionSSTable sstable : ssTables)
        {
            // Overlapping might include fully expired sstables. What we care about here is
            // the min timestamp of the overlapping sstables that actually contain live data.
            if (sstable.getMaxLocalDeletionTime() >= gcBefore)
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
        }
        return minTimestamp;
    }

    public static
    Set<CompactionSSTable> getFullyExpiredSSTables(CompactionRealm realm,
                                                   Iterable<? extends CompactionSSTable> compacting,
                                                   Iterable<? extends CompactionSSTable> overlapping,
                                                   int gcBefore)
    {
        return getFullyExpiredSSTables(realm, compacting, overlapping, gcBefore, false);
    }

    /**
     * @param key
     * @return a predicate for whether tombstones marked for deletion at the given time for the given partition are
     * purgeable; we calculate this by checking whether the deletion time is less than the min timestamp of all SSTables
     * containing his partition and not participating in the compaction. This means there isn't any data in those
     * sstables that might still need to be suppressed by a tombstone at this timestamp.
     */
    @Override
    public LongPredicate getPurgeEvaluator(DecoratedKey key)
    {
        if (overlapTracker == null || !compactingRepaired())
            return time -> false;

        Collection<? extends CompactionSSTable> filteredSSTables = overlapTracker.overlaps(key);
        Iterable<Memtable> memtables = realm.getAllMemtables();
        long minTimestampSeen = Long.MAX_VALUE;
        boolean hasTimestamp = false;

        // TODO: Evaluate if doing this in sort order to minimize couldContain calls is a performance improvement.
        for (CompactionSSTable sstable: filteredSSTables)
        {
            long sstableMinTimestamp = sstable.getMinTimestamp();
            // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
            // we check index file instead.
            if (sstableMinTimestamp < minTimestampSeen && sstable.couldContain(key))
            {
                minTimestampSeen = sstableMinTimestamp;
                hasTimestamp = true;
            }
        }

        for (Memtable memtable : memtables)
        {
            if (memtable.getMinTimestamp() >= minTimestampSeen)
                continue;

            Partition partition = memtable.getPartition(key);
            if (partition != null)
            {
                minTimestampSeen = Math.min(minTimestampSeen, partition.stats().minTimestamp);
                hasTimestamp = true;
            }
        }

        if (!hasTimestamp)
            return time -> true;
        else
        {
            final long finalTimestamp = minTimestampSeen;
            return time -> time < finalTimestamp;
        }
    }

    public void close()
    {
        closeDataFiles();
        FileUtils.closeQuietly(overlapTracker);
    }

    public boolean compactingRepaired()
    {
        return !realm.onlyPurgeRepairedTombstones() || compactingRepaired;
    }

    boolean shouldProvideTombstoneSources()
    {
        return tombstoneOption != TombstoneOption.NONE && compactingRepaired() && overlapTracker != null;
    }

    // caller must close iterators
    public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly)
    {
        if (!shouldProvideTombstoneSources())
            return null;

        return overlapTracker.openSelectedOverlappingSSTables(key,
                                                              tombstoneOnly ? this::isTombstoneShadowSource
                                                                            : this::isCellDataShadowSource,
                                                              sstable -> sstable.simpleIterator(openDataFiles.computeIfAbsent(sstable,
                                                                                                                              this::openDataFile),
                                                                                                key,
                                                                                                tombstoneOnly));
    }

    private boolean isTombstoneShadowSource(CompactionSSTable ssTable)
    {
        return isCellDataShadowSource(ssTable) && ssTable.mayHaveTombstones();
    }

    private boolean isCellDataShadowSource(CompactionSSTable ssTable)
    {
        return !ssTable.isMarkedSuspect() && ssTable.getMaxTimestamp() > minTimestamp;
    }


    /**
     * Is overlapped sstables ignored
     *
     * Control whether or not we are taking into account overlapping sstables when looking for fully expired sstables.
     * In order to reduce the amount of work needed, we look for sstables that can be dropped instead of compacted.
     * As a safeguard mechanism, for each time range of data in a sstable, we are checking globally to see if all data
     * of this time range is fully expired before considering to drop the sstable.
     * This strategy can retain for a long time a lot of sstables on disk (see CASSANDRA-13418) so this option
     * control whether or not this check should be ignored.
     *
     * @return false by default
     */
    protected boolean ignoreOverlaps()
    {
        return false;
    }

    private FileDataInput openDataFile(SSTableReader reader)
    {
        return limiter != null ? reader.openDataReader(limiter) : reader.openDataReader();
    }
}
