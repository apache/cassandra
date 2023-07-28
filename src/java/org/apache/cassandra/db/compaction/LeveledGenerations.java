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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_STRICT_LCS_CHECKS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Handles the leveled manifest generations
 *
 * Not thread safe, all access should be synchronized in LeveledManifest
 */
class LeveledGenerations
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledGenerations.class);
    private final boolean strictLCSChecksTest = TEST_STRICT_LCS_CHECKS.getBoolean();
    // It includes L0, i.e. we support [L0 - L8] levels
    static final int MAX_LEVEL_COUNT = 9;

    /**
     * This map is used to track the original NORMAL instances of sstables
     *
     * When aborting a compaction we can get notified that a MOVED_STARTS sstable is replaced with a NORMAL instance
     * of the same sstable but since we use sorted sets (on the first token) in L1+ we won't find it and won't remove it.
     * Then, when we add the NORMAL instance we have to replace the *instance* of the sstable to be able to later mark
     * it compacting again.
     *
     * In this map we rely on the fact that hashCode and equals do not care about first token, so when we
     * do allSSTables.get(instance_with_moved_starts) we will get the NORMAL sstable back, which we can then remove
     * from the TreeSet.
     */
    private final Map<SSTableReader, SSTableReader> allSSTables = new HashMap<>();
    private final Set<SSTableReader> l0 = new HashSet<>();
    private static long lastOverlapCheck = nanoTime();
    // note that since l0 is broken out, levels[0] represents L1:
    private final TreeSet<SSTableReader> [] levels = new TreeSet[MAX_LEVEL_COUNT - 1];

    private static final Comparator<SSTableReader> nonL0Comparator = (o1, o2) -> {
        int cmp = SSTableReader.firstKeyComparator.compare(o1, o2);
        if (cmp == 0)
            cmp = SSTableIdFactory.COMPARATOR.compare(o1.descriptor.id, o2.descriptor.id);
        return cmp;
    };

    LeveledGenerations()
    {
        for (int i = 0; i < MAX_LEVEL_COUNT - 1; i++)
            levels[i] = new TreeSet<>(nonL0Comparator);
    }

    Set<SSTableReader> get(int level)
    {
        if (level > levelCount() - 1 || level < 0)
            throw new ArrayIndexOutOfBoundsException("Invalid generation " + level + " - maximum is " + (levelCount() - 1));
        if (level == 0)
            return l0;
        return levels[level - 1];
    }

    int levelCount()
    {
        return levels.length + 1;
    }

    /**
     * Adds readers to the correct level
     *
     * If adding an sstable would cause an overlap in the level (if level > 1) we send it to L0. This can happen
     * for example when moving sstables from unrepaired to repaired.
     *
     * If the sstable is already in the manifest we replace the instance.
     *
     * If the sstable exists in the manifest but has the wrong level, it is removed from the wrong level and added to the correct one
     *
     * todo: group sstables per level, add all if level is currently empty, improve startup speed
     */
    void addAll(Iterable<SSTableReader> readers)
    {
        logDistribution();
        for (SSTableReader sstable : readers)
        {
            assert sstable.getSSTableLevel() < levelCount() : "Invalid level " + sstable.getSSTableLevel() + " out of " + (levelCount() - 1);
            int existingLevel = getLevelIfExists(sstable);
            if (existingLevel != -1)
            {
                if (sstable.getSSTableLevel() != existingLevel)
                {
                    logger.error("SSTable {} on the wrong level in the manifest - {} instead of {} as recorded in the sstable metadata, removing from level {}", sstable, existingLevel, sstable.getSSTableLevel(), existingLevel);
                    if (strictLCSChecksTest)
                        throw new AssertionError("SSTable not in matching level in manifest: "+sstable + ": "+existingLevel+" != " + sstable.getSSTableLevel());
                }
                else
                {
                    logger.info("Manifest already contains {} in level {} - replacing instance", sstable, existingLevel);
                }
                get(existingLevel).remove(sstable);
                allSSTables.remove(sstable);
            }

            allSSTables.put(sstable, sstable);
            if (sstable.getSSTableLevel() == 0)
            {
                l0.add(sstable);
                continue;
            }

            TreeSet<SSTableReader> level = levels[sstable.getSSTableLevel() - 1];
            /*
            current level: |-----||----||----|        |---||---|
              new sstable:                      |--|
                                          ^ before
                                                        ^ after
                overlap if before.last >= newsstable.first or after.first <= newsstable.last
             */
            SSTableReader after = level.ceiling(sstable);
            SSTableReader before = level.floor(sstable);

            if (before != null && before.getLast().compareTo(sstable.getFirst()) >= 0 ||
                after != null && after.getFirst().compareTo(sstable.getLast()) <= 0)
            {
                sendToL0(sstable);
            }
            else
            {
                level.add(sstable);
            }
        }
        maybeVerifyLevels();
    }

    /**
     * Sends sstable to L0 by mutating its level in the sstable metadata.
     *
     * SSTable should not exist in the manifest
     */
    private void sendToL0(SSTableReader sstable)
    {
        try
        {
            sstable.mutateLevelAndReload(0);
        }
        catch (IOException e)
        {
            // Adding it to L0 and marking suspect is probably the best we can do here - it won't create overlap
            // and we won't pick it for later compactions.
            logger.error("Failed mutating sstable metadata for {} - adding it to L0 to avoid overlap. Marking suspect", sstable, e);
            sstable.markSuspect();
        }
        l0.add(sstable);
    }

    /**
     * Tries to find the sstable in the levels without using the sstable-recorded level
     *
     * Used to make sure we don't try to re-add an existing sstable
     */
    private int getLevelIfExists(SSTableReader sstable)
    {
        for (int i = 0; i < levelCount(); i++)
        {
            if (get(i).contains(sstable))
                return i;
        }
        return -1;
    }

    int remove(Collection<SSTableReader> readers)
    {
        int minLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : readers)
        {
            int level = sstable.getSSTableLevel();
            minLevel = Math.min(minLevel, level);
            SSTableReader versionInManifest = allSSTables.get(sstable);
            if (versionInManifest != null)
            {
                get(level).remove(versionInManifest);
                allSSTables.remove(versionInManifest);
            }
        }
        return minLevel;
    }

    int[] getAllLevelSize()
    {
        int[] counts = new int[levelCount()];
        for (int i = 0; i < levelCount(); i++)
            counts[i] = get(i).size();
        return counts;
    }

    long[] getAllLevelSizeBytes()
    {
        long[] sums = new long[levelCount()];
        for (int i = 0; i < sums.length; i++)
            sums[i] = get(i).stream().map(SSTableReader::onDiskLength).reduce(0L, Long::sum);
        return sums;
    }

    Set<SSTableReader> allSSTables()
    {
        ImmutableSet.Builder<SSTableReader> builder = ImmutableSet.builder();
        builder.addAll(l0);
        for (Set<SSTableReader> sstables : levels)
            builder.addAll(sstables);
        return builder.build();
    }

    /**
     * given a level with sstables with first tokens [0, 10, 20, 30] and a lastCompactedSSTable with last = 15, we will
     * return an Iterator over [20, 30, 0, 10].
     */
    Iterator<SSTableReader> wrappingIterator(int lvl, SSTableReader lastCompactedSSTable)
    {
        assert lvl > 0; // only makes sense in L1+
        TreeSet<SSTableReader> level = levels[lvl - 1];
        if (level.isEmpty())
            return Collections.emptyIterator();
        if (lastCompactedSSTable == null)
            return level.iterator();

        PeekingIterator<SSTableReader> tail = Iterators.peekingIterator(level.tailSet(lastCompactedSSTable).iterator());
        SSTableReader pivot = null;
        // then we need to make sure that the first token of the pivot is greater than the last token of the lastCompactedSSTable
        while (tail.hasNext())
        {
            SSTableReader potentialPivot = tail.peek();
            if (potentialPivot.getFirst().compareTo(lastCompactedSSTable.getLast()) > 0)
            {
                pivot = potentialPivot;
                break;
            }
            tail.next();
        }

        if (pivot == null)
            return level.iterator();

        return Iterators.concat(tail, level.headSet(pivot, false).iterator());
    }

    void logDistribution()
    {
        if (logger.isTraceEnabled())
        {
            for (int i = 0; i < levelCount(); i++)
            {
                Set<SSTableReader> level = get(i);
                if (!level.isEmpty())
                {
                    logger.trace("L{} contains {} SSTables ({}) in {}",
                                 i,
                                 level.size(),
                                 FBUtilities.prettyPrintMemory(SSTableReader.getTotalBytes(level)),
                                 this);
                }
            }
        }
    }

    Set<SSTableReader>[] snapshot()
    {
        Set<SSTableReader> [] levelsCopy = new Set[levelCount()];
        for (int i = 0; i < levelCount(); i++)
            levelsCopy[i] = ImmutableSet.copyOf(get(i));
        return levelsCopy;
    }

    /**
     * do extra verification of the sstables in the generations
     *
     * only used during tests
     */
    private void maybeVerifyLevels()
    {
        if (!strictLCSChecksTest || nanoTime() - lastOverlapCheck <= TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS))
            return;
        logger.info("LCS verifying levels");
        lastOverlapCheck = nanoTime();
        for (int i = 1; i < levelCount(); i++)
        {
            SSTableReader prev = null;
            for (SSTableReader sstable : get(i))
            {
                // no overlap:
                assert prev == null || prev.getLast().compareTo(sstable.getFirst()) < 0;
                prev = sstable;
                // make sure it does not exist in any other level:
                for (int j = 0; j < levelCount(); j++)
                {
                    if (i == j)
                        continue;
                    assert !get(j).contains(sstable);
                }
            }
        }
    }

    void newLevel(SSTableReader sstable, int oldLevel)
    {
        SSTableReader versionInManifest = allSSTables.remove(sstable);
        boolean removed = false;
        if (versionInManifest != null)
            removed = get(oldLevel).remove(versionInManifest);
        if (!removed)
            logger.warn("Could not remove "+sstable+" from "+oldLevel);
        addAll(Collections.singleton(sstable));
    }
}
