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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.util.FileUtils;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    public static final String EXTENSION = ".json";

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    private static final int MAX_COMPACTING_L0 = 32;

    private final ColumnFamilyStore cfs;
    private final List<SSTableReader>[] generations;
    private final RowPosition[] lastCompactedKeys;
    private final int maxSSTableSizeInBytes;

    private LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024 * 1024;

        // allocate enough generations for a PB of data
        int n = (int) Math.log10(1000 * 1000 * 1000 / maxSSTableSizeInMB);
        generations = new List[n];
        lastCompactedKeys = new RowPosition[n];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<SSTableReader>();
            lastCompactedKeys[i] = cfs.partitioner.getMinimumToken().minKeyBound();
        }
    }

    static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize)
    {
        return create(cfs, maxSSTableSize, cfs.getSSTables());
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, Iterable<SSTableReader> sstables)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        for (int i = 1; i < manifest.getAllLevelSize().length; i++)
        {
            manifest.repairOverlappingSSTables(i);
        }
        return manifest;
    }

    public synchronized void add(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
        logDistribution();

        logger.debug("Adding {} to L{}", reader, level);
        generations[level].add(reader);
    }

    /**
     * if the number of SSTables in the current compacted set *by itself* exceeds the target level's
     * (regardless of the level's current contents), find an empty level instead
     */
    private int skipLevels(int newLevel, Iterable<SSTableReader> added)
    {
        // Note that we now check if the sstables included in the compaction, *before* the compaction, fit in the next level.
        // This is needed since we need to decide before the actual compaction what level they will be in.
        // This should be safe, we might skip levels where the compacted data could have fit but that should be ok.
        while (maxBytesForLevel(newLevel) < SSTableReader.getTotalBytes(added)
               && generations[(newLevel + 1)].isEmpty())
        {
            newLevel++;
        }
        return newLevel;
    }

    public synchronized void replace(Iterable<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        assert !Iterables.isEmpty(removed); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isDebugEnabled())
            logger.debug("Replacing [" + toString(removed) + "]");

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        for (SSTableReader sstable : removed)
        {
            remove(sstable);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (!added.iterator().hasNext())
            return;

        if (logger.isDebugEnabled())
            logger.debug("Adding [{}]", toString(added));

        int minLevel = Integer.MAX_VALUE;
        for (SSTableReader ssTableReader : added)
        {
            minLevel = Math.min(minLevel, ssTableReader.getSSTableLevel());
            add(ssTableReader);
        }
        lastCompactedKeys[minLevel] = SSTable.sstableOrdering.max(added).last;
    }

    public synchronized void repairOverlappingSSTables(int level)
    {
        SSTableReader previous = null;
        Collections.sort(generations[level], SSTable.sstableComparator);
        List<SSTableReader> outOfOrderSSTables = new ArrayList<SSTableReader>();
        for (SSTableReader current : generations[level])
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
            {
                logger.warn(String.format("At level %d, %s [%s, %s] overlaps %s [%s, %s].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. " +
                                          "Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable",
                                          level, previous, previous.first, previous.last, current, current.first, current.last));
                outOfOrderSSTables.add(current);
            }
            else
            {
                previous = current;
            }
        }

        if (!outOfOrderSSTables.isEmpty())
        {
            for (SSTableReader sstable : outOfOrderSSTables)
                sendBackToL0(sstable);
        }
    }

    private synchronized void sendBackToL0(SSTableReader sstable)
    {
        remove(sstable);
        String metaDataFile = sstable.descriptor.filenameFor(Component.STATS);
        try
        {
            mutateLevel(sstable.getSSTableMetadata(), sstable.descriptor, metaDataFile, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
    }

    private String toString(Iterable<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    @VisibleForTesting
    long maxBytesForLevel(int level)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(10, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact
     * If no compactions are necessary, will return an empty list.  Never returns null.
     */
    public synchronized Collection<SSTableReader> getCompactionCandidates()
    {
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we force compacting higher levels first.  This may not minimize the number
        // of reads done as quickly in the short term, but it minimizes the i/o needed to compact
        // optimially which gives us a long term win.
        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = generations[i];
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().getCompacting());
            double score = (double)SSTableReader.getTotalBytes(remaining) / (double)maxBytesForLevel(i);
            logger.debug("Compaction score for level {} is {}", i, score);

            // L0 gets a special case that if we don't have anything more important to do,
            // we'll go ahead and compact if we have more than one sstable
            if (score > 1.001 || (i == 0 && sstables.size() > 1))
            {
                Collection<SSTableReader> candidates = getCandidatesFor(i);
                if (logger.isDebugEnabled())
                    logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
                if (!candidates.isEmpty())
                    return candidates;
            }
        }

        return Collections.emptyList();
    }

    public int getLevelSize(int i)
    {
        return generations.length > i ? generations[i].size() : 0;
    }

    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = generations[i].size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!generations[i].isEmpty())
                {
                    logger.debug("L{} contains {} SSTables ({} bytes) in {}",
                            new Object[] {i, generations[i].size(), SSTableReader.getTotalBytes(generations[i]), this});
                }
            }
        }
    }

    @VisibleForTesting
    public int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations[level].remove(reader);
        return level;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.token;
        Token last = sstable.last.token;
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.token) <= 0 ? first : sstable.first.token;
            last = last.compareTo(sstable.last.token) >= 0 ? last : sstable.last.token;
        }
        return overlapping(first, last, others);
    }

    @VisibleForTesting
    static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        return overlapping(sstable.first.token, sstable.last.token, others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<SSTableReader>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<Token>(candidate.first.token, candidate.last.token);
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesFor(int level)
    {
        assert !generations[level].isEmpty();
        logger.debug("Choosing candidates for L{}", level);

        final Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();

        if (level == 0)
        {
            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<SSTableReader> candidates = new HashSet<SSTableReader>();
            Set<SSTableReader> remaining = new HashSet<SSTableReader>();
            Iterables.addAll(remaining, Iterables.filter(generations[0], Predicates.not(suspectP)));
            for (SSTableReader sstable : ageSortedSSTables(remaining))
            {
                if (candidates.contains(sstable))
                    continue;

                for (SSTableReader newCandidate : Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining)))
                {
                    if (!compacting.contains(newCandidate))
                    {
                        candidates.add(newCandidate);
                        remaining.remove(newCandidate);
                    }
                }

                if (candidates.size() > MAX_COMPACTING_L0)
                {
                    // limit to only the MAX_COMPACTING_L0 oldest candidates
                    candidates = new HashSet<SSTableReader>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_L0));
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (SSTable.getTotalBytes(candidates) > maxSSTableSizeInBytes)
            {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                candidates = Sets.union(candidates, overlapping(candidates, generations[1]));
                // check overlap with L0 compacting sstables to make sure we are not generating overlap in L1.
                Iterable<SSTableReader> compactingL0 = Iterables.filter(generations[0], Predicates.in(compacting));
                if (!Sets.intersection(candidates, compacting).isEmpty() || !overlapping(candidates, compactingL0).isEmpty())
                    return Collections.emptyList();
            }

            return candidates.size() > 1 ? candidates : Collections.<SSTableReader>emptyList();
        }

        // for non-L0 compactions, pick up where we left off last time
        Collections.sort(generations[level], SSTable.sstableComparator);
        int start = 0; // handles case where the prior compaction touched the very last range
        for (int i = 0; i < generations[level].size(); i++)
        {
            SSTableReader sstable = generations[level].get(i);
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }

        // look for a non-suspect table to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        for (int i = 0; i < generations[level].size(); i++)
        {
            SSTableReader sstable = generations[level].get((start + i) % generations[level].size());
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlapping(sstable, generations[level + 1]));
            if (Iterables.any(candidates, suspectP))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<SSTableReader>(candidates);
        Collections.sort(ageSortedCandidates, SSTable.maxTimestampComparator);
        return ageSortedCandidates;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (generations[i].size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, generations[level]);
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = generations[i];
            estimated[i] = Math.max(0L, SSTableReader.getTotalBytes(sstables) - maxBytesForLevel(i)) / maxSSTableSizeInBytes;
            tasks += estimated[i];
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                     new Object[] {Arrays.toString(estimated), cfs.table.getName(), cfs.name });
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables, OperationType operationType)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }
        switch(operationType)
        {
            case SCRUB:
            case TOMBSTONE_COMPACTION:
            case CLEANUP:
            case UPGRADE_SSTABLES:
                return minimumLevel;
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTable.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            newLevel = skipLevels(newLevel, sstables);
            assert newLevel > 0;
        }
        return newLevel;

    }

    /**
     * Scary method mutating existing sstable component
     *
     * Tries to do it safely by moving the new file on top of the old one
     *
     * Caller needs to reload the sstable metadata (sstableReader.reloadSSTableMetadata())
     *
     * @see org.apache.cassandra.io.sstable.SSTableReader#reloadSSTableMetadata()
     *
     * @param oldMetadata
     * @param descriptor
     * @param filename
     * @param level
     * @throws IOException
     */
    public static synchronized void mutateLevel(SSTableMetadata oldMetadata, Descriptor descriptor, String filename, int level) throws IOException
    {
        logger.debug("Mutating {} to level {}", descriptor.filenameFor(Component.STATS), level);
        SSTableMetadata metadata = SSTableMetadata.copyWithNewSSTableLevel(oldMetadata, level);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(filename + "-tmp"));
        SSTableMetadata.serializer.legacySerialize(metadata, descriptor, dos);
        dos.flush();
        dos.close();
        FileUtils.renameWithConfirm(filename + "-tmp", filename);
    }
}
