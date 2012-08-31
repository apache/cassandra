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
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected long minSSTableSize;
    protected volatile int estimatedRemainingTasks;

    public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
        cfs.setCompactionThresholds(cfs.metadata.getMinCompactionThreshold(), cfs.metadata.getMaxCompactionThreshold());
    }

    public AbstractCompactionTask getNextBackgroundTask(final int gcBefore)
    {
        if (cfs.isCompactionDisabled())
        {
            logger.debug("Compaction is currently disabled.");
            return null;
        }

        Set<SSTableReader> candidates = cfs.getUncompactingSSTables();
        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(filterSuspectSSTables(candidates)), minSSTableSize);
        logger.debug("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);

        List<List<SSTableReader>> prunedBuckets = new ArrayList<List<SSTableReader>>();
        for (List<SSTableReader> bucket : buckets)
        {
            if (bucket.size() < cfs.getMinimumCompactionThreshold())
                continue;

            Collections.sort(bucket, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return o1.descriptor.generation - o2.descriptor.generation;
                }
            });
            prunedBuckets.add(bucket.subList(0, Math.min(bucket.size(), cfs.getMaximumCompactionThreshold())));
        }

        if (prunedBuckets.isEmpty())
        {
            // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
            // ratio is greater than threshold.
            for (List<SSTableReader> bucket : buckets)
            {
                for (SSTableReader table : bucket)
                {
                    if (worthDroppingTombstones(table, gcBefore))
                        prunedBuckets.add(Collections.singletonList(table));
                }
            }

            if (prunedBuckets.isEmpty())
                return null;
        }

        List<SSTableReader> smallestBucket = Collections.min(prunedBuckets, new Comparator<List<SSTableReader>>()
        {
            public int compare(List<SSTableReader> o1, List<SSTableReader> o2)
            {
                long n = avgSize(o1) - avgSize(o2);
                if (n < 0)
                    return -1;
                if (n > 0)
                    return 1;
                return 0;
            }

            private long avgSize(List<SSTableReader> sstables)
            {
                long n = 0;
                for (SSTableReader sstable : sstables)
                    n += sstable.bytesOnDisk();
                return n / sstables.size();
            }
        });
        // when bucket only contains just one sstable, set userDefined to true to force single sstable compaction
        return new CompactionTask(cfs, smallestBucket, gcBefore).isUserDefined(smallestBucket.size() == 1);
    }

    public AbstractCompactionTask getMaximalTask(final int gcBefore)
    {
        return cfs.getSSTables().isEmpty() ? null : new CompactionTask(cfs, filterSuspectSSTables(cfs.getSSTables()), gcBefore);
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        return new CompactionTask(cfs, sstables, gcBefore).isUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    private static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Collection<SSTableReader> collection)
    {
        List<Pair<SSTableReader, Long>> tableLengthPairs = new ArrayList<Pair<SSTableReader, Long>>(collection.size());
        for(SSTableReader table: collection)
            tableLengthPairs.add(new Pair<SSTableReader, Long>(table, table.onDiskLength()));
        return tableLengthPairs;
    }

    /*
     * Group files of similar size into buckets.
     */
    static <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files, long minSSTableSize)
    {
        // Sort the list in order to get deterministic results during the grouping below
        List<Pair<T, Long>> sortedFiles = new ArrayList<Pair<T, Long>>(files);
        Collections.sort(sortedFiles, new Comparator<Pair<T, Long>>()
        {
            public int compare(Pair<T, Long> p1, Pair<T, Long> p2)
            {
                return p1.right.compareTo(p2.right);
            }
        });

        Map<Long, List<T>> buckets = new HashMap<Long, List<T>>();

        outer:
        for (Pair<T, Long> pair: sortedFiles)
        {
            long size = pair.right;

            // look for a bucket containing similar-sized files:
            // group in the same bucket if it's w/in 50% of the average for this bucket,
            // or this file and the bucket are all considered "small" (less than `minSSTableSize`)
            for (Entry<Long, List<T>> entry : buckets.entrySet())
            {
                List<T> bucket = entry.getValue();
                long oldAverageSize = entry.getKey();
                if ((size > (oldAverageSize / 2) && size < (3 * oldAverageSize) / 2)
                    || (size < minSSTableSize && oldAverageSize < minSSTableSize))
                {
                    // remove and re-add under new new average size
                    buckets.remove(oldAverageSize);
                    long totalSize = bucket.size() * oldAverageSize;
                    long newAverageSize = (totalSize + size) / (bucket.size() + 1);
                    bucket.add(pair.left);
                    buckets.put(newAverageSize, bucket);
                    continue outer;
                }
            }

            // no similar bucket found; put it in a new one
            ArrayList<T> bucket = new ArrayList<T>();
            bucket.add(pair.left);
            buckets.put(size, bucket);
        }

        return new ArrayList<List<T>>(buckets.values());
    }

    private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks)
    {
        int n = 0;
        for (List<SSTableReader> bucket: tasks)
        {
            if (bucket.size() >= cfs.getMinimumCompactionThreshold())
                n += Math.ceil((double)bucket.size() / cfs.getMaximumCompactionThreshold());
        }
        estimatedRemainingTasks = n;
    }

    public long getMinSSTableSize()
    {
        return minSSTableSize;
    }

    public long getMaxSSTableSize()
    {
        return Long.MAX_VALUE;
    }

    public boolean isKeyExistenceExpensive(Set<? extends SSTable> sstablesToIgnore)
    {
        return cfs.getSSTables().size() - sstablesToIgnore.size() > 20;
    }

    public String toString()
    {
        return String.format("SizeTieredCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }
}
