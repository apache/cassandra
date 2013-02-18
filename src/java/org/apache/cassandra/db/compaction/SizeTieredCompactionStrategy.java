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

import org.apache.cassandra.cql3.CFPropDefs;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.Pair;

public class SizeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(SizeTieredCompactionStrategy.class);
    protected static final long DEFAULT_MIN_SSTABLE_SIZE = 50L * 1024L * 1024L;
    protected static final double DEFAULT_BUCKET_LOW = 0.5;
    protected static final double DEFAULT_BUCKET_HIGH = 1.5;
    protected static final String MIN_SSTABLE_SIZE_KEY = "min_sstable_size";
    protected static final String BUCKET_LOW_KEY = "bucket_low";
    protected static final String BUCKET_HIGH_KEY = "bucket_high";

    protected long minSSTableSize;
    protected double bucketLow;
    protected double bucketHigh;
    protected volatile int estimatedRemainingTasks;

    public SizeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
        optionValue = options.get(BUCKET_LOW_KEY);
        bucketLow = optionValue == null ? DEFAULT_BUCKET_LOW : Double.parseDouble(optionValue);
        optionValue = options.get(BUCKET_HIGH_KEY);
        bucketHigh = optionValue == null ? DEFAULT_BUCKET_HIGH : Double.parseDouble(optionValue);

        cfs.setCompactionThresholds(cfs.metadata.getMinCompactionThreshold(), cfs.metadata.getMaxCompactionThreshold());
    }

    private List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        // make local copies so they can't be changed out from under us mid-method
        int minThreshold = cfs.getMinimumCompactionThreshold();
        int maxThreshold = cfs.getMaximumCompactionThreshold();
        if (minThreshold == 0 || maxThreshold == 0)
        {
            logger.debug("Compaction is currently disabled.");
            return Collections.emptyList();
        }

        Set<SSTableReader> candidates = cfs.getUncompactingSSTables();
        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndLengthPairs(filterSuspectSSTables(candidates)));
        logger.debug("Compaction buckets are {}", buckets);
        updateEstimatedCompactionsByTasks(buckets);

        List<List<SSTableReader>> prunedBuckets = new ArrayList<List<SSTableReader>>();
        for (List<SSTableReader> bucket : buckets)
        {
            if (bucket.size() < minThreshold)
                continue;

            Collections.sort(bucket, new Comparator<SSTableReader>()
            {
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return o1.descriptor.generation - o2.descriptor.generation;
                }
            });
            List<SSTableReader> prunedBucket = bucket.subList(0, Math.min(bucket.size(), maxThreshold));
            prunedBuckets.add(prunedBucket);
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
                return Collections.emptyList();
        }

        return Collections.min(prunedBuckets, new Comparator<List<SSTableReader>>()
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
    }

    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        while (true)
        {
            List<SSTableReader> smallestBucket = getNextBackgroundSSTables(gcBefore);

            if (smallestBucket.isEmpty())
                return null;

            if (cfs.getDataTracker().markCompacting(smallestBucket))
                return new CompactionTask(cfs, smallestBucket, gcBefore);
        }
    }

    public AbstractCompactionTask getMaximalTask(final int gcBefore)
    {
        while (true)
        {
            List<SSTableReader> sstables = filterSuspectSSTables(cfs.getUncompactingSSTables());
            if (sstables.isEmpty())
                return null;
            if (cfs.getDataTracker().markCompacting(sstables))
                return new CompactionTask(cfs, sstables, gcBefore);
        }
    }

    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, final int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        if (!cfs.getDataTracker().markCompacting(sstables))
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, sstables, gcBefore).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    private static List<Pair<SSTableReader, Long>> createSSTableAndLengthPairs(Collection<SSTableReader> collection)
    {
        List<Pair<SSTableReader, Long>> tableLengthPairs = new ArrayList<Pair<SSTableReader, Long>>(collection.size());
        for(SSTableReader table: collection)
            tableLengthPairs.add(Pair.create(table, table.onDiskLength()));
        return tableLengthPairs;
    }

    /*
     * Group files of similar size into buckets.
     */
    <T> List<List<T>> getBuckets(Collection<Pair<T, Long>> files)
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
                if ((size > (oldAverageSize * bucketLow) && size < (oldAverageSize * bucketHigh))
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

    public long getMaxSSTableSize()
    {
        return Long.MAX_VALUE;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);

        String optionValue = options.get(MIN_SSTABLE_SIZE_KEY);
        try
        {
            long minSSTableSize = optionValue == null ? DEFAULT_MIN_SSTABLE_SIZE : Long.parseLong(optionValue);
            if (minSSTableSize < 0)
            {
                throw new ConfigurationException(String.format("%s must be non negative: %d", MIN_SSTABLE_SIZE_KEY, minSSTableSize));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MIN_SSTABLE_SIZE_KEY), e);
        }

        double bucketLow, bucketHigh;
        optionValue = options.get(BUCKET_LOW_KEY);
        try
        {
            bucketLow = optionValue == null ? DEFAULT_BUCKET_LOW : Double.parseDouble(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, DEFAULT_BUCKET_LOW), e);
        }

        optionValue = options.get(BUCKET_HIGH_KEY);
        try
        {
            bucketHigh = optionValue == null ? DEFAULT_BUCKET_HIGH : Double.parseDouble(optionValue);
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, DEFAULT_BUCKET_HIGH), e);
        }

        if (bucketHigh <= bucketLow)
        {
            throw new ConfigurationException(String.format("BucketHigh value (%s) is less than or equal BucketLow value (%s)", bucketHigh, bucketLow));
        }

        uncheckedOptions.remove(MIN_SSTABLE_SIZE_KEY);
        uncheckedOptions.remove(BUCKET_LOW_KEY);
        uncheckedOptions.remove(BUCKET_HIGH_KEY);
        uncheckedOptions.remove(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD);
        uncheckedOptions.remove(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD);

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("SizeTieredCompactionStrategy[%s/%s]",
            cfs.getMinimumCompactionThreshold(),
            cfs.getMaximumCompactionThreshold());
    }
}
