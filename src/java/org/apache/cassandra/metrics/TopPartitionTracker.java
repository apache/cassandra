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

package org.apache.cassandra.metrics;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

/**
 * Tracks top partitions, currently by size and by tombstone count
 *
 * Collects during full and preview (-vd) repair since then we read the full partition
 *
 * Note that since we can run sub range repair there might be windows where the top partitions are not correct -
 * for example, assume we track the top 2 partitions for this node:
 *
 * tokens with size:
 * (a, 100); (b, 40); (c, 10); (d, 100); (e, 50); (f, 10)
 * - top2: a, d
 * now a is deleted and we run a repair for keys [a, c]
 * - top2: b, d
 * and when we repair [d, f]
 * - top2: d, e
 *
 */
public class TopPartitionTracker
{
    private final static String SIZES = "SIZES";
    private final static String TOMBSTONES = "TOMBSTONES";

    private final AtomicReference<TopHolder> topSizes = new AtomicReference<>();
    private final AtomicReference<TopHolder> topTombstones = new AtomicReference<>();
    private final IPartitioner partitioner;
    private final String keyspace;
    private final String table;

    public TopPartitionTracker(IPartitioner partitioner, String keyspace, String table)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.table = table;
        startup();
    }

    private void startup()
    {
        topSizes.set(new TopHolder(partitioner,
                                   SystemKeyspace.getTopPartitions(keyspace, table, SIZES),
                                   DatabaseDescriptor.getMaxTopSizePartitionCount(),
                                   DatabaseDescriptor.getMinTrackedPartitionSize(),
                                   null));
        topTombstones.set(new TopHolder(partitioner,
                                        SystemKeyspace.getTopPartitions(keyspace, table, TOMBSTONES),
                                        DatabaseDescriptor.getMaxTopTombstonePartitionCount(),
                                        DatabaseDescriptor.getMinTrackedPartitionTombstoneCount(),
                                        null));
        ScheduledExecutors.optionalTasks.scheduleAtFixedRate(this::save, 60, 60, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    public void save()
    {
        TopHolder topSize = topSizes.get();
        if (!topSize.top.isEmpty())
            SystemKeyspace.saveTopPartitions(keyspace, table, SIZES, topSize.top);
        TopHolder topTombstones = this.topTombstones.get();
        if (!topTombstones.top.isEmpty())
            SystemKeyspace.saveTopPartitions(keyspace, table, TOMBSTONES, topTombstones.top);
    }

    public void merge(Collector collector)
    {
        while (true)
        {
            TopHolder cur = topSizes.get();
            TopHolder newSizes = cur.merge(collector.sizes);
            if (topSizes.compareAndSet(cur, newSizes))
                break;
        }

        while (true)
        {
            TopHolder cur = topTombstones.get();
            TopHolder newTombstones = cur.merge(collector.tombstones);
            if (topTombstones.compareAndSet(cur, newTombstones))
                break;
        }
    }

    public String toString()
    {
        return "TopPartitionTracker:\n" +
                 "     topSizes:\n" + topSizes + '\n'
               + "topTombstones:\n" + topTombstones + '\n';
    }

    public Map<String, Long> getTopTombstonePartitionMap(TableMetadata metadata)
    {
        return topTombstones.get().toMap(metadata);
    }

    public Map<String, Long> getTopSizePartitionMap(TableMetadata metadata)
    {
        return topSizes.get().toMap(metadata);
    }

    @VisibleForTesting
    public TopHolder topSizes()
    {
        return topSizes.get();
    }

    @VisibleForTesting
    public TopHolder topTombstones()
    {
        return topTombstones.get();
    }

    public static class Collector
    {
        private final TopHolder tombstones;
        private final TopHolder sizes;

        public Collector(Collection<Range<Token>> ranges)
        {
            this.tombstones = new TopHolder(DatabaseDescriptor.getMaxTopTombstonePartitionCount(),
                                            DatabaseDescriptor.getMinTrackedPartitionTombstoneCount(),
                                            ranges);
            this.sizes = new TopHolder(DatabaseDescriptor.getMaxTopSizePartitionCount(),
                                       DatabaseDescriptor.getMinTrackedPartitionSize(),
                                       ranges);
        }

        public void trackTombstones(DecoratedKey key, long count)
        {
            tombstones.track(key, count);
        }

        public void trackSize(DecoratedKey key, long size)
        {
            sizes.track(key, size);
        }

        public String toString()
        {
            return "tombstones:\n"+tombstones+"\nsizes:\n"+sizes;
        }
    }

    public static class TopHolder
    {
        public final NavigableSet<TopPartition> top;
        private final int maxTopPartitionCount;
        private final long minTrackedValue;
        private final Collection<Range<Token>> ranges;
        private long currentMinValue = Integer.MAX_VALUE;

        private TopHolder(int maxTopPartitionCount, long minTrackedValue, Collection<Range<Token>> ranges)
        {
            this(maxTopPartitionCount, minTrackedValue, new TreeSet<>(), ranges);
        }

        private TopHolder(int maxTopPartitionCount, long minTrackedValue, NavigableSet<TopPartition> top, Collection<Range<Token>> ranges)
        {
            this.maxTopPartitionCount = maxTopPartitionCount;
            this.minTrackedValue = minTrackedValue;
            this.top = top;
            this.ranges = ranges;
        }

        private TopHolder(IPartitioner partitioner, List<Pair<ByteBuffer, Long>> entries, int maxTopPartitionCount, long minTrackedValue, Collection<Range<Token>> ranges)
        {
            this.maxTopPartitionCount = maxTopPartitionCount;
            this.minTrackedValue = minTrackedValue;
            top = new TreeSet<>();
            this.ranges = ranges;

            for (Pair<ByteBuffer, Long> entry : entries)
                track(new TopPartition(partitioner.decorateKey(entry.left), entry.right));
        }

        public void track(DecoratedKey key, long value)
        {
            if (value < minTrackedValue)
                return;

            if (top.size() < maxTopPartitionCount || value > currentMinValue)
                track(new TopPartition(SSTable.getMinimalKey(key), value));
        }

        private void track(TopPartition tp)
        {
            top.add(tp);
            while (top.size() > maxTopPartitionCount)
            {
                top.pollLast();
                currentMinValue = top.last().value;
            }
            currentMinValue = Math.min(tp.value, currentMinValue);
        }

        /**
         * we merge any pre-existing top partitions on to the ones we just collected if they are outside of the
         * range collected.
         *
         * This means that if a large partition is deleted it will disappear from the top partitions
         */
        public TopHolder merge(TopHolder holder)
        {
            TopHolder mergedHolder = holder.copy();
            for (TopPartition existingTop : top)
                if (!Range.isInRanges(existingTop.key.getToken(), mergedHolder.ranges))
                    mergedHolder.track(existingTop);

            return mergedHolder;
        }

        private TopHolder copy()
        {
            return new TopHolder(maxTopPartitionCount, minTrackedValue, new TreeSet<>(top), ranges);
        }

        public String toString()
        {
            int i = 0;
            Iterator<TopPartition> it = top.iterator();
            StringBuilder sb = new StringBuilder();
            while (it.hasNext())
            {
                i++;
                sb.append(i).append(':').append(it.next()).append(System.lineSeparator());
            }
            return sb.toString();
        }

        public Map<String, Long> toMap(TableMetadata metadata)
        {
            Map<String, Long> topPartitionsMap = new LinkedHashMap<>();
            for (TopPartitionTracker.TopPartition topPartition : top)
            {
                String key = metadata.partitionKeyType.getString(topPartition.key.getKey());
                topPartitionsMap.put(key, topPartition.value);
            }
            return topPartitionsMap;
        }
    }

    private static final Comparator<TopPartition> comparator = (o1, o2) -> {
        int cmp = -Long.compare(o1.value, o2.value);
        if (cmp != 0) return cmp;
        return o1.key.compareTo(o2.key);
    };

    public static class TopPartition implements Comparable<TopPartition>
    {
        public final DecoratedKey key;
        public final long value;

        public TopPartition(DecoratedKey key, long value)
        {
            this.key = key;
            this.value = value;
        }

        public int compareTo(TopPartition o)
        {
            return comparator.compare(this, o);
        }

        public String toString()
        {
            return "TopPartition{" +
                   "key=" + key +
                   ", value=" + value +
                   '}';
        }
    }

    public static class TombstoneCounter extends Transformation<UnfilteredRowIterator>
    {
        private final TopPartitionTracker.Collector collector;
        private final int nowInSec;
        private long tombstoneCount = 0;
        private DecoratedKey key = null;

        public TombstoneCounter(TopPartitionTracker.Collector collector, int nowInSec)
        {
            this.collector = collector;
            this.nowInSec = nowInSec;
        }

        @Override
        public Row applyToRow(Row row)
        {
            if (!row.deletion().isLive())
                tombstoneCount++;
            if (row.hasDeletion(nowInSec))
            {
                for (Cell<?> c : row.cells())
                    if (c.isTombstone())
                        tombstoneCount++;
            }
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            tombstoneCount++;
            return marker;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            reset(partition.partitionKey());
            if (!partition.partitionLevelDeletion().isLive())
                tombstoneCount++;
            return Transformation.apply(partition, this);
        }

        private void reset(DecoratedKey key)
        {
            tombstoneCount = 0;
            this.key = key;
        }

        @Override
        public void onPartitionClose()
        {
            collector.trackTombstones(key, tombstoneCount);
        }
    }
}
