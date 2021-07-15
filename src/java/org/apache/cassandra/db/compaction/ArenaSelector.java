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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Arena selector, used by UnifiedCompactionStrategy to distribute SSTables to separate compaction arenas.
 *
 * This is used to:
 * - ensure that sstables that should not be compacted together (e.g. repaired with unrepaired) are separated
 * - ensure that each disk's sstables are compacted separately
 * - implement compaction shards, subsections of the token space which compact separately for improved parallelism
 *   and compaction overheads.
 */
public class ArenaSelector implements Comparator<SSTableReader>
{
    private final EquivClassSplitter[] classSplitters;
    final List<PartitionPosition> shardBoundaries;
    final DiskBoundaries diskBoundaries;

    public ArenaSelector(DiskBoundaries diskBoundaries, List<PartitionPosition> shardBoundaries)
    {
        this.shardBoundaries = shardBoundaries;
        this.diskBoundaries = diskBoundaries;

        ArrayList<EquivClassSplitter> ret = new ArrayList<>(2);

        ret.add(RepairEquivClassSplitter.INSTANCE);

        if (diskBoundaries.getPositions() != null)
        {
            // The shard boundaries must also split on disks. Verify it.
            assert new HashSet<>(shardBoundaries).containsAll(diskBoundaries.getPositions());
        }
        else if (diskBoundaries.getNumBoundaries() > 1)
        {
            // We end up here if there are multiple disks, but not assigned according to token range.
            ret.add(new DiskIndexEquivClassSplitter());
        }

        if (shardBoundaries.size() > 1)
            ret.add(new ShardEquivClassSplitter());

        classSplitters = ret.toArray(new EquivClassSplitter[0]);
    }

    @Override
    public int compare(SSTableReader o1, SSTableReader o2)
    {
        int res = 0;
        for (int i = 0; res == 0 && i < classSplitters.length; i++)
            res = classSplitters[i].compare(o1, o2);
        return res;
    }

    public String name(SSTableReader t)
    {
        return Arrays.stream(classSplitters)
                     .map(e -> e.name(t))
                     .collect(Collectors.joining("-"));
    }

    /**
     * Returns the shard where this key belongs. Shards are given by their end boundaries (i.e. shard 0 covers the space
     * between minimum and shardBoundaries[0], shard 1 is is between shardBoundaries[0] and shardBoundaries[1]), thus
     * finding the index of the first bigger boundary gives the index of the covering shard.
     */
    public int shardFor(DecoratedKey key)
    {
        return shardFor(key, shardBoundaries);
    }

    public static int shardFor(DecoratedKey key, List<PartitionPosition> shardBoundaries)
    {
        int pos = Collections.binarySearch(shardBoundaries, key);
        assert pos < 0; // boundaries are .minkeybound and .maxkeybound so they should never be equal to a DecoratedKey
        return -pos - 1;
    }

    public static int shardsSpanned(SSTableReader rdr, List<PartitionPosition> shardBoundaries)
    {
        if (shardBoundaries.size() <= 1)
            return 1;
        int startIdx = shardFor(rdr.getFirst(), shardBoundaries);
        DecoratedKey last = rdr.getLast();
        if (last.compareTo(shardBoundaries.get(startIdx)) < 0)
            return 1;   // quick path, end boundary is in the same shard
        return shardFor(last, shardBoundaries) - startIdx + 1;
    }

    public long shardAdjustedSize(SSTableReader rdr)
    {
        return shardAdjustedSize(rdr, shardBoundaries);
    }

    public static long shardAdjustedSize(SSTableReader rdr, List<PartitionPosition> shardBoundaries)
    {
        // This may need to duplicate the above to avoid the division in the happy path
        return rdr.onDiskLength() / shardsSpanned(rdr, shardBoundaries);
    }

    public static Set<SSTableReader> sstablesFor(int boundaryIndex, List<PartitionPosition> shardBoundaries, Set<SSTableReader > sstables)
    {
        assert boundaryIndex < shardBoundaries.size();
        return sstables.stream()
                       .filter(sstable -> shardFor(sstable.getFirst(), shardBoundaries) <= boundaryIndex && shardFor(sstable.getLast(), shardBoundaries) >= boundaryIndex)
                       .collect(Collectors.toSet());
    }

    public int compareByShardAdjustedSize(SSTableReader a, SSTableReader b)
    {
        return Long.compare(shardAdjustedSize(a), shardAdjustedSize(b));
    }

    /**
     * An equivalence class is a function that compares two sstables and returns 0 when they fall in the same class.
     * For example, the repair status or disk index may define equivalence classes. See the concrete equivalence classes below.
     */
    private interface EquivClassSplitter extends Comparator<SSTableReader> {

        @Override
        int compare(SSTableReader a, SSTableReader b);

        /** Return a name that describes the equivalence class */
        String name(SSTableReader ssTableReader);
    }

    /**
     * Split sstables by their repair state: repaired, unrepaired, pending repair with a specific UUID (one group per pending repair).
     */
    private static final class RepairEquivClassSplitter implements EquivClassSplitter
    {
        public static final EquivClassSplitter INSTANCE = new RepairEquivClassSplitter();

        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            // This is the same as name(a).compareTo(name(b))
            int af = a.isRepaired() ? 1 : !a.isPendingRepair() ? 2 : 0;
            int bf = b.isRepaired() ? 1 : !b.isPendingRepair() ? 2 : 0;
            if (af != 0 || bf != 0)
                return Integer.compare(af, bf);
            return a.getPendingRepair().compareTo(b.getPendingRepair());
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            if (ssTableReader.isRepaired())
                return "repaired";
            else if (!ssTableReader.isPendingRepair())
                return "unrepaired";
            else
                return "pending_repair_" + ssTableReader.getPendingRepair();
        }
    }

    /**
     * Split sstables by their shard. If the data set size is larger than the shard size in the compaction options,
     * then we create an equivalence class based by shard. Each sstable ends up in a shard based on their first
     * key. Each shard is calculated by splitting the local token ranges into a number of shards, where the number
     * of shards is calculated as ceil(data_size / shard size);
     *
     * Shard boundaries also split the sstables that reside on different disks.
     */
    private final class ShardEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            return Integer.compare(shardFor(a.getFirst()), shardFor(b.getFirst()));
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            return "shard_" + shardFor(ssTableReader.getFirst());
        }
    }

    /**
     * Group sstables by their disk index.
     */
    private final class DiskIndexEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(SSTableReader a, SSTableReader b)
        {
            return Integer.compare(diskBoundaries.getDiskIndexFromKey(a), diskBoundaries.getDiskIndexFromKey(b));
        }

        @Override
        public String name(SSTableReader ssTableReader)
        {
            return "disk_" + diskBoundaries.getDiskIndexFromKey(ssTableReader);
        }
    }


    // TODO - missing equivalence classes:

    // - by time window to emulate TWCS, in this case only the latest shard will use size based buckets, the older
    //   shards will get major compactions
}
