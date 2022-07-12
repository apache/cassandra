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
import java.util.Comparator;
import java.util.stream.Collectors;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.compaction.unified.Controller;

/**
 * Arena selector, used by UnifiedCompactionStrategy to distribute SSTables to separate compaction arenas.
 *
 * This is used to:
 * - ensure that sstables that should not be compacted together (e.g. repaired with unrepaired) are separated
 * - ensure that each disk's sstables are compacted separately
 */
public class ArenaSelector implements Comparator<CompactionSSTable>
{
    private final EquivClassSplitter[] classSplitters;
    final Controller controller;
    final DiskBoundaries diskBoundaries;

    public ArenaSelector(Controller controller, DiskBoundaries diskBoundaries)
    {
        this.controller = controller;
        this.diskBoundaries = diskBoundaries;

        ArrayList<EquivClassSplitter> ret = new ArrayList<>(2);

        ret.add(RepairEquivClassSplitter.INSTANCE);

        if (diskBoundaries.getNumBoundaries() > 1)
        {
            ret.add(new DiskIndexEquivClassSplitter());
        }

        classSplitters = ret.toArray(new EquivClassSplitter[0]);
    }

    @Override
    public int compare(CompactionSSTable o1, CompactionSSTable o2)
    {
        int res = 0;
        for (int i = 0; res == 0 && i < classSplitters.length; i++)
            res = classSplitters[i].compare(o1, o2);
        return res;
    }

    public String name(CompactionSSTable t)
    {
        return Arrays.stream(classSplitters)
                     .map(e -> e.name(t))
                     .collect(Collectors.joining("-"));
    }

    /**
     * An equivalence class is a function that compares two sstables and returns 0 when they fall in the same class.
     * For example, the repair status or disk index may define equivalence classes. See the concrete equivalence classes below.
     */
    private interface EquivClassSplitter extends Comparator<CompactionSSTable> {

        @Override
        int compare(CompactionSSTable a, CompactionSSTable b);

        /** Return a name that describes the equivalence class */
        String name(CompactionSSTable ssTableReader);
    }

    /**
     * Split sstables by their repair state: repaired, unrepaired, pending repair with a specific UUID (one group per pending repair).
     */
    private static final class RepairEquivClassSplitter implements EquivClassSplitter
    {
        public static final EquivClassSplitter INSTANCE = new RepairEquivClassSplitter();

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            // This is the same as name(a).compareTo(name(b))
            int af = repairClassValue(a);
            int bf = repairClassValue(b);
            if (af != 0 || bf != 0)
                return Integer.compare(af, bf);
            return a.getPendingRepair().compareTo(b.getPendingRepair());
        }

        private static int repairClassValue(CompactionSSTable a)
        {
            if (a.isRepaired())
                return 1;
            if (!a.isPendingRepair())
                return 2;
            else
                return 0;
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
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
     * Group sstables by their disk index.
     */
    private final class DiskIndexEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Integer.compare(diskBoundaries.getDiskIndexFromKey(a), diskBoundaries.getDiskIndexFromKey(b));
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            return "disk_" + diskBoundaries.getDiskIndexFromKey(ssTableReader);
        }
    }
}
