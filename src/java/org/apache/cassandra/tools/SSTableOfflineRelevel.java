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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;

/**
 * Create a decent leveling for the given keyspace/column family
 *
 * Approach is to sort the sstables by their last token
 *
 * given an original leveling like this (note that [ ] indicates token boundaries, not sstable size on disk, all sstables are the same size)
 *
 * L3 [][][][][][][][][][][]
 * L2 [    ][    ][    ][  ]
 * L1 [          ][        ]
 * L0 [                    ]
 *
 * Will look like this after being dropped to L0 and sorted by last token (and, to illustrate overlap, the overlapping ones are put on a new line):
 *
 * [][][]
 * [    ][][][]
 *       [    ]
 * [          ]
 *...
 *
 * Then, we start iterating from the smallest last-token and adding all sstables that do not cause an overlap to
 * a level, we will reconstruct the original leveling top-down. Whenever we add an sstable to the level, we remove it from
 * the sorted list. Once we reach the end of the sorted list, we have a full level, and can start over with the level below.
 *
 * If we end up with more levels than expected, we put all levels exceeding the expected in L0, for example, original L0
 * files will most likely be put in a level of its own since they most often overlap many other sstables
 */
public class SSTableOfflineRelevel
{
    /**
     * @param args a list of sstables whose metadata we are changing
     */
    public static void main(String[] args) throws IOException
    {
        PrintStream out = System.out;
        if (args.length < 2)
        {
            out.println("This command should be run with Cassandra stopped!");
            out.println("Usage: sstableofflinerelevel [--dry-run] <keyspace> <columnfamily>");
            System.exit(1);
        }

        Util.initDatabaseDescriptor();

        boolean dryRun = args[0].equals("--dry-run");
        String keyspace = args[args.length - 2];
        String columnfamily = args[args.length - 1];
        Schema.instance.loadFromDisk();

        if (Schema.instance.getTableMetadataRef(keyspace, columnfamily) == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/table %s.%s",
                    keyspace,
                    columnfamily));

        // remove any leftovers in the transaction log
        Keyspace ks = Keyspace.openWithoutSSTables(keyspace);
        ColumnFamilyStore cfs = ks.getColumnFamilyStore(columnfamily);
        if (!LifecycleTransaction.removeUnfinishedLeftovers(cfs))
        {
            throw new RuntimeException(String.format("Cannot remove temporary or obsoleted files for %s.%s " +
                                                     "due to a problem with transaction log files.",
                                                     keyspace, columnfamily));
        }

        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.THROW).skipTemporary(true);
        SetMultimap<File, SSTableReader> sstableMultimap = HashMultimap.create();
        for (Map.Entry<Descriptor, Set<Component>> sstable : lister.list().entrySet())
        {
            if (sstable.getKey() != null)
            {
                try
                {
                    SSTableReader reader = SSTableReader.open(cfs, sstable.getKey());
                    sstableMultimap.put(reader.descriptor.directory, reader);
                }
                catch (Throwable t)
                {
                    out.println("Couldn't open sstable: "+sstable.getKey().fileFor(Components.DATA));
                    Throwables.throwIfUnchecked(t);
                    throw new RuntimeException(t);
                }
            }
        }
        if (sstableMultimap.isEmpty())
        {
            out.println("No sstables to relevel for "+keyspace+"."+columnfamily);
            System.exit(1);
        }
        for (File directory : sstableMultimap.keySet())
        {
            if (!sstableMultimap.get(directory).isEmpty())
            {
                Relevel rl = new Relevel(sstableMultimap.get(directory));
                out.println("For sstables in " + directory + ":");
                rl.relevel(dryRun);
            }
        }
        System.exit(0);

    }

    private static class Relevel
    {
        private final Set<SSTableReader> sstables;
        private final int approxExpectedLevels;
        public Relevel(Set<SSTableReader> sstables)
        {
            this.sstables = sstables;
            approxExpectedLevels = (int) Math.ceil(Math.log10(sstables.size()));
        }

        private void printLeveling(Iterable<SSTableReader> sstables)
        {
            Multimap<Integer, SSTableReader> leveling = ArrayListMultimap.create();
            int maxLevel = 0;
            for (SSTableReader sstable : sstables)
            {
                leveling.put(sstable.getSSTableLevel(), sstable);
                maxLevel = Math.max(sstable.getSSTableLevel(), maxLevel);
            }
            System.out.println("Current leveling:");
            for (int i = 0; i <= maxLevel; i++)
                System.out.println(String.format("L%d=%d", i, leveling.get(i).size()));
        }

        public void relevel(boolean dryRun) throws IOException
        {
            printLeveling(sstables);
            List<SSTableReader> sortedSSTables = new ArrayList<>(sstables);
            Collections.sort(sortedSSTables, new Comparator<SSTableReader>()
            {
                @Override
                public int compare(SSTableReader o1, SSTableReader o2)
                {
                    return o1.getLast().compareTo(o2.getLast());
                }
            });

            List<List<SSTableReader>> levels = new ArrayList<>();

            while (!sortedSSTables.isEmpty())
            {
                Iterator<SSTableReader> it = sortedSSTables.iterator();
                List<SSTableReader> level = new ArrayList<>();
                DecoratedKey lastLast = null;
                while (it.hasNext())
                {
                    SSTableReader sstable = it.next();
                    if (lastLast == null || lastLast.compareTo(sstable.getFirst()) < 0)
                    {
                        level.add(sstable);
                        lastLast = sstable.getLast();
                        it.remove();
                    }
                }
                levels.add(level);
            }
            List<SSTableReader> l0 = new ArrayList<>();
            if (approxExpectedLevels < levels.size())
            {
                for (int i = approxExpectedLevels; i < levels.size(); i++)
                    l0.addAll(levels.get(i));
                levels = levels.subList(0, approxExpectedLevels);
            }
            if (dryRun)
                System.out.println("Potential leveling: ");
            else
                System.out.println("New leveling: ");

            System.out.println("L0="+l0.size());
            // item 0 in levels is the highest level we will create, printing from L1 up here:
            for (int i = levels.size() - 1; i >= 0; i--)
                System.out.println(String.format("L%d=%d", levels.size() - i, levels.get(i).size()));

            if (!dryRun)
            {
                for (SSTableReader sstable : l0)
                {
                    if (sstable.getSSTableLevel() != 0)
                        sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
                }
                for (int i = levels.size() - 1; i >= 0; i--)
                {
                    for (SSTableReader sstable : levels.get(i))
                    {
                        int newLevel = levels.size() - i;
                        if (newLevel != sstable.getSSTableLevel())
                            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, newLevel);
                    }
                }
            }
        }
    }
}
