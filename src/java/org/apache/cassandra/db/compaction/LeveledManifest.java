package org.apache.cassandra.db.compaction;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    public static final String EXTENSION = ".json";

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    static int MAX_COMPACTING_L0 = 32;

    private final ColumnFamilyStore cfs;
    private final List<SSTableReader>[] generations;
    private final DecoratedKey[] lastCompactedKeys;
    private final int maxSSTableSizeInMB;
    private int levelCount;

    private LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInMB = maxSSTableSizeInMB;

        // allocate enough generations for a PB of data
        int n = (int) Math.log10(1000 * 1000 * 1000 / maxSSTableSizeInMB);
        generations = new List[n];
        lastCompactedKeys = new DecoratedKey[n];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<SSTableReader>();
            lastCompactedKeys[i] = new DecoratedKey(cfs.partitioner.getMinimumToken(), null);
        }
    }

    static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize);
        load(cfs, manifest);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : cfs.getSSTables())
        {
            if (manifest.levelOf(ssTableReader) < 0)
                manifest.add(ssTableReader);
        }

        return manifest;
    }

    private static void load(ColumnFamilyStore cfs, LeveledManifest manifest)
    {
        File manifestFile = tryGetManifest(cfs);
        if (manifestFile == null)
            return;

        ObjectMapper m = new ObjectMapper();
        try
        {
            JsonNode rootNode = m.readValue(manifestFile, JsonNode.class);
            JsonNode generations = rootNode.get("generations");
            assert generations.isArray();
            for (JsonNode generation : generations)
            {
                int level = generation.get("generation").getIntValue();
                JsonNode generationValues = generation.get("members");
                for (JsonNode generationValue : generationValues)
                {
                    for (SSTableReader ssTableReader : cfs.getSSTables())
                    {
                        if (ssTableReader.descriptor.generation == generationValue.getIntValue())
                        {
                            logger.debug("Loading {} at L{}", ssTableReader, level);
                            manifest.add(ssTableReader, level);
                        }
                    }
                }
            }
        }
        catch (Exception e)
        {
            // TODO try to recover -old first
            logger.error("Manifest present but corrupt. Cassandra will compact levels from scratch", e);
        }
    }

    public synchronized void add(SSTableReader reader)
    {
        logDistribution();
        logger.debug("Adding {} to L0", reader);
        add(reader, 0);
        serialize();
    }

    /**
     * if the number of SSTables in the current compacted set *by itself* exeeds the target level's
     * (regardless of the level's current contents), find an empty level instead
     */
    private int skipLevels(int newLevel, Iterable<SSTableReader> added)
    {
        while (maxBytesForLevel(newLevel) < SSTableReader.getTotalBytes(added)
            && generations[(newLevel + 1)].isEmpty())
        {
            newLevel++;
        }
        return newLevel;
    }

    public synchronized void promote(Iterable<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        assert !Iterables.isEmpty(removed); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isDebugEnabled())
            logger.debug("Replacing [" + toString(removed) + "]");

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minimumLevel = Integer.MAX_VALUE;
        int maximumLevel = 0;
        for (SSTableReader sstable : removed)
        {
            int thisLevel = levelOf(sstable);
            maximumLevel = Math.max(maximumLevel, thisLevel);
            minimumLevel = Math.min(minimumLevel, thisLevel);
            remove(sstable);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (!added.iterator().hasNext())
            return;

        int newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
        newLevel = skipLevels(newLevel, added);
        assert newLevel > 0;
        if (logger.isDebugEnabled())
            logger.debug("Adding [{}] at L{}", toString(added), newLevel);

        lastCompactedKeys[minimumLevel] = SSTable.sstableOrdering.max(added).last;
        for (SSTableReader ssTableReader : added)
            add(ssTableReader, newLevel);

        serialize();
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
                   .append(levelOf(sstable))
                   .append("), ");
        }
        return builder.toString();
    }

    private double maxBytesForLevel (int level)
    {
        return level == 0
               ? 4 * maxSSTableSizeInMB * 1024 * 1024
               : Math.pow(10, level) * maxSSTableSizeInMB * 1024 * 1024;
    }

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
            double score = SSTableReader.getTotalBytes(sstables) / maxBytesForLevel(i);
            logger.debug("Compaction score for level {} is {}", i, score);

            // L0 gets a special case that if we don't have anything more important to do,
            // we'll go ahead and compact even just one sstable
            if (score > 1.001 || i == 0)
            {
                Collection<SSTableReader> candidates = getCandidatesFor(i);
                if (logger.isDebugEnabled())
                    logger.debug("Compaction candidates for L{} are {}", i, toString(candidates));
                return candidates;
            }
        }

        return Collections.emptyList();
    }

    public int getLevelSize(int i)
    {

        return generations.length > i ? generations[i].size() : 0;
    }

    private void logDistribution()
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

    private int levelOf(SSTableReader sstable)
    {
        for (int level = 0; level < generations.length; level++)
        {
            if (generations[level].contains(sstable))
                return level;
        }
        return -1;
    }

    private void remove(SSTableReader reader)
    {
        int level = levelOf(reader);
        assert level >= 0 : reader + " not present in manifest";
        generations[level].remove(reader);
    }

    private void add(SSTableReader sstable, int level)
    {
        generations[level].add(sstable);
    }

    private static List<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> candidates)
    {
        List<SSTableReader> overlapped = new ArrayList<SSTableReader>();
        overlapped.add(sstable);

        Range promotedRange = new Range(sstable.first.token, sstable.last.token);
        for (SSTableReader candidate : candidates)
        {
            Range candidateRange = new Range(candidate.first.token, candidate.last.token);
            if (candidateRange.intersects(promotedRange))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private Collection<SSTableReader> getCandidatesFor(int level)
    {
        assert !generations[level].isEmpty();
        logger.debug("Choosing candidates for L{}", level);

        if (level == 0)
        {
            // because L0 files may overlap each other, we treat compactions there specially:
            // a L0 compaction also checks other L0 files for overlap.
            Set<SSTableReader> candidates = new HashSet<SSTableReader>();
            // pick the oldest sstable from L0, and any that overlap with it
            List<SSTableReader> ageSortedSSTables = new ArrayList<SSTableReader>(generations[0]);
            Collections.sort(ageSortedSSTables, SSTable.maxTimestampComparator);
            List<SSTableReader> L0 = overlapping(ageSortedSSTables.get(0), generations[0]);
            L0 = L0.size() > MAX_COMPACTING_L0 ? L0.subList(0, MAX_COMPACTING_L0) : L0;
            // add the overlapping ones from L1
            for (SSTableReader sstable : L0)
                candidates.addAll(overlapping(sstable, generations[1]));
            return candidates;
        }

        // for non-L0 compactions, pick up where we left off last time
        Collections.sort(generations[level], SSTable.sstableComparator);
        for (SSTableReader sstable : generations[level])
        {
            // the first sstable that is > than the marked
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
                return overlapping(sstable, generations[(level + 1)]);
        }
        // or if there was no last time, start with the first sstable
        return overlapping(generations[level].get(0), generations[(level + 1)]);
    }

    public synchronized void serialize()
    {
        File manifestFile = tryGetManifest(cfs);
        if (manifestFile == null)
            manifestFile = new File(new File(DatabaseDescriptor.getAllDataFileLocations()[0], cfs.table.name), cfs.columnFamily + ".json");
        File oldFile = new File(manifestFile.getPath().replace(EXTENSION, "-old.json"));
        File tmpFile = new File(manifestFile.getPath().replace(EXTENSION, "-tmp.json"));

        JsonFactory f = new JsonFactory();
        try
        {
            JsonGenerator g = f.createJsonGenerator(tmpFile, JsonEncoding.UTF8);
            g.useDefaultPrettyPrinter();
            g.writeStartObject();
            g.writeArrayFieldStart("generations");
            for (int level = 0; level < generations.length; level++)
            {
                g.writeStartObject();
                g.writeNumberField("generation", level);
                g.writeArrayFieldStart("members");
                for (SSTableReader ssTableReader : generations[level])
                    g.writeNumber(ssTableReader.descriptor.generation);
                g.writeEndArray(); // members

                g.writeEndObject(); // generation
            }
            g.writeEndArray(); // for field generations
            g.writeEndObject(); // write global object
            g.close();

            if (oldFile.exists() && manifestFile.exists())
                FileUtils.deleteWithConfirm(oldFile);
            if (manifestFile.exists())
                FileUtils.renameWithConfirm(manifestFile, oldFile);
            assert tmpFile.exists();
            FileUtils.renameWithConfirm(tmpFile, manifestFile);
            logger.debug("Saved manifest {}", manifestFile);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static File tryGetManifest(ColumnFamilyStore cfs)
    {
        for (String dir : DatabaseDescriptor.getAllDataFileLocations())
        {
            File manifestFile = new File(new File(dir, cfs.table.name), cfs.columnFamily + EXTENSION);
            if (manifestFile.exists())
            {
                logger.debug("Found manifest at {}", manifestFile);
                return manifestFile;
            }
        }
        logger.debug("No level manifest found");
        return null;
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

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public int getEstimatedTasks()
    {
        int n = 0;
        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = generations[i];
            n += Math.max(0L, SSTableReader.getTotalBytes(sstables) - maxBytesForLevel(i)) / (maxSSTableSizeInMB * 1024 * 1024);
        }
        return n;
    }
}
