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

import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);

    private final ColumnFamilyStore cfs;
    private final List<SSTableReader>[] generations;
    private final DecoratedKey[] lastCompactedKeys;
    private final int maxSSTableSizeInMB;

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
        ObjectMapper m = new ObjectMapper();
        try
        {
            File manifestFile = tryGetManifest(cfs);

            if (manifestFile != null && manifestFile.exists())
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
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public synchronized void add(SSTableReader reader)
    {
        logger.debug("Adding {} to L0", reader);
        add(reader, 0);
        serialize();
    }

    // if the number of SSTables in the current compacted set exeeds the target level, find an empty level
    private int skipLevels(int newLevel, Iterable<SSTableReader> added)
    {
        // skip newlevel if the resulting sstables exceed newlevel threshold
        if (maxBytesForLevel(newLevel) < SSTableReader.getTotalBytes(added)
            && SSTableReader.getTotalBytes(generations[(newLevel + 1)]) == 0)
        {
            newLevel = skipLevels(newLevel + 1, added);
        }
        return newLevel;
    }

    public synchronized void promote(Iterable<SSTableReader> removed, Iterable<SSTableReader> added)
    {
        logger.debug("Replacing [{}] with [{}]", StringUtils.join(removed.iterator(), ", "), StringUtils.join(added.iterator(), ", "));
        
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

        lastCompactedKeys[minimumLevel] = SSTable.sstableOrdering.max(added).last;
        logger.debug("Adding [{}] to L{}",
                     StringUtils.join(added.iterator(), ", "), newLevel);
        for (SSTableReader ssTableReader : added)
            add(ssTableReader, newLevel);

        serialize();
    }


    private double maxBytesForLevel (int level)
    {
        return level == 0
               ? 4 * maxSSTableSizeInMB * 1024 * 1024
               : Math.pow(10, level) * maxSSTableSizeInMB * 1024 * 1024;
    }

    public synchronized Collection<SSTableReader> getCompactionCandidates()
    {
        logDistribution();

        double bestScore = -1;
        int bestLevel = -1;
        for (int level = 0; level < generations.length; level++)
        {
            List<SSTableReader> sstables = generations[level];
            if (sstables.isEmpty())
                continue;

            double score = SSTableReader.getTotalBytes(sstables) / maxBytesForLevel(level);
            //if we're idle and we don't have anything better to do schedule a compaction for L0
            //by setting its threshold to some very low value
            score = (level == 0 && score < 1) ? 1.001 : 0;
            logger.debug("Compaction score for level {} is {}", level, score);
            if (score > bestScore)
            {
                bestScore = score;
                bestLevel = level;
            }
        }

        // if we have met at least one of our thresholds then trigger a compaction
        return bestScore > 1 ? getCandidatesFor(bestLevel) : Collections.<SSTableReader>emptyList();
    }

    public int getLevelSize(int i)
    {

        return generations.length > i ? generations[i].size() : 0;
    }

    public void logDistribution()
    {
        for (int i = 0; i < generations.length; i++)
            logger.debug("Level {} contains {} SSTables", i, generations[i].size());
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

        if (level == 0)
        {
            // because L0 files may overlap each other, we treat compactions there specially:
            // a L0 compaction also checks other L0 files for overlap.
            Set<SSTableReader> candidates = new HashSet<SSTableReader>();
            Set<SSTableReader> remaining = new HashSet<SSTableReader>(generations[0]);

            while (!remaining.isEmpty())
            {
                // pick a random sstable from L0, and any that overlap with it
                List<SSTableReader> L0 = overlapping(remaining.iterator().next(), remaining);
                // add the overlapping ones from L1
                for (SSTableReader sstable : L0)
                {
                    candidates.addAll(overlapping(sstable, generations[1]));
                    remaining.remove(sstable);
                }
            }
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
        String dataFileLocation = getDataFilePrefix(cfs);
        String tempManifestFileName = dataFileLocation + cfs.getColumnFamilyName() + "-" + "tmp.json";
        String manifestFileName = dataFileLocation + cfs.getColumnFamilyName() + ".json";
        String oldManifestFileName = dataFileLocation + cfs.getColumnFamilyName() + "-" + "old.json";

        File tmpManifest = new File(tempManifestFileName);

        JsonFactory f = new JsonFactory();

        try
        {
            JsonGenerator g = f.createJsonGenerator(tmpManifest, JsonEncoding.UTF8);
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
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        File oldFile = new File(oldManifestFileName);
        if (oldFile.exists())
            oldFile.delete();
        File currentManifest = new File(manifestFileName);
        if (currentManifest.exists())
            currentManifest.renameTo(new File(oldManifestFileName));
        if (tmpManifest.exists())
            tmpManifest.renameTo(new File(manifestFileName));
    }

    public static File tryGetManifest(ColumnFamilyStore cfs)
    {
        for (String dataFileLocation : DatabaseDescriptor.getAllDataFileLocations())
        {
            dataFileLocation = getDataFilePrefix(cfs);
            String manifestFileName = dataFileLocation + System.getProperty("file.separator") + cfs.table.name + ".json";
            File manifestFile = new File(manifestFileName);
            if (manifestFile.exists())
                return manifestFile;
        }
        return null;
    }

    public static String getDataFilePrefix(ColumnFamilyStore cfs)
    {
        return DatabaseDescriptor.getAllDataFileLocations()[0] + System.getProperty("file.separator") + cfs.table.name + System.getProperty("file.separator");
    }
}
