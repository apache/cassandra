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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * The statistics for levelled compaction.
 * <p/>
 * Implements serializable to allow structured info to be returned via JMX.
 */
public class LeveledCompactionStatistics extends CompactionAggregateStatistics
{
    private static final Collection<String> HEADER = ImmutableList.copyOf(Iterables.concat(ImmutableList.of("Level", "Score"),
                                                                                           CompactionAggregateStatistics.HEADER,
                                                                                           ImmutableList.of("Tot/Read/Written",
                                                                                                                            "Read: Tot/Prev/Next",
                                                                                                                            "Written: Tot/New",
                                                                                                                            "WA (tot_written/read_prev)")));

    private static final long serialVersionUID = 3695927592357744816L;

    /** The current level */
    private final int level;

    /** The score of this level */
    private final double score;

    /** Total bytes of the sstables selected for compaction */
    private final long tot;

    /** Total bytes read during compaction between levels N and N+1. This includes bytes read from this level (N) and from the next level (N+1) */
    private final long totRead;

    /** Bytes read from the current level (N) during compaction between levels N and N+1 */
    private final long readLevel;

    /** Total bytes written during compaction between levels N and N+1 */
    private final long totWritten;

    /**
     * Additional RocksDB metrics we may want to consider:
     * Moved(GB): Bytes moved to level N+1 during compaction. In this case there is no IO other than updating the manifest to indicate that a file which used to be in level X is now in level Y
     * Rd(MB/s): The rate at which data is read during compaction between levels N and N+1. This is (Read(GB) * 1024) / duration where duration is the time for which compactions are in progress from level N to N+1.
     * Wr(MB/s): The rate at which data is written during compaction. See Rd(MB/s).
     * Rn(cnt): Total files read from level N during compaction between levels N and N+1
     * Rnp1(cnt): Total files read from level N+1 during compaction between levels N and N+1
     * Wnp1(cnt): Total files written to level N+1 during compaction between levels N and N+1
     * Wnew(cnt): (Wnp1(cnt) - Rnp1(cnt)) -- Increase in file count as result of compaction between levels N and N+1
     * Comp(sec): Total time spent doing compactions between levels N and N+1
     * Comp(cnt): Total number of compactions between levels N and N+1
     * Avg(sec): Average time per compaction between levels N and N+1
     * Stall(sec): Total time writes were stalled because level N+1 was uncompacted (compaction score was high)
     * Stall(cnt): Total number of writes stalled because level N+1 was uncompacted
     * Avg(ms): Average time in milliseconds a write was stalled because level N+1 was uncompacted
     * KeyIn: number of records compared during compaction
     * KeyDrop: number of records dropped (not written out) during compaction
     */

    public LeveledCompactionStatistics(int level,
                                       double score,
                                       int numCompactions,
                                       int numCompactionsInProgress,
                                       int numSSTables,
                                       int numCandidateSSTables,
                                       int numCompactingSSTables,
                                       long sizeInBytes,
                                       double readThroughput,
                                       double writeThroughput,
                                       long tot,
                                       long totRead,
                                       long readLevel,
                                       long totWritten)
    {
        super(numCompactions, numCompactionsInProgress, numSSTables, numCandidateSSTables, numCompactingSSTables, sizeInBytes, readThroughput, writeThroughput);

        this.level = level;
        this.score = score;
        this.tot = tot;
        this.totRead = totRead;
        this.readLevel = readLevel;
        this.totWritten = totWritten;
    }

    /** The current level */
    @JsonProperty
    public int level()
    {
        return level;
    }

    /** The score of a level is the level size in bytes of all its files dived by the ideal
     * level size if applicable, or zero for tiered strategies */
    @JsonProperty
    public double score()
    {
        return score;
    }

    /** Total bytes of the sstables selected for compaction */
    @JsonProperty
    public long tot()
    {
        return tot;
    }

    /** Total uncompressed bytes read during compaction between this level and the next. This includes bytes read from this level (N) and from the next level (N+1) */
    @JsonProperty
    public long read()
    {
        return totRead;
    }

    /** Uncompressed bytes read from the previous level (N) during compaction between levels N and N+1*/
    @JsonProperty
    public long readLevel()
    {
        return readLevel;
    }

    /** Uncompressed bytes read from the next level (N+1) during compaction between levels N and N+1 */
    @JsonProperty
    public long readNext()
    {
        return totRead - readLevel;
    }

    /** Uncompressed  bytes written during compaction between levels N and N+1 */
    @JsonProperty
    public long written()
    {
        return totWritten;
    }

    /** Uncompressed  bytes written to level N+1, calculated as total bytes written - bytes read from N+1 */
    @JsonProperty
    public long writtenNew()
    {
        return totWritten - readNext();
    }

    /** W-Amp: total bytes written divided by the bytes read from level N. */
    @JsonProperty
    public double writeAmpl()
    {
        return readLevel() > 0 ? (double)totWritten / readLevel() : Double.NaN;
    }

    @Override
    protected Collection<String> header()
    {
        return HEADER;
    }

    @Override
    protected Collection<String> data()
    {
        List<String> data = new ArrayList<>(HEADER.size());
        data.add(Integer.toString(level()));
        data.add(String.format("%.3f", score()));

        data.addAll(super.data());

        data.add(toString(tot()) + '/' + toString(read()) + '/' + toString(written()));
        data.add(toString(read()) + '/' + toString(readLevel()) + '/' + toString(readNext()));
        data.add(toString(written()) + '/' + toString(writtenNew()));
        data.add(String.format("%.3f", writeAmpl()));
        return data;
    }
}