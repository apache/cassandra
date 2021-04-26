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

import java.io.Serializable;
import java.util.Collection;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemoryPerSecond;

/**
 * The statistics for a {@link CompactionAggregate}.
 * <p/>
 * It must be serializable for JMX and convertible to JSON for insights. The JSON
 * properties are published to insights so changing them has a downstream impact.
 */
public class CompactionAggregateStatistics implements Serializable
{
    protected static final Collection<String> HEADER = ImmutableList.of("Tot. sstables", "Size (bytes)", "Compactions", "Comp. Sstables", "Read (bytes/sec)", "Write (bytes/sec)");

    /** The number of compactions that are either pending or in progress */
    private final int numCompactions;

    /** The number of compactions that are in progress */
    private final int numCompactionsInProgress;

    /** The total number of sstables, whether they need compacting or not */
    private final int numSSTables;

    /** The number of sstables that are compaction candidates */
    private final int numCandidateSSTables;

    /** The number of sstables that are currently compacting */
    private final int numCompactingSSTables;

    /** The size in bytes (on disk) of the total sstables */
    private final long sizeInBytes;

    /** The read throughput in bytes per second */
    private final double readThroughput;

    /** The write throughput in bytes per second */
    private final double writeThroughput;

    CompactionAggregateStatistics(int numCompactions,
                                  int numCompactionsInProgress,
                                  int numSSTables,
                                  int numCandidateSSTables,
                                  int numCompactingSSTables,
                                  long sizeInBytes,
                                  double readThroughput,
                                  double writeThroughput)
    {
        this.numCompactions = numCompactions;
        this.numCompactionsInProgress = numCompactionsInProgress;
        this.numCandidateSSTables = numCandidateSSTables;
        this.numCompactingSSTables = numCompactingSSTables;
        this.numSSTables = numSSTables;
        this.sizeInBytes = sizeInBytes;
        this.readThroughput = readThroughput;
        this.writeThroughput = writeThroughput;
    }

    /** The number of compactions that are either pending or in progress */
    @JsonProperty
    public int numCompactions()
    {
        return numCompactions;
    }

    /** The number of compactions that are in progress */
    @JsonProperty
    public int numCompactionsInProgress()
    {
        return numCompactionsInProgress;
    }

    /** The total number of sstables, whether they need compacting or not */
    @JsonProperty
    public int numSSTables()
    {
        return numSSTables;
    }

    /** The number of sstables that are part of this level */
    @JsonProperty
    public int numCandidateSSTables()
    {
        return numCandidateSSTables;
    }

    /** The number of sstables that are currently part of a compaction operation */
    @JsonProperty
    public int numCompactingSSTables()
    {
        return numCompactingSSTables;
    }

    /** The size in bytes (on disk) of the total sstables */
    public long sizeInBytes()
    {
        return sizeInBytes;
    }

    /** The read throughput in bytes per second */
    @JsonProperty
    public double readThroughput()
    {
        return readThroughput;
    }

    /** The write throughput in bytes per second */
    @JsonProperty
    public double writeThroughput()
    {
        return writeThroughput;
    }

    @Override
    public String toString()
    {
        return data().toString();
    }

    protected Collection<String> header()
    {
        return HEADER;
    }

    protected Collection<String> data()
    {
        return ImmutableList.of(Integer.toString(numSSTables),
                                prettyPrintMemory(sizeInBytes),
                                        Integer.toString(numCompactions()) + '/' + numCompactionsInProgress(),
                                        Integer.toString(numCandidateSSTables()) + '/' + numCompactingSSTables(),
                                prettyPrintMemoryPerSecond((long) readThroughput()),
                                prettyPrintMemoryPerSecond((long) writeThroughput()));
    }

    protected String toString(long value)
    {
        return FBUtilities.prettyPrintMemory(value);
    }
}