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
import java.util.concurrent.TimeUnit;

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
    public static final String NO_SHARD = "";

    protected static final Collection<String> HEADER = ImmutableList.of("Tot. SSTables",
                                                                        "Tot. size (bytes)",
                                                                        "Compactions",
                                                                        "Comp. SSTables",
                                                                        "Read (bytes/sec)",
                                                                        "Write (bytes/sec)",
                                                                        "Tot. comp. size/Read/Written (bytes)");
    /** The number of compactions that are either pending or in progress */
    protected final int numCompactions;

    /** The number of compactions that are in progress */
    protected final int numCompactionsInProgress;

    /** The total number of sstables, whether they need compacting or not */
    protected final int numSSTables;

    /** The total number of expired sstables */
    protected final int numExpiredSSTables;

    /** The number of sstables that are compaction candidates */
    protected final int numCandidateSSTables;

    /** The number of sstables that are currently compacting */
    protected final int numCompactingSSTables;

    /** The size in bytes (on disk) of the total sstables */
    protected final long sizeInBytes;

    /** The total uncompressed size of the sstables selected for compaction */
    protected final long totBytesToCompact;

    /** The total uncompressed size of the expired sstables that are going to be dropped during compaction */
    protected final long totalBytesToDrop;

    /** The number of bytes read so far for the compactions here - read throughput is calculated based on this */
    protected final long readBytes;

    /** The number of bytes written so far for the compaction here - write throughput is calculated based on this */
    protected final long writtenBytes;

    /** The read throughput in bytes per second */
    protected final double readThroughput;

    /** The write throughput in bytes per second */
    protected final double writeThroughput;

    /** The hotness of this aggregate (where applicable) */
    protected final double hotness;

    CompactionAggregateStatistics(int numCompactions,
                                  int numCompactionsInProgress,
                                  int numSSTables,
                                  int numExpiredSSTables,
                                  int numCandidateSSTables,
                                  int numCompactingSSTables,
                                  long sizeInBytes,
                                  long totBytesToCompact,
                                  long totBytesToDrop,
                                  long readBytes,
                                  long writtenBytes,
                                  long durationNanos,
                                  double hotness)
    {
        this.numCompactions = numCompactions;
        this.numCompactionsInProgress = numCompactionsInProgress;
        this.numCandidateSSTables = numCandidateSSTables;
        this.numCompactingSSTables = numCompactingSSTables;
        this.numSSTables = numSSTables;
        this.numExpiredSSTables = numExpiredSSTables;
        this.sizeInBytes = sizeInBytes;
        this.totBytesToCompact = totBytesToCompact;
        this.totalBytesToDrop = totBytesToDrop;
        this.readBytes = readBytes;
        this.writtenBytes = writtenBytes;
        this.readThroughput = durationNanos == 0 ? 0 : ((double) readBytes / durationNanos) * TimeUnit.SECONDS.toNanos(1);
        this.writeThroughput = durationNanos == 0 ? 0 : ((double) writtenBytes / durationNanos) * TimeUnit.SECONDS.toNanos(1);
        this.hotness = hotness;
    }

    CompactionAggregateStatistics(CompactionAggregateStatistics base)
    {
        this.numCompactions = base.numCompactions;
        this.numCompactionsInProgress = base.numCompactionsInProgress;
        this.numCandidateSSTables = base.numCandidateSSTables;
        this.numCompactingSSTables = base.numCompactingSSTables;
        this.numExpiredSSTables = base.numExpiredSSTables;
        this.numSSTables = base.numSSTables;
        this.sizeInBytes = base.sizeInBytes;
        this.totBytesToCompact = base.totBytesToCompact;
        this.totalBytesToDrop = base.totalBytesToDrop;
        this.readBytes = base.readBytes;
        this.writtenBytes = base.writtenBytes;
        this.readThroughput = base.readThroughput;
        this.writeThroughput = base.writeThroughput;
        this.hotness = base.hotness;
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

    /** The total uncompressed size of the sstables selected for compaction */
    @JsonProperty
    public long tot()
    {
        return totBytesToCompact;
    }

    /** The number of bytes read so far for the compactions here - read throughput is calculated based on this */
    @JsonProperty
    public long read()
    {
        return readBytes;
    }

    /** The number of bytes written so far for the compaction here - write throughput is calculated based on this */
    @JsonProperty
    public long written()
    {
        return writtenBytes;
    }

    /** The hotness of this aggregate (where applicable) */
    @JsonProperty
    public double hotness()
    {
        return hotness;
    }

    /** The name of the shard, empty if the compaction is not sharded (the default). */
    @JsonProperty
    public String shard()
    {
        return NO_SHARD;
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
                                prettyPrintMemoryPerSecond((long) writeThroughput()),
                                prettyPrintMemory(totBytesToCompact) + '/' + prettyPrintMemory(readBytes) + '/' + prettyPrintMemory(writtenBytes));
    }

    protected String toString(long value)
    {
        return FBUtilities.prettyPrintMemory(value);
    }
}