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

public abstract class TieredCompactionStatistics extends CompactionAggregateStatistics
{
    private static final Collection<String> HEADER = ImmutableList.copyOf(Iterables.concat(ImmutableList.of("Bucket", "Hotness"),
                                                                                           CompactionAggregateStatistics.HEADER,
                                                                                           ImmutableList.of("Tot/Read/Written")));

    private static final long serialVersionUID = 3695927592357987916L;
    /** The total read hotness of the sstables */
    protected final double hotness;
    /** Total uncompressed bytes of the sstables */
    protected final long tot;
    /** Total bytes read by ongoing compactions */
    protected final long read;
    /** Total bytes written by ongoing compactions */
    protected final long written;

    public TieredCompactionStatistics(int numCompactions,
                                      int numCompactionsInProgress,
                                      int numSSTables,
                                      int numCandidateSSTables,
                                      int numCompactingSSTables,
                                      long sizeInBytes,
                                      double readThroughput,
                                      double writeThroughput,
                                      double hotness,
                                      long tot,
                                      long read,
                                      long written)
    {
        super(numCompactions, numCompactionsInProgress, numSSTables, numCandidateSSTables, numCompactingSSTables, sizeInBytes, readThroughput, writeThroughput);

        this.hotness = hotness;
        this.tot = tot;
        this.read = read;
        this.written = written;
    }

    /** The total read hotness of the sstables */
    @JsonProperty
    public double hotness()
    {
        return hotness;
    }

    /** Total uncompressed bytes of the sstables */
    @JsonProperty
    public long tot()
    {
        return tot;
    }

    /** Uncompressed bytes read by compactions so far. */
    @JsonProperty
    public long read()
    {
        return read;
    }

    /** Uncompressed  bytes written by compactions so far. */
    @JsonProperty
    public long written()
    {
        return written;
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
        data.add(tierValue());
        data.add(String.format("%.4f", hotness));

        data.addAll(super.data());

        data.add(toString(tot()) + '/' + toString(read()) + '/' + toString(written()));
        return data;
    }

    protected abstract String tierValue();
}
