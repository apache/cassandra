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
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The statistics for size tiered compaction.
 * <p/>
 * Implements serializable to allow structured info to be returned via JMX.
 */
public class UnifiedCompactionStatistics extends CompactionAggregateStatistics
{
    private static final Collection<String> HEADER = ImmutableList.copyOf(Iterables.concat(ImmutableList.of("Bucket", "W", "T", "F", "min size", "max size"),
                                                                                           CompactionAggregateStatistics.HEADER));

    private static final long serialVersionUID = 3695927592357345266L;

    /** The bucket number */
    private final int bucket;

    /** The survival factor o */
    private final double survivalFactor;

    /** The scaling parameter W */
    private final int scalingParameter;

    /** The number of SSTables T that trigger a compaction */
    private final int threshold;

    /** The fanout size F */
    private final int fanout;

    /** The minimum size for an SSTable that belongs to this bucket */
    private final long minSizeBytes;

    /** The maximum size for an SSTable run that belongs to this bucket */
    private final long maxSizeBytes;

    /** The name of the shard */
    private final String shard;

    UnifiedCompactionStatistics(CompactionAggregateStatistics base,
                                int bucketIndex,
                                double survivalFactor,
                                int scalingParameter,
                                int threshold,
                                int fanout,
                                long minSizeBytes,
                                long maxSizeBytes,
                                String shard)
    {
        super(base);

        this.bucket = bucketIndex;
        this.survivalFactor = survivalFactor;
        this.scalingParameter = scalingParameter;
        this.threshold = threshold;
        this.fanout = fanout;
        this.minSizeBytes = minSizeBytes;
        this.maxSizeBytes = maxSizeBytes;
        this.shard = shard;
    }

    /** The bucket number */
    @JsonProperty
    public int bucket()
    {
        return bucket;
    }

    /** The survival factor o, currently always one */
    @JsonProperty
    public double survivalFactor()
    {
        return survivalFactor;
    }

    /** The scaling parameter W */
    @JsonProperty
    public int scalingParameter()
    {
        return scalingParameter;
    }

    /** The number of SSTables T that trigger a compaction */
    @JsonProperty
    public int threshold()
    {
        return threshold;
    }

    /** The fanout size F */
    @JsonProperty
    public int fanout()
    {
        return fanout;
    }

    /** The minimum size for an SSTable that belongs to this bucket */
    @JsonProperty
    public long minSizeBytes()
    {
        return minSizeBytes;
    }

    /** The maximum size for an SSTable that belongs to this bucket */
    @JsonProperty
    public long maxSizeBytes()
    {
        return maxSizeBytes;
    }

    /** The name of the shard, empty if the compaction is not sharded (the default). */
    @JsonProperty
    @Override
    public String shard()
    {
        return shard;
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
        data.add(Integer.toString(bucket()));
        data.add(Integer.toString(scalingParameter));
        data.add(Integer.toString(threshold));
        data.add(Integer.toString(fanout));
        data.add(FBUtilities.prettyPrintMemory(minSizeBytes));
        data.add(FBUtilities.prettyPrintMemory(maxSizeBytes));

        data.addAll(super.data());

        return data;
    }
}