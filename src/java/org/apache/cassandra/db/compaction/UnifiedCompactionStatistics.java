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
    private static final Collection<String> HEADER = ImmutableList.copyOf(Iterables.concat(ImmutableList.of("Level", "W", "Min Density", "Max Density", "Overlap"),
                                                                                           CompactionAggregateStatistics.HEADER));

    private static final long serialVersionUID = 3695927592357345266L;

    /** The bucket number */
    private final int bucket;

    /** The survival factor o */
    private final double survivalFactor;

    /** The scaling parameter W */
    private final int scalingParameter;

    /** The minimum density for an SSTable that belongs to this bucket */
    private final double minDensityBytes;

    /** The maximum density for an SSTable run that belongs to this bucket */
    private final double maxDensityBytes;

    /** The maximum number of overlapping sstables in the shard */
    private final int maxOverlap;

    /** The name of the shard */
    private final String shard;

    UnifiedCompactionStatistics(CompactionAggregateStatistics base,
                                int bucketIndex,
                                double survivalFactor,
                                int scalingParameter,
                                double minDensityBytes,
                                double maxDensityBytes,
                                int maxOverlap,
                                String shard)
    {
        super(base);

        this.bucket = bucketIndex;
        this.survivalFactor = survivalFactor;
        this.scalingParameter = scalingParameter;
        this.minDensityBytes = minDensityBytes;
        this.maxDensityBytes = maxDensityBytes;
        this.maxOverlap = maxOverlap;
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

    /** The minimum size for an SSTable that belongs to this bucket */
    @JsonProperty
    public double minDensityBytes()
    {
        return minDensityBytes;
    }

    /** The maximum size for an SSTable that belongs to this bucket */
    @JsonProperty
    public double maxDensityBytes()
    {
        return maxDensityBytes;
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
        data.add(UnifiedCompactionStrategy.printScalingParameter(scalingParameter));
        data.add(FBUtilities.prettyPrintBinary(minDensityBytes, "B", " "));
        data.add(FBUtilities.prettyPrintBinary(maxDensityBytes, "B", " "));

        data.add(Integer.toString(maxOverlap));

        data.addAll(super.data());

        return data;
    }
}