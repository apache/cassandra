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
package org.apache.cassandra.utils.streamhist;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder.DataHolder;


public class TombstoneHistogram
{
    public static final HistogramSerializer serializer = new HistogramSerializer();

    // Buffer with point-value pair
    private final DataHolder bin;

    // maximum bin size for this histogram
    private final int maxBinSize;

    // voluntarily give up resolution for speed
    private final int roundSeconds;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     *
     * @param maxBinSize maximum number of bins this histogram can have
     */
    private TombstoneHistogram(int maxBinSize, int roundSeconds, Map<Double, Long> bin)
    {
        this.maxBinSize = maxBinSize;
        this.roundSeconds = roundSeconds;
        this.bin = new DataHolder(maxBinSize + 1, roundSeconds);

        for (Map.Entry<Double, Long> entry : bin.entrySet())
        {
            final int current = entry.getKey().intValue();
            this.bin.addValue(current, entry.getValue().intValue());
        }
    }

    TombstoneHistogram(int maxBinSize, int roundSeconds, DataHolder holder)
    {
        this.maxBinSize = maxBinSize;
        this.roundSeconds = roundSeconds;
        bin = new DataHolder(holder);
    }

    public static TombstoneHistogram createDefault()
    {
        return new TombstoneHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS, Collections.emptyMap());
    }

    /**
     * Merges given histogram with this histogram.
     *
     * @param other histogram to merge
     */
    public TombstoneHistogram merge(TombstoneHistogram other)
    {
        if (other == null)
            return this;

        StreamingTombstoneHistogramBuilder builder = new StreamingTombstoneHistogramBuilder(maxBinSize, SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE, roundSeconds);

        // This can be optimized, but it's only called in tests atm ... maybe can remove
        for (Map.Entry<Integer, long[]> entry : getAsMap().entrySet())
            builder.update(entry.getKey(), (int) entry.getValue()[0]);
        for (Map.Entry<Integer, long[]> entry : other.getAsMap().entrySet())
            builder.update(entry.getKey(), (int) entry.getValue()[0]);

        return builder.build();
    }

    /**
     * Calculates estimated number of points in interval [-inf,b].
     *
     * @param b upper bound of a interval to calculate sum
     * @return estimated number of points in a interval [-inf,b].
     */
    public double sum(double b)
    {
        return bin.sum((int) b);
    }

    public Map<Integer, long[]> getAsMap()
    {
        TreeMap<Integer, long[]> bin = new TreeMap<>();
        this.bin.forEach(datum -> bin.put(datum[0], new long[]{ datum[1] }));
        return bin;
    }

    public static class HistogramSerializer implements ISerializer<TombstoneHistogram>
    {
        public void serialize(TombstoneHistogram histogram, DataOutputPlus out) throws IOException
        {
            out.writeInt(histogram.maxBinSize);
            Map<Integer, long[]> entries = histogram.getAsMap();
            out.writeInt(entries.size());
            for (Map.Entry<Integer, long[]> entry : entries.entrySet())
            {
                out.writeDouble(entry.getKey().doubleValue());
                out.writeLong(entry.getValue()[0]);
            }
        }

        public TombstoneHistogram deserialize(DataInputPlus in) throws IOException
        {
            int maxBinSize = in.readInt();
            int size = in.readInt();
            Map<Double, Long> tmp = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                tmp.put(in.readDouble(), in.readLong());
            }

            return new TombstoneHistogram(maxBinSize, maxBinSize, tmp);
        }

        public long serializedSize(TombstoneHistogram histogram)
        {
            long size = TypeSizes.sizeof(histogram.maxBinSize);
            Map<Integer, long[]> entries = histogram.getAsMap();
            size += TypeSizes.sizeof(entries.size());
            // size of entries = size * (8(double) + 8(long))
            size += entries.size() * (8L + 8L);
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TombstoneHistogram))
            return false;

        TombstoneHistogram that = (TombstoneHistogram) o;
        return maxBinSize == that.maxBinSize &&
               bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bin.hashCode(), maxBinSize);
    }


}
