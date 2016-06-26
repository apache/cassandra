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
package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Histogram that can be constructed from streaming of data.
 *
 * The algorithm is taken from following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A Streaming Parallel Decision Tree Algorithm" (2010)
 * http://jmlr.csail.mit.edu/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
public class StreamingHistogram
{
    public static final StreamingHistogramSerializer serializer = new StreamingHistogramSerializer();

    // TreeMap to hold bins of histogram.
    // The key is a numeric type so we can avoid boxing/unboxing streams of different key types
    // The value is a unboxed long array always of length == 1
    // Serialized Histograms always writes with double keys for backwards compatibility
    private final TreeMap<Number, long[]> bin;

    // maximum bin size for this histogram
    private final int maxBinSize;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     * @param maxBinSize maximum number of bins this histogram can have
     */
    public StreamingHistogram(int maxBinSize)
    {
        this.maxBinSize = maxBinSize;
        bin = new TreeMap<>((o1, o2) -> {
            if (o1.getClass().equals(o2.getClass()))
                return ((Comparable)o1).compareTo(o2);
            else
            	return Double.compare(o1.doubleValue(), o2.doubleValue());
        });
    }

    private StreamingHistogram(int maxBinSize, Map<Double, Long> bin)
    {
        this(maxBinSize);
        for (Map.Entry<Double, Long> entry : bin.entrySet())
            this.bin.put(entry.getKey(), new long[]{entry.getValue()});
    }

    /**
     * Adds new point p to this histogram.
     * @param p
     */
    public void update(Number p)
    {
        update(p, 1L);
    }

    /**
     * Adds new point p with value m to this histogram.
     * @param p
     * @param m
     */
    public void update(Number p, long m)
    {
        long[] mi = bin.get(p);
        if (mi != null)
        {
            // we found the same p so increment that counter
            mi[0] += m;
        }
        else
        {
            mi = new long[]{m};
            bin.put(p, mi);
            // if bin size exceeds maximum bin size then trim down to max size
            while (bin.size() > maxBinSize)
            {
                // find points p1, p2 which have smallest difference
                Iterator<Number> keys = bin.keySet().iterator();
                double p1 = keys.next().doubleValue();
                double p2 = keys.next().doubleValue();
                double smallestDiff = p2 - p1;
                double q1 = p1, q2 = p2;
                while (keys.hasNext())
                {
                    p1 = p2;
                    p2 = keys.next().doubleValue();
                    double diff = p2 - p1;
                    if (diff < smallestDiff)
                    {
                        smallestDiff = diff;
                        q1 = p1;
                        q2 = p2;
                    }
                }
                // merge those two
                long[] a1 = bin.remove(q1);
                long[] a2 = bin.remove(q2);
                long k1 = a1[0];
                long k2 = a2[0];

                a1[0] += k2;
                bin.put((q1 * k1 + q2 * k2) / (k1 + k2), a1);
            }
        }
    }

    /**
     * Merges given histogram with this histogram.
     *
     * @param other histogram to merge
     */
    public void merge(StreamingHistogram other)
    {
        if (other == null)
            return;

        for (Map.Entry<Number, long[]> entry : other.getAsMap().entrySet())
            update(entry.getKey(), entry.getValue()[0]);
    }

    /**
     * Calculates estimated number of points in interval [-inf,b].
     *
     * @param b upper bound of a interval to calculate sum
     * @return estimated number of points in a interval [-inf,b].
     */
    public double sum(double b)
    {
        double sum = 0;
        // find the points pi, pnext which satisfy pi <= b < pnext
        Map.Entry<Number, long[]> pnext = bin.higherEntry(b);
        if (pnext == null)
        {
            // if b is greater than any key in this histogram,
            // just count all appearance and return
            for (long[] value : bin.values())
                sum += value[0];
        }
        else
        {
            Map.Entry<Number, long[]> pi = bin.floorEntry(b);
            if (pi == null)
                return 0;
            // calculate estimated count mb for point b
            double weight = (b - pi.getKey().doubleValue()) / (pnext.getKey().doubleValue() - pi.getKey().doubleValue());
            double mb = pi.getValue()[0] + (pnext.getValue()[0] - pi.getValue()[0]) * weight;
            sum += (pi.getValue()[0] + mb) * weight / 2;

            sum += pi.getValue()[0] / 2.0;
            for (long[] value : bin.headMap(pi.getKey(), false).values())
                sum += value[0];
        }
        return sum;
    }

    public Map<Number, long[]> getAsMap()
    {
        return Collections.unmodifiableMap(bin);
    }

    public static class StreamingHistogramSerializer implements ISerializer<StreamingHistogram>
    {
        public void serialize(StreamingHistogram histogram, DataOutputPlus out) throws IOException
        {
            out.writeInt(histogram.maxBinSize);
            Map<Number, long[]> entries = histogram.getAsMap();
            out.writeInt(entries.size());
            for (Map.Entry<Number, long[]> entry : entries.entrySet())
            {
                out.writeDouble(entry.getKey().doubleValue());
                out.writeLong(entry.getValue()[0]);
            }
        }

        public StreamingHistogram deserialize(DataInputPlus in) throws IOException
        {
            int maxBinSize = in.readInt();
            int size = in.readInt();
            Map<Double, Long> tmp = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                tmp.put(in.readDouble(), in.readLong());
            }

            return new StreamingHistogram(maxBinSize, tmp);
        }

        public long serializedSize(StreamingHistogram histogram)
        {
            long size = TypeSizes.sizeof(histogram.maxBinSize);
            Map<Number, long[]> entries = histogram.getAsMap();
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

        if (!(o instanceof StreamingHistogram))
            return false;

        StreamingHistogram that = (StreamingHistogram) o;
        return maxBinSize == that.maxBinSize && bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bin.hashCode(), maxBinSize);
    }

}
