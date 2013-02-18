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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

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
    private final TreeMap<Double, Long> bin;

    // maximum bin size for this histogram
    private final int maxBinSize;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     * @param maxBinSize maximum number of bins this histogram can have
     */
    public StreamingHistogram(int maxBinSize)
    {
        this.maxBinSize = maxBinSize;
        bin = new TreeMap<Double, Long>();
    }

    private StreamingHistogram(int maxBinSize, Map<Double, Long> bin)
    {
        this.maxBinSize = maxBinSize;
        this.bin = new TreeMap<Double, Long>(bin);
    }

    /**
     * Adds new point p to this histogram.
     * @param p
     */
    public void update(double p)
    {
        update(p, 1);
    }

    /**
     * Adds new point p with value m to this histogram.
     * @param p
     * @param m
     */
    public void update(double p, long m)
    {
        Long mi = bin.get(p);
        if (mi != null)
        {
            // we found the same p so increment that counter
            bin.put(p, mi + m);
        }
        else
        {
            bin.put(p, m);
            // if bin size exceeds maximum bin size then trim down to max size
            while (bin.size() > maxBinSize)
            {
                // find points p1, p2 which have smallest difference
                Iterator<Double> keys = bin.keySet().iterator();
                double p1 = keys.next();
                double p2 = keys.next();
                double smallestDiff = p2 - p1;
                double q1 = p1, q2 = p2;
                while (keys.hasNext())
                {
                    p1 = p2;
                    p2 = keys.next();
                    double diff = p2 - p1;
                    if (diff < smallestDiff)
                    {
                        smallestDiff = diff;
                        q1 = p1;
                        q2 = p2;
                    }
                }
                // merge those two
                long k1 = bin.remove(q1);
                long k2 = bin.remove(q2);
                bin.put((q1 * k1 + q2 * k2) / (k1 + k2), k1 + k2);
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

        for (Map.Entry<Double, Long> entry : other.getAsMap().entrySet())
            update(entry.getKey(), entry.getValue());
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
        Map.Entry<Double, Long> pnext = bin.higherEntry(b);
        if (pnext == null)
        {
            // if b is greater than any key in this histogram,
            // just count all appearance and return
            for (Long value : bin.values())
                sum += value;
        }
        else
        {
            Map.Entry<Double, Long> pi = bin.floorEntry(b);
            if (pi == null)
                return 0;
            // calculate estimated count mb for point b
            double weight = (b - pi.getKey()) / (pnext.getKey() - pi.getKey());
            double mb = pi.getValue() + (pnext.getValue() - pi.getValue()) * weight;
            sum += (pi.getValue() + mb) * weight / 2;

            sum += pi.getValue() / 2.0;
            for (Long value : bin.headMap(pi.getKey(), false).values())
                sum += value;
        }
        return sum;
    }

    public Map<Double, Long> getAsMap()
    {
        return Collections.unmodifiableMap(bin);
    }

    public static class StreamingHistogramSerializer implements ISerializer<StreamingHistogram>
    {
        public void serialize(StreamingHistogram histogram, DataOutput dos) throws IOException
        {
            dos.writeInt(histogram.maxBinSize);
            Map<Double, Long> entries = histogram.getAsMap();
            dos.writeInt(entries.size());
            for (Map.Entry<Double, Long> entry : entries.entrySet())
            {
                dos.writeDouble(entry.getKey());
                dos.writeLong(entry.getValue());
            }
        }

        public StreamingHistogram deserialize(DataInput dis) throws IOException
        {
            int maxBinSize = dis.readInt();
            int size = dis.readInt();
            Map<Double, Long> tmp = new HashMap<Double, Long>(size);
            for (int i = 0; i < size; i++)
            {
                tmp.put(dis.readDouble(), dis.readLong());
            }

            return new StreamingHistogram(maxBinSize, tmp);
        }

        public long serializedSize(StreamingHistogram histogram, TypeSizes typeSizes)
        {
            throw new UnsupportedOperationException();
        }
    }
}
