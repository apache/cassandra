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
    private final TreeMap<Integer, long[]> bin;

    // Keep a second, larger buffer to spool data in, before finalizing it into `bin`
    private final Map<Integer, long[]> spool;

    // maximum bin size for this histogram
    private final int maxBinSize;

    // maximum size of the spool
    private final int maxSpoolSize;

    // voluntarily give up resolution for speed
    private final int roundSeconds;

    /**
     * To calculate nearest points. First element is distance between points and second element is left point.
     */
    private final TreeSet<int[]> distances;

    /**
     * Creates a new histogram with max bin size of maxBinSize
     * @param maxBinSize maximum number of bins this histogram can have
     */
    public StreamingHistogram(int maxBinSize, int maxSpoolSize, int roundSeconds)
    {
        this.maxBinSize = maxBinSize;
        this.maxSpoolSize = maxSpoolSize;
        this.roundSeconds = roundSeconds;
        bin = new TreeMap<>((o1, o2) ->
                            {
                                if (o1.getClass().equals(o2.getClass()))
                                    return ((Comparable) o1).compareTo(o2);
                                else
                                    return Double.compare(o1.doubleValue(), o2.doubleValue());
                            });
        distances = new TreeSet<>((distPoint1, distPoint2) ->
                                  {
                                      final int result = Integer.compare(distPoint1[0], distPoint2[0]);
                                      if (result == 0)
                                          return Integer.compare(distPoint1[1], distPoint2[1]);
                                      else
                                          return result;
                                  });
        spool = new HashMap<>();
    }

    private StreamingHistogram(int maxBinSize, int maxSpoolSize, int roundSeconds, Map<Double, Long> bin)
    {
        this(maxBinSize, maxSpoolSize, roundSeconds);
        for (Map.Entry<Double, Long> entry : bin.entrySet())
        {
            final int current = entry.getKey().intValue();
            this.bin.put(current, new long[]{ entry.getValue() });
        }
        Integer prev = null;
        for (Integer key : this.bin.keySet())
        {
            if (prev != null)
                this.distances.add(distanceKey(prev, key));
            prev = key;
        }
    }

    /**
     * Adds new point p to this histogram.
     * @param p
     */
    public void update(int p)
    {
        update(p, 1L);
    }

    /**
     * Adds new point p with value m to this histogram.
     * @param p
     * @param m
     */
    public void update(int p, long m)
    {
        p = roundKey(p);

        final long[] oldValue = spool.computeIfAbsent(p, key -> new long[]{ 0 });
        oldValue[0] += m;

        // If spool has overflowed, compact it
        if(spool.size() > maxSpoolSize)
            flushHistogram();
    }

    private int roundKey(int p)
    {
        int d = p % this.roundSeconds;
        if (d > 0)
            return p + (this.roundSeconds - d);
        else
            return p;
    }

    /**
     * Drain the temporary spool into the final bins
     */
    public void flushHistogram()
    {
        if (spool.size() > 0)
        {
            long[] spoolValue;

            // Iterate over the spool, copying the value into the primary bin map
            // and compacting that map as necessary
            for (Map.Entry<Integer, long[]> entry : spool.entrySet())
            {
                int key = entry.getKey();
                spoolValue = entry.getValue();
                flushValue(key, spoolValue[0]);

                if (bin.size() > maxBinSize)
                {
                    mergeBin();
                }
            }
            spool.clear();
        }
    }

    private void flushValue(int key, long spoolValue)
    {
        long[] binValue = bin.computeIfAbsent(key, k -> new long[]{ 0 });
        boolean isNewPoint = binValue[0] == 0;
        binValue[0] += spoolValue;
        if (isNewPoint)
        {
            final Integer prevPoint = bin.lowerKey(key);
            final Integer nextPoint = bin.higherKey(key);
            if (prevPoint != null && nextPoint != null)
                distances.remove(distanceKey(prevPoint, nextPoint));
            if (prevPoint != null)
                distances.add(distanceKey(prevPoint, key));
            if (nextPoint != null)
                distances.add(distanceKey(key, nextPoint));
        }
    }

    private int[] distanceKey(Integer prevPoint, Integer nextPoint)
    {
        final int distance = nextPoint - prevPoint;
        return new int[]{ distance, prevPoint };
    }

    private void mergeBin()
    {
        // find points q1, q2 which have smallest difference
        final int[] smallestDifference = distances.pollFirst();

        final int q1 = smallestDifference[1];
        final int q2 = smallestDifference[1] + smallestDifference[0];

        // merge those two
        long[] a1 = bin.remove(q1);
        long[] a2 = bin.remove(q2);

        final Integer pointAfterQ2 = bin.higherKey(q2);
        if (pointAfterQ2 != null)
            distances.remove(distanceKey(q2, pointAfterQ2));

        final Integer pointBeforeQ1 = bin.lowerKey(q1);
        if (pointBeforeQ1!=null)
            distances.remove(distanceKey(pointBeforeQ1, q1));

        if (pointBeforeQ1 != null && pointAfterQ2 != null)
            distances.add(distanceKey(pointBeforeQ1, pointAfterQ2));

        long k1 = a1[0];
        long k2 = a2[0];
        long sum = k1 + k2;

        final int key = roundKey((int) ((q1 * k1 + q2 * k2) / (k1 + k2)));
        flushValue(key, sum);
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

        other.flushHistogram();

        for (Map.Entry<Integer, long[]> entry : other.getAsMap().entrySet())
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
        flushHistogram();
        double sum = 0;
        // find the points pi, pnext which satisfy pi <= b < pnext
        Map.Entry<Integer, long[]> pnext = bin.higherEntry((int) b);
        if (pnext == null)
        {
            // if b is greater than any key in this histogram,
            // just count all appearance and return
            for (long[] value : bin.values())
                sum += value[0];
        }
        else
        {
            Map.Entry<Integer, long[]> pi = bin.floorEntry((int) b);
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

    public Map<Integer, long[]> getAsMap()
    {
        flushHistogram();
        return Collections.unmodifiableMap(bin);
    }

    public static class StreamingHistogramSerializer implements ISerializer<StreamingHistogram>
    {
        public void serialize(StreamingHistogram histogram, DataOutputPlus out) throws IOException
        {
            histogram.flushHistogram();
            out.writeInt(histogram.maxBinSize);
            Map<Integer, long[]> entries = histogram.getAsMap();
            out.writeInt(entries.size());
            for (Map.Entry<Integer, long[]> entry : entries.entrySet())
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

            return new StreamingHistogram(maxBinSize, maxBinSize, 1, tmp);
        }

        public long serializedSize(StreamingHistogram histogram)
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

        if (!(o instanceof StreamingHistogram))
            return false;

        StreamingHistogram that = (StreamingHistogram) o;
        return maxBinSize == that.maxBinSize &&
               spool.equals(that.spool) &&
               bin.equals(that.bin);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(bin.hashCode(), spool.hashCode(), maxBinSize);
    }
}
