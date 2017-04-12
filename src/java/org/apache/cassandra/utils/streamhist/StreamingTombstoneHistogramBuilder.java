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

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.google.common.math.IntMath;

import static org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder.AddResult.ACCUMULATED;
import static org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder.AddResult.INSERTED;

/**
 * Histogram that can be constructed from streaming of data.
 * <p>
 * The original algorithm is taken from following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A Streaming Parallel Decision Tree Algorithm" (2010)
 * http://jmlr.csail.mit.edu/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * <p>
 * Algorithm: Histogram is represented as collection of {point, weight} pairs. When new point <i>p</i> with weight <i>m</i> is added:
 * <ol>
 * <li>If point <i>p</i> is already exists in collection, add <i>m</i> to recorded value of point <i>p</i> </li>
 * <li>If there is no point <i>p</i> in the collection, add point <i>p</i> with weight <i>m</i> </li>
 * <li>If point was added and collection size became lorger than maxBinSize:</li>
 * <ol type="a">
 * <li>Find nearest points <i>p1</i> and <i>p2</i> in the collection </li>
 * <li>Replace theese two points with one weighted point <i>p3 = (p1*m1+p2*m2)/(p1+p2)</i></li>
 * </ol>
 * </ol>
 * <p>
 * There are some optimization to make histogram builder faster:
 * <ol>
 *     <li>Spool: big map that saves from excessively merging of small bin. This map can contains up to maxSpoolSize points and accumulate weight from same points.
 *     For example, if spoolSize=100, binSize=10 and there are only 50 different points. it will be only 40 merges regardless how many points will be added.</li>
 *     <li>Spool is organized as open-addressing primitive hash map where odd elements are points and event elements are values.
 *     Spool can not resize => when number of collisions became bigger than threashold or size became large that <i>array_size/2</i> Spool is drained to bin</li>
 *     <li>DistanceHolder - sorted collection of distances between points in Bin. It is used to find nearest points in constant time</li>
 *     <li>Distances and Bin organized as sorted arrays. It reduces garbage collection pressure and allows to find elements in log(binSize) time via binary search</li>
 *     <li>To use existing Arrays.binarySearch <i></>{point, values}</i> in bin and <i></>{distance, left_point}</i> pairs is packed in one long</li>
 * </ol>
 */
public class StreamingTombstoneHistogramBuilder
{
    // Buffer with point-value pair
    private final DataHolder bin;

    // Buffer with distance between points, sorted from nearest to furthest
    private final DistanceHolder distances;

    // Keep a second, larger buffer to spool data in, before finalizing it into `bin`
    private final Spool spool;

    // voluntarily give up resolution for speed
    private final int roundSeconds;

    public StreamingTombstoneHistogramBuilder(int maxBinSize, int maxSpoolSize, int roundSeconds)
    {
        this.roundSeconds = roundSeconds;
        this.bin = new DataHolder(maxBinSize + 1, roundSeconds);
        distances = new DistanceHolder(maxBinSize);

        //for spool we need power-of-two cells
        maxSpoolSize = maxSpoolSize == 0 ? 0 : IntMath.pow(2, IntMath.log2(maxSpoolSize, RoundingMode.CEILING));
        spool = new Spool(maxSpoolSize);
    }

    /**
     * Adds new point p to this histogram.
     *
     * @param p
     */
    public void update(int p)
    {
        update(p, 1);
    }

    /**
     * Adds new point p with value m to this histogram.
     *
     * @param p
     * @param m
     */
    public void update(int p, int m)
    {
        p = roundKey(p, roundSeconds);

        if (spool.capacity > 0)
        {
            if (!spool.tryAddOrAccumulate(p, m))
            {
                flushHistogram();
                final boolean success = spool.tryAddOrAccumulate(p, m);
                assert success : "Can not add value to spool"; // after spool flushing we should always be able to insert new value
            }
        }
        else
        {
            flushValue(p, m);
        }
    }

    /**
     * Drain the temporary spool into the final bins
     */
    public void flushHistogram()
    {
        spool.forEach(this::flushValue);
        spool.clear();
    }

    private void flushValue(int key, int spoolValue)
    {
        DataHolder.NeighboursAndResult addResult = bin.addValue(key, spoolValue);
        if (addResult.result == INSERTED)
        {
            final int prevPoint = addResult.prevPoint;
            final int nextPoint = addResult.nextPoint;
            if (prevPoint != -1 && nextPoint != -1)
                distances.remove(prevPoint, nextPoint);
            if (prevPoint != -1)
                distances.add(prevPoint, key);
            if (nextPoint != -1)
                distances.add(key, nextPoint);
        }

        if (bin.isFull())
        {
            mergeBin();
        }
    }

    private void mergeBin()
    {
        // find points point1, point2 which have smallest difference
        final int[] smallestDifference = distances.getFirstAndRemove();

        final int point1 = smallestDifference[0];
        final int point2 = smallestDifference[1];

        // merge those two
        DataHolder.MergeResult mergeResult = bin.merge(point1, point2);

        final int nextPoint = mergeResult.nextPoint;
        final int prevPoint = mergeResult.prevPoint;
        final int newPoint = mergeResult.newPoint;

        if (nextPoint != -1)
        {
            distances.remove(point2, nextPoint);
            distances.add(newPoint, nextPoint);
        }

        if (prevPoint != -1)
        {
            distances.remove(prevPoint, point1);
            distances.add(prevPoint, newPoint);
        }
    }

    /**
     * Creates a 'finished' snapshot of the current state of the historgram, but leaves this builder instance
     * open for subsequent additions to the histograms. Basically, this allows us to have some degree of sanity
     * wrt sstable early open.
     */
    public TombstoneHistogram build()
    {
        flushHistogram();
        return new TombstoneHistogram(bin);
    }

    private static class DistanceHolder
    {
        private static final long EMPTY = Long.MAX_VALUE;
        private final long[] data;

        DistanceHolder(int maxCapacity)
        {
            data = new long[maxCapacity];
            Arrays.fill(data, EMPTY);
        }

        void add(int prev, int next)
        {
            long key = getKey(prev, next);
            int index = Arrays.binarySearch(data, key);

            assert (index < 0) : "Element already exists";
            assert (data[data.length - 1] == EMPTY) : "No more space in array";

            index = -index - 1;
            System.arraycopy(data, index, data, index + 1, data.length - index - 1);
            data[index] = key;
        }

        void remove(int prev, int next)
        {
            long key = getKey(prev, next);
            int index = Arrays.binarySearch(data, key);
            if (index >= 0)
            {
                if (index < data.length)
                    System.arraycopy(data, index + 1, data, index, data.length - index - 1);
                data[data.length - 1] = EMPTY;
            }
        }

        int[] getFirstAndRemove()
        {
            if (data[0] == EMPTY)
                return null;

            int[] result = unwrapKey(data[0]);
            System.arraycopy(data, 1, data, 0, data.length - 1);
            data[data.length - 1] = EMPTY;
            return result;
        }

        private int[] unwrapKey(long key)
        {
            final int distance = (int) (key >> 32);
            final int prev = (int) (key & 0xFF_FF_FF_FFL);
            return new int[]{ prev, prev + distance };
        }

        private long getKey(int prev, int next)
        {
            long distance = next - prev;
            return (distance << 32) | prev;
        }

        public String toString()
        {
            return Arrays.stream(data).filter(x -> x != EMPTY).boxed().map(this::unwrapKey).map(Arrays::toString).collect(Collectors.joining());
        }
    }

    static class DataHolder
    {
        private static final long EMPTY = Long.MAX_VALUE;
        private final long[] data;
        private final int roundSeconds;

        DataHolder(int maxCapacity, int roundSeconds)
        {
            data = new long[maxCapacity];
            Arrays.fill(data, EMPTY);
            this.roundSeconds = roundSeconds;
        }

        DataHolder(DataHolder holder)
        {
            data = Arrays.copyOf(holder.data, holder.data.length);
            roundSeconds = holder.roundSeconds;
        }

        NeighboursAndResult addValue(int point, int delta)
        {
            long key = wrap(point, 0);
            int index = Arrays.binarySearch(data, key);
            AddResult addResult;
            if (index < 0)
            {
                index = -index - 1;
                assert (index < data.length) : "No more space in array";

                if (unwrapPoint(data[index]) != point) //ok, someone else at this point, let's shift array and insert
                {
                    assert (data[data.length - 1] == EMPTY) : "No more space in array";

                    System.arraycopy(data, index, data, index + 1, data.length - index - 1);

                    data[index] = wrap(point, delta);
                    addResult = INSERTED;
                }
                else
                {
                    data[index] += delta;
                    addResult = ACCUMULATED;
                }
            }
            else
            {
                data[index] += delta;
                addResult = ACCUMULATED;
            }

            return new NeighboursAndResult(getPrevPoint(index), getNextPoint(index), addResult);
        }

        public MergeResult merge(int point1, int point2)
        {
            long key = wrap(point1, 0);
            int index = Arrays.binarySearch(data, key);
            if (index < 0)
            {
                index = -index - 1;
                assert (index < data.length) : "Not found in array";
                assert (unwrapPoint(data[index]) == point1) : "Not found in array";
            }

            final int prevPoint = getPrevPoint(index);
            final int nextPoint = getNextPoint(index + 1);

            int value1 = unwrapValue(data[index]);
            int value2 = unwrapValue(data[index + 1]);

            assert (unwrapPoint(data[index + 1]) == point2) : "point2 should follow point1";

            int sum = value1 + value2;

            //let's evaluate in long values to handle overflow in multiplication
            int newPoint = (int) (((long) point1 * value1 + (long) point2 * value2) / (value1 + value2));
            newPoint = roundKey(newPoint, roundSeconds);
            data[index] = wrap(newPoint, sum);

            System.arraycopy(data, index + 2, data, index + 1, data.length - index - 2);
            data[data.length - 1] = EMPTY;

            return new MergeResult(prevPoint, newPoint, nextPoint);
        }

        private int getPrevPoint(int index)
        {
            if (index > 0)
                if (data[index - 1] != EMPTY)
                    return (int) (data[index - 1] >> 32);
                else
                    return -1;
            else
                return -1;
        }

        private int getNextPoint(int index)
        {
            if (index < data.length - 1)
                if (data[index + 1] != EMPTY)
                    return (int) (data[index + 1] >> 32);
                else
                    return -1;
            else
                return -1;
        }

        private int[] unwrap(long key)
        {
            final int point = unwrapPoint(key);
            final int value = unwrapValue(key);
            return new int[]{ point, value };
        }

        private int unwrapPoint(long key)
        {
            return (int) (key >> 32);
        }

        private int unwrapValue(long key)
        {
            return (int) (key & 0xFF_FF_FF_FFL);
        }

        private long wrap(int point, int value)
        {
            return (((long) point) << 32) | value;
        }


        public String toString()
        {
            return Arrays.stream(data).filter(x -> x != EMPTY).boxed().map(this::unwrap).map(Arrays::toString).collect(Collectors.joining());
        }

        public boolean isFull()
        {
            return data[data.length - 1] != EMPTY;
        }

        public <E extends Exception> void forEach(HistogramDataConsumer<E> histogramDataConsumer) throws E
        {
            for (long datum : data)
            {
                if (datum == EMPTY)
                {
                    break;
                }

                histogramDataConsumer.consume(unwrapPoint(datum), unwrapValue(datum));
            }
        }

        public int size()
        {
            int[] accumulator = new int[1];
            forEach((point, value) -> accumulator[0]++);
            return accumulator[0];
        }

        public double sum(int b)
        {
            double sum = 0;

            for (int i = 0; i < data.length; i++)
            {
                long pointAndValue = data[i];
                if (pointAndValue == EMPTY)
                {
                    break;
                }
                final int point = unwrapPoint(pointAndValue);
                final int value = unwrapValue(pointAndValue);
                if (point > b)
                {
                    if (i == 0)
                    { // no prev point
                        return 0;
                    }
                    else
                    {
                        final int prevPoint = unwrapPoint(data[i - 1]);
                        final int prevValue = unwrapValue(data[i - 1]);
                        double weight = (b - prevPoint) / (double) (point - prevPoint);
                        double mb = prevValue + (value - prevValue) * weight;
                        sum -= prevValue;
                        sum += (prevValue + mb) * weight / 2;
                        sum += prevValue / 2.0;
                        return sum;
                    }
                }
                else
                {
                    sum += value;
                }
            }
            return sum;
        }

        static class MergeResult
        {
            int prevPoint;
            int newPoint;
            int nextPoint;

            MergeResult(int prevPoint, int newPoint, int nextPoint)
            {
                this.prevPoint = prevPoint;
                this.newPoint = newPoint;
                this.nextPoint = nextPoint;
            }
        }

        static class NeighboursAndResult
        {
            int prevPoint;
            int nextPoint;
            AddResult result;

            NeighboursAndResult(int prevPoint, int nextPoint, AddResult result)
            {
                this.prevPoint = prevPoint;
                this.nextPoint = nextPoint;
                this.result = result;
            }
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(data);
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof DataHolder))
                return false;

            final DataHolder other = ((DataHolder) o);

            if (this.size()!=other.size())
                return false;

            for (int i=0; i<size(); i++)
            {
                if (data[i]!=other.data[i])
                {
                    return false;
                }
            }
            return true;
        }
    }

    public enum AddResult
    {
        INSERTED,
        ACCUMULATED
    }

    static class Spool
    {
        // odd elements - points, even elements - values
        final int[] map;
        final int capacity;
        int size;

        Spool(int capacity)
        {
            this.capacity = capacity;
            if (capacity == 0)
            {
                map = new int[0];
            }
            else
            {
                assert IntMath.isPowerOfTwo(capacity) : "should be power of two";
                // x2 because we want to save points and values in consecutive cells and x2 because we want reprobing less that two when _capacity_ values will be written
                map = new int[capacity * 2 * 2];
                clear();
            }
        }

        void clear()
        {
            Arrays.fill(map, -1);
            size = 0;
        }

        boolean tryAddOrAccumulate(int point, int delta)
        {
            if (size > capacity)
            {
                return false;
            }

            final int cell = 2 * ((capacity - 1) & hash(point));

            // We use linear scanning. I think cluster of 100 elements is large enough to give up.
            for (int attempt = 0; attempt < 100; attempt++)
            {
                if (tryCell(cell + attempt * 2, point, delta))
                    return true;
            }
            return false;
        }

        private int hash(int i)
        {
            long largePrime = 948701839L;
            return (int) (i * largePrime);
        }

        <E extends Exception> void forEach(HistogramDataConsumer<E> consumer) throws E
        {
            for (int i = 0; i < map.length; i += 2)
            {
                if (map[i] != -1)
                {
                    consumer.consume(map[i], map[i + 1]);
                }
            }
        }

        private boolean tryCell(int cell, int point, int delta)
        {
            cell = cell % map.length;
            if (map[cell] == -1)
            {
                map[cell] = point;
                map[cell + 1] = delta;
                size++;
                return true;
            }
            if (map[cell] == point)
            {
                map[cell + 1] += delta;
                return true;
            }
            return false;
        }
    }

    private static int roundKey(int p, int roundSeconds)
    {
        int d = p % roundSeconds;
        if (d > 0)
            return p + (roundSeconds - d);
        else
            return p;
    }
}
