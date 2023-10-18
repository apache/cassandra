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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.rows.Cell;

/**
 * Histogram that can be constructed from streaming of data.
 *
 * Histogram used to retrieve the number of droppable tombstones for example via
 * {@link org.apache.cassandra.io.sstable.format.SSTableReader#getDroppableTombstonesBefore(long)}.
 * <p>
 * When an sstable is written (or streamed), this histogram-builder receives the "local deletion timestamp"
 * as an {@code long} via {@link #update(long)}. Negative values are not supported.
 * <p>
 * Algorithm: Histogram is represented as collection of {point, weight} pairs. When new point <i>p</i> with weight <i>m</i> is added:
 * <ol>
 *     <li>If point <i>p</i> is already exists in collection, add <i>m</i> to recorded value of point <i>p</i> </li>
 *     <li>If there is no point <i>p</i> in the collection, add point <i>p</i> with weight <i>m</i> </li>
 *     <li>If point was added and collection size became larger than maxBinSize:</li>
 * </ol>
 *
 * <ol type="a">
 *     <li>Find nearest points <i>p1</i> and <i>p2</i> in the collection </li>
 *     <li>Replace these two points with one weighted point <i>p3 = (p1*m1+p2*m2)/(p1+p2)</i></li>
 * </ol>
 *
 * <p>
 * There are some optimization to make histogram builder faster:
 * <ol>
 *     <li>Spool: big map that saves from excessively merging of small bin. This map can contains up to maxSpoolSize points and accumulate weight from same points.
 *     For example, if spoolSize=100, binSize=10 and there are only 50 different points. it will be only 40 merges regardless how many points will be added.</li>
 *     <li>Spool is organized as open-addressing primitive hash map where odd elements are points and event elements are values.
 *     Spool can not resize => when number of collisions became bigger than threshold or size became large that <i>array_size/2</i> Spool is drained to bin</li>
 *     <li>Bin is organized as sorted arrays. It reduces garbage collection pressure and allows to find elements in log(binSize) time via binary search</li>
 *     <li>To use existing Arrays.binarySearch <i>{point, values}</i> in bin pairs is packed in one long</li>
 * </ol>
 * <p>
 * The original algorithm is taken from following paper:
 * Yael Ben-Haim and Elad Tom-Tov, "A Streaming Parallel Decision Tree Algorithm" (2010)
 * http://jmlr.csail.mit.edu/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
public class StreamingTombstoneHistogramBuilder
{
    // Buffer with point-value pair
    private final DataHolder bin;

    // Keep a second, larger buffer to spool data in, before finalizing it into `bin`
    private Spool spool;

    // voluntarily give up resolution for speed
    private final int roundSeconds;

    public StreamingTombstoneHistogramBuilder(int maxBinSize, int maxSpoolSize, int roundSeconds)
    {
        assert maxBinSize > 0 && maxSpoolSize >= 0 && roundSeconds > 0: "Invalid arguments: maxBinSize:" + maxBinSize + " maxSpoolSize:" + maxSpoolSize + " delta:" + roundSeconds;

        this.roundSeconds = roundSeconds;
        this.bin = new DataHolder(maxBinSize + 1, roundSeconds);
        this.spool = new Spool(maxSpoolSize);
    }

    /**
     * Adds new point to this histogram with a default value of 1.
     *
     * @param point the point to be added
     */
    public void update(long point)
    {
        update(point, 1);
    }

    /**
     * Adds new point {@param point} with value {@param value} to this histogram.
     */
    public void update(long point, int value)
    {
        assert spool != null: "update is being called after releaseBuffers. This could be functionally okay, but this assertion is a canary to alert about unintended use before it is necessary.";
        point = ceilKey(point, roundSeconds);

        if (spool.capacity > 0)
        {
            if (!spool.tryAddOrAccumulate(point, value))
            {
                flushHistogram();
                final boolean success = spool.tryAddOrAccumulate(point, value);
                assert success : "Can not add value to spool"; // after spool flushing we should always be able to insert new value
            }
        }
        else
        {
            flushValue(point, value);
        }
    }

    /**
     * Drain the temporary spool into the final bins
     */
    public void flushHistogram()
    {
        Spool spool = this.spool;
        if (spool != null)
        {
            spool.forEach(this::flushValue);
            spool.clear();
        }
    }

    /**
     * Release inner spool buffers. Histogram remains readable and writable, but with lesser performance.
     * Not intended for use before finalization.
     */
    public void releaseBuffers()
    {
       flushHistogram();
       spool = null;
    }

    private void flushValue(long key, int spoolValue)
    {
        bin.addValue(key, spoolValue);

        if (bin.isFull())
        {
            bin.mergeNearestPoints();
        }
    }

    /**
     * Creates a 'finished' snapshot of the current state of the histogram, but leaves this builder instance
     * open for subsequent additions to the histograms. Basically, this allows us to have some degree of sanity
     * wrt sstable early open.
     */
    public TombstoneHistogram build()
    {
        flushHistogram();
        return new TombstoneHistogram(bin);
    }

    /**
     * An ordered collection of histogram buckets, each entry in the collection represents a pair (bucket, count).
     * Once the collection is full it merges the closest buckets using a weighted approach see {@link #mergeNearestPoints()}.
     */
    static class DataHolder
    {
        private static final long EMPTY = Long.MAX_VALUE;
        private final long[] points;
        private final int[] values;
        private final int roundSeconds;

        DataHolder(int maxCapacity, int roundSeconds)
        {
            points = new long[maxCapacity];
            values = new int[maxCapacity];
            Arrays.fill(points, EMPTY);
            Arrays.fill(values, 0);
            this.roundSeconds = roundSeconds;
        }

        DataHolder(DataHolder holder)
        {
            points = Arrays.copyOf(holder.points, holder.points.length);
            values = Arrays.copyOf(holder.values, holder.values.length);
            roundSeconds = holder.roundSeconds;
        }

        @VisibleForTesting
        int getValue(long point)
        {
            int index = Arrays.binarySearch(points, point);
            if (index < 0)
                index = -index - 1;
            if (index >= points.length)
                return -1; // not-found sentinel
            if (points[index] != point)
                return -2; // not-found sentinel
            return values[index];
        }

        /**
         * Adds value {@code delta} to the point {@code point}.
         *
         * @return {@code true} if inserted, {@code false} if accumulated
         */
        boolean addValue(long point, int delta)
        {
            int index = Arrays.binarySearch(points, point);
            if (index < 0)
            {
                index = -index - 1;
                assert (index < points.length) : "No more space in array";

                if (points[index] != point) //ok, someone else at this point, let's shift array and insert
                {
                    assert (points[points.length - 1] == EMPTY) : "No more space in array";

                    System.arraycopy(points, index, points, index + 1, points.length - index - 1);
                    System.arraycopy(values, index, values, index + 1, values.length - index - 1);

                    points[index] = point;
                    values[index] = saturatingCastToInt(delta);
                    return true;
                }
                else
                {
                    values[index] = saturatingCastToInt((long)values[index] + (long)delta);
                }
            }
            else
            {
                values[index] = saturatingCastToInt((long)values[index] + (long)delta);
            }

            return false;
        }

        /**
         *  Finds nearest points <i>p1</i> and <i>p2</i> in the collection
         *  Replaces these two points with one weighted point <i>p3 = (p1*m1+p2*m2)/(p1+p2)</i>
         */
        @VisibleForTesting
        void mergeNearestPoints()
        {
            assert isFull() : "DataHolder must be full in order to merge two points";

            final long[] smallestDifference = findPointPairWithSmallestDistance();

            final long point1 = smallestDifference[0];
            final long point2 = smallestDifference[1];

            int index = Arrays.binarySearch(points, point1);
            if (index < 0)
            {
                index = -index - 1;
                assert (index < points.length) : "Not found in array";
                assert (points[index] == point1) : "Not found in array";
            }

            long value1 = values[index];
            long value2 = values[index + 1];

            assert (points[index + 1] == point2) : "point2 should follow point1";

            long a = saturatingCastToLong(point1 * value1);
            long b = saturatingCastToLong(point2 * value2);
            long sum = saturatingCastToLong(value1 + value2);
            long newPoint = saturatingCastToMaxDeletionTime(saturatingCastToLong(a + b) / sum);
            newPoint = newPoint <= point1 ? saturatingCastToMaxDeletionTime(point1 + 1) : newPoint;
            newPoint = Math.min(newPoint, point2);
            newPoint = ceilKey(newPoint, roundSeconds);
            points[index] = newPoint;
            values[index] = saturatingCastToInt(sum);

            System.arraycopy(points, index + 2, points, index + 1, points.length - index - 2);
            System.arraycopy(values, index + 2, values, index + 1, values.length - index - 2);
            points[points.length - 1] = EMPTY;
            values[values.length - 1] = 0;
        }

        private long[] findPointPairWithSmallestDistance()
        {
            assert isFull(): "The DataHolder must be full in order to find the closest pair of points";

            long point1 = 0;
            long point2 = Long.MAX_VALUE;

            for (int i = 0; i < points.length - 1; i++)
            {
                long pointA = points[i];
                long pointB = points[i + 1];

                assert pointB > pointA : "DataHolder not sorted, p2(" + pointB +") < p1(" + pointA + ") for " + this;

                if (point2 - point1 > pointB - pointA)
                {
                    point1 = pointA;
                    point2 = pointB;
                }
            }

            return new long[]{point1, point2};
        }

        public String toString()
        {
            List<String> entries = new ArrayList<>();
            for (int i = 0; i < points.length; i++)
            {
                if (points[i] == EMPTY)
                    break;

                entries.add("[" + points[i] + "], [" + values[i] + "]");
            }
            return StringUtils.join(entries, ",");
        }

        public boolean isFull()
        {
            return points[points.length - 1] != EMPTY;
        }

        public <E extends Exception> void forEach(HistogramDataConsumer<E> histogramDataConsumer) throws E
        {
            for (int i = 0; i < points.length; i++)
            {
                if (points[i] == EMPTY)
                    break;

                histogramDataConsumer.consume(points[i], values[i]);
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

            for (int i = 0; i < points.length; i++)
            {
                final long point = points[i];
                if (point == EMPTY)
                    break;
                final int value = values[i];
                if (point > b)
                {
                    if (i == 0)
                    { // no prev point
                        return 0;
                    }
                    else
                    {
                        final long prevPoint = points[i - 1];
                        final int prevValue = values[i - 1];
                        // calculate estimated count mb for point b
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

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(points);
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
                if (points[i]!=other.points[i] || values[i]!=other.values[i])
                {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * This class is a specialized open addressing HashMap that uses int as keys and int as values.
     * This is an optimization to avoid allocating objects.
     * In order for this class to work correctly it should have a power of 2 capacity.
     * This last invariant is taken care of during construction.
     */
    static class Spool
    {
        final long[] points;
        final int[] values;

        final int capacity;
        int size;

        Spool(int requestedCapacity)
        {
            if (requestedCapacity < 0)
                throw new IllegalArgumentException("Illegal capacity " + requestedCapacity);

            this.capacity = getPowerOfTwoCapacity(requestedCapacity);

            // x2 because we want no more than two reprobes on average when _capacity_ entries will be written
            points = new long[capacity * 2];
            values = new int[capacity * 2];
            clear();
        }

        private int getPowerOfTwoCapacity(int requestedCapacity)
        {
            //for spool we need power-of-two cells
            return requestedCapacity == 0 ? 0 : IntMath.pow(2, IntMath.log2(requestedCapacity, RoundingMode.CEILING));
        }

        void clear()
        {
            Arrays.fill(points, -1);
            size = 0;
        }

        boolean tryAddOrAccumulate(long point, int delta)
        {
            if (size > capacity)
            {
                return false;
            }

            final int cell = (capacity - 1) & hash(point);

            // We use linear scanning. I think cluster of 100 elements is large enough to give up.
            for (int attempt = 0; attempt < 100; attempt++)
            {
                if (tryCell(cell + attempt, point, delta))
                    return true;
            }
            return false;
        }

        private int hash(long i)
        {
            long largePrime = 948701839L;
            return (int) (i * largePrime);
        }

        <E extends Exception> void forEach(HistogramDataConsumer<E> consumer) throws E
        {
            for (int i = 0; i < points.length; i++)
            {
                if (points[i] != -1)
                {
                    consumer.consume(points[i], values[i]);
                }
            }
        }

        private boolean tryCell(int cell, long point, int delta)
        {
            assert cell >= 0 && point >= 0 && delta >= 0 : "Invalid arguments: cell:" + cell + " point:" + point + " delta:" + delta;

            cell = cell % points.length;
            if (points[cell] == -1)
            {
                points[cell] = point;
                values[cell] = delta;
                size++;
                return true;
            }
            if (points[cell] == point)
            {
                values[cell] = (int) saturatingCastToInt((long) values[cell] + (long) delta);
                return true;
            }
            return false;
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < points.length; i++)
            {
                if (points[i] == -1)
                    continue;
                if (sb.length() > 1)
                    sb.append(", ");
                sb.append('[').append(points[i]).append(',').append(values[i]).append(']');
            }
            sb.append(']');
            return sb.toString();
        }
    }

    private static long ceilKey(long point, int bucketSize)
    {
        long delta = point % bucketSize;

        if (delta == 0)
            return point;

        return saturatingCastToMaxDeletionTime((long) point + (long) bucketSize - (long) delta);
    }

    public static int saturatingCastToInt(long value)
    {
        return (int) (value > Integer.MAX_VALUE ? Integer.MAX_VALUE : value);
    }
    
    public static long saturatingCastToLong(long value)
    {
        return value < 0L ? Long.MAX_VALUE : value;
    }

    /**
     * Cast to an long with maximum value of {@code Cell.MAX_DELETION_TIME} to avoid representing values that
     * aren't a tombstone
     */
    public static long saturatingCastToMaxDeletionTime(long value)
    {
        return (value < 0L || value > Cell.MAX_DELETION_TIME)
               ? Cell.MAX_DELETION_TIME
               : value;
    }
}
