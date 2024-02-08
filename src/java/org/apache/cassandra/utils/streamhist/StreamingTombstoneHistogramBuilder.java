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
import com.google.common.primitives.UnsignedInts;
import com.google.common.primitives.UnsignedLongs;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.utils.CassandraUInt;

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
 *     <li>To use binary search <i></>{point, values}</i> in bin pairs is packed in one long</li>
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
        // ceilKey always calls saturatingCastToMaxDeletionTime so all points fit an unsigned int
        // and also don't collide with DataHolder.EMPTY
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
            flushValue(CassandraUInt.fromLong(point), value);
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

    private void flushValue(int pointUnsigned, int spoolValue)
    {
        bin.addValue(pointUnsigned, spoolValue);

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
        static final long EMPTY = UnsignedLongs.MAX_VALUE;
        final long[] data;
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

        @VisibleForTesting
        int getValue(int pointUnsigned)
        {
            long key = wrap(pointUnsigned, 0);
            int index = unsignedBinarySearch(data, key);
            if (index < 0)
                index = -index - 1;
            if (index >= data.length)
                return -1; // not-found sentinel
            if (unwrapPointUnsigned(data[index]) != pointUnsigned)
                return -2; // not-found sentinel
            return unwrapValue(data[index]);
        }

        /**
         * Adds value {@code delta} to the point {@code point}.
         *
         * @return {@code true} if inserted, {@code false} if accumulated
         */
        public boolean addValue(int pointUnsigned, int delta)
        {
            long key = wrap(pointUnsigned, 0);
            int index = unsignedBinarySearch(data, key);
            if (index < 0)
            {
                index = -index - 1;
                assert (index < data.length) : "No more space in array";

                if (unwrapPointUnsigned(data[index]) != pointUnsigned) //ok, someone else at this point, let's shift array and insert
                {
                    assert (data[data.length - 1] == EMPTY) : "No more space in array";

                    System.arraycopy(data, index, data, index + 1, data.length - index - 1);

                    data[index] = wrap(pointUnsigned, delta);
                    return true;
                }
                else
                {
                    data[index] = wrap(pointUnsigned, (long) unwrapValue(data[index]) + delta);
                }
            }
            else
            {
                data[index] = wrap(pointUnsigned, (long) unwrapValue(data[index]) + delta);
            }

            return false;
        }

        /**
         * Calculate the weighted midpoint using a slightly different approach from the documented
         * formula that should calculate the same values without overflowing long arithmetic.
         */
        long calculateWeightedMidpoint(long point1, long value1, long point2, long value2)
        {
            assert point1 >= 0 && point1 <= CassandraUInt.MAX_VALUE_LONG;
            assert point2 >= 0 && point2 <= CassandraUInt.MAX_VALUE_LONG;
            assert value1 >= 0 && value1 <= Integer.MAX_VALUE;
            assert value2 >= 0 && value2 <= Integer.MAX_VALUE;
            long distance = point2 - point1;
            long weightedDistance = value2 * distance;
            long totalWeight = value1 + value2;
            long weightedMidpoint = point1 + weightedDistance / totalWeight;
            return ceilKey(weightedMidpoint, roundSeconds);
        }

        /**
         *  Finds nearest points <i>p1</i> and <i>p2</i> in the collection
         *  Replaces these two points with one weighted point <i>p3 = (p1*m1+p2*m2)/(p1+p2)
         */
        @VisibleForTesting
        void mergeNearestPoints()
        {
            assert isFull() : "DataHolder must be full in order to merge two points";

            final long[] smallestDifference = findPointPairWithSmallestDistance();
            final int index = (int)smallestDifference[0];
            final long point1 = smallestDifference[1];
            final long point2 = smallestDifference[2];

            assert (unwrapPointSigned(data[index + 1]) == point2) : "point2 should follow point1";
            final long value1 = unwrapValue(data[index]);
            final long value2 = unwrapValue(data[index + 1]);
            final long sum = value1 + value2;

            long newPoint = calculateWeightedMidpoint(point1, value1, point2, value2);
            data[index] = wrap(UnsignedInts.checkedCast(newPoint), sum);

            System.arraycopy(data, index + 2, data, index + 1, data.length - index - 2);
            data[data.length - 1] = EMPTY;
        }

        private long[] findPointPairWithSmallestDistance()
        {
            assert isFull(): "The DataHolder must be full in order to find the closest pair of points";

            long point1 = 0;
            long point2 = Long.MAX_VALUE;

            int index = 0;
            for (int i = 0; i < data.length - 1; i++)
            {
                long pointA = unwrapPointSigned(data[i]);
                long pointB = unwrapPointSigned(data[i + 1]);

                assert pointB > pointA : "DataHolder not sorted, p2(" + pointB +") < p1(" + pointA + ") for " + this;

                if (point2 - point1 > pointB - pointA)
                {
                    index = i;
                    point1 = pointA;
                    point2 = pointB;
                }
            }

            return new long[]{index, point1, point2};
        }

        static long unwrapPointSigned(long key)
        {
            return UnsignedInts.toLong(unwrapPointUnsigned(key));
        }

        static int unwrapPointUnsigned(long key)
        {
            return (int) (key >> 32);
        }

        static int unwrapValue(long key)
        {
            return (int) (key & 0xFF_FF_FF_FFL);
        }

        static long wrap(int pointUnsigned, long value)
        {
            return (((long)pointUnsigned) << 32) | saturatingCastToInt(value);
        }

        private int unsignedBinarySearch(long[] a, long key)
        {
            int low = 0;
            int high = a.length - 1;

            long comparableKey = key ^ Long.MIN_VALUE;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                long midVal = a[mid] ^ Long.MIN_VALUE;

                if (midVal < comparableKey)
                    low = mid + 1;
                else if (midVal != comparableKey)
                    high = mid - 1;
                else
                    return mid; // key found
            }
            return -(low + 1);  // key not found.
        }

        public String toString()
        {
            List<String> entries = new ArrayList<>();
            for (int i = 0; i < data.length; i++)
            {
                if (data[i] == EMPTY)
                    break;

                entries.add("[" + unwrapPointSigned(data[i]) + "], [" + unwrapValue(data[i]) + "]");
            }
            return StringUtils.join(entries, ",");
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

                histogramDataConsumer.consume(unwrapPointUnsigned(datum), unwrapValue(datum));
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
                final long point = unwrapPointSigned(pointAndValue);
                final int value = unwrapValue(pointAndValue);
                if (point > b)
                {
                    if (i == 0)
                    { // no prev point
                        return 0;
                    }
                    else
                    {
                        final long prevPoint = unwrapPointSigned(data[i - 1]);
                        final int prevValue = unwrapValue(data[i - 1]);
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

    /**
     * This class is a specialized open addressing HashMap that uses int as keys and int as values.
     * This is an optimization to avoid allocating objects.
     * In order for this class to work correctly it should have a power of 2 capacity.
     * This last invariant is taken care of during construction.
     */
    static class Spool
    {
        final int[] points;
        final int[] values;

        final int capacity;
        int size;

        Spool(int requestedCapacity)
        {
            if (requestedCapacity < 0)
                throw new IllegalArgumentException("Illegal capacity " + requestedCapacity);

            this.capacity = getPowerOfTwoCapacity(requestedCapacity);

            // x2 because we want no more than two reprobes on average when _capacity_ entries will be written
            points = new int[capacity * 2];
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

            int pointUnsigned = UnsignedInts.checkedCast(point);
            final int cell = (capacity - 1) & hash(point);

            // We use linear scanning. I think cluster of 100 elements is large enough to give up.
            for (int attempt = 0; attempt < 100; attempt++)
            {
                if (tryCell(cell + attempt, pointUnsigned, delta))
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

        private boolean tryCell(int cell, int pointUnsigned, int delta)
        {
            assert cell >= 0 && delta >= 0 : "Invalid arguments: cell:" + cell + " point:" + UnsignedInts.toLong(pointUnsigned) + " delta:" + delta;

            cell = cell % points.length;
            if (points[cell] == -1)
            {
                points[cell] = pointUnsigned;
                values[cell] = delta;
                size++;
                return true;
            }
            if (points[cell] == pointUnsigned)
            {
                values[cell] = saturatingCastToInt((long) values[cell] + (long) delta);
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

        if (delta != 0)
            point = point + bucketSize - delta;

        return saturatingCastToMaxDeletionTime(point);
    }

    public static int saturatingCastToInt(long value)
    {
        return (int) (value > Integer.MAX_VALUE ? Integer.MAX_VALUE : value);
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