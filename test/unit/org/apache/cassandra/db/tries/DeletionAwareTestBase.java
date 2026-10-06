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

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.DataPoint.contentOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.deletionOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

public class DeletionAwareTestBase
{
    /// Change to true to pring debug info
    static final boolean VERBOSE = false;

    static final int bitsNeeded = 6;
    int bits = bitsNeeded;

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static String toString(ByteComparable ranges)
    {
        if (ranges == null)
            return "null";
        return ranges.byteComparableAsString(TrieUtil.VERSION);
    }

    private static DeletionMarker makeActiveMarker(int active, int rangeIndex, ByteComparable nextRange)
    {
        if (active >= 0) // cmp > 0, must covert active to marker
        {
            if ((rangeIndex & 1) != 0)
                return new DeletionMarker(nextRange, active, -1, -1);
            else
                return new DeletionMarker(nextRange, -1, active, active);
        }
        return null;
    }

    protected static void assertDeletionAwareEqual(String msg, List<DataPoint> merged, DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        try
        {
            assertEquals(msg, merged, toList(trie));
            assertEquals(msg + " live",
                         merged.stream().map(DataPoint::live).filter(x -> x != null).collect(Collectors.toList()),
                         contentOnlyList(trie));
            assertEquals(msg + " deletions",
                         merged.stream().map(DataPoint::marker).filter(x -> x != null).collect(Collectors.toList()),
                         deletionOnlyList(trie));
            System.out.println(msg + " matched.");
        }
        catch (AssertionError e)
        {
            System.out.println();
            DataPoint.dumpDeletionAwareTrie(trie);
            throw e;
        }
    }

    static <T> void maybeAdd(List<T> list, T value)
    {
        if (value == null)
            return;
        list.add(value);
    }

    /// Creates a [ByteComparable] for the provided value by splitting the integer in sequences of "bits" bits.
    private ByteComparable of(int value, int terminator)
    {
        // TODO: Also in all other tests of this type
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits + 1];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos++] = (byte) (value & mask);
        splitBytes[pos++] = (byte) terminator;
        return ByteComparable.preencoded(VERSION, splitBytes);
    }

    ByteComparable at(int value)
    {
        return of(value, ByteSource.TERMINATOR);
    }

    ByteComparable before(int value)
    {
        return of(value, ByteSource.LT_NEXT_COMPONENT);
    }

    ByteComparable after(int value)
    {
        return of(value, ByteSource.GT_NEXT_COMPONENT);
    }

    DeletionMarker from(int where, int value)
    {
        return new DeletionMarker(before(where), -1, value, value);
    }

    DeletionMarker to(int where, int value)
    {
        return new DeletionMarker(before(where), value, -1, -1);
    }

    DeletionMarker change(int where, int from, int to)
    {
        return new DeletionMarker(before(where), from, to, to);
    }

    DeletionMarker[] deletedPoint(int where, int value)
    {
        return deletedPointInside(where, value, -1);
    }

    DeletionMarker[] deletedPointInside(int where, int value, int active)
    {
        return new DeletionMarker[]
               {
               new DeletionMarker(before(where), active, value, value),
               new DeletionMarker(after(where), value, active, active)
               };
    }

    DataPoint livePoint(int where, int timestamp)
    {
        return new LivePoint(at(where), timestamp);
    }

    protected ByteComparable[] array(ByteComparable... data)
    {
        return data;
    }

    protected List<DataPoint> flatten(List<Object> pointsOrArrays)
    {
        return pointsOrArrays.stream()
                             .flatMap(x -> x instanceof DataPoint ? Stream.of((DataPoint) x) : Arrays.stream((DeletionMarker[]) x))
                             .collect(Collectors.toList());
    }

    String toString(ByteComparable[] ranges)
    {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < ranges.length; i += 2)
        {
            b.append('[');
            b.append(DeletionAwareTestBase.toString(ranges[i]));
            b.append(';');
            b.append(DeletionAwareTestBase.toString(ranges[i + 1]));
            b.append(')');
        }
        return b.toString();
    }

    List<DataPoint> intersect(List<DataPoint> dataPoints, ByteComparable... ranges)
    {
        int rangeIndex = 0;
        int active = -1;
        ByteComparable nextRange = ranges[0];
        if (nextRange == null)
            nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint dp : dataPoints)
        {
            DeletionMarker marker = dp.marker();
            while (true)
            {
                int cmp;
                if (nextRange == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(dp.position(), nextRange, TrieUtil.VERSION);

                if (cmp < 0)
                {
                    if ((rangeIndex & 1) != 0)
                        DeletionAwareTestBase.maybeAdd(result, dp);
                    break;
                }

                if (cmp == 0)
                {
                    DeletionMarker adjustedMarker = marker != null ? marker : DeletionAwareTestBase.makeActiveMarker(active, rangeIndex, nextRange);

                    if ((rangeIndex & 1) == 0)
                        DeletionAwareTestBase.maybeAdd(result, dp.withMarker(startOf(adjustedMarker)));
                    else
                        DeletionAwareTestBase.maybeAdd(result, dp.withMarker(endOf(adjustedMarker)));   // live points are included at starts as well as ends

                    nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
                    break;
                }
                else
                    DeletionAwareTestBase.maybeAdd(result, DeletionAwareTestBase.makeActiveMarker(active, rangeIndex, nextRange));

                nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
            }
            if (marker != null)
                active = marker.rightSide;
        }
        assert active == -1;
        return result;
    }

    DeletionMarker startOf(DeletionMarker marker)
    {
        return marker != null ? marker.restrict(false, true) : null;
    }

    DeletionMarker endOf(DeletionMarker marker)
    {
        return marker != null ? marker.restrict(true, false) : null;
    }
}
