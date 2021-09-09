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
package org.apache.cassandra.service.reads.trackwarnings;

public class TrackWarningsSnapshot
{
    private static final TrackWarningsSnapshot EMPTY = new TrackWarningsSnapshot(Warnings.EMPTY, Warnings.EMPTY, Warnings.EMPTY);

    public final Warnings tombstones, localReadSize, rowIndexTooLarge;

    private TrackWarningsSnapshot(Warnings tombstones, Warnings localReadSize, Warnings rowIndexTooLarge)
    {
        this.tombstones = tombstones;
        this.localReadSize = localReadSize;
        this.rowIndexTooLarge = rowIndexTooLarge;
    }

    public static TrackWarningsSnapshot create(Warnings tombstones, Warnings localReadSize, Warnings rowIndexTooLarge)
    {
        if (tombstones == localReadSize && tombstones == rowIndexTooLarge && tombstones == Warnings.EMPTY)
            return EMPTY;
        return new TrackWarningsSnapshot(tombstones, localReadSize, rowIndexTooLarge);
    }

    public static TrackWarningsSnapshot merge(TrackWarningsSnapshot... values)
    {
        if (values == null || values.length == 0)
            return null;

        TrackWarningsSnapshot accum = EMPTY;
        for (TrackWarningsSnapshot a : values)
            accum = accum.merge(a);
        return accum == EMPTY ? null : accum;
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    private TrackWarningsSnapshot merge(TrackWarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return TrackWarningsSnapshot.create(tombstones.merge(other.tombstones), localReadSize.merge(other.localReadSize), rowIndexTooLarge.merge(other.rowIndexTooLarge));
    }

    public static final class Warnings
    {
        private static final Warnings EMPTY = new Warnings(Counter.EMPTY, Counter.EMPTY);

        public final Counter warnings;
        public final Counter aborts;

        private Warnings(Counter warnings, Counter aborts)
        {
            this.warnings = warnings;
            this.aborts = aborts;
        }

        public static Warnings create(Counter warnings, Counter aborts)
        {
            if (warnings == Counter.EMPTY && aborts == Counter.EMPTY)
                return EMPTY;
            return new Warnings(warnings, aborts);
        }

        public Warnings merge(Warnings other)
        {
            if (other == EMPTY)
                return this;
            return Warnings.create(warnings.merge(other.warnings), aborts.merge(other.aborts));
        }
    }

    public static final class Counter
    {
        private static final Counter EMPTY = new Counter(0, 0);

        public final int count;
        public final long maxValue;

        private Counter(int count, long maxValue)
        {
            this.count = count;
            this.maxValue = maxValue;
        }

        public static Counter create(int count, long maxValue)
        {
            // if count=0 and maxValue != 0... this shouldn't happen but not expliclty blocking it
            if (count == maxValue && count == 0)
                return EMPTY;
            return new Counter(count, maxValue);
        }

        public Counter merge(Counter other)
        {
            if (other == EMPTY)
                return this;
            return Counter.create(count + other.count, Math.max(maxValue, other.maxValue));
        }
    }
}
