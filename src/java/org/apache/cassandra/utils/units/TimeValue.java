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
package org.apache.cassandra.utils.units;

import java.util.concurrent.TimeUnit;

/**
 * A {@code TimeValue} represents a particular duration in a particular {@link TimeUnit}.
 */
public class TimeValue implements Comparable<TimeValue>
{
    public static final TimeValue ZERO = new TimeValue(0, TimeUnit.NANOSECONDS);

    final long value;
    final TimeUnit unit;

    private TimeValue(long value, TimeUnit unit)
    {
        this.value = value;
        this.unit = unit;
    }

    /**
     * Creates a new {@link TimeValue} for the provided value in the provided unit.
     *
     * @param value the value in {@code unit}.
     * @param unit  the unit of {@code value}.
     * @return a newly created {@link TimeValue} for {@code value} in {@code unit}.
     */
    public static TimeValue of(long value, TimeUnit unit)
    {
        return new TimeValue(value, unit);
    }

    /**
     * Returns the value this represents in the provided unit.
     *
     * @param destinationUnit the unit to return the value in.
     * @return the value this represent in {@code unit}.
     */
    public long in(TimeUnit destinationUnit)
    {
        return destinationUnit.convert(value, unit);
    }

    static TimeUnit smallestRepresentableUnit(long value, TimeUnit unit)
    {
        long v = value;
        int i = unit.ordinal();
        TimeUnit u = unit;
        while (i > 0 && v < Long.MAX_VALUE)
        {
            TimeUnit current = u;
            u = TimeUnit.values()[--i];
            v = u.convert(v, current);
        }
        return u;
    }

    private TimeUnit smallestRepresentableUnit()
    {
        return smallestRepresentableUnit(value, unit);
    }

    /**
     * Returns a string representation of this value in the unit it was created with.
     */
    public String toRawString()
    {
        return Units.formatValue(value) + Units.TIME_UNIT_SYMBOL_FCT.apply(unit);
    }

    /**
     * Returns a Human Readable representation of this value.
     * <p>
     * Note that this method may discard precision for the sake of returning a more human readable value. In other
     * words, this will display the value is a bigger unit than the one it was created with if that improve readability
     * and this even this imply truncating the value.
     *
     * @return a potentially truncated but human readable representation of this value.
     */
    @Override
    public String toString()
    {
        return Units.toString(value, unit);
    }

    @Override
    public int hashCode()
    {
        // Make sure that equals() => same hashCode()
        return Long.hashCode(in(smallestRepresentableUnit()));
    }

    /**
     * Checks the equality of this value with another value.
     * <p>
     * Two {@link TimeValue} are equal if they represent exactly the same number of nanoseconds.
     *
     * @param other the value to check equality with.
     * @return whether this value and {@code other} represent the same number of nanoseconds.
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof TimeValue))
            return false;

        TimeValue that = (TimeValue) other;

        // Convert both value to the most precise unit in which they can both be represented without overflowing and
        // check we get the same value. If both don't have the same smallest representable unit, they can't be
        // representing the same number of bytes.
        TimeUnit smallest = this.smallestRepresentableUnit();
        return smallest == that.smallestRepresentableUnit() && this.in(smallest) == that.in(smallest);
    }

    public int compareTo(TimeValue that)
    {
        // To compare, we need to have the same unit.
        TimeUnit thisSmallest = this.smallestRepresentableUnit();
        TimeUnit thatSmallest = that.smallestRepresentableUnit();

        if (thisSmallest == thatSmallest)
            return Long.compare(this.in(thisSmallest), that.in(thatSmallest));

        // If one value overflow "before" (it has a bigger smallest representable unit) the other one, then that value
        // is bigger.
        return thisSmallest.compareTo(thatSmallest) > 0 ? 1 : -1;
    }
}
