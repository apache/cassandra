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

import org.apache.cassandra.utils.Comparables;

/**
 * A {@code RateValue} represents a particular rate in a particular {@link RateUnit}.
 * <p>
 * Note that this can only represent positive sizes.
 */
public class RateValue implements Comparable<RateValue>
{
    public static final RateValue ZERO = new RateValue(0, RateUnit.B_S);

    public final long value;
    public final RateUnit unit;

    private RateValue(long value, RateUnit unit)
    {
        assert value >= 0 && value != Long.MAX_VALUE;
        this.value = value;
        this.unit = unit;
    }

    /**
     * Creates a new {@link RateValue} for the provided value in the provided unit.
     *
     * @param value the value in {@code unit}, which must be positive and strictly less than {@code Long.MAX_VALUE}
     *              (the latter being used to represent overflows).
     * @param unit  the unit of {@code value}.
     * @return a newly created {@link RateValue} for {@code value} in {@code unit}.
     * @throws IllegalArgumentException if {@code value} is negative or equal to {@code Long.MAX_VALUE}.
     */
    public static RateValue of(long value, RateUnit unit)
    {
        if (value < 0)
            throw new IllegalArgumentException("Invalid negative value for a rate: " + value);
        if (value == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid value for a rate, cannot be Long.MAX_VALUE");
        return new RateValue(value, unit);
    }

    /**
     * Computes the rate corresponding to "processing" {@code size} in {@code duration}.
     *
     * @param size     the size processed.
     * @param duration the duration of the process.
     * @return the rate corresponding to processing {@code size} in {@code duration}.
     */
    public static RateValue compute(SizeValue size, TimeValue duration)
    {
        SizeUnit bestSizeUnit = size.smallestRepresentableUnit();
        return RateValue.of(size.in(bestSizeUnit) / duration.value, RateUnit.of(bestSizeUnit, duration.unit));
    }

    /**
     * Returns the value this represents in the provided unit.
     *
     * @param destinationUnit the unit to return the value in.
     * @return the value this represent in {@code unit}.
     */
    public long in(RateUnit destinationUnit)
    {
        return destinationUnit.convert(value, unit);
    }

    public RateValue convert(RateUnit destinationUnit)
    {
        return RateValue.of(in(destinationUnit), destinationUnit);
    }

    /**
     * Returns the time required to "process" the provided size at this rate.
     */
    public TimeValue timeFor(SizeValue size)
    {
        // Convert both the rate and size in the smallest unit in which they don't overflow: this will ensure the most
        // precise return value.
        RateUnit smallestForRate = smallestRepresentableUnit();
        SizeUnit smallestForSize = size.smallestRepresentableUnit();

        SizeUnit toConvert = Comparables.max(smallestForSize, smallestForRate.sizeUnit);
        return TimeValue.of(size.in(toConvert) / toConvert.convert(value, unit.sizeUnit), unit.timeUnit);
    }

    private RateUnit smallestRepresentableUnit()
    {
        return unit.smallestRepresentableUnit(value);
    }

    /**
     * Returns a string representation of this value in the unit it was created with.
     */
    public String toRawString()
    {
        return unit.toString(value);
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
        return unit.toHumanReadableString(value);
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
     * Two {@link RateValue} are equal if they represent exactly the same number of bytes in the same number of time.
     *
     * @param other the value to check equality with.
     * @return whether this value and {@code other} represent the same rate.
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof RateValue))
            return false;

        RateValue that = (RateValue) other;

        // Convert both value to the most precise unit in which they can both be represented without overflowing and
        // check we get the same value. If both don't have the same smallest representable unit, they can't be
        // representing the same number of bytes.
        RateUnit smallest = this.smallestRepresentableUnit();
        return smallest.equals(that.smallestRepresentableUnit()) && this.in(smallest) == that.in(smallest);
    }

    public int compareTo(RateValue that)
    {
        // To compare, we need to have the same unit.
        RateUnit thisSmallest = this.smallestRepresentableUnit();
        RateUnit thatSmallest = that.smallestRepresentableUnit();

        if (thisSmallest.equals(thatSmallest))
            return Long.compare(this.in(thisSmallest), that.in(thatSmallest));

        // If one value overflow "before" (it has a bigger smallest representable unit) the other one, then that value
        // is bigger. Note that rate units are not comparable in the absolute
        return thisSmallest.compareTo(thatSmallest) > 0 ? 1 : -1;
    }
}
