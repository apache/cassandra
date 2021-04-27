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

/**
 * A {@code SizeValue} represents a particular size in a particular {@link SizeUnit}.
 * <p>
 * Note that this can only represent positive sizes.
 */
public class SizeValue implements Comparable<SizeValue>
{
    public static final SizeValue ZERO = new SizeValue(0, SizeUnit.BYTES);

    public final long value;
    public final SizeUnit unit;

    private SizeValue(long value, SizeUnit unit)
    {
        assert value >= 0 && value != Long.MAX_VALUE;
        this.value = value;
        this.unit = unit;
    }

    /**
     * Creates a new {@link SizeValue} for the provided value in the provided unit.
     *
     * @param value the value in {@code unit}, which must be positive and strictly less than {@code Long.MAX_VALUE}
     *              (the latter being used to represent overflows).
     * @param unit  the unit of {@code value}.
     * @return a newly created {@link SizeValue} for {@code value} in {@code unit}.
     * @throws IllegalArgumentException if {@code value} is negative or equal to {@code Long.MAX_VALUE}.
     */
    public static SizeValue of(long value, SizeUnit unit)
    {
        if (value < 0)
            throw new IllegalArgumentException("Invalid negative value for a size in bytes: " + value);
        if (value == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid value for a size in bytes, cannot be Long.MAX_VALUE");
        return new SizeValue(value, unit);
    }

    /**
     * Returns the value this represents in the provided unit.
     *
     * @param destinationUnit the unit to return the value in.
     * @return the value this represent in {@code unit}.
     */
    public long in(SizeUnit destinationUnit)
    {
        return destinationUnit.convert(value, unit);
    }

    SizeUnit smallestRepresentableUnit()
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
     * Returns a string representation particularly suitable for logging the value.
     * <p>
     * The returned representation combines the value displayed in bytes (for the sake of script parsing the log, so
     * they don't have to bother with unit conversion), followed by the representation from
     * {@link SizeUnit#toHumanReadableString(long)} for humans.
     *
     * @return a string representation suitable for logging the value.
     */
    public String toLogString()
    {
        return unit.toLogString(value);
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
     * Two {@link SizeValue} are equal if they represent exactly the same number of bytes.
     *
     * @param other the value to check equality with.
     * @return whether this value and {@code other} represent the same number of bytes.
     */
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof SizeValue))
            return false;

        SizeValue that = (SizeValue) other;

        // Convert both value to the most precise unit in which they can both be represented without overflowing and
        // check we get the same value. If both don't have the same smallest representable unit, they can't be
        // representing the same number of bytes.
        SizeUnit smallest = this.smallestRepresentableUnit();
        return smallest == that.smallestRepresentableUnit() && this.in(smallest) == that.in(smallest);
    }

    public int compareTo(SizeValue that)
    {
        // To compare, we need to have the same unit.
        SizeUnit thisSmallest = this.smallestRepresentableUnit();
        SizeUnit thatSmallest = that.smallestRepresentableUnit();

        if (thisSmallest == thatSmallest)
            return Long.compare(this.in(thisSmallest), that.in(thatSmallest));

        // If one value overflow "before" (it has a bigger smallest representable unit) the other one, then that value
        // is bigger.
        return thisSmallest.compareTo(thatSmallest) > 0 ? 1 : -1;
    }
}
