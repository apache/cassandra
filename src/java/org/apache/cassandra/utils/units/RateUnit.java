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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Comparables;

/**
 * Represents the unit of a rate of transfer/work in term of byte sizes dealt with in a given time. As such, a
 * {@link RateUnit} unit is simply the combination of a {@link SizeUnit} and a {@link TimeUnit}.
 * <p>
 * Note that while the code is relatively in that it can manipulate any combination of size unit and time unit, we
 * pre-declare only a handful of the most common rates (only in seconds in practice).
 */
public class RateUnit implements Comparable<RateUnit>
{
    /**
     * Bytes per Seconds.
     */
    public static final RateUnit B_S = RateUnit.of(SizeUnit.BYTES, TimeUnit.SECONDS);
    /**
     * KiloBytes per Seconds.
     */
    public static final RateUnit KB_S = RateUnit.of(SizeUnit.KILOBYTES, TimeUnit.SECONDS);
    /**
     * MegaBytes per Seconds.
     */
    public static final RateUnit MB_S = RateUnit.of(SizeUnit.MEGABYTES, TimeUnit.SECONDS);
    /**
     * GigaBytes per Seconds.
     */
    public static final RateUnit GB_S = RateUnit.of(SizeUnit.GIGABYTES, TimeUnit.SECONDS);
    /**
     * TeraBytes per Seconds.
     */
    public static final RateUnit TB_S = RateUnit.of(SizeUnit.TERABYTES, TimeUnit.SECONDS);

    public final SizeUnit sizeUnit;
    public final TimeUnit timeUnit;

    private RateUnit(SizeUnit sizeUnit, TimeUnit timeUnit)
    {
        this.sizeUnit = sizeUnit;
        this.timeUnit = timeUnit;
    }

    public static RateUnit of(SizeUnit sizeUnit, TimeUnit timeUnit)
    {
        return new RateUnit(sizeUnit, timeUnit);
    }

    /**
     * Convert the given rate in the given unit to this unit. Conversions from finer to coarser granularities truncate,
     * so lose precision, conversions from coarser to finer granularities with arguments that would numerically overflow
     * saturate to <tt>Long.MIN_VALUE</tt> if negative or <tt>Long.MAX_VALUE</tt> if positive.
     * <p>
     * For example, to convert 10 megabytes per seconds to bytes per seconds, use: {@code B_S.convert(10L, MB_S)}.
     *
     * @param sourceRate the rate to convert in the given {@code sourceUnit}.
     * @param sourceUnit the unit of the {@code sourceSize} argument
     * @return the converted size in this unit, or {@code Long.MIN_VALUE} if conversion would negatively overflow, or
     * {@code Long.MAX_VALUE} if it would positively overflow.
     */
    public long convert(long sourceRate, RateUnit sourceUnit)
    {
        // We need to convert the size unit and the time unit. For the time unit, since it's a rate, we basically want
        // to do the opposite of converting from the sourceUnit to the destinationUnit, so we convert from the
        // destinationUnit to the sourceUnit, even though the value is obviously not in the destination unit in the
        // first place.
        // The order we apply the conversion matters however: say we convert '10 MB/s' to 'GB/days': if we were to apply
        // the size conversion first, we'd get 0, since 10MB is 0GB. So we should apply the time conversion first
        // ('10 MB/s' is '10 * 3600 * 24 MB/days') and then do the size conversion. Conversely, when converting
        // '10 MB/s' to 'B/ms', we shouldn't convert by time first, as 10ms is 0s (we do the inverse conversion).
        // In practice, if the source size unit is smaller than the destination one, we want to apply the time conversion
        // first, otherwise, we can apply the size one first.
        if (sourceUnit.sizeUnit.compareTo(sizeUnit) < 0)
            return sizeUnit.convert(sourceUnit.timeUnit.convert(sourceRate, timeUnit), sourceUnit.sizeUnit);

        return sourceUnit.timeUnit.convert(sizeUnit.convert(sourceRate, sourceUnit.sizeUnit), timeUnit);
    }

    /**
     * Returns a Human Readable representation of the provided value in this unit.
     * <p>
     * Note that this method may discard precision for the sake of returning a more human readable value. In other
     * words, if {@code value} is large, it will be converted to a bigger, more readable unit, even this imply
     * truncating the value.
     *
     * @param value the value in this unit.
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    public String toHumanReadableString(long value)
    {
        return Units.toString(value, this);
    }

    public String toString(long value)
    {
        return Units.formatValue(value) + this;
    }

    static String toString(SizeUnit sizeUnit, TimeUnit timeUnit)
    {
        return String.format("%s/%s", sizeUnit.symbol, Units.TIME_UNIT_SYMBOL_FCT.apply(timeUnit));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sizeUnit, timeUnit);
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof RateUnit))
            return false;

        RateUnit that = (RateUnit) other;
        return this.sizeUnit == that.sizeUnit && this.timeUnit == that.timeUnit;
    }

    @Override
    public String toString()
    {
        return toString(sizeUnit, timeUnit);
    }

    /**
     * Given a value in this unit, returns the smallest (most fine grained) unit in which that value can be represented
     * without overflowing.
     *
     * @param value the value in this unit.
     * @return the smallest unit, potentially this unit, at which the value can be represented without overflowing. If
     * {@code value == Long.MAX_VALUE}, then this unit is returned.
     */
    RateUnit smallestRepresentableUnit(long value)
    {
        // This is kind of subtle because we get a smaller unit that this one by both decreasing the size unit
        // and increasing the time unit, and both don't have the same effect, so we need to find the most optimal
        // application of both operation that don't overflow our value.
        // For instance, consider v1=(Long.MAX_VALUE-1 / 1000), then the smallest representable unit for
        // v1 MB/ms is MB/s (kB/ms doesn't work), while for v2=(Long.MAX_VALUE-1 / 1024) MB/ms, the smallest
        // representable unit is actually kB/ms (it's also representable as MB/s, but it's a bigger unit).
        //
        // So we proceed by applying each option (decreasing size or incrementing time), check if we overflow with each
        // and if we don't apply recursively. We then compare the unit from both recursive call to find the smallest
        // one.
        if (value == Long.MAX_VALUE)
            return this;

        SizeUnit nextSizeUnit = next(sizeUnit);
        TimeUnit nextTimeUnit = next(timeUnit);

        long vSize = nextSizeUnit == null ? Long.MAX_VALUE : nextSizeUnit.convert(value, sizeUnit);
        // Reminder that because the time divide the rate, the conversion should be applied in reverse
        long vTime = nextTimeUnit == null ? Long.MAX_VALUE : timeUnit.convert(value, nextTimeUnit);

        RateUnit smallestWithSize = vSize == Long.MAX_VALUE
                                    ? this
                                    : RateUnit.of(nextSizeUnit, timeUnit).smallestRepresentableUnit(vSize);
        RateUnit smallestWithTime = vTime == Long.MAX_VALUE
                                    ? this
                                    : RateUnit.of(sizeUnit, nextTimeUnit).smallestRepresentableUnit(vTime);

        return Comparables.min(smallestWithSize, smallestWithTime);
    }

    private static SizeUnit next(SizeUnit unit)
    {
        int ordinal = unit.ordinal();
        return ordinal == 0 ? null : SizeUnit.values()[ordinal - 1];
    }

    private static TimeUnit next(TimeUnit unit)
    {
        int ordinal = unit.ordinal();
        return ordinal == TimeUnit.values().length - 1 ? null : TimeUnit.values()[ordinal + 1];
    }

    public int compareTo(RateUnit that)
    {
        // Comparing rate units is a tad tricky. We're asking what is the biggest "transfer rate" between 1 of this unit
        // versus 1 of 'that' unit. This is easier when one of the unit is the same in each unit however.
        if (this.sizeUnit == that.sizeUnit)
            return that.timeUnit.compareTo(this.timeUnit); // 1 MB/h is smaller/slower than 1 MB/s

        if (this.timeUnit == that.timeUnit)
            return this.sizeUnit.compareTo(that.sizeUnit); // 1 MB/s is smaller/slower than 1 TB/s

        // Otherwise, we have to compute by how much it differs in size versus by how much it differs in time.
        if (this.sizeUnit.compareTo(that.sizeUnit) < 0)
        {
            if (this.timeUnit.compareTo(that.timeUnit) < 0)
            {
                // this = 1 B/ms and that = 1 MB/s
                // How much we'll multiply 'that' to get it into 'this' size unit
                long thatScale = valueDiff(this.sizeUnit, that.sizeUnit);
                // How much we'll multiply 'this' to get it into 'that' time unit
                long thisScale = valueDiff(this.timeUnit, that.timeUnit);
                // 'that' is bigger if it is bigger when put in the same unit than 'this', that is if we'll multiply it
                // by a bigger value
                return Long.compare(thisScale, thatScale);
            }
            else
            {
                // this = 1 B/s and that = 1 MB/ms
                // That transfers more data in less time, it's definitively faster (bigger)
                return -1;
            }
        }
        else
        {
            if (this.timeUnit.compareTo(that.timeUnit) < 0)
            {
                // This transfers more data in less time, it's definitively faster (bigger)
                return 1;
            }
            else
            {
                // this = 1 MB/s and that = 1 B/ms
                // How much we'll multiply 'this' to get it into 'that' size unit
                long thisScale = valueDiff(that.sizeUnit, this.sizeUnit);
                // How much we'll multiply 'that' to get it into 'this' time unit
                long thatScale = valueDiff(that.timeUnit, this.timeUnit);
                // 'that' is bigger if it is bigger when put in the same unit than 'this', that is if we'll multiply it
                // by a bigger value
                return Long.compare(thisScale, thatScale);
            }
        }
    }

    /**
     * The difference in value between 2 different size unit min and max, where min < max.
     */
    private static long valueDiff(SizeUnit min, SizeUnit max)
    {
        return 1024L * (max.ordinal() - min.ordinal());
    }

    /**
     * The difference in value between 2 different time unit min and max, where min < max.
     */
    private static long valueDiff(TimeUnit min, TimeUnit max)
    {
        TimeUnit[] all = TimeUnit.values();
        long val = 1;
        for (int i = min.ordinal(); i < max.ordinal(); i++)
            val *= Units.TIME_UNIT_SCALE_FCT.applyAsLong(all[i]);
        return val;
    }
}
