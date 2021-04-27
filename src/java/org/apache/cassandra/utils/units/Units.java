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
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Static methods used by work with units.
 * <p>
 * This is mostly useful for {@link TimeUnit}, as for other units the method provided  are more directly accessible in the
 * unit class itself (we can't modify {@link TimeUnit}), but contains methods for all unit for symmetry.
 */
public class Units
{
    static final ToLongFunction<TimeUnit> TIME_UNIT_SCALE_FCT = u ->
    {
        switch (u)
        {
            case NANOSECONDS:
            case MICROSECONDS:
            case MILLISECONDS:
                return 1000L;
            case SECONDS:
            case MINUTES:
                return 60L;
            case HOURS:
                return 24L;
            case DAYS:
                return 365; // Never actually use but well...
            default:
                throw new AssertionError();
        }
    };
    static final Function<TimeUnit, String> TIME_UNIT_SYMBOL_FCT = u ->
    {
        switch (u)
        {
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "us";
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new AssertionError();
        }
    };

    private static final ToLongFunction<SizeUnit> SIZE_UNIT_SCALE_FCT = u -> SizeUnit.C1;
    private static final Function<SizeUnit, String> SIZE_UNIT_SYMBOL_FCT = u -> u.symbol;


    /**
     * Returns a Human Readable representation of the provided duration given the unit of said duration.
     * <p>
     * This method strives to produce a short and human readable representation and may trade precision for that. In
     * other words, if the value is large, this will display the value in a bigger unit than the one provided to improve
     * readability and this even this imply truncating the value.
     *
     * @param value the value to build a string of.
     * @param unit  the unit of {@code value}.
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    public static String toString(long value, TimeUnit unit)
    {
        return toString(value, unit, TimeUnit.class, TIME_UNIT_SCALE_FCT, TIME_UNIT_SYMBOL_FCT);
    }

    /**
     * Returns a Human Readable representation of the provided size given the unit of said size.
     * <p>
     * This method strives to produce a short and human readable representation and may trade precision for that. In
     * other words, if the value is large, this will display the value in a bigger unit than the one provided to improve
     * readability and this even this imply truncating the value.
     *
     * @param value the value to build a string of.
     * @param unit  the unit of {@code value}.
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    public static String toString(long value, SizeUnit unit)
    {
        return toString(value, unit, SizeUnit.class, SIZE_UNIT_SCALE_FCT, SIZE_UNIT_SYMBOL_FCT);
    }

    /**
     * Returns a string representation for a size value (in a particular unit) that is suitable for logging the value.
     * <p>
     * The returned representation combines the value displayed in bytes (for the sake of script parsing the log, so
     * they don't have to bother with unit conversion), followed by the representation from {@link #toString} for
     * humans.
     *
     * @param value a size in {@code unit}.
     * @param unit  the unit for {@code value}.
     * @return a string representation suitable for logging the value.
     */
    public static String toLogString(long value, SizeUnit unit)
    {
        return String.format("%s (%s)", SizeUnit.BYTES.toString(unit.toBytes(value)), toString(value, unit));
    }

    /**
     * Returns a Human Readable representation of the provided rate given the unit of said rate.
     * <p>
     * This method strives to produce a short and human readable representation and may trade precision for that. In
     * other words, if the value is large, this will display the value in a bigger unit than the one provided to improve
     * readability and this even this imply truncating the value.
     *
     * @param value the value to build a string of.
     * @param unit  the unit of {@code value}.
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    public static String toString(long value, RateUnit unit)
    {
        // There is theoretically multiple options for any given (large) value since we can play on both the size
        // and time unit. In practice though, it's much more common to reason with rate 'per second' so we force
        // seconds as unit of time and play only on the size unit.
        value = RateUnit.of(unit.sizeUnit, TimeUnit.SECONDS).convert(value, unit);
        return toString(value, unit.sizeUnit, SizeUnit.class, SIZE_UNIT_SCALE_FCT, u -> RateUnit.toString(u, unit.timeUnit));
    }

    /**
     * Format a value a in a human readable way, adding a comma (',') to separate every thousands.
     * <p>
     * For instance, {@code formatValue(4693234L) == "4,693,234"}
     *
     * @param value the value to format.
     * @return a more human readable representation of {@code value}.
     */
    static String formatValue(long value)
    {
        return String.format("%,d", value);
    }

    /**
     * The number of comma to use to format {@code digits} digit using ',' on every thousands.
     */
    private static int commaCount(int digits)
    {
        return (digits - 1) / 3;
    }

    /**
     * Returns a Human Readable representation of the provided size/rate given the unit of said size/rate.
     * <p>
     * This method strives to produce a short and human readable representation and may trade precision for that. In
     * other words, if the value is large, this will display the value in a bigger unit than the one provided to improve
     * readability and this even this imply truncating the value.
     *
     * @param value     the value to build a string of.
     * @param unit      the unit of {@code value}, which is currently either {@link SizeUnit} or {@link RateUnit}
     * @param klass     Currently can be either a {@link SizeUnit} or {@link RateUnit} class
     * @param scaleFct  A function that knows how to scale between units of the given {@code unit}
     * @param symbolFct A function that knows how to scale between symbols of the given {@code unit}
     * @param <E>       currently either {@link SizeUnit} or {@link RateUnit}
     * @return a potentially truncated but human readable representation of {@code value}.
     */
    private static <E extends Enum<E>> String toString(long value,
                                                       E unit,
                                                       Class<E> klass,
                                                       ToLongFunction<E> scaleFct,
                                                       Function<E, String> symbolFct)
    {
        E[] enumVals = klass.getEnumConstants();

        long v = value;
        int i = unit.ordinal();
        long remainder = 0;
        // The scale is how much we need to go from unit to the next one
        long scale = scaleFct.applyAsLong(unit);

        while (i < enumVals.length - 1 && v >= scale)
        {
            remainder = v % scale;
            v = v / scale;
            unit = enumVals[++i];
            scale = scaleFct.applyAsLong(unit);
        }

        // If the value is small (<10), include one decimal so the precision is not too truncated. Otherwise, don't
        // bother, it's less relevant.
        if (v >= 10 || remainder == 0)
            return fmt(v, unit, symbolFct);

        // Note that scale is the scale of the current unit, but remainder relates to the previous unit. Also not that
        // can only get here is remainder != 0 so we know accessing the previous unit is legit
        long prevScale = scaleFct.applyAsLong(enumVals[i - 1]);
        int decimal = Math.round(((float) remainder / prevScale) * 10);
        if (decimal == 0)
            return fmt(v, unit, symbolFct);

        // If the remainder amounts to more than 0.95 of C1, decimal will be 10. In that case, just bump the value by 1
        if (decimal == 10)
            return fmt(v + 1, unit, symbolFct);

        return formatValue(v) + '.' + decimal + symbolFct.apply(unit);
    }

    private static <E extends Enum<E>> String fmt(long value, E unit, Function<E, String> symbolFct)
    {
        return formatValue(value) + symbolFct.apply(unit);
    }
}
