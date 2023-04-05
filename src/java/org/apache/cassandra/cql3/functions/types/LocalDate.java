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
package org.apache.cassandra.cql3.functions.types;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A date with no time components, no time zone, in the ISO 8601 calendar.
 *
 * <p>Note that ISO 8601 has a number of differences with the default gregorian calendar used in
 * Java:
 *
 * <ul>
 * <li>it uses a proleptic gregorian calendar, meaning that it's gregorian indefinitely back in
 * the past (there is no gregorian change);
 * <li>there is a year 0.
 * </ul>
 *
 * <p>This class implements these differences, so that year/month/day fields match exactly the ones
 * in CQL string literals.
 *
 * @since 2.2
 */
public final class LocalDate
{

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final long millisSinceEpoch;
    private final int daysSinceEpoch;

    // This gets initialized lazily if we ever need it. Once set, it is effectively immutable.
    private volatile GregorianCalendar calendar;

    private LocalDate(int daysSinceEpoch)
    {
        this.daysSinceEpoch = daysSinceEpoch;
        this.millisSinceEpoch = TimeUnit.DAYS.toMillis(daysSinceEpoch);
    }

    /**
     * Builds a new instance from a number of days since January 1st, 1970 GMT.
     *
     * @param daysSinceEpoch the number of days.
     * @return the new instance.
     */
    public static LocalDate fromDaysSinceEpoch(int daysSinceEpoch)
    {
        return new LocalDate(daysSinceEpoch);
    }

    /**
     * Builds a new instance from a number of milliseconds since January 1st, 1970 GMT. Note that if
     * the given number does not correspond to a whole number of days, it will be rounded towards 0.
     *
     * @param millisSinceEpoch the number of milliseconds since January 1st, 1970 GMT.
     * @return the new instance.
     * @throws IllegalArgumentException if the date is not in the range [-5877641-06-23;
     *                                  5881580-07-11].
     */
    public static LocalDate fromMillisSinceEpoch(long millisSinceEpoch)
    throws IllegalArgumentException
    {
        long daysSinceEpoch = TimeUnit.MILLISECONDS.toDays(millisSinceEpoch);
        checkArgument(
        daysSinceEpoch >= Integer.MIN_VALUE && daysSinceEpoch <= Integer.MAX_VALUE,
        "Date should be in the range [-5877641-06-23; 5881580-07-11]");

        return new LocalDate((int) daysSinceEpoch);
    }

    /**
     * Returns the number of days since January 1st, 1970 GMT.
     *
     * @return the number of days.
     */
    public int getDaysSinceEpoch()
    {
        return daysSinceEpoch;
    }

    /**
     * Returns the year.
     *
     * @return the year.
     */
    public int getYear()
    {
        GregorianCalendar c = getCalendar();
        int year = c.get(Calendar.YEAR);
        if (c.get(Calendar.ERA) == GregorianCalendar.BC) year = -year + 1;
        return year;
    }

    /**
     * Returns the month.
     *
     * @return the month. It is 1-based, e.g. 1 for January.
     */
    public int getMonth()
    {
        return getCalendar().get(Calendar.MONTH) + 1;
    }

    /**
     * Returns the day in the month.
     *
     * @return the day in the month.
     */
    public int getDay()
    {
        return getCalendar().get(Calendar.DAY_OF_MONTH);
    }

    /**
     * Return a new {@link LocalDate} with the specified (signed) amount of time added to (or
     * subtracted from) the given {@link Calendar} field, based on the calendar's rules.
     *
     * <p>Note that adding any amount to a field smaller than {@link Calendar#DAY_OF_MONTH} will
     * remain without effect, as this class does not keep time components.
     *
     * <p>See {@link Calendar} javadocs for more information.
     *
     * @param field  a {@link Calendar} field to modify.
     * @param amount the amount of date or time to be added to the field.
     * @return a new {@link LocalDate} with the specified (signed) amount of time added to (or
     * subtracted from) the given {@link Calendar} field.
     * @throws IllegalArgumentException if the new date is not in the range [-5877641-06-23;
     *                                  5881580-07-11].
     */
    public LocalDate add(int field, int amount)
    {
        GregorianCalendar newCalendar = isoCalendar();
        newCalendar.setTimeInMillis(millisSinceEpoch);
        newCalendar.add(field, amount);
        LocalDate newDate = fromMillisSinceEpoch(newCalendar.getTimeInMillis());
        newDate.calendar = newCalendar;
        return newDate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;

        if (o instanceof LocalDate)
        {
            LocalDate that = (LocalDate) o;
            return this.daysSinceEpoch == that.daysSinceEpoch;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return daysSinceEpoch;
    }

    @Override
    public String toString()
    {
        return String.format("%d-%s-%s", getYear(), pad2(getMonth()), pad2(getDay()));
    }

    private static String pad2(int i)
    {
        String s = Integer.toString(i);
        return s.length() == 2 ? s : '0' + s;
    }

    private GregorianCalendar getCalendar()
    {
        // Two threads can race and both create a calendar. This is not a problem.
        if (calendar == null)
        {

            // Use a local variable to only expose after we're done mutating it.
            GregorianCalendar tmp = isoCalendar();
            tmp.setTimeInMillis(millisSinceEpoch);

            calendar = tmp;
        }
        return calendar;
    }

    // This matches what Cassandra uses server side (from Joda Time's LocalDate)
    private static GregorianCalendar isoCalendar()
    {
        GregorianCalendar calendar = new GregorianCalendar(UTC);
        calendar.setGregorianChange(new Date(Long.MIN_VALUE));
        return calendar;
    }
}
