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

package org.apache.cassandra.db.marshal.datetime;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound;

import static java.time.temporal.TemporalAdjusters.firstDayOfMonth;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;
import static java.time.temporal.TemporalAdjusters.lastDayOfMonth;
import static java.time.temporal.TemporalAdjusters.lastDayOfYear;

public class DateRangeUtil
{
    private static final int YEAR_LEVEL = 3;
    private static final int[] FIELD_BY_LEVEL =
    {
        -1/*unused*/, -1, -1, Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
        Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND
    };
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

    public static DateRange parseDateRange(String source) throws ParseException
    {
        if (StringUtils.isBlank(source))
        {
            throw new IllegalArgumentException("Date range is null or blank");
        }
        if (source.charAt(0) == '[')
        {
            if (source.charAt(source.length() - 1) != ']')
            {
                throw new IllegalArgumentException("If date range starts with [ must end with ]; got " + source);
            }
            int middle = source.indexOf(" TO ");
            if (middle < 0)
            {
                throw new IllegalArgumentException("If date range starts with [ must contain ' TO '; got " + source);
            }
            String lowerBoundString = source.substring(1, middle);
            String upperBoundString = source.substring(middle + " TO ".length(), source.length() - 1);
            return new DateRange(parseLowerBound(lowerBoundString), parseUpperBound(upperBoundString));
        }
        else
        {
            return new DateRange(parseLowerBound(source));
        }
    }

    public static ZonedDateTime roundUpperBoundTimestampToPrecision(ZonedDateTime timestamp, DateRangeBound.Precision precision)
    {
        switch (precision)
        {
            case YEAR:
                timestamp = timestamp.with(lastDayOfYear());
            case MONTH:
                timestamp = timestamp.with(lastDayOfMonth());
            case DAY:
                timestamp = timestamp.with(ChronoField.HOUR_OF_DAY, 23);
            case HOUR:
                timestamp = timestamp.with(ChronoField.MINUTE_OF_HOUR, 59);
            case MINUTE:
                timestamp = timestamp.with(ChronoField.SECOND_OF_MINUTE, 59);
            case SECOND:
                timestamp = timestamp.with(ChronoField.MILLI_OF_SECOND, 999);
            case MILLISECOND:
                // DateRangeField ignores any precision beyond milliseconds
                return timestamp;
            default:
                throw new IllegalStateException("Unsupported date time precision for the upper bound: " + precision);
        }
    }

    public static ZonedDateTime roundLowerBoundTimestampToPrecision(ZonedDateTime timestamp, DateRangeBound.Precision precision)
    {
        switch (precision)
        {
            case YEAR:
                timestamp = timestamp.with(firstDayOfYear());
            case MONTH:
                timestamp = timestamp.with(firstDayOfMonth());
            case DAY:
                timestamp = timestamp.with(ChronoField.HOUR_OF_DAY, 0);
            case HOUR:
                timestamp = timestamp.with(ChronoField.MINUTE_OF_HOUR, 0);
            case MINUTE:
                timestamp = timestamp.with(ChronoField.SECOND_OF_MINUTE, 0);
            case SECOND:
                timestamp = timestamp.with(ChronoField.MILLI_OF_SECOND, 0);
            case MILLISECOND:
                // DateRangeField ignores any precision beyond milliseconds
                return timestamp;
            default:
                throw new IllegalStateException("Unsupported date time precision for the upper bound: " + precision);
        }
    }

    private static DateRangeBound parseLowerBound(String source) throws ParseException
    {
        Calendar lowerBoundCalendar = parseCalendar(source);
        int calPrecisionField = getCalPrecisionField(lowerBoundCalendar);
        if (calPrecisionField < 0)
        {
            return DateRangeBound.UNBOUNDED;
        }
        return DateRangeBound.lowerBound(toZonedDateTime(lowerBoundCalendar), getCalendarPrecision(calPrecisionField));
    }

    private static DateRangeBound parseUpperBound(String source) throws ParseException
    {
        Calendar upperBoundCalendar = parseCalendar(source);
        int calPrecisionField = getCalPrecisionField(upperBoundCalendar);
        if (calPrecisionField < 0)
        {
            return DateRangeBound.UNBOUNDED;
        }
        ZonedDateTime upperBoundDateTime = toZonedDateTime(upperBoundCalendar);
        DateRangeBound.Precision precision = getCalendarPrecision(calPrecisionField);
        return DateRangeBound.upperBound(upperBoundDateTime, precision);
    }

    /**
     * This method was extracted from org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree
     * (Apache Lucene™) for compatibility with DSE.
     * The class is distributed under Apache-2.0 License attached to this release.
     *
     * Calendar utility method:
     * Gets the Calendar field code of the last field that is set prior to an unset field. It only
     * examines fields relevant to the prefix tree. If no fields are set, it returns -1. */
    private static int getCalPrecisionField(Calendar cal) {
        int lastField = -1;
        for (int level = YEAR_LEVEL; level < FIELD_BY_LEVEL.length; level++) {
            int field = FIELD_BY_LEVEL[level];
            if (!cal.isSet(field))
                break;
            lastField = field;
        }
        return lastField;
    }

    /**
     * This method was extracted from org.apache.lucene.spatial.prefix.tree.DateRangePrefixTree
     * (Apache Lucene™) for compatibility with DSE.
     * The class is distributed under Apache-2.0 License attached to this release.
     *
     * Calendar utility method:
     * It will only set the fields found, leaving
     * the remainder in an un-set state. A leading '-' or '+' is optional (positive assumed), and a
     * trailing 'Z' is also optional.
     * @param str not null and not empty
     * @return not null
     */
    private static Calendar parseCalendar(String str) throws ParseException {
        // example: +2014-10-23T21:22:33.159Z
        if (str == null || str.isEmpty())
            throw new IllegalArgumentException("str is null or blank");
        Calendar cal = Calendar.getInstance(UTC_TIME_ZONE, Locale.ROOT);
        cal.clear();
        if (str.equals("*"))
            return cal;
        int offset = 0;//a pointer
        try {
            //year & era:
            int lastOffset = str.charAt(str.length()-1) == 'Z' ? str.length() - 1 : str.length();
            int hyphenIdx = str.indexOf('-', 1);//look past possible leading hyphen
            if (hyphenIdx < 0)
                hyphenIdx = lastOffset;
            int year = Integer.parseInt(str.substring(offset, hyphenIdx));
            cal.set(Calendar.ERA, year <= 0 ? 0 : 1);
            cal.set(Calendar.YEAR, year <= 0 ? -1*year + 1 : year);
            offset = hyphenIdx + 1;
            if (lastOffset < offset)
                return cal;

            //NOTE: We aren't validating separator chars, and we unintentionally accept leading +/-.
            // The str.substring()'s hopefully get optimized to be stack-allocated.

            //month:
            cal.set(Calendar.MONTH, Integer.parseInt(str.substring(offset, offset+2)) - 1);//starts at 0
            offset += 3;
            if (lastOffset < offset)
                return cal;
            //day:
            cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(str.substring(offset, offset+2)));
            offset += 3;
            if (lastOffset < offset)
                return cal;
            //hour:
            cal.set(Calendar.HOUR_OF_DAY, Integer.parseInt(str.substring(offset, offset+2)));
            offset += 3;
            if (lastOffset < offset)
                return cal;
            //minute:
            cal.set(Calendar.MINUTE, Integer.parseInt(str.substring(offset, offset+2)));
            offset += 3;
            if (lastOffset < offset)
                return cal;
            //second:
            cal.set(Calendar.SECOND, Integer.parseInt(str.substring(offset, offset+2)));
            offset += 3;
            if (lastOffset < offset)
                return cal;
            //ms:
            cal.set(Calendar.MILLISECOND, Integer.parseInt(str.substring(offset, offset+3)));
            offset += 3;//last one, move to next char
            if (lastOffset == offset)
                return cal;
        } catch (Exception e) {
            ParseException pe = new ParseException("Improperly formatted date: "+str, offset);
            pe.initCause(e);
            throw pe;
        }
        throw new ParseException("Improperly formatted date: "+str, offset);
    }

    private static DateRangeBound.Precision getCalendarPrecision(int calendarPrecision)
    {
        switch (calendarPrecision)
        {
            case Calendar.YEAR:
                return DateRangeBound.Precision.YEAR;
            case Calendar.MONTH:
                return DateRangeBound.Precision.MONTH;
            case Calendar.DAY_OF_MONTH:
                return DateRangeBound.Precision.DAY;
            case Calendar.HOUR_OF_DAY:
                return DateRangeBound.Precision.HOUR;
            case Calendar.MINUTE:
                return DateRangeBound.Precision.MINUTE;
            case Calendar.SECOND:
                return DateRangeBound.Precision.SECOND;
            case Calendar.MILLISECOND:
                return DateRangeBound.Precision.MILLISECOND;
            default:
                throw new IllegalStateException("Unsupported date time precision: " + calendarPrecision);
        }
    }

    private static ZonedDateTime toZonedDateTime(Calendar calendar)
    {
        int year = calendar.get(Calendar.YEAR);
        if (calendar.get(Calendar.ERA) == 0)
        {
            // BC era; 1 BC == 0 AD, 0 BD == -1 AD, etc
            year -= 1;
            if (year > 0)
            {
                year = -year;
            }
        }
        LocalDateTime localDateTime = LocalDateTime.of(year,
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND));
        localDateTime = localDateTime.with(ChronoField.MILLI_OF_SECOND, calendar.get(Calendar.MILLISECOND));
        return ZonedDateTime.of(localDateTime, ZoneOffset.UTC);
    }
}
