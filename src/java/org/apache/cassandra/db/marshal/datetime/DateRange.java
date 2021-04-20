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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Locale;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.EqualsBuilder;

import org.apache.cassandra.db.marshal.DateRangeType;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

/**
 * Domain object of type {@link DateRangeType}. Lower and upper bounds are inclusive. Value type.
 */
public class DateRange
{
    private final DateRangeBound lowerBound;
    private final DateRangeBound upperBound;

    public DateRange(DateRangeBound lowerBound)
    {
        Preconditions.checkArgument(lowerBound != null);
        this.lowerBound = lowerBound;
        this.upperBound = null;
    }

    public DateRange(DateRangeBound lowerBound, DateRangeBound upperBound)
    {
        Preconditions.checkArgument(lowerBound != null);
        Preconditions.checkArgument(upperBound != null);
        Preconditions.checkArgument(upperBound.isAfter(lowerBound), "Wrong order: " + lowerBound + " TO " + upperBound);
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    private DateRange(DateRangeBuilder builder)
    {
        this.lowerBound = builder.lowerBound;
        this.upperBound = builder.upperBound;
    }

    public DateRangeBound getLowerBound()
    {
        return lowerBound;
    }

    public DateRangeBound getUpperBound()
    {
        return upperBound;
    }

    public boolean isUpperBoundDefined()
    {
        return upperBound != null;
    }

    public String formatToSolrString()
    {
        if (isUpperBoundDefined())
        {
            return String.format("[%s TO %s]", lowerBound, upperBound);
        }
        else
        {
            return lowerBound.toString();
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("lowerBound", lowerBound)
                .add("precision", lowerBound.getPrecision())
                .add("upperBound", upperBound)
                .add("precision", upperBound != null ? upperBound.getPrecision() : "null")
                .toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }

        DateRange rhs = (DateRange) obj;
        return new EqualsBuilder()
                .append(lowerBound, rhs.lowerBound)
                .append(upperBound, rhs.upperBound)
                .isEquals();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(lowerBound, upperBound);
    }

    public static class DateRangeBound
    {
        public static final DateRangeBound UNBOUNDED = new DateRangeBound();

        private final ZonedDateTime timestamp;
        private final Precision precision;

        private DateRangeBound(ZonedDateTime timestamp, Precision precision)
        {
            Preconditions.checkArgument(timestamp != null);
            Preconditions.checkArgument(precision != null);
            this.timestamp = timestamp;
            this.precision = precision;
        }

        private DateRangeBound()
        {
            this.timestamp = null;
            this.precision = null;
        }

        public static DateRangeBound lowerBound(Instant timestamp, Precision precision)
        {
            return lowerBound(ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC), precision);
        }

        public static DateRangeBound lowerBound(ZonedDateTime timestamp, Precision precision)
        {
            ZonedDateTime roundedLowerBound = DateRangeUtil.roundLowerBoundTimestampToPrecision(timestamp, precision);
            return new DateRangeBound(roundedLowerBound, precision);
        }

        public static DateRangeBound upperBound(Instant timestamp, Precision precision)
        {
            return upperBound(ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC), precision);
        }

        public static DateRangeBound upperBound(ZonedDateTime timestamp, Precision precision)
        {
            ZonedDateTime roundedUpperBound = DateRangeUtil.roundUpperBoundTimestampToPrecision(timestamp, precision);
            return new DateRangeBound(roundedUpperBound, precision);
        }

        public boolean isUnbounded()
        {
            return timestamp == null;
        }

        public boolean isAfter(DateRangeBound other)
        {
            return isUnbounded() || other.isUnbounded() || timestamp.isAfter(other.timestamp);
        }

        public Instant getTimestamp()
        {
            return timestamp.toInstant();
        }

        public Precision getPrecision()
        {
            return precision;
        }

        @Override
        public String toString()
        {
            if (isUnbounded())
            {
                return "*";
            }

            return precision.formatter.format(timestamp);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || obj.getClass() != getClass())
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }

            DateRangeBound rhs = (DateRangeBound) obj;
            return new EqualsBuilder()
                    .append(isUnbounded(), rhs.isUnbounded())
                    .append(timestamp, rhs.timestamp)
                    .append(precision, rhs.precision)
                    .isEquals();
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(timestamp, precision);
        }

        public enum Precision
        {
            YEAR(0x00,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu")
                            .parseDefaulting(MONTH_OF_YEAR, 1)
                            .parseDefaulting(DAY_OF_MONTH, 1)
                            .parseDefaulting(HOUR_OF_DAY, 0)
                            .parseDefaulting(MINUTE_OF_HOUR, 0)
                            .parseDefaulting(SECOND_OF_MINUTE, 0)
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            MONTH(0x01,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM")
                            .parseDefaulting(DAY_OF_MONTH, 1)
                            .parseDefaulting(HOUR_OF_DAY, 0)
                            .parseDefaulting(MINUTE_OF_HOUR, 0)
                            .parseDefaulting(SECOND_OF_MINUTE, 0)
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            DAY(0x02,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM-dd")
                            .parseDefaulting(HOUR_OF_DAY, 0)
                            .parseDefaulting(MINUTE_OF_HOUR, 0)
                            .parseDefaulting(SECOND_OF_MINUTE, 0)
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            HOUR(0x03,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM-dd'T'HH")
                            .parseDefaulting(MINUTE_OF_HOUR, 0)
                            .parseDefaulting(SECOND_OF_MINUTE, 0)
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            MINUTE(0x04,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM-dd'T'HH:mm")
                            .parseDefaulting(SECOND_OF_MINUTE, 0)
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            SECOND(0x05,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
                            .parseDefaulting(MILLI_OF_SECOND, 0)
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT)),

            MILLISECOND(0x06,
                    new DateTimeFormatterBuilder()
                            .parseCaseSensitive()
                            .parseStrict()
                            .appendPattern("uuuu-MM-dd'T'HH:mm:ss.SSS")
                            .optionalStart()
                            .appendZoneId()
                            .optionalEnd()
                            .toFormatter()
                            .withZone(ZoneOffset.UTC)
                            .withLocale(Locale.ROOT));

            private final int encoded;
            private final DateTimeFormatter formatter;

            Precision(int encoded, DateTimeFormatter formatter)
            {
                this.encoded = encoded;
                this.formatter = formatter;
            }

            public int toEncoded()
            {
                return encoded;
            }

            public static Precision fromEncoded(byte encoded)
            {
                for (Precision precision : values())
                {
                    if (precision.encoded == encoded)
                    {
                        return precision;
                    }
                }
                throw new IllegalArgumentException("Invalid precision encoding: " + encoded);
            }
        }
    }

    public static class DateRangeBuilder
    {
        private DateRangeBound lowerBound = null;
        private DateRangeBound upperBound = null;

        private DateRangeBuilder() {}

        public static DateRangeBuilder dateRange()
        {
            return new DateRangeBuilder();
        }

        public DateRangeBuilder withLowerBound(String lowerBound, DateRangeBound.Precision precision)
        {
            return withLowerBound(Instant.parse(lowerBound), precision);
        }

        public DateRangeBuilder withUnboundedLowerBound()
        {
            this.lowerBound = DateRangeBound.UNBOUNDED;
            return this;
        }

        public DateRangeBuilder withUnboundedUpperBound()
        {
            this.upperBound = DateRangeBound.UNBOUNDED;
            return this;
        }

        public DateRangeBuilder withUpperBound(String upperBound, DateRangeBound.Precision precision)
        {
            return withUpperBound(Instant.parse(upperBound), precision);
        }

        DateRangeBuilder withLowerBound(Instant lowerBound, DateRangeBound.Precision precision)
        {
            this.lowerBound = DateRangeBound.lowerBound(lowerBound, precision);
            return this;
        }

        DateRangeBuilder withUpperBound(Instant upperBound, DateRangeBound.Precision precision)
        {
            this.upperBound = DateRangeBound.upperBound(upperBound, precision);
            return this;
        }

        public DateRange build()
        {
            return new DateRange(this);
        }
    }
}
