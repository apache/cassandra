/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBuilder.dateRange;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DateRangeTypeTest
{
    private final DateRangeType dateRangeType = DateRangeType.instance;

    @Parameterized.Parameter(0)
    public DateRange dataRange;

    @Parameterized.Parameter(1)
    public String source;

    @Test
    public void shouldFormatDateRangeAsJson()
    {
        ByteBuffer bytes = dateRangeType.decompose(dataRange);
        String actualJson = dateRangeType.toJSONString(bytes, ProtocolVersion.CURRENT);
        assertEquals('"' + source + '"', actualJson);
    }

    @Test
    public void shouldCreateProperDateRangeFromString()
    {
        ByteBuffer dateRangeBytes = dateRangeType.fromString(source);
        DateRange actual = dateRangeType.getSerializer().deserialize(dateRangeBytes);
        assertEquals(dataRange, actual);
    }

    @SuppressWarnings("unused")
    @Parameterized.Parameters(name = "dataRange = {0}, source = {1}")
    public static Collection<Object[]> testData()
    {
        return Arrays.asList(
                new Object[]{
                        dateRange()
                                .withLowerBound("1950-01-01T00:00:00.000Z", Precision.YEAR)
                                .withUnboundedUpperBound()
                                .build(),
                        "[1950 TO *]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1998-01-01T00:00:00.000Z", Precision.MILLISECOND)
                                .withUpperBound("1999-02-01T00:00:00.000Z", Precision.DAY)
                                .build(),
                        "[1998-01-01T00:00:00.000Z TO 1999-02-01]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1930-12-03T01:01:01.003Z", Precision.DAY)
                                .withUpperBound("1951-01-02T00:00:00.003Z", Precision.MILLISECOND)
                                .build(),
                        "[1930-12-03 TO 1951-01-02T00:00:00.003Z]"
                },
                new Object[]{
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUpperBound("2014-01-02T00:00:00.003Z", Precision.YEAR)
                                .build(),
                        "[* TO 2014]"
                },
                new Object[]{
                        dateRange()
                                .withUnboundedLowerBound()
                                .withUnboundedUpperBound()
                                .build(),
                        "[* TO *]"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1966-03-03T03:30:30.030Z", Precision.YEAR)
                                .build(),
                        "1966"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("1700-01-01T00:00:00.000Z", Precision.MILLISECOND)
                                .build(),
                        "1700-01-01T00:00:00.000Z"
                },
                new Object[]{
                        dateRange()
                                .withLowerBound("-0009-01-01T00:00:00.000Z", Precision.YEAR)
                                .build(),
                        "-0009"
                }
        );
    }
}
