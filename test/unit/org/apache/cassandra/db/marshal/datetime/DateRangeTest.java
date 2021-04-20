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

package org.apache.cassandra.db.marshal.datetime;

import org.junit.Test;

import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DateRangeTest
{
    @Test
    public void testDateRangeEquality()
    {
        DateRange first = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", DateRangeBound.Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();
        DateRange second = DateRange.DateRangeBuilder.dateRange()
                // millis are off, but precision is higher so we skip them
                .withLowerBound("2015-12-03T10:15:30.01Z", DateRangeBound.Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();
        DateRange third = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", DateRangeBound.Precision.MILLISECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", DateRangeBound.Precision.MILLISECOND)
                .build();

        assertEquals(first, second);
        assertEquals(first, first);
        assertEquals(first.hashCode(), second.hashCode());
        assertEquals(first.hashCode(), first.hashCode());
        assertEquals(first.formatToSolrString(), second.formatToSolrString());
        assertNotEquals(first, third);
        assertNotEquals(first.hashCode(), third.hashCode());
    }
}
