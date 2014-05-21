/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.serializers;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TimestampSerializer;

public class TimestampSerializerTest
{
    private String dates[] = new String[]
    {
        "2014-04-01",
        "2014-04-01+0000",
        "2014-04-01 20:30",
        "2014-04-01 20:30:35",
        "2014-04-01 20:30:35Z",
        "2014-04-01 20:30+07",
        "2014-04-01 20:30+0700",
        "2014-04-01 20:30+07:00",
        "2014-04-01 20:30:35+07",
        "2014-04-01 20:30:35+0700",
        "2014-04-01 20:30:35+07:00",
        "2014-04-01 20:30:35.898",
        "2014-04-01 20:30:35.898Z",
        "2014-04-01 20:30:35.898+07",
        "2014-04-01 20:30:35.898+0700",
        "2014-04-01 20:30:35.898+07:00",
        "2014-04-01T20:30",
        "2014-04-01T20:30:25",
        "2014-04-01T20:30:35Z",
        "2014-04-01T20:30:35+00:00",
        "2014-04-01T20:30:35+0700",
        "2014-04-01T20:30:35+07:00",
        "2014-04-01T20:30:35.898",
        "2014-04-01T20:30:35.898+00:00"
    };

    @Test
    public void testDateStringToTimestamp()
    {
        List<String> unparsedDates = new ArrayList<>();
        for (String date: dates)
        {
            try
            {
                long millis = TimestampSerializer.dateStringToTimestamp(date);
            }
            catch (MarshalException e)
            {
                unparsedDates.add(date);
            }
        }
        assertTrue("Unable to parse: " + unparsedDates, unparsedDates.isEmpty());
    }
}
