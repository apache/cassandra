/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.marshal.datetime;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.db.marshal.datetime.DateRange.DateRangeBound.Precision;

import static org.junit.Assert.assertEquals;

public class DateRangeCodecTest
{
    private final DateRangeCodec codec = DateRangeCodec.instance;

    @Test
    public void testSerializeRoundTrip()
    {
        DateRange expected = DateRange.DateRangeBuilder.dateRange()
                .withLowerBound("2015-12-03T10:15:30.00Z", Precision.SECOND)
                .withUpperBound("2016-01-01T00:00:01.00Z", Precision.MILLISECOND)
                .build();

        ByteBuffer serialized = codec.serialize(expected, ProtocolVersion.NEWEST_SUPPORTED);

        // For UDT or tuple type buffer contains whole cell payload, and codec can't rely on absolute byte addressing
        ByteBuffer payload = ByteBuffer.allocate(5 + serialized.capacity());
        // put serialized date range in between other data
        payload.putInt(44).put(serialized).put((byte) 1);
        payload.position(4);

        DateRange actual = codec.deserialize(payload, ProtocolVersion.NEWEST_SUPPORTED);

        assertEquals(expected, actual);
        //provided ByteBuffer should never be consumed by read operations that modify its current position
        assertEquals(4, payload.position());
    }
}
