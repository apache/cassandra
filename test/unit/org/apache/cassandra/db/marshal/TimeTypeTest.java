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
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.apache.cassandra.serializers.TimeSerializer;

public class TimeTypeTest
{
    @Test public void TestComparison()
    {
        Long t1 = TimeSerializer.timeStringToLong("01:00:00.123456789");
        Long t2 = new Long((1L * 60L * 60L * 1000L * 1000L * 1000L) + 123456789);
        ByteBuffer b1 = TimeSerializer.instance.serialize(t1);
        ByteBuffer b2 = TimeSerializer.instance.serialize(t2);
        assert TimeType.instance.compare(b1, b2) == 0 : "Failed == comparison";

        b2 = TimeSerializer.instance.serialize(123456789L);
        assert TimeType.instance.compare(b1, b2) > 0 : "Failed > comparison";

        t2 = new Long(2L * 60L * 60L * 1000L * 1000L * 1000L + 123456789);
        b2 = TimeSerializer.instance.serialize(t2);
        assert TimeType.instance.compare(b1, b2) < 0 : "Failed < comparison";

        b1 = TimeSerializer.instance.serialize(0L);
        b2 = TimeSerializer.instance.serialize(0L);
        assert TimeType.instance.compare(b1, b2) == 0 : "Failed == comparison on 0";

        b1 = TimeSerializer.instance.serialize(0L);
        b2 = TimeSerializer.instance.serialize(10000000L);
        assert TimeType.instance.compare(b1, b2) == -1 : "Failed < comparison on 0";

        b1 = TimeSerializer.instance.serialize(0L);
        b2 = TimeSerializer.instance.serialize(TimeUnit.DAYS.toNanos(1));
        assert TimeType.instance.compare(b1, b2) == -1 : "Failed < comparison against max range.";

        b1 = TimeSerializer.instance.serialize(TimeUnit.DAYS.toNanos(1));
        b2 = TimeSerializer.instance.serialize(0L);
        assert TimeType.instance.compare(b1, b2) == 1 : "Failed > comparison against max range.";
    }
}
