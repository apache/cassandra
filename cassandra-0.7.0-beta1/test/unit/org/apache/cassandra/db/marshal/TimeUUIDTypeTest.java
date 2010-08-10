/*
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
package org.apache.cassandra.db.marshal;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.marshal.TimeUUIDType;

import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;

public class TimeUUIDTypeTest extends CleanupHelper
{
    TimeUUIDType timeUUIDType = new TimeUUIDType();
    UUIDGenerator generator = UUIDGenerator.getInstance();

    @Test
    public void testEquality()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = new UUID(a.asByteArray());

        assertEquals(0, timeUUIDType.compare(a.asByteArray(), b.asByteArray()));
    }

    @Test
    public void testSmaller()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = generator.generateTimeBasedUUID();
        UUID c = generator.generateTimeBasedUUID();

        assertEquals(-1, timeUUIDType.compare(a.asByteArray(), b.asByteArray()));
        assertEquals(-1, timeUUIDType.compare(b.asByteArray(), c.asByteArray()));
        assertEquals(-1, timeUUIDType.compare(a.asByteArray(), c.asByteArray()));
    }

    @Test
    public void testBigger()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = generator.generateTimeBasedUUID();
        UUID c = generator.generateTimeBasedUUID();

        assertEquals(1, timeUUIDType.compare(c.asByteArray(), b.asByteArray()));
        assertEquals(1, timeUUIDType.compare(b.asByteArray(), a.asByteArray()));
        assertEquals(1, timeUUIDType.compare(c.asByteArray(), a.asByteArray()));
    }

    @Test
    public void testTimestamp()
    {
        for (int i = 0; i < 100; i++)
        {
            UUID uuid = generator.generateTimeBasedUUID();
            assert TimeUUIDType.getTimestamp(uuid.asByteArray()) == LexicalUUIDType.getUUID(uuid.asByteArray()).timestamp();
        }
    }
}
