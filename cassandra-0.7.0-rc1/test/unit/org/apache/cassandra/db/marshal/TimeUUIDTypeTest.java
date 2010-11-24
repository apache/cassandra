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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.utils.UUIDGen;
import org.safehaus.uuid.UUID;
import org.safehaus.uuid.UUIDGenerator;

public class TimeUUIDTypeTest
{
    TimeUUIDType timeUUIDType = new TimeUUIDType();
    UUIDGenerator generator = UUIDGenerator.getInstance();

    @Test
    public void testEquality()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = new UUID(a.asByteArray());

        assertEquals(0, timeUUIDType.compare(ByteBuffer.wrap(a.asByteArray()), ByteBuffer.wrap(b.asByteArray())));
    }

    @Test
    public void testSmaller()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = generator.generateTimeBasedUUID();
        UUID c = generator.generateTimeBasedUUID();

        assert timeUUIDType.compare(ByteBuffer.wrap(a.asByteArray()), ByteBuffer.wrap(b.asByteArray())) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(b.asByteArray()), ByteBuffer.wrap(c.asByteArray())) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(a.asByteArray()), ByteBuffer.wrap(c.asByteArray())) < 0;
    }

    @Test
    public void testBigger()
    {
        UUID a = generator.generateTimeBasedUUID();
        UUID b = generator.generateTimeBasedUUID();
        UUID c = generator.generateTimeBasedUUID();

        assert timeUUIDType.compare(ByteBuffer.wrap(c.asByteArray()), ByteBuffer.wrap(b.asByteArray())) > 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(b.asByteArray()), ByteBuffer.wrap(a.asByteArray())) > 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(c.asByteArray()), ByteBuffer.wrap(a.asByteArray())) > 0;
    }

    @Test
    public void testTimestampComparison()
    {
        Random rng = new Random();
        ByteBuffer[] uuids = new ByteBuffer[100];
        for (int i = 0; i < uuids.length; i++)
        {
            uuids[i] = ByteBuffer.allocate(16);
            rng.nextBytes(uuids[i].array());
            // set version to 1
            uuids[i].array()[6] &= 0x0F;
            uuids[i].array()[6] |= 0x10;
        }
        Arrays.sort(uuids, timeUUIDType);
        for (int i = 1; i < uuids.length; i++)
        {
            long i0 = UUIDGen.getUUID(uuids[i - 1]).timestamp();
            long i1 = UUIDGen.getUUID(uuids[i]).timestamp();
            assert i0 <= i1;
        }
    }
}
