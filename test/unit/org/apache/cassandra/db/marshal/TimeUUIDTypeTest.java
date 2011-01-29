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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.utils.UUIDGen;

public class TimeUUIDTypeTest
{
    TimeUUIDType timeUUIDType = new TimeUUIDType();

    @Test
    public void testEquality() throws UnknownHostException
    {
        UUID a = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        UUID b = new UUID(a.getMostSignificantBits(), a.getLeastSignificantBits());
        
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(a)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(b)));
        assertEquals(0, timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(b))));
    }

    @Test
    public void testSmaller() throws UnknownHostException
    {
        UUID a = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        UUID b = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        UUID c = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());

        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(a)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(b)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(c)));
        
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(b))) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(b)), ByteBuffer.wrap(UUIDGen.decompose(c))) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(c))) < 0;
    }

    @Test
    public void testBigger() throws UnknownHostException
    {
        UUID a = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        UUID b = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        UUID c = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());

        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(a)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(b)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(c)));

        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(c)), ByteBuffer.wrap(UUIDGen.decompose(b))) > 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(b)), ByteBuffer.wrap(UUIDGen.decompose(a))) > 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(c)), ByteBuffer.wrap(UUIDGen.decompose(a))) > 0;
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
    
    @Test
    public void testValidTimeVersion()
    {
        UUID uuid1 = UUID.fromString("00000000-0000-1000-0000-000000000000");
        assert uuid1.version() == 1;
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(uuid1)));
    }
    
    @Test(expected = MarshalException.class)
    public void testInvalidTimeVersion()
    {
        UUID uuid2 = UUID.fromString("00000000-0000-2100-0000-000000000000");
        assert uuid2.version() == 2;
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(uuid2)));
    }
    
    
}
