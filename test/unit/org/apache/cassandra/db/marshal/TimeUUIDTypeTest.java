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

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

public class TimeUUIDTypeTest
{
    TimeUUIDType timeUUIDType = new TimeUUIDType();

    @Test
    public void testTimestampComparison()
    {
        Random rng = new Random();
        byte[][] uuids = new byte[100][];
        for (int i = 0; i < uuids.length; i++)
        {
            uuids[i] = new byte[16];
            rng.nextBytes(uuids[i]);
            // set version to 1
            uuids[i][6] &= 0x0F;
            uuids[i][6] |= 0x10;
        }
        Arrays.sort(uuids, timeUUIDType);
        for (int i = 1; i < uuids.length; i++)
        {
            long i0 = LexicalUUIDType.getUUID(uuids[i - 1]).timestamp();
            long i1 = LexicalUUIDType.getUUID(uuids[i]).timestamp();
            assert i0 <= i1;
        }
    }
    
    @Test
    public void testValidTimeVersion()
    {
        java.util.UUID uuid1 = java.util.UUID.fromString("00000000-0000-1000-0000-000000000000");
        assert uuid1.version() == 1;
        timeUUIDType.validate(decompose(uuid1));
    }
    
    @Test(expected = MarshalException.class)
    public void testInvalidTimeVersion()
    {
        java.util.UUID uuid2 = java.util.UUID.fromString("00000000-0000-2100-0000-000000000000");
        assert uuid2.version() == 2;
        timeUUIDType.validate(decompose(uuid2));
    }
    
    /** decomposes a uuid into raw bytes. */
    private static byte[] decompose(UUID uuid)
    {
        long most = uuid.getMostSignificantBits();
        long least = uuid.getLeastSignificantBits();
        byte[] b = new byte[16];
        for (int i = 0; i < 8; i++)
        {
            b[i] = (byte)(most >>> ((7-i) * 8));
            b[8+i] = (byte)(least >>> ((7-i) * 8));
        }
        return b;
    }
}
