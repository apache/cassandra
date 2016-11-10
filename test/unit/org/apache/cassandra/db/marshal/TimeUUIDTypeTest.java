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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import junit.framework.Assert;
import org.apache.cassandra.serializers.MarshalException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.utils.UUIDGen;

public class TimeUUIDTypeTest
{
    TimeUUIDType timeUUIDType = new TimeUUIDType();

    @Test
    public void testEquality()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = new UUID(a.getMostSignificantBits(), a.getLeastSignificantBits());

        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(a)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(b)));
        assertEquals(0, timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(b))));
    }

    @Test
    public void testSmaller()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = UUIDGen.getTimeUUID();
        UUID c = UUIDGen.getTimeUUID();

        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(a)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(b)));
        timeUUIDType.validate(ByteBuffer.wrap(UUIDGen.decompose(c)));

        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(b))) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(b)), ByteBuffer.wrap(UUIDGen.decompose(c))) < 0;
        assert timeUUIDType.compare(ByteBuffer.wrap(UUIDGen.decompose(a)), ByteBuffer.wrap(UUIDGen.decompose(c))) < 0;
    }

    @Test
    public void testBigger()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = UUIDGen.getTimeUUID();
        UUID c = UUIDGen.getTimeUUID();

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
        compareAll(UUIDTypeTest.random(1000, (byte) 0x10));
        for (ByteBuffer[] permutations : UUIDTypeTest.permutations(100, (byte) 0x10))
            compareAll(permutations);
    }

    private void compareAll(ByteBuffer[] uuids)
    {
        for (int i = 0 ; i < uuids.length ; i++)
        {
            for (int j = i + 1 ; j < uuids.length ; j++)
            {
                ByteBuffer bi = uuids[i];
                ByteBuffer bj = uuids[j];
                long i0 = UUIDGen.getUUID(bi).timestamp();
                long i1 = UUIDGen.getUUID(bj).timestamp();
                int c = timeUUIDType.compare(bi, bj);
                if (i0 == i1) Assert.assertTrue(isComparisonEquivalent(bi.compareTo(bj), c));
                else Assert.assertTrue(isComparisonEquivalent(Long.compare(i0, i1), c));
                Assert.assertTrue(isComparisonEquivalent(compareV1(bi, bj), c));
            }
        }
    }

    private static int compareV1(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        int res = compareTimestampBytes(o1, o2);
        if (res != 0)
            return res;
        return o1.compareTo(o2);
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
    {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
        if (d != 0) return d;

        d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
        if (d != 0) return d;

        return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
    }

    private static boolean isComparisonEquivalent(int c1, int c2)
    {
        c1 = c1 < -1 ? -1 : c1 > 1 ? 1 : c1;
        c2 = c2 < -1 ? -1 : c2 > 1 ? 1 : c2;
        return c1 == c2;
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
