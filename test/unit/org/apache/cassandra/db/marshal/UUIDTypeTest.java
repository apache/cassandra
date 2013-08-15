package org.apache.cassandra.db.marshal;

/*
 *
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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

public class UUIDTypeTest
{

    private static final Logger logger = LoggerFactory.getLogger(UUIDTypeTest.class);

    UUIDType uuidType = new UUIDType();

    @Test
    public void testCompare()
    {

        UUID t1 = UUIDGen.getTimeUUID();
        UUID t2 = UUIDGen.getTimeUUID();

        testCompare(null, t2, -1);
        testCompare(t1, null, 1);

        testCompare(t1, t2, -1);
        testCompare(t1, t1, 0);
        testCompare(t2, t2, 0);

        UUID nullId = new UUID(0, 0);

        testCompare(nullId, t1, -1);
        testCompare(t2, nullId, 1);
        testCompare(nullId, nullId, 0);

        for (int test = 1; test < 32; test++)
        {
            UUID r1 = UUID.randomUUID();
            UUID r2 = UUID.randomUUID();

            testCompare(r1, r2, compareUUID(r1, r2));
            testCompare(r1, r1, 0);
            testCompare(r2, r2, 0);

            testCompare(t1, r1, -1);
            testCompare(r2, t2, 1);
        }
    }

    public static int compareUnsigned(long n1, long n2)
    {
        if (n1 == n2)
        {
            return 0;
        }
        if ((n1 < n2) ^ ((n1 < 0) != (n2 < 0)))
        {
            return -1;
        }
        return 1;
    }

    public static int compareUUID(UUID u1, UUID u2)
    {
        int c = compareUnsigned(u1.getMostSignificantBits(),
                u2.getMostSignificantBits());
        if (c != 0)
        {
            return c;
        }
        return compareUnsigned(u1.getLeastSignificantBits(),
                u2.getLeastSignificantBits());
    }

    public String describeCompare(UUID u1, UUID u2, int c)
    {
        String tb1 = (u1 == null) ? "null" : (u1.version() == 1) ? "time-based " : "random ";
        String tb2 = (u2 == null) ? "null" : (u2.version() == 1) ? "time-based " : "random ";
        String comp = (c < 0) ? " < " : ((c == 0) ? " = " : " > ");
        return tb1 + u1 + comp + tb2 + u2;
    }

    public int sign(int i)
    {
        if (i < 0)
        {
            return -1;
        }
        if (i > 0)
        {
            return 1;
        }
        return 0;
    }

    public static ByteBuffer bytebuffer(UUID uuid)
    {
        if (uuid == null)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        byte[] bytes = new byte[16];

        for (int i = 0; i < 8; i++)
        {
            bytes[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++)
        {
            bytes[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return ByteBuffer.wrap(bytes);
    }

    public void logJdkUUIDCompareToVariance(UUID u1, UUID u2, int expC)
    {
        if ((u1 == null) || (u2 == null))
            return;
        if (u1.version() != u2.version())
            return;
        if (u1.version() == 1)
            return;
        if (u1.compareTo(u2) != expC)
            logger.info("*** Note: java.util.UUID.compareTo() would have compared this differently");
    }

    public void testCompare(UUID u1, UUID u2, int expC)
    {
        int c = sign(uuidType.compare(bytebuffer(u1), bytebuffer(u2)));
        expC = sign(expC);
        assertEquals("Expected " + describeCompare(u1, u2, expC) + ", got " + describeCompare(u1, u2, c), expC, c);

        if (((u1 != null) && (u1.version() == 1)) && ((u2 != null) && (u2.version() == 1)))
            assertEquals(c, sign(TimeUUIDType.instance.compare(bytebuffer(u1), bytebuffer(u2))));

        logJdkUUIDCompareToVariance(u1, u2, c);
    }

    @Test
    public void testTimeEquality()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = new UUID(a.getMostSignificantBits(),
                a.getLeastSignificantBits());

        assertEquals(0, uuidType.compare(bytebuffer(a), bytebuffer(b)));
    }

    @Test
    public void testTimeSmaller()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = UUIDGen.getTimeUUID();
        UUID c = UUIDGen.getTimeUUID();

        assert uuidType.compare(bytebuffer(a), bytebuffer(b)) < 0;
        assert uuidType.compare(bytebuffer(b), bytebuffer(c)) < 0;
        assert uuidType.compare(bytebuffer(a), bytebuffer(c)) < 0;
    }

    @Test
    public void testTimeBigger()
    {
        UUID a = UUIDGen.getTimeUUID();
        UUID b = UUIDGen.getTimeUUID();
        UUID c = UUIDGen.getTimeUUID();

        assert uuidType.compare(bytebuffer(c), bytebuffer(b)) > 0;
        assert uuidType.compare(bytebuffer(b), bytebuffer(a)) > 0;
        assert uuidType.compare(bytebuffer(c), bytebuffer(a)) > 0;
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
        Arrays.sort(uuids, uuidType);
        for (int i = 1; i < uuids.length; i++)
        {
            long i0 = UUIDGen.getUUID(uuids[i - 1]).timestamp();
            long i1 = UUIDGen.getUUID(uuids[i]).timestamp();
            assert i0 <= i1;
        }
    }
}
