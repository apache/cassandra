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
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UUIDTypeTest
{

    private static final Logger logger = LoggerFactory.getLogger(UUIDTypeTest.class);

    UUIDType uuidType = new UUIDType();

    @Test
    public void testRandomCompare()
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
    public void testPermutations()
    {
        compareAll(random(1000, (byte) 0x00, (byte) 0x10, (byte) 0x20));
        for (ByteBuffer[] permutations : permutations(10,  (byte) 0x00, (byte) 0x10, (byte) 0x20))
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
                UUID ui = UUIDGen.getUUID(bi);
                UUID uj = UUIDGen.getUUID(bj);
                int c = uuidType.compare(bi, bj);
                if (ui.version() != uj.version())
                {
                    Assert.assertTrue(isComparisonEquivalent(ui.version() - uj.version(), c));
                }
                else if (ui.version() == 1)
                {
                    long i0 = ui.timestamp();
                    long i1 = uj.timestamp();
                    if (i0 == i1) Assert.assertTrue(isComparisonEquivalent(ByteBufferUtil.compareUnsigned(bi, bj), c));
                    else Assert.assertTrue(isComparisonEquivalent(Long.compare(i0, i1), c));
                }
                else
                {
                    Assert.assertTrue(isComparisonEquivalent(ByteBufferUtil.compareUnsigned(bi, bj), c));
                }
                Assert.assertTrue(isComparisonEquivalent(compareV1(bi, bj), c));
            }
        }
    }

    private static boolean isComparisonEquivalent(int c1, int c2)
    {
        c1 = c1 < -1 ? -1 : c1 > 1 ? 1 : c1;
        c2 = c2 < -1 ? -1 : c2 > 1 ? 1 : c2;
        return c1 == c2;
    }

    // produce randomCount random byte strings, and permute every possible byte within each
    // for all provided types, using permute()
    static Iterable<ByteBuffer[]> permutations(final int randomCount, final byte ... types)
    {
        final Random random = new Random();
        long seed = random.nextLong();
        random.setSeed(seed);
        System.out.println("UUIDTypeTest.permutations.seed=" + seed);
        return new Iterable<ByteBuffer[]>()
        {
            public Iterator<ByteBuffer[]> iterator()
            {
                return new Iterator<ByteBuffer[]>()
                {
                    byte[] bytes = new byte[16];
                    int c = -1, i = 16;
                    public boolean hasNext()
                    {
                        return i < 16 || c < randomCount - 1;
                    }

                    public ByteBuffer[] next()
                    {
                        if (i == 16)
                        {
                            random.nextBytes(bytes);
                            i = 0;
                            c++;
                        }
                        return permute(bytes, i++, types);
                    }
                    public void remove()
                    {
                    }
                };
            }
        };
    }

    // for each of the given UUID types provided, produce every possible
    // permutation of the provided byte[] for the given index
    static ByteBuffer[] permute(byte[] src, int byteIndex, byte ... types)
    {
        assert src.length == 16;
        assert byteIndex < 16;
        byte[] bytes = src.clone();
        ByteBuffer[] permute;
        if (byteIndex == 6)
        {
            permute = new ByteBuffer[16 * types.length];
            for (int i = 0 ; i < types.length ; i++)
            {
                for (int j = 0 ; j < 16 ; j++)
                {
                    int k = i * 16 + j;
                    bytes[6] = (byte)(types[i] | j);
                    permute[k] = ByteBuffer.wrap(bytes.clone());
                }
            }
        }
        else
        {
            permute = new ByteBuffer[256 * types.length];
            for (int i = 0 ; i < types.length ; i++)
            {
                bytes[6] = types[i];
                for (int j = 0 ; j < 256 ; j++)
                {
                    int k = i * 256 + j;
                    bytes[byteIndex] = (byte) ((bytes[byteIndex] & 0x0F) | i);
                    permute[k] = ByteBuffer.wrap(bytes.clone());
                }
            }
        }
        return permute;
    }

    static ByteBuffer[] random(int count, byte ... types)
    {
        Random random = new Random();
        long seed = random.nextLong();
        random.setSeed(seed);
        System.out.println("UUIDTypeTest.random.seed=" + seed);
        ByteBuffer[] uuids = new ByteBuffer[count * types.length];
        for (int i = 0 ; i < types.length ; i++)
        {
            for (int j = 0; j < count; j++)
            {
                int k = (i * count) + j;
                uuids[k] = ByteBuffer.allocate(16);
                random.nextBytes(uuids[k].array());
                // set version to 1
                uuids[k].array()[6] &= 0x0F;
                uuids[k].array()[6] |= types[i];
            }
        }
        return uuids;
    }

    private static int compareV1(ByteBuffer b1, ByteBuffer b2)
    {

        // Compare for length

        if ((b1 == null) || (b1.remaining() < 16))
        {
            return ((b2 == null) || (b2.remaining() < 16)) ? 0 : -1;
        }
        if ((b2 == null) || (b2.remaining() < 16))
        {
            return 1;
        }

        int s1 = b1.position();
        int s2 = b2.position();

        // Compare versions

        int v1 = (b1.get(s1 + 6) >> 4) & 0x0f;
        int v2 = (b2.get(s2 + 6) >> 4) & 0x0f;

        if (v1 != v2)
        {
            return v1 - v2;
        }

        // Compare timestamps for version 1

        if (v1 == 1)
        {
            // if both time-based, compare as timestamps
            int c = compareTimestampBytes(b1, b2);
            if (c != 0)
            {
                return c;
            }
        }

        // Compare the two byte arrays starting from the first
        // byte in the sequence until an inequality is
        // found. This should provide equivalent results
        // to the comparison performed by the RFC 4122
        // Appendix A - Sample Implementation.
        // Note: java.util.UUID.compareTo is not a lexical
        // comparison
        for (int i = 0; i < 16; i++)
        {
            int c = ((b1.get(s1 + i)) & 0xFF) - ((b2.get(s2 + i)) & 0xFF);
            if (c != 0)
            {
                return c;
            }
        }

        return 0;
    }

    private static int compareTimestampBytes(ByteBuffer o1, ByteBuffer o2)
    {
        int o1Pos = o1.position();
        int o2Pos = o2.position();

        int d = (o1.get(o1Pos + 6) & 0xF) - (o2.get(o2Pos + 6) & 0xF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 7) & 0xFF) - (o2.get(o2Pos + 7) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 4) & 0xFF) - (o2.get(o2Pos + 4) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 5) & 0xFF) - (o2.get(o2Pos + 5) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos) & 0xFF) - (o2.get(o2Pos) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 1) & 0xFF) - (o2.get(o2Pos + 1) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        d = (o1.get(o1Pos + 2) & 0xFF) - (o2.get(o2Pos + 2) & 0xFF);
        if (d != 0)
        {
            return d;
        }

        return (o1.get(o1Pos + 3) & 0xFF) - (o2.get(o2Pos + 3) & 0xFF);
    }
}
