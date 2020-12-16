/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.bytecomparable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.UUIDGen;

public class ByteSourceTestBase
{
    String[] testStrings = new String[]{ "", "\0", "\0\0", "\001", "A\0\0B", "A\0B\0", "0", "0\0", "00", "1", "\377" };
    Integer[] testInts = new Integer[]{ null,
                                        Integer.MIN_VALUE,
                                        Integer.MIN_VALUE + 1,
                                        -256,
                                        -255,
                                        -128,
                                        -127,
                                        -1,
                                        0,
                                        1,
                                        127,
                                        128,
                                        255,
                                        256,
                                        Integer.MAX_VALUE - 1,
                                        Integer.MAX_VALUE };
    Byte[] testBytes = new Byte[]{ -128, -127, -1, 0, 1, 127 };
    Short[] testShorts = new Short[]{ Short.MIN_VALUE,
                                      Short.MIN_VALUE + 1,
                                      -256,
                                      -255,
                                      -128,
                                      -127,
                                      -1,
                                      0,
                                      1,
                                      127,
                                      128,
                                      255,
                                      256,
                                      Short.MAX_VALUE - 1,
                                      Short.MAX_VALUE };
    Long[] testLongs = new Long[]{ null,
                                   Long.MIN_VALUE,
                                   Long.MIN_VALUE + 1,
                                   Integer.MIN_VALUE - 1L,
                                   -256L,
                                   -255L,
                                   -128L,
                                   -127L,
                                   -1L,
                                   0L,
                                   1L,
                                   127L,
                                   128L,
                                   255L,
                                   256L,
                                   Integer.MAX_VALUE + 1L,
                                   Long.MAX_VALUE - 1,
                                   Long.MAX_VALUE };
    Double[] testDoubles = new Double[]{ null,
                                         Double.NEGATIVE_INFINITY,
                                         -Double.MAX_VALUE,
                                         -1e+200,
                                         -1e3,
                                         -1e0,
                                         -1e-3,
                                         -1e-200,
                                         -Double.MIN_VALUE,
                                         -0.0,
                                         0.0,
                                         Double.MIN_VALUE,
                                         1e-200,
                                         1e-3,
                                         1e0,
                                         1e3,
                                         1e+200,
                                         Double.MAX_VALUE,
                                         Double.POSITIVE_INFINITY,
                                         Double.NaN };
    Float[] testFloats = new Float[]{ null,
                                      Float.NEGATIVE_INFINITY,
                                      -Float.MAX_VALUE,
                                      -1e+30f,
                                      -1e3f,
                                      -1e0f,
                                      -1e-3f,
                                      -1e-30f,
                                      -Float.MIN_VALUE,
                                      -0.0f,
                                      0.0f,
                                      Float.MIN_VALUE,
                                      1e-30f,
                                      1e-3f,
                                      1e0f,
                                      1e3f,
                                      1e+30f,
                                      Float.MAX_VALUE,
                                      Float.POSITIVE_INFINITY,
                                      Float.NaN };
    Boolean[] testBools = new Boolean[]{ null, false, true };
    UUID[] testUUIDs = new UUID[]{ null,
                                   UUIDGen.getTimeUUID(),
                                   UUID.randomUUID(),
                                   UUID.randomUUID(),
                                   UUID.randomUUID(),
                                   UUIDGen.getTimeUUID(123, 234),
                                   UUIDGen.getTimeUUID(123, 234),
                                   UUIDGen.getTimeUUID(123),
                                   UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
                                   UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
                                   UUID.fromString("e902893a-9d22-3c7e-a7b8-d6e313b71d9f"),
                                   UUID.fromString("74738ff5-5367-5958-9aee-98fffdcd1876"),
                                   UUID.fromString("52df1bb0-6a2f-11e6-b6e4-a6dea7a01b67"),
                                   UUID.fromString("52df1bb0-6a2f-11e6-362d-aff2143498ea"),
                                   UUID.fromString("52df1bb0-6a2f-11e6-b62d-aff2143498ea") };
    // Instant.MIN/MAX fail Date.from.
    Date[] testDates = new Date[]{ null,
                                   Date.from(Instant.ofEpochSecond(Integer.MIN_VALUE)),
                                   Date.from(Instant.ofEpochSecond(Short.MIN_VALUE)),
                                   Date.from(Instant.ofEpochMilli(-2000)),
                                   Date.from(Instant.EPOCH),
                                   Date.from(Instant.ofEpochMilli(2000)),
                                   Date.from(Instant.ofEpochSecond(Integer.MAX_VALUE)),
                                   Date.from(Instant.now()) };
    InetAddress[] testInets;
    {
        try
        {
            testInets = new InetAddress[]{ null,
                                           InetAddress.getLocalHost(),
                                           InetAddress.getLoopbackAddress(),
                                           InetAddress.getByName("192.168.0.1"),
                                           InetAddress.getByName("fe80::428d:5cff:fe53:1dc9"),
                                           InetAddress.getByName("2001:610:3:200a:192:87:36:2"),
                                           InetAddress.getByName("10.0.0.1"),
                                           InetAddress.getByName("0a00:0001::"),
                                           InetAddress.getByName("::10.0.0.1") };
        }
        catch (UnknownHostException e)
        {
            throw Throwables.propagate(e);
        }
    }

    BigInteger[] testBigInts;

    {
        Set<BigInteger> bigs = new TreeSet<>();
        for (Long l : testLongs)
            if (l != null)
                bigs.add(BigInteger.valueOf(l));
        for (int i = 0; i < 11; ++i)
        {
            bigs.add(BigInteger.valueOf(i));
            bigs.add(BigInteger.valueOf(-i));

            bigs.add(BigInteger.valueOf((1L << 4 * i) - 1));
            bigs.add(BigInteger.valueOf((1L << 4 * i)));
            bigs.add(BigInteger.valueOf(-(1L << 4 * i) - 1));
            bigs.add(BigInteger.valueOf(-(1L << 4 * i)));
            String p = exp10(i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));
            p = exp10(1 << i);
            bigs.add(new BigInteger(p));
            bigs.add(new BigInteger("-" + p));

            BigInteger base = BigInteger.ONE.shiftLeft(512 * i);
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
            base = base.negate();
            bigs.add(base);
            bigs.add(base.add(BigInteger.ONE));
            bigs.add(base.subtract(BigInteger.ONE));
        }
        testBigInts = bigs.toArray(new BigInteger[0]);
    }

    static String exp10(int pow)
    {
        StringBuilder builder = new StringBuilder();
        builder.append('1');
        for (int i=0; i<pow; ++i)
            builder.append('0');
        return builder.toString();
    }

    BigDecimal[] testBigDecimals;
    {
        String vals = "0, 1, 1.1, 21, 98.9, 99, 99.9, 100, 100.1, 101, 331, 0.4, 0.07, 0.0700, 0.005, " +
                      "6e4, 7e200, 6e-300, 8.1e2000, 8.1e-2000, 9e2000000000, " +
                      "123456789012.34567890e-1000000000, 123456.78901234, 1234.56789012e2, " +
                      "1.0000, 0.01e2, 100e-2, 00, 0.000, 0E-18, 0E+18";
        List<BigDecimal> decs = new ArrayList<>();
        for (String s : vals.split(", "))
        {
            decs.add(new BigDecimal(s));
            decs.add(new BigDecimal("-" + s));
        }
        testBigDecimals = decs.toArray(new BigDecimal[0]);
    }

    Object[][] testValues = new Object[][]{ testStrings,
                                            testInts,
                                            testBools,
                                            testDoubles,
                                            testBigInts,
                                            testBigDecimals };

    AbstractType[] testTypes = new AbstractType[]{ UTF8Type.instance,
                                                   Int32Type.instance,
                                                   BooleanType.instance,
                                                   DoubleType.instance,
                                                   IntegerType.instance,
                                                   DecimalType.instance };
}
