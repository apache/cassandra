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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.agrona.collections.IntArrayList;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class ByteSourceDuplicationTest extends ByteSourceTestBase
{
    public static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;
    private static final int TRIES = 25;
    private static final double TARGET_DUPES_PER_TRY = 7;
    Random rand = new Random(1);

    @Test
    public void testDuplication()
    {
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                testDuplication(typeToComparable(testTypes[i], o), "Type " + testTypes[i].getClass().getSimpleName() + " value " + o);
    }

    @Test
    public void testDuplicationPairs()
    {
        for (int i = 0; i < testValues.length; ++i)
            for (Object o : testValues[i])
                testDuplication(typeToComparableInPair(testTypes[i], o), "Type " + testTypes[i].getClass().getSimpleName() + " value " + o + " in pair");
    }

    public ByteComparable typeToComparable(AbstractType t, Object o)
    {
        return v -> t.asComparableBytes(t.decompose(o), v);
    }

    public ByteComparable typeToComparableInPair(AbstractType t, Object o)
    {
        int ti = rand.nextInt(testValues.length);
        Object oo = testValues[ti][rand.nextInt(testValues[ti].length)];
        AbstractType tt = testTypes[ti];
        return v -> ByteSource.withTerminator(ByteSource.TERMINATOR,
                                              t.asComparableBytes(t.decompose(o), v),
                                              tt.asComparableBytes(tt.decompose(oo), v));
    }

    private void testDuplication(ByteComparable comparable, String msg)
    {
        if (comparable == null || comparable.asComparableBytes(VERSION) == null)
            return; // nothing to check

        byte[] bytes = ByteSourceInverse.readBytes(comparable.asComparableBytes(VERSION));
        msg += " encoding " + ByteBufferUtil.bytesToHex(ByteBuffer.wrap(bytes));
        double dupeProbability = Math.min(0.6, TARGET_DUPES_PER_TRY / bytes.length); // A few duplications per entry on average

        IntArrayList positions = new IntArrayList();
        List<ByteSource> sources = new ArrayList<>();
        for (int test = 0; test < Math.max(1, TRIES / bytes.length); ++test)
        {
            sources.add(comparable.asComparableBytes(VERSION));
            positions.add(0);

            while (!sources.isEmpty())
            {
                int index = rand.nextInt(sources.size());
                int pos = positions.getInt(index);
                ByteSource source = sources.get(index);
                if (rand.nextDouble() <= dupeProbability)
                {
                    ByteSource.Duplicatable duplicatable = ByteSource.duplicatable(source);
                    sources.set(index, duplicatable);
                    source = duplicatable;
                    ByteSource duplicate = duplicatable.duplicate();
                    sources.add(duplicate);
                    positions.add(pos);
                }
                int next = source.next();
                if (next == ByteSource.END_OF_STREAM)
                {
                    assertEquals(msg, bytes.length, pos);
                    sources.remove(index);
                    positions.remove(index);
                }
                else
                {
                    assertEquals(msg, bytes[pos++], (byte) next);
                    positions.setInt(index, pos);
                }
            }
        }
    }
}
