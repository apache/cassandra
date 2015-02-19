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
package org.apache.cassandra.utils;

import org.apache.cassandra.utils.CRC32Factory.CRC32Ex;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.Assert.*;

import org.junit.Test;

public class CRC32FactoryTest
{

    @Test
    public void updateInt()
    {
        ICRC32 crcA = new CRC32Ex();
        PureJavaCrc32 crcB = new PureJavaCrc32();

        crcA.updateInt(42);
        crcB.updateInt(42);

        assertEquals(crcA.getCrc(), crcB.getCrc());
        assertEquals(crcA.getValue(), crcB.getValue());
    }

    @Test
    public void testFuzzz()
    {
        for (int ii = 0; ii < 100; ii++)
        {
            testOnce();
        }
    }

    private void testOnce()
    {
        final long seed = System.nanoTime();
        System.out.println("Seed is " + seed);
        Random r = new java.util.Random(seed);

        ByteBuffer source = null;
        int nextSize = r.nextDouble() < .9 ? r.nextInt(1024 * 1024) : r.nextInt(16);

        if (r.nextDouble() > .5)
        {
            source = ByteBuffer.allocate(nextSize);
            r.nextBytes(source.array());
        }
        else
        {
            source = ByteBuffer.allocateDirect(nextSize);
            while (source.hasRemaining())
            {
                source.put((byte)(r.nextInt() % 127));
            }
            source.clear();
        }

        ICRC32 crcA = new CRC32Ex();
        PureJavaCrc32 crcB = new PureJavaCrc32();
        if (source.hasArray())
        {
            if (r.nextDouble() > 0.5)
            {
                crcA.update(source.array(), 0, source.remaining());
                crcB.update(source.array(), 0, source.remaining());
            }
            else
            {
                crcA.update(source, 0, source.remaining());
                assertEquals(0, source.position());
                assertEquals(source.capacity(), source.limit());
                crcB.update(source, 0, source.remaining());
                assertEquals(0, source.position());
                assertEquals(source.capacity(), source.limit());
            }
        }
        else
        {
            crcA.update(source, 0, source.remaining());
            assertEquals(0, source.position());
            assertEquals(source.capacity(), source.limit());
            crcB.update(source, 0, source.remaining());
            assertEquals(0, source.position());
            assertEquals(source.capacity(), source.limit());
        }
        assertEquals(crcA.getCrc(), crcB.getCrc());
        assertEquals(crcA.getValue(), crcB.getValue());
    }

    @Test
    public void jdkDetection()
    {
        if (System.getProperty("java.version").startsWith("1.7"))
            assertFalse(CRC32Factory.create() instanceof CRC32Factory.CRC32Ex);
        else
            assertTrue(CRC32Factory.create() instanceof CRC32Factory.CRC32Ex);
    }
}
