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

package org.apache.cassandra.fuzz.harry.util;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO: these are not real tests
public class BitSetTest
{
    @Test
    public void testBitMask()
    {
        Assert.assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L, BitSet.bitMask(-1));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L, BitSet.bitMask(0));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001L, BitSet.bitMask(1));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000011L, BitSet.bitMask(2));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00001111L, BitSet.bitMask(4));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_11111111L, BitSet.bitMask(8));
        assertEquals(0b00000000_00000000_00000000_00000000_00000000_00000000_11111111_11111111L, BitSet.bitMask(16));
        assertEquals(0b00000000_00000000_00000000_00000000_11111111_11111111_11111111_11111111L, BitSet.bitMask(32));
        assertEquals(0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111L, BitSet.bitMask(64));
        assertEquals(0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111L, BitSet.bitMask(65));
    }

    @Test
    public void testSetBits()
    {
        BitSet bs = BitSet.allUnset(32);
        bs.set(10);
        bs.set(13);
        bs.set(15);
        assertEquals(3, bs.setCount());
    }

    @Test
    public void testEachSetBit()
    {
        BitSet bs = BitSet.allUnset(32);
        bs.set(10);
        bs.set(13);
        bs.set(15);
        bs.eachSetBit(i -> assertTrue(i == 10 || i == 13 || i == 15));
    }
}
