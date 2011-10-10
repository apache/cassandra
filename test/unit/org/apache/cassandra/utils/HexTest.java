/**
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

package org.apache.cassandra.utils;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import org.junit.Test;

public class HexTest
{
    @Test
    public void testHexBytesConversion()
    {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++)
        {
            byte[] b = new byte[]{ (byte)i };
            String s = Hex.bytesToHex(b);
            byte[] c = Hex.hexToBytes(s);
            assertArrayEquals(b, c);
        }
    }

    @Test
    public void testHexToBytesStringConversion()
    {
        String[] values = new String[]
        {
            "0",
            "10",
            "100",
            "101",
            "f",
            "ff"
        };
        byte[][] expected = new byte[][]
        {
            new byte[] { 0x00 },
            new byte[] { 0x10 },
            new byte[] { 0x01, 0x00 },
            new byte[] { 0x01, 0x01 },
            new byte[] { 0x0f },
            new byte[] { (byte)0x000000ff }
        };
        
        for (int i = 0; i < values.length; i++)
            assert Arrays.equals(Hex.hexToBytes(values[i]), expected[i]);
    }
}
