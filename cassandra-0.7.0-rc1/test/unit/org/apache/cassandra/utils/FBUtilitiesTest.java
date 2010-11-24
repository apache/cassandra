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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;

import org.junit.Test;

public class FBUtilitiesTest 
{
	@Test
    public void testHexBytesConversion()
    {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++)
        {
            byte[] b = new byte[]{ (byte)i };
            String s = FBUtilities.bytesToHex(b);
            byte[] c = FBUtilities.hexToBytes(s);
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
            assert Arrays.equals(FBUtilities.hexToBytes(values[i]), expected[i]);
    }

    @Test
    public void testIntBytesConversions()
    {
        // positive, negative, 1 and 2 byte cases, including a few edges that would foul things up unless you're careful
        // about masking away sign extension.
        int[] ints = new int[]
        {
            -20, -127, -128, 0, 1, 127, 128, 65534, 65535, -65534, -65535
        };

        for (int i : ints) {
            ByteBuffer ba = FBUtilities.toByteBuffer(i);
            int actual = FBUtilities.byteBufferToInt(ba);
            assertEquals(i, actual);
        }
    }

    @Test(expected=CharacterCodingException.class)
    public void testDecode() throws IOException
    {
        ByteBuffer bytes = ByteBuffer.wrap(new byte[]{(byte)0xff, (byte)0xfe});
        FBUtilities.decodeToUTF8(bytes);
    } 
}
