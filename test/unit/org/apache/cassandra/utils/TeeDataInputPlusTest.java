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

package org.apache.cassandra.utils;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.TeeDataInputPlus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TeeDataInputPlusTest
{
    @Test
    public void testTeeBuffer() throws Exception
    {
        DataOutputBuffer out = new DataOutputBuffer();
        byte[] testData;

        // boolean
        out.writeBoolean(true);
        // byte
        out.writeByte(0x1);
        // char
        out.writeChar('a');
        // short
        out.writeShort(1);
        // int
        out.writeInt(1);
        // long
        out.writeLong(1L);
        // float
        out.writeFloat(1.0f);
        // double
        out.writeDouble(1.0d);
        // vint
        out.writeVInt(-1337L);
        //unsigned vint
        out.writeUnsignedVInt(1337L);
        // String
        out.writeUTF("abc");
        //Another string to test skip
        out.writeUTF("garbagetoskipattheend");
        testData = out.toByteArray();

        int LIMITED_SIZE = 40;
        DataInputBuffer reader = new DataInputBuffer(testData);
        DataInputBuffer reader2 = new DataInputBuffer(testData);
        DataOutputBuffer teeOut = new DataOutputBuffer();
        DataOutputBuffer limitedTeeOut = new DataOutputBuffer();
        TeeDataInputPlus tee = new TeeDataInputPlus(reader, teeOut);
        TeeDataInputPlus limitedTee = new TeeDataInputPlus(reader2, limitedTeeOut, LIMITED_SIZE);

        // boolean = 1byte
        boolean bool = tee.readBoolean();
        assertTrue(bool);
        bool = limitedTee.readBoolean();
        assertTrue(bool);
        // byte = 1byte
        byte b = tee.readByte();
        assertEquals(b, 0x1);
        b = limitedTee.readByte();
        assertEquals(b, 0x1);
        // char = 2byte
        char c = tee.readChar();
        assertEquals('a', c);
        c = limitedTee.readChar();
        assertEquals('a', c);
        // short = 2bytes
        short s = tee.readShort();
        assertEquals(1, s);
        s = limitedTee.readShort();
        assertEquals(1, s);
        // int = 4bytes
        int i = tee.readInt();
        assertEquals(1, i);
        i = limitedTee.readInt();
        assertEquals(1, i);
        // long = 8bytes
        long l = tee.readLong();
        assertEquals(1L, l);
        l = limitedTee.readLong();
        assertEquals(1L, l);
        // float = 4bytes
        float f = tee.readFloat();
        assertEquals(1.0f, f, 0);
        f = limitedTee.readFloat();
        assertEquals(1.0f, f, 0);
        // double = 8bytes
        double d = tee.readDouble();
        assertEquals(1.0d, d, 0);
        d = limitedTee.readDouble();
        assertEquals(1.0d, d, 0);
        long vint = tee.readVInt();
        assertEquals(-1337L, vint);
        vint = limitedTee.readVInt();
        assertEquals(-1337L, vint);
        long uvint = tee.readUnsignedVInt();
        assertEquals(1337L, uvint);
        uvint = limitedTee.readUnsignedVInt();
        assertEquals(1337L, uvint);
        // String("abc") = 2(string size) + 3 = 5 bytes
        String str = tee.readUTF();
        assertEquals("abc", str);
        str = limitedTee.readUTF();
        assertEquals("abc", str);
        int skipped = tee.skipBytes(100);
        assertEquals(23, skipped);
        skipped = limitedTee.skipBytes(100);
        assertEquals(23, skipped);

        byte[] teeData = teeOut.toByteArray();
        assertFalse(tee.isLimitReached());
        assertTrue(Arrays.equals(testData, teeData));

        byte[] limitedTeeData = limitedTeeOut.toByteArray();
        assertTrue(limitedTee.isLimitReached());
        assertTrue(Arrays.equals(Arrays.copyOf(testData, LIMITED_SIZE - 1 ), limitedTeeData));
    }
}
