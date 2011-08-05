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
package org.apache.cassandra.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

public class BytesReadTrackerTest
{

    @Test
    public void testBytesRead() throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try
        {
            // boolean
            dos.writeBoolean(true);
            // byte
            dos.writeByte(0x1);
            // char
            dos.writeChar('a');
            // short
            dos.writeShort(1);
            // int
            dos.writeInt(1);
            // long
            dos.writeLong(1L);
            // float
            dos.writeFloat(1.0f);
            // double
            dos.writeDouble(1.0d);

            // String
            dos.writeUTF("abc");
            testData = baos.toByteArray();
        }
        finally
        {
            dos.close();
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = new BytesReadTracker(dis);

        try
        {
            // boolean = 1byte
            boolean bool = tracker.readBoolean();
            assertTrue(bool);
            assertEquals(1, tracker.getBytesRead());
            // byte = 1byte
            byte b = tracker.readByte();
            assertEquals(b, 0x1);
            assertEquals(2, tracker.getBytesRead());
            // char = 2byte
            char c = tracker.readChar();
            assertEquals('a', c);
            assertEquals(4, tracker.getBytesRead());
            // short = 2bytes
            short s = tracker.readShort();
            assertEquals(1, s);
            assertEquals((short) 6, tracker.getBytesRead());
            // int = 4bytes
            int i = tracker.readInt();
            assertEquals(1, i);
            assertEquals(10, tracker.getBytesRead());
            // long = 8bytes
            long l = tracker.readLong();
            assertEquals(1L, l);
            assertEquals(18, tracker.getBytesRead());
            // float = 4bytes
            float f = tracker.readFloat();
            assertEquals(1.0f, f, 0);
            assertEquals(22, tracker.getBytesRead());
            // double = 8bytes
            double d = tracker.readDouble();
            assertEquals(1.0d, d, 0);
            assertEquals(30, tracker.getBytesRead());
            // String("abc") = 2(string size) + 3 = 5 bytes
            String str = tracker.readUTF();
            assertEquals("abc", str);
            assertEquals(35, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            dis.close();
        }

        tracker.reset(0);
        assertEquals(0, tracker.getBytesRead());
    }

    @Test
    public void testUnsignedRead() throws Exception
    {
        byte[] testData;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try
        {
            // byte
            dos.writeByte(0x1);
            // short
            dos.writeShort(1);
            testData = baos.toByteArray();
        }
        finally
        {
            dos.close();
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = new BytesReadTracker(dis);

        try
        {
            // byte = 1byte
            int b = tracker.readUnsignedByte();
            assertEquals(b, 1);
            assertEquals(1, tracker.getBytesRead());
            // short = 2bytes
            int s = tracker.readUnsignedShort();
            assertEquals(1, s);
            assertEquals(3, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            dis.close();
        }
    }

    @Test
    public void testSkipBytesAndReadFully() throws Exception
    {
        String testStr = "1234567890";
        byte[] testData = testStr.getBytes();

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(testData));
        BytesReadTracker tracker = new BytesReadTracker(dis);

        try
        {
            // read first 5 bytes
            byte[] out = new byte[5];
            tracker.readFully(out, 0, 5);
            assertEquals("12345", new String(out));
            assertEquals(5, tracker.getBytesRead());

            // then skip 2 bytes
            tracker.skipBytes(2);
            assertEquals(7, tracker.getBytesRead());

            // and read the rest
            out = new byte[3];
            tracker.readFully(out);
            assertEquals("890", new String(out));
            assertEquals(10, tracker.getBytesRead());

            assertEquals(testData.length, tracker.getBytesRead());
        }
        finally
        {
            dis.close();
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadLine() throws Exception
    {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream("1".getBytes()));
        BytesReadTracker tracker = new BytesReadTracker(dis);

        try
        {
            // throws UnsupportedOperationException
            tracker.readLine();
        }
        finally
        {
            dis.close();
        }
    }
}
