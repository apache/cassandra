package org.apache.cassandra.db.marshal;

import org.apache.cassandra.Util;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.UUID;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class TypeValidationTest
{
    @Test(expected = MarshalException.class)
    public void testInvalidAscii()
    {
        AsciiType.instance.validate(ByteBuffer.wrap(new byte[]{ (byte)0x80 }));
    }

    @Test(expected = MarshalException.class)
    public void testInvalidTimeUUID()
    {
        UUID uuid = UUID.randomUUID();
        TimeUUIDType.instance.validate(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
    }

    @Test
    public void testValidTimeUUID()
    {
        TimeUUIDType.instance.validate(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
    }

    @Test
    public void testLong()
    {
        LongType.instance.validate(Util.getBytes(5L));
        LongType.instance.validate(Util.getBytes(5555555555555555555L));
    }

    @Test
    public void testInt()
    {
        Int32Type.instance.validate(Util.getBytes(5));
        Int32Type.instance.validate(Util.getBytes(2057022603));
    }

    @Test
    public void testValidUtf8() throws UnsupportedEncodingException
    {
        assert Character.MAX_CODE_POINT == 0x0010ffff;
        CharBuffer cb = CharBuffer.allocate(2837314);
        // let's test all of the unicode space.
        for (int i = 0; i < Character.MAX_CODE_POINT; i++)
        {
            // skip U+D800..U+DFFF. those CPs are invalid in utf8. java tolerates them, but doesn't convert them to
            // valid byte sequences (gives us '?' instead), so there is no point testing them.
            if (i >= 55296 && i <= 57343)
                continue;
            char[] ch = Character.toChars(i);
            for (char c : ch)
                cb.append(c);
        }
        String s = new String(cb.array());
        byte[] arr = s.getBytes("UTF8");
        ByteBuffer buf = ByteBuffer.wrap(arr);
        UTF8Type.instance.validate(buf);

        // some you might not expect.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {}));
        // valid Utf8, unspecified in modified utf8.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {0}));

        // modified utf8 null.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {99, (byte)0xc0, (byte)0x80, 112}));

        // edges, for my sanity.
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xc2, (byte)0x81}));
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xe0, (byte)0xa0, (byte)0x81}));
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xf0, (byte)0x90, (byte)0x81, (byte)0x81}));
    }

    // now test for bogies.

    @Test(expected = MarshalException.class)
    public void testFloatingc0()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {99, (byte)0xc0, 112}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid2nd()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xc2, (byte)0xff}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid3rd()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xe0, (byte)0xa0, (byte)0xff}));
    }

    @Test(expected = MarshalException.class)
    public void testInvalid4th()
    {
        UTF8Type.instance.validate(ByteBuffer.wrap(new byte[] {(byte)0xf0, (byte)0x90, (byte)0x81, (byte)0xff}));
    }

    // todo: for completeness, should test invalid two byte pairs.
}
