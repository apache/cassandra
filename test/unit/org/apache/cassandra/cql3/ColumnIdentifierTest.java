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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import static org.junit.Assert.assertEquals;

public class ColumnIdentifierTest
{

    @Test
    public void testComparisonMethod()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        byte[] commonBytes = new byte[10];
        byte[] aBytes = new byte[16];
        byte[] bBytes = new byte[16];
        for (int i = 0 ; i < 100000 ; i++)
        {
            int commonLength = random.nextInt(0, 10);
            random.nextBytes(commonBytes);
            random.nextBytes(aBytes);
            random.nextBytes(bBytes);
            System.arraycopy(commonBytes, 0, aBytes, 0, commonLength);
            System.arraycopy(commonBytes, 0, bBytes, 0, commonLength);
            int aLength = random.nextInt(commonLength, 16);
            int bLength = random.nextInt(commonLength, 16);
            ColumnIdentifier a = new ColumnIdentifier(ByteBuffer.wrap(aBytes, 0, aLength), BytesType.instance);
            ColumnIdentifier b = new ColumnIdentifier(ByteBuffer.wrap(bBytes, 0, bLength), BytesType.instance);
            Assert.assertEquals("" + i, compareResult(a.compareTo(b)), compareResult(ByteBufferUtil.compareUnsigned(a.bytes, b.bytes)));
        }
    }

    private static int compareResult(int v)
    {
        return v < 0 ? -1 : v > 0 ? 1 : 0;
    }
    
    @Test
    public void testMaybeQuote()
    {
        String unquotable = "a";
        assertEquals(unquotable, ColumnIdentifier.maybeQuote(unquotable));
        unquotable = "z4";
        assertEquals(unquotable, ColumnIdentifier.maybeQuote(unquotable));
        unquotable = "m_4_";
        assertEquals(unquotable, ColumnIdentifier.maybeQuote(unquotable));
        unquotable = "f__";
        assertEquals(unquotable, ColumnIdentifier.maybeQuote(unquotable));
        
        assertEquals("\"A\"", ColumnIdentifier.maybeQuote("A"));
        assertEquals("\"4b\"", ColumnIdentifier.maybeQuote("4b"));
        assertEquals("\"\"\"\"", ColumnIdentifier.maybeQuote("\""));
        assertEquals("\"\"\"a\"\"b\"\"\"", ColumnIdentifier.maybeQuote("\"a\"b\""));
    }

    @Test
    public void testInternedCache()
    {
        AbstractType<?> utf8Type = UTF8Type.instance;
        AbstractType<?> bytesType = BytesType.instance;

        byte[] bytes = new byte [] { 0x63, (byte) 0x32 };
        String text = "c2"; // the UTF-8 encoding of this string is the same as bytes, 0x630x32

        ColumnIdentifier c1 = ColumnIdentifier.getInterned(ByteBuffer.wrap(bytes), bytesType);
        ColumnIdentifier c2 = ColumnIdentifier.getInterned(utf8Type, utf8Type.fromString(text), text);
        ColumnIdentifier c3 = ColumnIdentifier.getInterned(text, true);

        Assert.assertTrue(c1.isInterned());
        Assert.assertTrue(c2.isInterned());
        Assert.assertTrue(c3.isInterned());

        Assert.assertEquals("6332", c1.toString());
        Assert.assertEquals(text, c2.toString());
        Assert.assertEquals(text, c3.toString());
    }

    @Test
    public void testInterningUsesMinimalByteBuffer()
    {
        byte[] bytes = new byte[2];
        bytes[0] = 0x63;
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.limit(1);

        ColumnIdentifier c1 = ColumnIdentifier.getInterned(byteBuffer, UTF8Type.instance);

        Assert.assertEquals(2, byteBuffer.capacity());
        Assert.assertEquals(1, c1.bytes.capacity());
    }
}
