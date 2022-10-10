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

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldIdentifierTest
{

    @Test
    public void testEqualsMethod()
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
            FieldIdentifier a = new FieldIdentifier(ByteBuffer.wrap(aBytes, 0, aLength));
            FieldIdentifier b = new FieldIdentifier(ByteBuffer.wrap(bBytes, 0, bLength));
            Assert.assertEquals("" + i, a.equals(b), a.bytes.equals(b.bytes));
        }
    }

    @Test
    public void testForQuoted()
    {
        assertEquals("\"A\"", FieldIdentifier.forQuoted("A").toString());
        assertEquals("\"\"\"", FieldIdentifier.forQuoted("\"").toString());
        assertEquals("\"\"a\"b\"\"", FieldIdentifier.forQuoted("\"a\"b\"").toString());
    }

    @Test
    public void testForUnquoted()
    {
        assertEquals("a", FieldIdentifier.forUnquoted("A").toString());
        assertEquals("\"", FieldIdentifier.forUnquoted("\"").toString());
        assertEquals("\"a\"b\"", FieldIdentifier.forUnquoted("\"a\"b\"").toString());
    }

    @Test
    public void testForInternalString()
    {
        assertEquals("a", FieldIdentifier.forUnquoted("A").toString());
        assertEquals("\"", FieldIdentifier.forUnquoted("\"").toString());
        assertEquals("\"a\"b\"", FieldIdentifier.forUnquoted("\"a\"b\"").toString());
    }
}
