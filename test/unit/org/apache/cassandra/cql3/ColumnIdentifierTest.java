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

import junit.framework.Assert;
import org.apache.cassandra.db.marshal.BytesType;
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

}
