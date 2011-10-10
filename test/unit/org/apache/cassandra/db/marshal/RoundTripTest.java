package org.apache.cassandra.db.marshal;
/*
 * 
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


import com.google.common.base.Charsets;

import org.apache.cassandra.cql.jdbc.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

public class RoundTripTest
{
    @Test
    public void testInteger()
    {
        BigInteger bi = new BigInteger("1");
        assert bi.intValue() == 1;
        assert IntegerType.instance.getString(IntegerType.instance.fromString("1")).equals("1");
        assert IntegerType.instance.fromString(IntegerType.instance.getString(ByteBuffer.wrap(bi.toByteArray())))
                .equals(ByteBuffer.wrap(bi.toByteArray()));
        assert IntegerType.instance.compose(ByteBuffer.wrap(bi.toByteArray())).equals(bi);
        assert JdbcInteger.instance.toString(bi).equals("1");
    }
    
    @Test
    public void testLong()
    {
        byte[] v = new byte[]{0,0,0,0,0,0,0,1};
        assert LongType.instance.getString(LongType.instance.fromString("1")).equals("1");
        assert LongType.instance.fromString(LongType.instance.getString(ByteBuffer.wrap(v)))
                .equals(ByteBuffer.wrap(v));
        assert LongType.instance.compose(ByteBuffer.wrap(v)) == 1L;
        assert JdbcLong.instance.toString(1L).equals("1");
    }
    
    @Test
    public void intLong()
    {
        byte[] v = new byte[]{0,0,0,1};
        assert Int32Type.instance.getString(Int32Type.instance.fromString("1")).equals("1");
        assert Int32Type.instance.fromString(Int32Type.instance.getString(ByteBuffer.wrap(v)))
                .equals(ByteBuffer.wrap(v));
        assert Int32Type.instance.compose(ByteBuffer.wrap(v)) == 1;
        // assert Int32Type.instance.toString(1).equals("1");
    }
    
    @Test
    public void testAscii() throws Exception
    {
        byte[] abc = "abc".getBytes(Charsets.US_ASCII);
        assert AsciiType.instance.getString(AsciiType.instance.fromString("abc")).equals("abc");
        assert AsciiType.instance.fromString(AsciiType.instance.getString(ByteBuffer.wrap(abc)))
                .equals(ByteBuffer.wrap(abc));
        assert AsciiType.instance.compose(ByteBuffer.wrap(abc)).equals("abc");
        assert JdbcAscii.instance.toString("abc").equals("abc");
    }
    
    @Test
    public void testBytes()
    {
        byte[] v = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assert JdbcBytes.instance.toString(BytesType.instance.fromString(Hex.bytesToHex(v)))
                .equals(Hex.bytesToHex(v));
        assert BytesType.instance.fromString(JdbcBytes.instance.toString(ByteBuffer.wrap(v)))
                .equals(ByteBuffer.wrap(v));
        assert BytesType.instance.compose(ByteBuffer.wrap(v)).equals(ByteBuffer.wrap(v));
        assert JdbcBytes.instance.toString(ByteBuffer.wrap(v)).equals(Hex.bytesToHex(v));
    }
    
    @Test
    public void testLexicalUUID()
    {
        UUID uuid = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        assert JdbcLexicalUUID.instance.getString(LexicalUUIDType.instance.fromString(uuid.toString()))
                .equals(uuid.toString());
        assert LexicalUUIDType.instance.fromString(LexicalUUIDType.instance.getString(ByteBuffer.wrap(UUIDGen.decompose(uuid))))
                .equals(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
        assert LexicalUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.decompose(uuid))).equals(uuid);
        assert JdbcLexicalUUID.instance.toString(uuid).equals(uuid.toString());
    }
    
    @Test
    public void testTimeUUID()
    {
        UUID uuid = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        assert TimeUUIDType.instance.getString(TimeUUIDType.instance.fromString(uuid.toString()))
                .equals(uuid.toString());
        assert TimeUUIDType.instance.fromString(TimeUUIDType.instance.getString(ByteBuffer.wrap(UUIDGen.decompose(uuid))))
                .equals(ByteBuffer.wrap(UUIDGen.decompose(uuid)));
        assert TimeUUIDType.instance.compose(ByteBuffer.wrap(UUIDGen.decompose(uuid))).equals(uuid);
        
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString(uuid.toString())));
        assert JdbcTimeUUID.instance.toString(uuid).equals(uuid.toString());
        
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2011);
        c.set(Calendar.MONTH, Calendar.MARCH);
        c.set(Calendar.DAY_OF_MONTH, 8);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        Long ts = new Long(1299560400000L);
        assert ts.equals(c.getTimeInMillis()) : c.getTimeInMillis();
        
        // create a uuid with a known time.
        uuid = TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString(ts.toString()));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString(uuid.toString())));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08 00:00")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08 00:00:00")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08 00:00-0500")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08 00:00:00-0500")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08T00:00")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08T00:00-0500")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08T00:00:00")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08T00:00:00-0500")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08")));
        assert uuid.equals(TimeUUIDType.instance.compose(TimeUUIDType.instance.fromString("2011-03-08-0500")));  
    }
    
    @Test
    public void testUtf8() throws Exception
    {
        String v = "\u2297\u5432\u2376\u263d\uf543";
        assert UTF8Type.instance.getString(UTF8Type.instance.fromString(v)).equals(v);
        assert UTF8Type.instance.fromString(UTF8Type.instance.getString(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8))))
                .equals(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8)));
        assert UTF8Type.instance.compose(ByteBuffer.wrap(v.getBytes(Charsets.UTF_8))).equals(v);
        assert JdbcUTF8.instance.toString(v).equals(v);
    }
}
