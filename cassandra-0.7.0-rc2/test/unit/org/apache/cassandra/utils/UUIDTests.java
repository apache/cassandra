package org.apache.cassandra.utils;
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


import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.junit.Test;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;


public class UUIDTests
{
    @Test
    public void verifyType1() throws UnknownHostException
    {
        
        UUID uuid = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        assert uuid.version() == 1;
    }

    @Test
    public void verifyOrdering1() throws UnknownHostException
    {
        UUID one = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        UUID two = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.2"));
        assert one.timestamp() < two.timestamp();
    }


    @Test
    public void testDecomposeAndRaw() throws UnknownHostException
    {
        UUID a = UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"));
        byte[] decomposed = UUIDGen.decompose(a);
        UUID b = UUIDGen.getUUID(ByteBuffer.wrap(decomposed));
        assert a.equals(b);
    }

    @Test
    public void testTimeUUIDType() throws UnknownHostException
    {
        TimeUUIDType comp = TimeUUIDType.instance;
        ByteBuffer first = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"))));
        ByteBuffer second = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.makeType1UUIDFromHost(InetAddress.getByName("127.0.0.1"))));
        assert comp.compare(first, second) < 0;
        assert comp.compare(second, first) > 0;
        ByteBuffer sameAsFirst = ByteBuffer.wrap(UUIDGen.decompose(UUIDGen.getUUID(first)));
        assert comp.compare(first, sameAsFirst) == 0;
    }

    private void assertNonZero(BigInteger i)
    {
        assert i.toString(2).indexOf("1") > -1;
    }
}
