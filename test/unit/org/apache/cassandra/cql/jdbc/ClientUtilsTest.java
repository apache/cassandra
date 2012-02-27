package org.apache.cassandra.cql.jdbc;
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


import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

public class ClientUtilsTest
{
    /** Exercises the classes in the clientutil jar to expose missing dependencies. */
    @Test
    public void test() throws UnknownHostException
    {
        JdbcAscii.instance.compose(JdbcAscii.instance.decompose("string"));
        JdbcBoolean.instance.compose(JdbcBoolean.instance.decompose(true));
        JdbcBytes.instance.compose(JdbcBytes.instance.decompose(ByteBuffer.wrap("string".getBytes())));

        Date date = new Date(System.currentTimeMillis());
        ByteBuffer dateBB = JdbcDate.instance.decompose(date);
        JdbcDate.instance.compose(dateBB);
        assert (JdbcDate.instance.toString(date).equals(JdbcDate.instance.getString(dateBB)));

        JdbcDecimal.instance.compose(JdbcDecimal.instance.decompose(new BigDecimal(1)));
        JdbcDouble.instance.compose(JdbcDouble.instance.decompose(new Double(1.0d)));
        JdbcFloat.instance.compose(JdbcFloat.instance.decompose(new Float(1.0f)));
        JdbcInt32.instance.compose(JdbcInt32.instance.decompose(1));
        JdbcInteger.instance.compose(JdbcInteger.instance.decompose(new BigInteger("1")));
        JdbcLong.instance.compose(JdbcLong.instance.decompose(1L));
        JdbcUTF8.instance.compose(JdbcUTF8.instance.decompose("string"));

        // UUIDGen
        UUID uuid = UUIDGen.makeType1UUIDFromHost(InetAddress.getLocalHost());
        JdbcTimeUUID.instance.compose(JdbcTimeUUID.instance.decompose(uuid));
        JdbcUUID.instance.compose(JdbcUUID.instance.decompose(uuid));
        JdbcLexicalUUID.instance.compose(JdbcLexicalUUID.instance.decompose(uuid));

        // Raise a MarshalException
        try
        {
            JdbcLexicalUUID.instance.getString(ByteBuffer.wrap("notauuid".getBytes()));
        }
        catch (MarshalException me)
        {
            // Success
        }
    }
}
