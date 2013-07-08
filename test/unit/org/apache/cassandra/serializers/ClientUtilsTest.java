package org.apache.cassandra.serializers;
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
        AsciiSerializer.instance.serialize(AsciiSerializer.instance.deserialize("string"));
        BooleanSerializer.instance.serialize(BooleanSerializer.instance.deserialize(true));
        BytesSerializer.instance.serialize(BytesSerializer.instance.deserialize(ByteBuffer.wrap("string".getBytes())));

        Date date = new Date(System.currentTimeMillis());
        ByteBuffer dateBB = TimestampSerializer.instance.deserialize(date);
        TimestampSerializer.instance.serialize(dateBB);
        assert (TimestampSerializer.instance.toString(date).equals(TimestampSerializer.instance.getString(dateBB)));

        DecimalSerializer.instance.serialize(DecimalSerializer.instance.deserialize(new BigDecimal(1)));
        DoubleSerializer.instance.serialize(DoubleSerializer.instance.deserialize(new Double(1.0d)));
        FloatSerializer.instance.serialize(FloatSerializer.instance.deserialize(new Float(1.0f)));
        Int32Serializer.instance.serialize(Int32Serializer.instance.deserialize(1));
        IntegerSerializer.instance.serialize(IntegerSerializer.instance.deserialize(new BigInteger("1")));
        LongSerializer.instance.serialize(LongSerializer.instance.deserialize(1L));
        UTF8Serializer.instance.serialize(UTF8Serializer.instance.deserialize("string"));

        // UUIDGen
        UUID uuid = UUIDGen.getTimeUUID();
        UUIDSerializer.instance.serialize(UUIDSerializer.instance.deserialize(uuid));

        // Raise a MarshalException
        try
        {
            UUIDSerializer.instance.getString(ByteBuffer.wrap("notauuid".getBytes()));
        }
        catch (MarshalException me)
        {
            // Success
        }
    }
}
