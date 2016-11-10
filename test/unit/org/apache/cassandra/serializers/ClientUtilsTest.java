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
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;
import org.junit.Test;

public class ClientUtilsTest
{
    /** Exercises the classes in the clientutil jar to expose missing dependencies. */
    @Test
    public void test()
    {
        AsciiSerializer.instance.deserialize(AsciiSerializer.instance.serialize("string"));
        BooleanSerializer.instance.deserialize(BooleanSerializer.instance.serialize(true));
        BytesSerializer.instance.deserialize(BytesSerializer.instance.serialize(ByteBuffer.wrap("string".getBytes())));

        Date date = new Date(System.currentTimeMillis());
        ByteBuffer dateBB = TimestampSerializer.instance.serialize(date);
        TimestampSerializer.instance.deserialize(dateBB);

        DecimalSerializer.instance.deserialize(DecimalSerializer.instance.serialize(new BigDecimal(1)));
        DoubleSerializer.instance.deserialize(DoubleSerializer.instance.serialize(new Double(1.0d)));
        FloatSerializer.instance.deserialize(FloatSerializer.instance.serialize(new Float(1.0f)));
        Int32Serializer.instance.deserialize(Int32Serializer.instance.serialize(1));
        IntegerSerializer.instance.deserialize(IntegerSerializer.instance.serialize(new BigInteger("1")));
        LongSerializer.instance.deserialize(LongSerializer.instance.serialize(1L));
        UTF8Serializer.instance.deserialize(UTF8Serializer.instance.serialize("string"));

        // UUIDGen
        UUID uuid = UUIDGen.getTimeUUID();
        UUIDSerializer.instance.deserialize(UUIDSerializer.instance.serialize(uuid));
    }
}
