/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.MarshalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BooleanType extends AbstractType<Boolean>
{
    private static final Logger logger = LoggerFactory.getLogger(BooleanType.class);

    public static final BooleanType instance = new BooleanType();

    BooleanType() {} // singleton

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        // False is 0, True is anything else, makes False sort before True.
        byte b1 = o1.get(o1.position());
        byte b2 = o2.get(o2.position());
        if (b1 == 0)
            return b2 == 0 ? 0 : -1;
        return b2 == 0 ? 1 : 0;
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {

        if (source.isEmpty()|| source.equalsIgnoreCase(Boolean.FALSE.toString()))
            return decompose(false);

        if (source.equalsIgnoreCase(Boolean.TRUE.toString()))
            return decompose(true);

        throw new MarshalException(String.format("unable to make boolean from '%s'", source));
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BOOLEAN;
    }

    public TypeSerializer<Boolean> getSerializer()
    {
        return BooleanSerializer.instance;
    }
}
