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
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Int32Type extends AbstractType<Integer>
{
    public static final Int32Type instance = new Int32Type();

    Int32Type()
    {
        super(ComparisonType.CUSTOM);
    } // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        int diff = o1.get(o1.position()) - o2.get(o2.position());
        if (diff != 0)
            return diff;

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        int int32Type;

        try
        {
            int32Type = Integer.parseInt(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make int from '%s'", source), e);
        }

        return decompose(int32Type);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));

            Number parsedNumber = (Number) parsed;
            if (!(parsedNumber instanceof Integer))
                throw new MarshalException(String.format("Expected an int value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

            return new Constants.Value(getSerializer().serialize(parsedNumber.intValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected an int value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.INT;
    }

    public TypeSerializer<Integer> getSerializer()
    {
        return Int32Serializer.instance;
    }

    @Override
    protected int valueLengthIfFixed()
    {
        return 4;
    }
}
