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
import java.util.UUID;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

public class LexicalUUIDType extends AbstractType<UUID>
{
    public static final LexicalUUIDType instance = new LexicalUUIDType();

    LexicalUUIDType()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9901
        super(ComparisonType.CUSTOM);
    } // singleton

    public boolean isEmptyValueMeaningless()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9457
        return true;
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-0
        return UUIDGen.getUUID(o1).compareTo(UUIDGen.getUUID(o2));
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2262
        try
        {
            return decompose(UUID.fromString(source));
        }
        catch (IllegalArgumentException e)
        {
            throw new MarshalException(String.format("unable to make UUID from '%s'", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7970
        try
        {
            return new Constants.Value(fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a string representation of a uuid, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    public TypeSerializer<UUID> getSerializer()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-4495
        return UUIDSerializer.instance;
    }

    @Override
    public int valueLengthIfFixed()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        return 16;
    }
}
