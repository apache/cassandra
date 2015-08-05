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

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class InetAddressType extends AbstractType<InetAddress>
{
    public static final InetAddressType instance = new InetAddressType();

    InetAddressType() {super(ComparisonType.BYTE_ORDER);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        InetAddress address;

        try
        {
            address = InetAddress.getByName(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make inet address from '%s'", source), e);
        }

        return decompose(address);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(InetAddressType.instance.fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a string representation of an inet value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        return '"' + getSerializer().deserialize(buffer).getHostAddress() + '"';
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.INET;
    }

    public TypeSerializer<InetAddress> getSerializer()
    {
        return InetAddressSerializer.instance;
    }
}
