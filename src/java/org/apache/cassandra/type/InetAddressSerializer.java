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

package org.apache.cassandra.type;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class InetAddressSerializer extends AbstractSerializer<InetAddress>
{
    public static final InetAddressSerializer instance = new InetAddressSerializer();

    @Override
    public InetAddress serialize(ByteBuffer bytes)
    {
        try
        {
            return InetAddress.getByAddress(ByteBufferUtil.getArray(bytes));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @Override
    public ByteBuffer deserialize(InetAddress value)
    {
        return ByteBuffer.wrap(value.getAddress());
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        try
        {
            InetAddress.getByAddress(ByteBufferUtil.getArray(bytes));
        }
        catch (UnknownHostException e)
        {
            throw new MarshalException(String.format("Expected 4 or 16 byte inetaddress; got %s", ByteBufferUtil.bytesToHex(bytes)));
        }
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        return serialize(bytes).getHostAddress();
    }

    @Override
    public String toString(InetAddress value)
    {
        return value.getHostAddress();
    }

    @Override
    public Class<InetAddress> getType()
    {
        return InetAddress.class;
    }
}
