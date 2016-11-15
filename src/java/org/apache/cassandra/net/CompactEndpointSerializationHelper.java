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
package org.apache.cassandra.net;

import java.io.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

/*
 * As of version 4.0 the endpoint description includes a port number as an unsigned short
 */
public class CompactEndpointSerializationHelper implements IVersionedSerializer<InetAddressAndPort>
{
    public static final CompactEndpointSerializationHelper instance = new CompactEndpointSerializationHelper();

    private CompactEndpointSerializationHelper() {}

    public void serialize(InetAddressAndPort endpoint, DataOutputPlus out, int version) throws IOException
    {
        if (version >= MessagingService.VERSION_40)
        {
            byte[] buf = endpoint.address.getAddress();
            out.writeByte(buf.length + 2);
            out.write(buf);
            out.writeShort(endpoint.port);
        }
        else
        {
            byte[] buf = endpoint.address.getAddress();
            out.writeByte(buf.length);
            out.write(buf);
        }
    }

    public InetAddressAndPort deserialize(DataInputPlus in, int version) throws IOException
    {
        int size = in.readByte() & 0xFF;
        switch(size)
        {
            //The original pre-4.0 serialiation of just an address
            case 4:
            case 16:
            {
                byte[] bytes = new byte[size];
                in.readFully(bytes, 0, bytes.length);
                return InetAddressAndPort.getByAddress(bytes);
            }
            //Address and one port
            case 6:
            case 18:
            {
                byte[] bytes = new byte[size - 2];
                in.readFully(bytes);

                int port = in.readShort() & 0xFFFF;
                return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), port);
            }
            default:
                throw new AssertionError("Unexpected size " + size);

        }
    }

    public long serializedSize(InetAddressAndPort from, int version)
    {
        //4.0 includes port numbers for regular and SSL
        if (version >= MessagingService.VERSION_40)
        {
            if (from.address instanceof Inet4Address)
                return 1 + 4 + 2;
            assert from.address instanceof Inet6Address;
            return 1 + 16 + 2;
        }
        else
        {
            if (from.address instanceof Inet4Address)
                return 1 + 4;
            assert from.address instanceof Inet6Address;
            return 1 + 16;
        }
    }

    public static InetAddressAndPort fromBytes(ByteBuffer buffer, int version)
    {
        try (DataInputBuffer input = new DataInputBuffer(buffer, false))
        {
            return instance.deserialize(input, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

}
