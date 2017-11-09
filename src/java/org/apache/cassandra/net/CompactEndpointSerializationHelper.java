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
import org.apache.cassandra.streaming.messages.StreamMessage;

/*
 * As of version 4.0 the endpoint description includes a port number as an unsigned short
 */
public class CompactEndpointSerializationHelper implements IVersionedSerializer<InetAddressAndPort>
{
    public static final IVersionedSerializer<InetAddressAndPort> instance = new CompactEndpointSerializationHelper();

    /**
     * Streaming uses its own version numbering so we need to ignore it and always use currrent version.
     * There is no cross version streaming so it will always use the latest address serialization.
     **/
    public static final IVersionedSerializer<InetAddressAndPort> streamingInstance = new IVersionedSerializer<InetAddressAndPort>()
    {
        public void serialize(InetAddressAndPort inetAddressAndPort, DataOutputPlus out, int version) throws IOException
        {
            instance.serialize(inetAddressAndPort, out, MessagingService.current_version);
        }

        public InetAddressAndPort deserialize(DataInputPlus in, int version) throws IOException
        {
            return instance.deserialize(in, MessagingService.current_version);
        }

        public long serializedSize(InetAddressAndPort inetAddressAndPort, int version)
        {
            return instance.serializedSize(inetAddressAndPort, MessagingService.current_version);
        }
    };

    private CompactEndpointSerializationHelper() {}

    public void serialize(InetAddressAndPort endpoint, DataOutputPlus out, int version) throws IOException
    {
        if (version >= MessagingService.VERSION_40)
        {
            byte[] buf = endpoint.addressBytes;
            out.writeByte(buf.length + 2);
            out.write(buf);
            out.writeShort(endpoint.port);
        }
        else
        {
            byte[] buf = endpoint.addressBytes;
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
                return InetAddressAndPort.getByAddressOverrideDefaults(InetAddress.getByAddress(bytes), bytes, port);
            }
            default:
                throw new AssertionError("Unexpected size " + size);

        }
    }

    public long serializedSize(InetAddressAndPort from, int version)
    {
        //4.0 includes a port number
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
}
