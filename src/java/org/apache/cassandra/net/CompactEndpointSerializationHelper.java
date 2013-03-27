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

public class CompactEndpointSerializationHelper
{
    public static void serialize(InetAddress endpoint, DataOutput out) throws IOException
    {
        byte[] buf = endpoint.getAddress();
        out.writeByte(buf.length);
        out.write(buf);
    }

    public static InetAddress deserialize(DataInput in) throws IOException
    {
        byte[] bytes = new byte[in.readByte()];
        in.readFully(bytes, 0, bytes.length);
        return InetAddress.getByAddress(bytes);
    }

    public static int serializedSize(InetAddress from)
    {
        if (from instanceof Inet4Address)
            return 1 + 4;
        assert from instanceof Inet6Address;
        return 1 + 16;
    }
}
