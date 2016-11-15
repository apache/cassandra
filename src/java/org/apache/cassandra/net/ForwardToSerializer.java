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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class ForwardToSerializer implements IVersionedSerializer<ForwardToContainer>
{
    public static ForwardToSerializer instance = new ForwardToSerializer();

    private ForwardToSerializer() {}

    public void serialize(ForwardToContainer forwardToContainer, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(forwardToContainer.targets.size());
        Iterator<InetAddressAndPort> iter = forwardToContainer.targets.iterator();
        for (int ii = 0; ii < forwardToContainer.messageIds.length; ii++)
        {
            CompactEndpointSerializationHelper.instance.serialize(iter.next(), out, version);
            out.writeInt(forwardToContainer.messageIds[ii]);
        }
    }

    public ForwardToContainer deserialize(DataInputPlus in, int version) throws IOException
    {
        int[] ids = new int[in.readInt()];
        List<InetAddressAndPort> hosts = new ArrayList<>(ids.length);
        for (int ii = 0; ii < ids.length; ii++)
        {
           hosts.add(CompactEndpointSerializationHelper.instance.deserialize(in, version));
           ids[ii] = in.readInt();
        }
        return new ForwardToContainer(hosts, ids);
    }

    public long serializedSize(ForwardToContainer forwardToContainer, int version)
    {
        //Number of forward addresses, 4 bytes per for each id
        long size = 4 +
                    (4 * forwardToContainer.targets.size());
        //Depending on ipv6 or ipv4 the address size is different.
        for (InetAddressAndPort forwardTo : forwardToContainer.targets)
        {
            size += CompactEndpointSerializationHelper.instance.serializedSize(forwardTo, version);
        }

        return size;
    }

    public static ForwardToContainer fromBytes(byte[] bytes, int version)
    {
        try (DataInputBuffer input = new DataInputBuffer(bytes))
        {
            return instance.deserialize(input, version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
