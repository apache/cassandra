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
import java.net.InetAddress;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class Header
{
    private static final IVersionedSerializer<Header> serializer;

    static
    {
        serializer = new HeaderSerializer();
    }

    public static IVersionedSerializer<Header> serializer()
    {
        return serializer;
    }

    // "from" is the ultimate origin of this request (the coordinator), which in a multi-DC setup
    // is not necessarily the same as the node that forwards us the request (see StorageProxy.sendMessages
    // and RowMutationVerbHandler.forwardToLocalNodes)
    private final InetAddress from;
    private final MessagingService.Verb verb;
    protected final Map<String, byte[]> details;

    Header(InetAddress from, MessagingService.Verb verb)
    {
        this(from, verb, Collections.<String, byte[]>emptyMap());
    }

    Header(InetAddress from, MessagingService.Verb verb, Map<String, byte[]> details)
    {
        assert from != null;
        assert verb != null;

        this.from = from;
        this.verb = verb;
        this.details = ImmutableMap.copyOf(details);
    }

    InetAddress getFrom()
    {
        return from;
    }

    MessagingService.Verb getVerb()
    {
        return verb;
    }

    byte[] getDetail(String key)
    {
        return details.get(key);
    }

    Header withDetailsAdded(String key, byte[] value)
    {
        Map<String, byte[]> detailsCopy = Maps.newHashMap(details);
        detailsCopy.put(key, value);
        return new Header(from, verb, detailsCopy);
    }

    Header withDetailsRemoved(String key)
    {
        if (!details.containsKey(key))
            return this;
        Map<String, byte[]> detailsCopy = Maps.newHashMap(details);
        detailsCopy.remove(key);
        return new Header(from, verb, detailsCopy);
    }

    public int serializedSize()
    {
        int size = 0;
        size += CompactEndpointSerializationHelper.serializedSize(getFrom());
        size += 4;
        size += 4;
        for (String key : details.keySet())
        {
            size += 2 + FBUtilities.encodedUTF8Length(key);
            byte[] value = details.get(key);
            size += 4 + value.length;
        }
        return size;
    }
}

class HeaderSerializer implements IVersionedSerializer<Header>
{
    public void serialize(Header t, DataOutput dos, int version) throws IOException
    {
        CompactEndpointSerializationHelper.serialize(t.getFrom(), dos);
        dos.writeInt(t.getVerb().ordinal());
        dos.writeInt(t.details.size());
        for (String key : t.details.keySet())
        {
            dos.writeUTF(key);
            byte[] value = t.details.get(key);
            dos.writeInt(value.length);
            dos.write(value);
        }
    }

    public Header deserialize(DataInput dis, int version) throws IOException
    {
        InetAddress from = CompactEndpointSerializationHelper.deserialize(dis);
        int verbOrdinal = dis.readInt();
        int size = dis.readInt();
        Map<String, byte[]> details = new Hashtable<String, byte[]>(size);
        for ( int i = 0; i < size; ++i )
        {
            String key = dis.readUTF();
            int length = dis.readInt();
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            details.put(key, bytes);
        }
        return new Header(from, MessagingService.VERBS[verbOrdinal], details);
    }

    public long serializedSize(Header header, int version)
    {
        throw new UnsupportedOperationException();
    }
}


