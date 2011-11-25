/**
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;

public class Header
{
    private static ICompactSerializer<Header> serializer_;

    static
    {
        serializer_ = new HeaderSerializer();        
    }
    
    static ICompactSerializer<Header> serializer()
    {
        return serializer_;
    }

    private final InetAddress from_;
    // TODO STAGE can be determined from verb
    private final StorageService.Verb verb_;
    protected Map<String, byte[]> details_ = new Hashtable<String, byte[]>();

    Header(InetAddress from, StorageService.Verb verb)
    {
        assert from != null;
        assert verb != null;

        from_ = from;
        verb_ = verb;
    }

    Header(InetAddress from, StorageService.Verb verb, Map<String, byte[]> details)
    {
        this(from, verb);
        details_ = details;
    }

    InetAddress getFrom()
    {
        return from_;
    }

    StorageService.Verb getVerb()
    {
        return verb_;
    }
    
    byte[] getDetail(String key)
    {
        return details_.get(key);
    }

    void setDetail(String key, byte[] value)
    {
        details_.put(key, value);
    }

    void removeDetail(String key)
    {
        details_.remove(key);
    }
}

class HeaderSerializer implements ICompactSerializer<Header>
{
    public void serialize(Header t, DataOutputStream dos, int version) throws IOException
    {           
        CompactEndpointSerializationHelper.serialize(t.getFrom(), dos);
        dos.writeInt(t.getVerb().ordinal());
        
        /* Serialize the message header */
        int size = t.details_.size();
        dos.writeInt(size);
        Set<String> keys = t.details_.keySet();
        
        for( String key : keys )
        {
            dos.writeUTF(key);
            byte[] value = t.details_.get(key);
            dos.writeInt(value.length);
            dos.write(value);
        }
    }

    public Header deserialize(DataInputStream dis, int version) throws IOException
    {
        InetAddress from = CompactEndpointSerializationHelper.deserialize(dis);
        int verbOrdinal = dis.readInt();
        
        /* Deserializing the message header */
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
        
        return new Header(from, StorageService.VERBS[verbOrdinal], details);
    }
}


