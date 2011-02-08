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

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class Message
{
    private static ICompactSerializer<Message> serializer_;

    static
    {
        serializer_ = new MessageSerializer();        
    }
    
    public static ICompactSerializer<Message> serializer()
    {
        return serializer_;
    }
    
    final Header header_;
    private final byte[] body_;
    private final transient int version;

    private Message(Header header, byte[] body, int version)
    {
        assert header != null;
        assert body != null;

        header_ = header;
        body_ = body;
        this.version = version;
    }
    
    public Message(InetAddress from, StorageService.Verb verb, byte[] body, int version)
    {
        this(new Header(from, verb), body, version);
    } 
        
    public byte[] getHeader(String key)
    {
        return header_.getDetail(key);
    }
    
    public void setHeader(String key, byte[] value)
    {
        header_.setDetail(key, value);
    }
    
    public void removeHeader(String key)
    {
        header_.removeDetail(key);
    }

    public byte[] getMessageBody()
    {
        return body_;
    }
    
    public int getVersion()
    {
        return version;
    }

    public InetAddress getFrom()
    {
        return header_.getFrom();
    }

    public Stage getMessageType()
    {
        return StorageService.verbStages.get(getVerb());
    }

    public StorageService.Verb getVerb()
    {
        return header_.getVerb();
    }

    // TODO should take byte[] + length so we don't have to copy to a byte[] of exactly the right len
    // TODO make static
    public Message getReply(InetAddress from, byte[] body, int version)
    {
        Header header = new Header(from, StorageService.Verb.REQUEST_RESPONSE);
        return new Message(header, body, version);
    }

    public Message getInternalReply(byte[] body, int version)
    {
        Header header = new Header(FBUtilities.getLocalAddress(), StorageService.Verb.INTERNAL_RESPONSE);
        return new Message(header, body, version);
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder("");
        String separator = System.getProperty("line.separator");
        sbuf.append("FROM:" + getFrom())
        	.append(separator)
        	.append("TYPE:" + getMessageType())
        	.append(separator)
        	.append("VERB:" + getVerb())
        	.append(separator);
        return sbuf.toString();
    }
    
    private static class MessageSerializer implements ICompactSerializer<Message>
    {
        public void serialize(Message t, DataOutputStream dos, int version) throws IOException
        {
            assert t.getVersion() == version : "internode protocol version mismatch"; // indicates programmer error.
            Header.serializer().serialize( t.header_, dos, version);
            byte[] bytes = t.getMessageBody();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    
        public Message deserialize(DataInputStream dis, int version) throws IOException
        {
            Header header = Header.serializer().deserialize(dis, version);
            int size = dis.readInt();
            byte[] bytes = new byte[size];
            dis.readFully(bytes);
            return new Message(header, bytes, version);
        }
    }
}
