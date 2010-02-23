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
import java.util.Map;
import java.net.InetAddress;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.service.StorageService;

public class Message
{
    private static MessageSerializer serializer_;
    
    static
    {
        serializer_ = new MessageSerializer();        
    }
    
    public static MessageSerializer serializer()
    {
        return serializer_;
    }
    
    final Header header_;
    private final byte[] body_;

    Message(Header header, byte[] body)
    {
        assert header != null;
        assert body != null;

        header_ = header;
        body_ = body;
    }

    public Message(InetAddress from, String messageType, StorageService.Verb verb, byte[] body)
    {
        this(new Header(from, messageType, verb), body);
    }    
    
    public byte[] getHeader(Object key)
    {
        return header_.getDetail(key);
    }
    
    public void addHeader(String key, byte[] value)
    {
        header_.addDetail(key, value);
    }
    
    public Map<String, byte[]> getHeaders()
    {
        return header_.getDetails();
    }

    public byte[] getMessageBody()
    {
        return body_;
    }

    public InetAddress getFrom()
    {
        return header_.getFrom();
    }

    public String getMessageType()
    {
        return header_.getMessageType();
    }

    public StorageService.Verb getVerb()
    {
        return header_.getVerb();
    }

    public String getMessageId()
    {
        return header_.getMessageId();
    }

    void setMessageId(String id)
    {
        header_.setMessageId(id);
    }    

    // TODO should take byte[] + length so we don't have to copy to a byte[] of exactly the right len
    public Message getReply(InetAddress from, byte[] args)
    {
        Header header = new Header(getMessageId(), from, StageManager.RESPONSE_STAGE, StorageService.Verb.READ_RESPONSE);
        return new Message(header, args);
    }
    
    public String toString()
    {
        StringBuilder sbuf = new StringBuilder("");
        String separator = System.getProperty("line.separator");
        sbuf.append("ID:" + getMessageId())
        	.append(separator)
        	.append("FROM:" + getFrom())
        	.append(separator)
        	.append("TYPE:" + getMessageType())
        	.append(separator)
        	.append("VERB:" + getVerb())
        	.append(separator);
        return sbuf.toString();
    }
}

class MessageSerializer implements ICompactSerializer<Message>
{
    public void serialize(Message t, DataOutputStream dos) throws IOException
    {
        Header.serializer().serialize( t.header_, dos);
        byte[] bytes = t.getMessageBody();
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public Message deserialize(DataInputStream dis) throws IOException
    {
        Header header = Header.serializer().deserialize(dis);
        int size = dis.readInt();
        byte[] bytes = new byte[size];
        dis.readFully(bytes);
        // return new Message(header.getMessageId(), header.getFrom(), header.getMessageType(), header.getVerb(), new Object[]{bytes});
        return new Message(header, bytes);
    }
}
