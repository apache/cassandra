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

import java.lang.reflect.Array;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.log4j.Logger;
import org.apache.cassandra.utils.*;
/**
 * Author : Avinash Lakshman ( alakshman@facebook.com) & Prashant Malik ( pmalik@facebook.com )
 */

public class Message implements java.io.Serializable
{    
    static final long serialVersionUID = 6329198792470413221L;
    private static ICompactSerializer<Message> serializer_;
    
    static
    {
        serializer_ = new MessageSerializer();        
    }
    
    public static ICompactSerializer<Message> serializer()
    {
        return serializer_;
    }
    
    Header header_;
    private Object[] body_ = new Object[0];
    
    /* Ctor for JAXB. DO NOT DELETE */
    private Message()
    {
    }

    protected Message(String id, EndPoint from, String messageType, String verb, Object[] body)
    {
        header_ = new Header(id, from, messageType, verb);
        body_ = body;
    }
    
    protected Message(Header header, Object[] body)
    {
        header_ = header;
        body_ = body;
    }

    public Message(EndPoint from, String messageType, String verb, Object[] body)
    {
        header_ = new Header(from, messageType, verb);
        body_ = body;
    }    
    
    public byte[] getHeader(Object key)
    {
        return header_.getDetail(key);
    }
    
    public void removeHeader(Object key)
    {
        header_.removeDetail(key);
    }
    
    public void setMessageType(String type)
    {
        header_.setMessageType(type);
    }

    public void setMessageVerb(String verb)
    {
        header_.setMessageVerb(verb);
    }

    public void addHeader(String key, byte[] value)
    {
        header_.addDetail(key, value);
    }
    
    public Map<String, byte[]> getHeaders()
    {
        return header_.getDetails();
    }

    public Object[] getMessageBody()
    {
        return body_;
    }
    
    public void setMessageBody(Object[] body)
    {
        body_ = body;
    }

    public EndPoint getFrom()
    {
        return header_.getFrom();
    }

    public String getMessageType()
    {
        return header_.getMessageType();
    }

    public String getVerb()
    {
        return header_.getVerb();
    }

    public String getMessageId()
    {
        return header_.getMessageId();
    }
    
    public Class[] getTypes()
    {
        List<Class> types = new ArrayList<Class>();
        
        for ( int i = 0; i < body_.length; ++i )
        {
            if ( body_[i].getClass().isArray() )
            {
                int size = Array.getLength(body_[i]);
                if ( size > 0 )
                {
                    types.add( Array.get( body_[i], 0).getClass() );
                }
            }
            else
            {
                types.add(body_[i].getClass());
            }
        }
        
        return types.toArray( new Class[0] );
    }    

    void setMessageId(String id)
    {
        header_.setMessageId(id);
    }    

    public Message getReply(EndPoint from, Object[] args)
    {        
        Message response = new Message(getMessageId(),
                                       from,
                                       MessagingService.responseStage_,
                                       MessagingService.responseVerbHandler_,
                                       args);
        return response;
    }
    
    public String toString()
    {
        StringBuffer sbuf = new StringBuffer("");
        String separator = System.getProperty("line.separator");
        sbuf.append("ID:" + getMessageId());
        sbuf.append(separator);
        sbuf.append("FROM:" + getFrom());
        sbuf.append(separator);
        sbuf.append("TYPE:" + getMessageType());
        sbuf.append(separator);
        sbuf.append("VERB:" + getVerb());
        sbuf.append(separator);
        sbuf.append("BODY TYPE:" + getBodyTypes());        
        sbuf.append(separator);
        return sbuf.toString();
    }
    
    private String getBodyTypes()
    {
        StringBuffer sbuf = new StringBuffer("");
        Class[] types = getTypes();
        for ( int i = 0; i < types.length; ++i )
        {
            sbuf.append(types[i].getName());
            sbuf.append(" ");         
        }
        return sbuf.toString();
    }    
}

class MessageSerializer implements ICompactSerializer<Message>
{
    public void serialize(Message t, DataOutputStream dos) throws IOException
    {
        Header.serializer().serialize( t.header_, dos);
        byte[] bytes = (byte[])t.getMessageBody()[0];
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
        return new Message(header, new Object[]{bytes});
    }
}
