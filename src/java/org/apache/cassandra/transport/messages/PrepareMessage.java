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
package org.apache.cassandra.transport.messages;

import java.util.EnumMap;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.InvalidRequestException;

public class PrepareMessage extends Message.Request
{
    public static final Message.Codec<PrepareMessage> codec = new Message.Codec<PrepareMessage>()
    {
        public PrepareMessage decode(ChannelBuffer body)
        {
            String query = CBUtil.readLongString(body);
            return new PrepareMessage(query);
        }

        public ChannelBuffer encode(PrepareMessage msg)
        {
            return CBUtil.longStringToCB(msg.query);
        }
    };

    private final String query;

    public PrepareMessage(String query)
    {
        super(Message.Type.PREPARE);
        this.query = query;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute()
    {
        try
        {
            return QueryProcessor.prepare(query, connection.clientState());
        }
        catch (Exception e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "PREPARE " + query;
    }
}
