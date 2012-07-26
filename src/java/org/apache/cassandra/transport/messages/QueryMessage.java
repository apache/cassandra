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

import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ChannelBuffer body)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query);
        }

        public ChannelBuffer encode(QueryMessage msg)
        {
            return CBUtil.longStringToCB(msg.query);
        }
    };

    public final String query;

    public QueryMessage(String query)
    {
        super(Message.Type.QUERY);
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
            return QueryProcessor.process(query, connection.clientState());
        }
        catch (Exception e)
        {
            if (!((e instanceof UnavailableException)
               || (e instanceof InvalidRequestException)
               || (e instanceof TimedOutException)
               || (e instanceof SchemaDisagreementException)))
            {
                logger.error("Unexpected error during query", e);
            }
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "QUERY " + query;
    }
}
