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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;

public class ExecuteMessage extends Message.Request
{
    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ChannelBuffer body)
        {
            byte[] id = CBUtil.readBytes(body);

            int count = body.readUnsignedShort();
            List<ByteBuffer> values = new ArrayList<ByteBuffer>(count);
            for (int i = 0; i < count; i++)
                values.add(CBUtil.readValue(body));

            return new ExecuteMessage(id, values);
        }

        public ChannelBuffer encode(ExecuteMessage msg)
        {
            // We have:
            //   - statementId
            //   - Number of values
            //   - The values
            //   - options
            int vs = msg.values.size();
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(2, 0, vs);
            builder.add(CBUtil.bytesToCB(msg.statementId.bytes));
            builder.add(CBUtil.shortToCB(vs));

            // Values
            for (ByteBuffer value : msg.values)
                builder.addValue(value);

            return builder.build();
        }
    };

    public final MD5Digest statementId;
    public final List<ByteBuffer> values;

    public ExecuteMessage(byte[] statementId, List<ByteBuffer> values)
    {
        this(MD5Digest.wrap(statementId), values);
    }

    public ExecuteMessage(MD5Digest statementId, List<ByteBuffer> values)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.values = values;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute()
    {
        try
        {
            ServerConnection c = (ServerConnection)connection;
            CQLStatement statement = QueryProcessor.getPrepared(statementId);

            if (statement == null)
                throw new PreparedQueryNotFoundException(statementId);

            return QueryProcessor.processPrepared(statement, c.clientState(), values);
        }
        catch (Exception e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "EXECUTE " + statementId + " with " + values.size() + " values";
    }
}
