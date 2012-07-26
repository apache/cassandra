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
import org.apache.cassandra.transport.*;
import org.apache.cassandra.thrift.InvalidRequestException;

public class ExecuteMessage extends Message.Request
{
    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ChannelBuffer body)
        {
            int id = body.readInt();

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
            builder.add(CBUtil.intToCB(msg.statementId));
            builder.add(CBUtil.shortToCB(vs));

            // Values
            for (ByteBuffer value : msg.values)
                builder.addValue(value);

            return builder.build();
        }
    };

    public final int statementId;
    public final List<ByteBuffer> values;

    public ExecuteMessage(int statementId, List<ByteBuffer> values)
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
            CQLStatement statement = connection.clientState().getCQL3Prepared().get(statementId);

            if (statement == null)
                throw new InvalidRequestException(String.format("Prepared query with ID %d not found", statementId));

            return QueryProcessor.processPrepared(statement, connection.clientState(), values);
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
