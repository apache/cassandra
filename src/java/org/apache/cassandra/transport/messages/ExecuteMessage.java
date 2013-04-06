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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.jboss.netty.buffer.ChannelBuffer;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

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

            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            return new ExecuteMessage(id, values, consistency);
        }

        public ChannelBuffer encode(ExecuteMessage msg)
        {
            // We have:
            //   - statementId
            //   - Number of values
            //   - The values
            //   - options
            int vs = msg.values.size();
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(3, 0, vs);
            builder.add(CBUtil.bytesToCB(msg.statementId.bytes));
            builder.add(CBUtil.shortToCB(vs));

            // Values
            for (ByteBuffer value : msg.values)
                builder.addValue(value);

            builder.add(CBUtil.consistencyLevelToCB(msg.consistency));
            return builder.build();
        }
    };

    public final MD5Digest statementId;
    public final List<ByteBuffer> values;
    public final ConsistencyLevel consistency;

    public ExecuteMessage(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        this(MD5Digest.wrap(statementId), values, consistency);
    }

    public ExecuteMessage(MD5Digest statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.values = values;
        this.consistency = consistency;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            CQLStatement statement = QueryProcessor.getPrepared(statementId);

            if (statement == null)
                throw new PreparedQueryNotFoundException(statementId);

            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();
                // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
                Tracing.instance().begin("Execute CQL3 prepared query", Collections.<String, String>emptyMap());
            }

            Message.Response response = QueryProcessor.processPrepared(statement, consistency, state, values);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance().stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "EXECUTE " + statementId + " with " + values.size() + " values at consistency " + consistency;
    }
}
