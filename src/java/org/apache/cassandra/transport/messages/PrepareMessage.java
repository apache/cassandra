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

import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareMessage extends Message.Request
{
    public static final Message.Codec<PrepareMessage> codec = new Message.Codec<PrepareMessage>()
    {
        public PrepareMessage decode(ByteBuf body, int version)
        {
            String query = CBUtil.readLongString(body);
            return new PrepareMessage(query);
        }

        public void encode(PrepareMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeLongString(msg.query, dest);
        }

        public int encodedSize(PrepareMessage msg, int version)
        {
            return CBUtil.sizeOfLongString(msg.query);
        }
    };

    private final String query;

    public PrepareMessage(String query)
    {
        super(Message.Type.PREPARE);
        this.query = query;
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();
                Tracing.instance.begin("Preparing CQL3 query", ImmutableMap.of("query", query));
            }

            Message.Response response = state.getClientState().getCQLQueryHandler().prepare(query, state);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            return ErrorMessage.fromException(e);
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "PREPARE " + query;
    }
}
