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

import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.UUIDGen;

public class PrepareMessage extends Message.Request
{
    public static final Message.Codec<PrepareMessage> codec = new Message.Codec<PrepareMessage>()
    {
        public PrepareMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            String keyspace = null;
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5)) {
                // If flags grows, we may want to consider creating a PrepareOptions class with an internal codec
                // class that handles flags and options of the prepare message. Since there's only one right now,
                // we just take care of business here.

                int flags = (int)body.readUnsignedInt();
                if ((flags & 0x1) == 0x1)
                    keyspace = CBUtil.readString(body);
            }
            return new PrepareMessage(query, keyspace);
        }

        public void encode(PrepareMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                // If we have no keyspace, write out a 0-valued flag field.
                if (msg.keyspace == null)
                    dest.writeInt(0x0);
                else {
                    dest.writeInt(0x1);
                    CBUtil.writeString(msg.keyspace, dest);
                }
            }
        }

        public int encodedSize(PrepareMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
            {
                // We always emit a flags int
                size += 4;

                // If we have a keyspace, we'd write it out. Otherwise, we'd write nothing.
                size += msg.keyspace == null
                    ? 0
                    : CBUtil.sizeOfString(msg.keyspace);
            }
            return size;
        }
    };

    private final String query;
    private final String keyspace;

    public PrepareMessage(String query, String keyspace)
    {
        super(Message.Type.PREPARE);
        this.query = query;
        this.keyspace = keyspace;
    }

    public Message.Response execute(QueryState state, long queryStartNanoTime)
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
                state.createTracingSession(getCustomPayload());
                Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", query));
            }

            Message.Response response = ClientState.getCQLQueryHandler().prepare(query,
                                                                                 state.getClientState().cloneWithKeyspaceIfSet(keyspace),
                                                                                 getCustomPayload());

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
