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
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<QueryMessage>()
    {
        public QueryMessage decode(ChannelBuffer body, int version)
        {
            String query = CBUtil.readLongString(body);
            ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
            List<ByteBuffer> values = new ArrayList<ByteBuffer>();
            if (body.readable())
            {
                int paramCount = body.readUnsignedShort();
                for (int i = 0; i < paramCount; i++)
                     values.add(CBUtil.readValue(body));
            }
            return new QueryMessage(query, values, consistency);
        }

        public ChannelBuffer encode(QueryMessage msg)
        {
            // We have:
            //   - query
            //   - options
            //     * optional:
            //   - Number of values
            //   - The values
            int vs = msg.values.size();
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(3, 0, vs);
            builder.add(CBUtil.longStringToCB(msg.query));
            builder.add(CBUtil.consistencyLevelToCB(msg.consistency));
            if (vs > 0 && msg.getVersion() > 1)
            {
                builder.add(CBUtil.shortToCB(vs));
                for (ByteBuffer value : msg.values)
                    builder.addValue(value);
            }
            return builder.build();
        }
    };

    public final String query;
    public final ConsistencyLevel consistency;
    public final List<ByteBuffer> values;

    public QueryMessage(String query, ConsistencyLevel consistency)
    {
        this(query, Collections.<ByteBuffer>emptyList(), consistency);
    }

    public QueryMessage(String query, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        super(Type.QUERY);
        this.query = query;
        this.consistency = consistency;
        this.values = values;
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
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
                Tracing.instance().begin("Execute CQL3 query", ImmutableMap.of("query", query));
            }

            Message.Response response = QueryProcessor.process(query, values, consistency, state);

            if (tracingId != null)
                response.setTracingId(tracingId);

            return response;
        }
        catch (Exception e)
        {
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
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
        return "QUERY " + query;
    }
}
