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
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

public class ExecuteMessage extends Message.Request
{
    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ChannelBuffer body, int version)
        {
            byte[] id = CBUtil.readBytes(body);

            if (version == 1)
            {
                int count = body.readUnsignedShort();
                List<ByteBuffer> values = new ArrayList<ByteBuffer>(count);
                for (int i = 0; i < count; i++)
                    values.add(CBUtil.readValue(body));

                ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
                return new ExecuteMessage(id, values, consistency);
            }
            else
            {
                return new ExecuteMessage(MD5Digest.wrap(id), QueryOptions.codec.decode(body, version));
            }
        }

        public ChannelBuffer encode(ExecuteMessage msg, int version)
        {
            ChannelBuffer idBuffer = CBUtil.bytesToCB(msg.statementId.bytes);
            ChannelBuffer optBuffer;
            if (version == 1)
            {
                CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(2, 0, msg.options.getValues().size());
                builder.add(CBUtil.shortToCB(msg.options.getValues().size()));

                // Values
                for (ByteBuffer value : msg.options.getValues())
                    builder.addValue(value);

                builder.add(CBUtil.consistencyLevelToCB(msg.options.getConsistency()));
                optBuffer = builder.build();
            }
            else
            {
                optBuffer = QueryOptions.codec.encode(msg.options, version);
            }
            return ChannelBuffers.wrappedBuffer(idBuffer, optBuffer);
        }
    };

    public final MD5Digest statementId;
    public final QueryOptions options;

    public ExecuteMessage(byte[] statementId, List<ByteBuffer> values, ConsistencyLevel consistency)
    {
        this(MD5Digest.wrap(statementId), new QueryOptions(consistency, values));
    }

    public ExecuteMessage(MD5Digest statementId, QueryOptions options)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.options = options;
    }

    public ChannelBuffer encode(int version)
    {
        return codec.encode(this, version);
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            CQLStatement statement = QueryProcessor.getPrepared(statementId);

            if (statement == null)
                throw new PreparedQueryNotFoundException(statementId);

            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            UUID tracingId = null;
            if (isTracingRequested())
            {
                tracingId = UUIDGen.getTimeUUID();
                state.prepareTracingSession(tracingId);
            }

            if (state.traceNextQuery())
            {
                state.createTracingSession();

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                if (options.getPageSize() > 0)
                    builder.put("page_size", Integer.toString(options.getPageSize()));

                // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
                Tracing.instance.begin("Execute CQL3 prepared query", builder.build());
            }

            Message.Response response = QueryProcessor.processPrepared(statement, state, options);
            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows)response).result.metadata.setSkipMetadata();

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
            Tracing.instance.stopSession();
        }
    }

    @Override
    public String toString()
    {
        return "EXECUTE " + statementId + " with " + options.getValues().size() + " values at consistency " + options.getConsistency();
    }
}
