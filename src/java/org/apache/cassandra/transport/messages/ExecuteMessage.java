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
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

public class ExecuteMessage extends Message.Request
{
    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ByteBuf body, int version)
        {
            byte[] id = CBUtil.readBytes(body);
            if (version == 1)
            {
                List<ByteBuffer> values = CBUtil.readValueList(body);
                ConsistencyLevel consistency = CBUtil.readConsistencyLevel(body);
                return new ExecuteMessage(MD5Digest.wrap(id), QueryOptions.fromProtocolV1(consistency, values));
            }
            else
            {
                return new ExecuteMessage(MD5Digest.wrap(id), QueryOptions.codec.decode(body, version));
            }
        }

        public void encode(ExecuteMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeBytes(msg.statementId.bytes, dest);
            if (version == 1)
            {
                CBUtil.writeValueList(msg.options.getValues(), dest);
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            }
            else
            {
                QueryOptions.codec.encode(msg.options, dest, version);
            }
        }

        public int encodedSize(ExecuteMessage msg, int version)
        {
            int size = 0;
            size += CBUtil.sizeOfBytes(msg.statementId.bytes);
            if (version == 1)
            {
                size += CBUtil.sizeOfValueList(msg.options.getValues());
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final MD5Digest statementId;
    public final QueryOptions options;

    public ExecuteMessage(MD5Digest statementId, QueryOptions options)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.options = options;
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            QueryHandler handler = state.getClientState().getCQLQueryHandler();
            ParsedStatement.Prepared prepared = handler.getPrepared(statementId);
            if (prepared == null)
                throw new PreparedQueryNotFoundException(statementId);

            options.prepare(prepared.boundNames);
            CQLStatement statement = prepared.statement;

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

            Message.Response response = handler.processPrepared(statement, state, options);
            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows)response).result.metadata.setSkipMetadata();

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
        return "EXECUTE " + statementId + " with " + options.getValues().size() + " values at consistency " + options.getConsistency();
    }
}
