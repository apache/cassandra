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
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.UUIDGen;

public class BatchMessage extends Message.Request
{
    public static final Message.Codec<BatchMessage> codec = new Message.Codec<BatchMessage>()
    {
        public BatchMessage decode(ByteBuf body, ProtocolVersion version)
        {
            byte type = body.readByte();
            int n = body.readUnsignedShort();
            List<Object> queryOrIds = new ArrayList<>(n);
            List<List<ByteBuffer>> variables = new ArrayList<>(n);
            for (int i = 0; i < n; i++)
            {
                byte kind = body.readByte();
                if (kind == 0)
                    queryOrIds.add(CBUtil.readLongString(body));
                else if (kind == 1)
                    queryOrIds.add(MD5Digest.wrap(CBUtil.readBytes(body)));
                else
                    throw new ProtocolException("Invalid query kind in BATCH messages. Must be 0 or 1 but got " + kind);
                variables.add(CBUtil.readValueList(body, version));
            }
            QueryOptions options = QueryOptions.codec.decode(body, version);

            return new BatchMessage(toType(type), queryOrIds, variables, options);
        }

        public void encode(BatchMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            int queries = msg.queryOrIdList.size();

            dest.writeByte(fromType(msg.batchType));
            dest.writeShort(queries);

            for (int i = 0; i < queries; i++)
            {
                Object q = msg.queryOrIdList.get(i);
                dest.writeByte((byte)(q instanceof String ? 0 : 1));
                if (q instanceof String)
                    CBUtil.writeLongString((String)q, dest);
                else
                    CBUtil.writeBytes(((MD5Digest)q).bytes, dest);

                CBUtil.writeValueList(msg.values.get(i), dest);
            }

            if (version.isSmallerThan(ProtocolVersion.V3))
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(BatchMessage msg, ProtocolVersion version)
        {
            int size = 3; // type + nb queries
            for (int i = 0; i < msg.queryOrIdList.size(); i++)
            {
                Object q = msg.queryOrIdList.get(i);
                size += 1 + (q instanceof String
                             ? CBUtil.sizeOfLongString((String)q)
                             : CBUtil.sizeOfBytes(((MD5Digest)q).bytes));

                size += CBUtil.sizeOfValueList(msg.values.get(i));
            }
            size += version.isSmallerThan(ProtocolVersion.V3)
                  ? CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency())
                  : QueryOptions.codec.encodedSize(msg.options, version);
            return size;
        }

        private BatchStatement.Type toType(byte b)
        {
            if (b == 0)
                return BatchStatement.Type.LOGGED;
            else if (b == 1)
                return BatchStatement.Type.UNLOGGED;
            else if (b == 2)
                return BatchStatement.Type.COUNTER;
            else
                throw new ProtocolException("Invalid BATCH message type " + b);
        }

        private byte fromType(BatchStatement.Type type)
        {
            switch (type)
            {
                case LOGGED:   return 0;
                case UNLOGGED: return 1;
                case COUNTER:  return 2;
                default:
                    throw new AssertionError();
            }
        }
    };

    public final BatchStatement.Type batchType;
    public final List<Object> queryOrIdList;
    public final List<List<ByteBuffer>> values;
    public final QueryOptions options;

    public BatchMessage(BatchStatement.Type type, List<Object> queryOrIdList, List<List<ByteBuffer>> values, QueryOptions options)
    {
        super(Message.Type.BATCH);
        this.batchType = type;
        this.queryOrIdList = queryOrIdList;
        this.values = values;
        this.options = options;
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

                ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                if(options.getConsistency() != null)
                    builder.put("consistency_level", options.getConsistency().name());
                if(options.getSerialConsistency() != null)
                    builder.put("serial_consistency_level", options.getSerialConsistency().name());

                // TODO we don't have [typed] access to CQL bind variables here.  CASSANDRA-4560 is open to add support.
                Tracing.instance.begin("Execute batch of CQL3 queries", state.getClientAddress(), builder.build());
            }

            QueryHandler handler = ClientState.getCQLQueryHandler();
            List<ParsedStatement.Prepared> prepared = new ArrayList<>(queryOrIdList.size());
            for (int i = 0; i < queryOrIdList.size(); i++)
            {
                Object query = queryOrIdList.get(i);
                ParsedStatement.Prepared p;
                if (query instanceof String)
                {
                    p = QueryProcessor.parseStatement((String)query, state);
                }
                else
                {
                    p = handler.getPrepared((MD5Digest)query);
                    if (p == null)
                        throw new PreparedQueryNotFoundException((MD5Digest)query);
                }

                List<ByteBuffer> queryValues = values.get(i);
                if (queryValues.size() != p.statement.getBoundTerms())
                    throw new InvalidRequestException(String.format("There were %d markers(?) in CQL but %d bound variables",
                                                                    p.statement.getBoundTerms(),
                                                                    queryValues.size()));

                prepared.add(p);
            }

            BatchQueryOptions batchOptions = BatchQueryOptions.withPerStatementVariables(options, values, queryOrIdList);
            List<ModificationStatement> statements = new ArrayList<>(prepared.size());
            for (int i = 0; i < prepared.size(); i++)
            {
                ParsedStatement.Prepared p = prepared.get(i);
                batchOptions.prepareStatement(i, p.boundNames);

                if (!(p.statement instanceof ModificationStatement))
                    throw new InvalidRequestException("Invalid statement in batch: only UPDATE, INSERT and DELETE statements are allowed.");

                statements.add((ModificationStatement)p.statement);
            }

            // Note: It's ok at this point to pass a bogus value for the number of bound terms in the BatchState ctor
            // (and no value would be really correct, so we prefer passing a clearly wrong one).
            BatchStatement batch = new BatchStatement(-1, batchType, statements, Attributes.none());
            Message.Response response = handler.processBatch(batch, state, batchOptions, getCustomPayload(), queryStartNanoTime);

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
        StringBuilder sb = new StringBuilder();
        sb.append("BATCH of [");
        for (int i = 0; i < queryOrIdList.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(queryOrIdList.get(i)).append(" with ").append(values.get(i).size()).append(" values");
        }
        sb.append("] at consistency ").append(options.getConsistency());
        return sb.toString();
    }
}
