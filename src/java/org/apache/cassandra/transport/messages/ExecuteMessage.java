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

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;

public class ExecuteMessage extends Message.Request
{
    public static final Message.Codec<ExecuteMessage> codec = new Message.Codec<ExecuteMessage>()
    {
        public ExecuteMessage decode(ByteBuf body, ProtocolVersion version)
        {
            MD5Digest statementId = MD5Digest.wrap(CBUtil.readBytes(body));

            MD5Digest resultMetadataId = null;
            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));

            return new ExecuteMessage(statementId, resultMetadataId, QueryOptions.codec.decode(body, version));
        }

        public void encode(ExecuteMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeBytes(msg.statementId.bytes, dest);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                CBUtil.writeBytes(msg.resultMetadataId.bytes, dest);

            if (version == ProtocolVersion.V1)
            {
                CBUtil.writeValueList(msg.options.getValues(), dest);
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            }
            else
            {
                QueryOptions.codec.encode(msg.options, dest, version);
            }
        }

        public int encodedSize(ExecuteMessage msg, ProtocolVersion version)
        {
            int size = 0;
            size += CBUtil.sizeOfBytes(msg.statementId.bytes);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                size += CBUtil.sizeOfBytes(msg.resultMetadataId.bytes);

            if (version == ProtocolVersion.V1)
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
    public final MD5Digest resultMetadataId;
    public final QueryOptions options;

    public ExecuteMessage(MD5Digest statementId, MD5Digest resultMetadataId, QueryOptions options)
    {
        super(Message.Type.EXECUTE);
        this.statementId = statementId;
        this.options = options;
        this.resultMetadataId = resultMetadataId;
    }

    @Override
    protected boolean isTraceable()
    {
        return true;
    }

    @Override
    protected Message.Response execute(QueryState state, long queryStartNanoTime, boolean traceRequest)
    {
        AuditLogManager auditLogManager = AuditLogManager.getInstance();

        try
        {
            QueryHandler handler = ClientState.getCQLQueryHandler();
            QueryHandler.Prepared prepared = handler.getPrepared(statementId);
            if (prepared == null)
                throw new PreparedQueryNotFoundException(statementId);

            CQLStatement statement = prepared.statement;
            options.prepare(statement.getBindVariables());

            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            if (traceRequest)
                traceQuery(state, prepared);

            // Some custom QueryHandlers are interested by the bound names. We provide them this information
            // by wrapping the QueryOptions.
            QueryOptions queryOptions = QueryOptions.addColumnSpecifications(options, prepared.statement.getBindVariables());

            long requestStartTime = auditLogManager.isLoggingEnabled() ? System.currentTimeMillis() : 0L;

            Message.Response response = handler.processPrepared(statement, state, queryOptions, getCustomPayload(), queryStartNanoTime);

            if (auditLogManager.isLoggingEnabled())
                logSuccess(state, prepared, requestStartTime);

            if (response instanceof ResultMessage.Rows)
            {
                ResultMessage.Rows rows = (ResultMessage.Rows) response;

                ResultSet.ResultMetadata resultMetadata = rows.result.metadata;

                if (options.getProtocolVersion().isGreaterOrEqualTo(ProtocolVersion.V5))
                {
                    // For LWTs, always send a resultset metadata but avoid setting a metadata changed flag. This way
                    // Client will always receive fresh metadata, but will avoid caching and reusing it. See CASSANDRA-13992
                    // for details.
                    if (!statement.hasConditions())
                    {
                        // Starting with V5 we can rely on the result metadata id coming with execute message in order to
                        // check if there was a change, comparing it with metadata that's about to be returned to client.
                        if (!resultMetadata.getResultMetadataId().equals(resultMetadataId))
                            resultMetadata.setMetadataChanged();
                        else if (options.skipMetadata())
                            resultMetadata.setSkipMetadata();
                    }
                }
                else
                {
                    // Pre-V5 code has to rely on the difference between the metadata in the prepared message cache
                    // and compare it with the metadata to be returned to client.
                    if (options.skipMetadata() && prepared.resultMetadataId.equals(resultMetadata.getResultMetadataId()))
                        resultMetadata.setSkipMetadata();
                }
            }

            return response;
        }
        catch (Exception e)
        {
            if (auditLogManager.isAuditingEnabled())
                logException(state, e);
            JVMStabilityInspector.inspectThrowable(e);
            return ErrorMessage.fromException(e);
        }
    }

    private void traceQuery(QueryState state, QueryHandler.Prepared prepared)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (options.getPageSize() > 0)
            builder.put("page_size", Integer.toString(options.getPageSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency() != null)
            builder.put("serial_consistency_level", options.getSerialConsistency().name());

        builder.put("query", prepared.rawCQLStatement);

        for (int i = 0; i < prepared.statement.getBindVariables().size(); i++)
        {
            ColumnSpecification cs = prepared.statement.getBindVariables().get(i);
            String boundName = cs.name.toString();
            String boundValue = cs.type.asCQL3Type().toCQLLiteral(options.getValues().get(i), options.getProtocolVersion());
            if (boundValue.length() > 1000)
                boundValue = boundValue.substring(0, 1000) + "...'";

            //Here we prefix boundName with the index to avoid possible collission in builder keys due to
            //having multiple boundValues for the same variable
            builder.put("bound_var_" + i + '_' + boundName, boundValue);
        }

        Tracing.instance.begin("Execute CQL3 prepared query", state.getClientAddress(), builder.build());
    }

    private void logSuccess(QueryState state, QueryHandler.Prepared prepared, long requestStartTime)
    {
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setType(prepared.statement.getAuditLogContext().auditLogEntryType)
                             .setOperation(prepared.rawCQLStatement)
                             .setTimestamp(requestStartTime)
                             .setScope(prepared.statement)
                             .setKeyspace(state, prepared.statement)
                             .setOptions(options)
                             .build();
        AuditLogManager.getInstance().log(entry);
    }

    private void logException(QueryState state, Exception e)
    {
        if (e instanceof PreparedQueryNotFoundException)
        {
            AuditLogEntry entry =
                new AuditLogEntry.Builder(state)
                                 .setOperation(toString())
                                 .setOptions(options)
                                 .build();
            AuditLogManager.getInstance().log(entry, e);
            return;
        }

        QueryHandler.Prepared prepared = ClientState.getCQLQueryHandler().getPrepared(statementId);
        if (prepared != null)
        {
            AuditLogEntry entry =
                new AuditLogEntry.Builder(state)
                                 .setOperation(toString())
                                 .setType(prepared.statement.getAuditLogContext().auditLogEntryType)
                                 .setScope(prepared.statement)
                                 .setKeyspace(state, prepared.statement)
                                 .setOptions(options)
                                 .build();
            AuditLogManager.getInstance().log(entry, e);
        }
    }

    @Override
    public String toString()
    {
        return String.format("EXECUTE %s with %d values at consistency %s", statementId, options.getValues().size(), options.getConsistency());
    }
}
