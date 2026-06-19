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

import java.net.InetSocketAddress;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.telemetry.CassandraAttributes;
import org.apache.cassandra.telemetry.Telemetry;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.LocalizeString;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.ClientAttributes;
import io.opentelemetry.semconv.DbAttributes;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * A CQL query
 */
public class QueryMessage extends Message.Request
{
    public static final Message.Codec<QueryMessage> codec = new Message.Codec<>()
    {
        public QueryMessage decode(ByteBuf body, ProtocolVersion version)
        {
            String query = CBUtil.readLongString(body);
            return new QueryMessage(query, QueryOptions.codec.decode(body, version));
        }

        public void encode(QueryMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeLongString(msg.query, dest);
            if (version == ProtocolVersion.V1)
                CBUtil.writeConsistencyLevel(msg.options.getConsistency(), dest);
            else
                QueryOptions.codec.encode(msg.options, dest, version);
        }

        public int encodedSize(QueryMessage msg, ProtocolVersion version)
        {
            int size = CBUtil.sizeOfLongString(msg.query);

            if (version == ProtocolVersion.V1)
            {
                size += CBUtil.sizeOfConsistencyLevel(msg.options.getConsistency());
            }
            else
            {
                size += QueryOptions.codec.encodedSize(msg.options, version);
            }
            return size;
        }
    };

    public final String query;
    public final QueryOptions options;

    public QueryMessage(String query, QueryOptions options)
    {
        super(Type.QUERY);
        this.query = query;
        this.options = options;
    }

    @Override
    protected boolean isTraceable()
    {
        return true;
    }

    @Override
    protected boolean isTrackable()
    {
        return true;
    }

    @Override
    protected Message.Response execute(QueryState state, Dispatcher.RequestTime requestTime, boolean traceRequest)
    {
        CQLStatement statement = null;
        Span span = Span.current();
        try
        {
            if (options.getPageSize() == 0)
                throw new ProtocolException("The page size cannot be 0");

            if (traceRequest)
                traceQuery(state);

            long queryStartTime = currentTimeMillis();

            QueryHandler queryHandler = ClientState.getCQLQueryHandler();
            statement = queryHandler.parse(query, state, options);
            // update span name
            if (span.getSpanContext().isValid())
            {
                span.updateName(String.format("%s %s", type.name(), statement.getQuerySummary()));
            }
            Message.Response response = queryHandler.process(statement, state, options, getCustomPayload(), requestTime);
            QueryEvents.instance.notifyQuerySuccess(statement, query, options, state, queryStartTime, response);

            if (options.skipMetadata() && response instanceof ResultMessage.Rows)
                ((ResultMessage.Rows) response).result.metadata.setSkipMetadata();

            return response;
        }
        catch (Exception e)
        {
            QueryEvents.instance.notifyQueryFailure(statement, query, options, state, e);
            JVMStabilityInspector.inspectThrowable(e);
            if (!((e instanceof RequestValidationException) || (e instanceof RequestExecutionException)))
                logger.error("Unexpected error during query", e);
            span.recordException(e);
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    protected Span createSpan(InetSocketAddress clientAddress, Context context)
    {
        String consistencyValue = options.getConsistency() != null ? LocalizeString.toLowerCaseLocalized(options.getConsistency().name()) : "";
        String serialConsistencyValue = options.getSerialConsistency() != null ? LocalizeString.toLowerCaseLocalized(options.getSerialConsistency().name()) : "";
        return Telemetry.getRequestTracer().spanBuilder(type.name()) // Span name will be updated after successful statement parsing
                        .setSpanKind(SpanKind.SERVER)
                        .setParent(context)
                        .setAttribute(DbAttributes.DB_SYSTEM_NAME, CassandraAttributes.DB_SYSTEM_NAME_CASSANDRA)
                        .setAttribute(CassandraAttributes.CASSANDRA_QUERY_TYPE, type.name())
                        .setAttribute(ClientAttributes.CLIENT_ADDRESS, clientAddress.getAddress().getHostAddress())
                        .setAttribute(ClientAttributes.CLIENT_PORT, clientAddress.getPort())
                        .setAttribute(CassandraAttributes.CASSANDRA_COORDINATOR_ADDRESS, FBUtilities.getBroadcastNativeAddressAndPort().getHostAddress(false))
                        .setAttribute(CassandraAttributes.CASSANDRA_COORDINATOR_PORT, FBUtilities.getBroadcastNativeAddressAndPort().getPort())
                        .setAttribute(CassandraAttributes.CASSANDRA_PAGE_SIZE, options.getPageSize())
                        .setAttribute(CassandraAttributes.CASSANDRA_CONSISTENCY_LEVEL, consistencyValue)
                        .setAttribute(CassandraAttributes.CASSANDRA_SERIAL_CONSISTENCY_LEVEL, serialConsistencyValue)
                        .startSpan();
    }

    private void traceQuery(QueryState state)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put("query", query);
        if (options.getPageSize() > 0)
            builder.put("page_size", Integer.toString(options.getPageSize()));
        if (options.getConsistency() != null)
            builder.put("consistency_level", options.getConsistency().name());
        if (options.getSerialConsistency() != null)
            builder.put("serial_consistency_level", options.getSerialConsistency().name());

        Tracing.instance.begin("Execute CQL3 query", state.getClientAddress(), builder.build());
    }

    @Override
    public String toString()
    {
        return String.format("QUERY %s [pageSize = %d] at consistency %s",
                             query, options.getPageSize(), options.getConsistency());
    }
}
