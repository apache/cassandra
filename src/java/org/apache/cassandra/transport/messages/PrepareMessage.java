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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryEvents;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.telemetry.CassandraAttributes;
import org.apache.cassandra.telemetry.Telemetry;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import io.netty.buffer.ByteBuf;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.semconv.ClientAttributes;
import io.opentelemetry.semconv.DbAttributes;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class PrepareMessage extends Message.Request
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareMessage.class);
    private static final NoSpamLogger nospam = NoSpamLogger.getLogger(logger, 10, TimeUnit.MINUTES);

    public static final Message.Codec<PrepareMessage> codec = new Message.Codec<>()
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
                {
                    keyspace = CBUtil.readString(body);
                    nospam.warn("Keyspace is set via query options. This is considered dangerous and should not be used. Query: {}. Keyspace: {}",
                                query, keyspace);
                }
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
                    CBUtil.writeAsciiString(msg.keyspace, dest);
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
                    : CBUtil.sizeOfAsciiString(msg.keyspace);
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

    @Override
    protected boolean isTraceable()
    {
        return true;
    }

    @Override
    protected Span createSpan(InetSocketAddress clientAddress, Context context)
    {
        return Telemetry.getRequestTracer().spanBuilder(type.name())
                        .setSpanKind(SpanKind.SERVER)
                        .setParent(context)
                        .setAttribute(DbAttributes.DB_SYSTEM_NAME, CassandraAttributes.DB_SYSTEM_NAME_CASSANDRA)
                        .setAttribute(DbAttributes.DB_QUERY_TEXT, query)
                        .setAttribute(CassandraAttributes.CASSANDRA_QUERY_TYPE, type.name())
                        .setAttribute(ClientAttributes.CLIENT_ADDRESS, clientAddress.getAddress().getHostAddress())
                        .setAttribute(ClientAttributes.CLIENT_PORT, clientAddress.getPort())
                        .setAttribute(CassandraAttributes.CASSANDRA_COORDINATOR_ADDRESS, FBUtilities.getBroadcastNativeAddressAndPort().getHostAddress(false))
                        .setAttribute(CassandraAttributes.CASSANDRA_COORDINATOR_PORT, FBUtilities.getBroadcastNativeAddressAndPort().getPort())
                        .startSpan();
    }

    @Override
    protected Message.Response execute(QueryState state, Dispatcher.RequestTime requestTime, boolean traceRequest)
    {
        try
        {
            if (traceRequest)
                Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", query));

            ClientState clientState = state.getClientState().cloneWithKeyspaceIfSet(keyspace);
            QueryHandler queryHandler = ClientState.getCQLQueryHandler();
            long queryTime = currentTimeMillis();
            ResultMessage.Prepared response = queryHandler.prepare(query, clientState, getCustomPayload());
            if (Span.current().getSpanContext().isValid())
            {
                QueryHandler.Prepared prepared = queryHandler.getPrepared(response.statementId);
                if (prepared != null && prepared.statement != null)
                {
                    Span.current().updateName(String.format("%s %s", type.name(), prepared.statement.getQuerySummary()));
                }
            }
            QueryEvents.instance.notifyPrepareSuccess(() -> queryHandler.getPrepared(response.statementId), query, state, queryTime, response);
            return response;
        }
        catch (Exception e)
        {
            QueryEvents.instance.notifyPrepareFailure(null, query, state, e);
            JVMStabilityInspector.inspectThrowable(e);
            Span.current().recordException(e);
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "PREPARE " + query;
    }
}
