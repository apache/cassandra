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
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.JVMStabilityInspector;

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
            if (traceRequest)
                Tracing.instance.begin("Preparing CQL3 query", state.getClientAddress(), ImmutableMap.of("query", query));

            ClientState clientState = state.getClientState().cloneWithKeyspaceIfSet(keyspace);
            Message.Response response = ClientState.getCQLQueryHandler().prepare(query, clientState, getCustomPayload());

            if (auditLogManager.isAuditingEnabled())
                logSuccess(state);

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

    private void logSuccess(QueryState state)
    {
        // The statement gets parsed twice. Not a big deal given that this is PREPARE, but still, why?
        CQLStatement statement = QueryProcessor.parseStatement(query, state.getClientState());
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setOperation(query)
                             .setType(AuditLogEntryType.PREPARE_STATEMENT)
                             .setScope(statement)
                             .setKeyspace(statement)
                             .build();
        AuditLogManager.getInstance().log(entry);
    }

    private void logException(QueryState state, Exception e)
    {
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setOperation(query)
                             .setKeyspace(keyspace)
                             .setType(AuditLogEntryType.PREPARE_STATEMENT)
                             .build();
        AuditLogManager.getInstance().log(entry, e);
    }

    @Override
    public String toString()
    {
        return "PREPARE " + query;
    }
}
