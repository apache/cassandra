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
package org.apache.cassandra.audit;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class AuditLogEntry
{
    private final InetAddressAndPort host = FBUtilities.getBroadcastAddressAndPort();
    private final InetAddressAndPort source;
    private final String user;
    private final long timestamp;
    private final AuditLogEntryType type;
    private final UUID batch;
    private final String keyspace;
    private final String scope;
    private final String operation;
    private final QueryOptions options;
    private final QueryState state;

    private AuditLogEntry(AuditLogEntryType type,
                          InetAddressAndPort source,
                          String user,
                          long timestamp,
                          UUID batch,
                          String keyspace,
                          String scope,
                          String operation,
                          QueryOptions options,
                          QueryState state)
    {
        this.type = type;
        this.source = source;
        this.user = user;
        this.timestamp = timestamp;
        this.batch = batch;
        this.keyspace = keyspace;
        this.scope = scope;
        this.operation = operation;
        this.options = options;
        this.state = state;
    }

    String getLogString()
    {
        StringBuilder builder = new StringBuilder(100);
        builder.append("user:").append(user)
               .append("|host:").append(host)
               .append("|source:").append(source.getAddress());
        if (source.getPort() > 0)
        {
            builder.append("|port:").append(source.getPort());
        }

        builder.append("|timestamp:").append(timestamp)
               .append("|type:").append(type)
               .append("|category:").append(type.getCategory());

        if (batch != null)
        {
            builder.append("|batch:").append(batch);
        }
        if (StringUtils.isNotBlank(keyspace))
        {
            builder.append("|ks:").append(keyspace);
        }
        if (StringUtils.isNotBlank(scope))
        {
            builder.append("|scope:").append(scope);
        }
        if (StringUtils.isNotBlank(operation))
        {
            builder.append("|operation:").append(operation);
        }
        return builder.toString();
    }

    public InetAddressAndPort getHost()
    {
        return host;
    }

    public InetAddressAndPort getSource()
    {
        return source;
    }

    public String getUser()
    {
        return user;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public AuditLogEntryType getType()
    {
        return type;
    }

    public UUID getBatch()
    {
        return batch;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getScope()
    {
        return scope;
    }

    public String getOperation()
    {
        return operation;
    }

    public QueryOptions getOptions()
    {
        return options;
    }

    public QueryState getState()
    {
        return state;
    }

    public static class Builder
    {
        private static final InetAddressAndPort DEFAULT_SOURCE;

        static
        {
            try
            {
                DEFAULT_SOURCE = InetAddressAndPort.getByNameOverrideDefaults("0.0.0.0", 0);
            }
            catch (UnknownHostException e)
            {

                throw new RuntimeException("failed to create default source address", e);
            }
        }

        private static final String DEFAULT_OPERATION = StringUtils.EMPTY;

        private AuditLogEntryType type;
        private InetAddressAndPort source;
        private String user;
        private long timestamp;
        private UUID batch;
        private String keyspace;
        private String scope;
        private String operation;
        private QueryOptions options;
        private QueryState state;

        public Builder(QueryState queryState)
        {
            state = queryState;

            ClientState clientState = queryState.getClientState();

            if (clientState != null)
            {
                if (clientState.getRemoteAddress() != null)
                {
                    InetSocketAddress addr = clientState.getRemoteAddress();
                    source = InetAddressAndPort.getByAddressOverrideDefaults(addr.getAddress(), addr.getPort());
                }

                if (clientState.getUser() != null)
                {
                    user = clientState.getUser().getName();
                }
                keyspace = clientState.getRawKeyspace();
            }
            else
            {
                source = DEFAULT_SOURCE;
                user = AuthenticatedUser.SYSTEM_USER.getName();
            }

            timestamp = currentTimeMillis();
        }

        public Builder(AuditLogEntry entry)
        {
            type = entry.type;
            source = entry.source;
            user = entry.user;
            timestamp = entry.timestamp;
            batch = entry.batch;
            keyspace = entry.keyspace;
            scope = entry.scope;
            operation = entry.operation;
            options = entry.options;
            state = entry.state;
        }

        public Builder setType(AuditLogEntryType type)
        {
            this.type = type;
            return this;
        }

        public Builder(AuditLogEntryType type)
        {
            this.type = type;
            operation = DEFAULT_OPERATION;
        }

        public Builder setUser(String user)
        {
            this.user = user;
            return this;
        }

        public Builder setBatch(UUID batch)
        {
            this.batch = batch;
            return this;
        }

        public Builder setTimestamp(long timestampMillis)
        {
            this.timestamp = timestampMillis;
            return this;
        }

        public Builder setKeyspace(QueryState queryState, @Nullable CQLStatement statement)
        {
            keyspace = statement != null && statement.getAuditLogContext().keyspace != null
                       ? statement.getAuditLogContext().keyspace
                       : queryState.getClientState().getRawKeyspace();
            return this;
        }

        public Builder setKeyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public Builder setKeyspace(CQLStatement statement)
        {
            this.keyspace = statement.getAuditLogContext().keyspace;
            return this;
        }

        public Builder setScope(CQLStatement statement)
        {
            this.scope = statement.getAuditLogContext().scope;
            return this;
        }

        public Builder setOperation(String operation)
        {
            this.operation = operation;
            return this;
        }

        public void appendToOperation(String str)
        {
            if (StringUtils.isNotBlank(str))
            {
                if (operation.isEmpty())
                    operation = str;
                else
                    operation = operation.concat("; ").concat(str);
            }
        }

        public Builder setOptions(QueryOptions options)
        {
            this.options = options;
            return this;
        }

        public AuditLogEntry build()
        {
            timestamp = timestamp > 0 ? timestamp : currentTimeMillis();
            return new AuditLogEntry(type, source, user, timestamp, batch, keyspace, scope, operation, options, state);
        }
    }
}

