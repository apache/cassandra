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
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
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
    static final String DEFAULT_KEY_VALUE_SEPARATOR = ":";
    static final String DEFAULT_FIELD_SEPARATOR = "|";
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
    private final Map<String, Object> metadata;

    private AuditLogEntry(Builder builder)
    {
        this.type = builder.type;
        this.source = builder.source;
        this.user = builder.user;
        this.timestamp = builder.timestamp;
        this.batch = builder.batch;
        this.keyspace = builder.keyspace;
        this.scope = builder.scope;
        this.operation = builder.operation;
        this.options = builder.options;
        this.state = builder.state;
        this.metadata = builder.metadata;
    }

    @VisibleForTesting
    public String getLogString()
    {
        return getLogString(DEFAULT_KEY_VALUE_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    }

    String getLogString(String keyValueSeparator, String fieldSeparator)
    {
        StringBuilder builder = new StringBuilder(100);
        builder.append("user").append(keyValueSeparator).append(user)
               .append(fieldSeparator).append("host").append(keyValueSeparator).append(host);

        // Source is only expected to be null during testing
        // in MacOS when running in-jvm dtests
        if (source != null)
        {
            builder.append(fieldSeparator).append("source").append(keyValueSeparator).append(source.getAddress());
            if (source.getPort() > 0)
            {
                builder.append(fieldSeparator).append("port").append(keyValueSeparator).append(source.getPort());
            }
        }

        builder.append(fieldSeparator).append("timestamp").append(keyValueSeparator).append(timestamp)
               .append(fieldSeparator).append("type").append(keyValueSeparator).append(type)
               .append(fieldSeparator).append("category").append(keyValueSeparator).append(type.getCategory());

        if (batch != null)
        {
            builder.append(fieldSeparator).append("batch").append(keyValueSeparator).append(batch);
        }
        if (StringUtils.isNotBlank(keyspace))
        {
            builder.append(fieldSeparator).append("ks").append(keyValueSeparator).append(keyspace);
        }
        if (StringUtils.isNotBlank(scope))
        {
            builder.append(fieldSeparator).append("scope").append(keyValueSeparator).append(scope);
        }
        if (StringUtils.isNotBlank(operation))
        {
            builder.append(fieldSeparator).append("operation").append(keyValueSeparator).append(operation);
        }
        if (metadata != null && !metadata.isEmpty())
        {
            metadata.forEach((key, value) -> builder.append(fieldSeparator).append(key).append(keyValueSeparator).append(value));
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
        private Map<String, Object> metadata;

        public Builder(QueryState queryState)
        {
            state = queryState;

            ClientState clientState = queryState.getClientState();

            if (clientState != null)
            {
                InetSocketAddress addr = clientState.getRemoteAddress();
                if (addr != null)
                {
                    source = InetAddressAndPort.getByAddressOverrideDefaults(addr.getAddress(), addr.getPort());
                }

                AuthenticatedUser authenticatedUser = clientState.getUser();
                if (authenticatedUser != null)
                {
                    user = authenticatedUser.getName();

                    if (authenticatedUser.getMetadata() != null)
                    {
                        metadata = Map.copyOf(authenticatedUser.getMetadata());
                    }
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
            metadata = entry.metadata != null ? Map.copyOf(entry.metadata) : null;
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

        public Builder setMetadata(Map<String, Object> metadata)
        {
            this.metadata = metadata != null ? Map.copyOf(metadata) : null;
            return this;
        }

        public AuditLogEntry build()
        {
            timestamp = timestamp > 0 ? timestamp : currentTimeMillis();
            return new AuditLogEntry(this);
        }
    }
}

