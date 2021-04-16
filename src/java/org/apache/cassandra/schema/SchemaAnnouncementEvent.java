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

package org.apache.cassandra.schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Events emitted by {@link MigrationManager} around propagating schema changes to remote nodes.
 */
final class SchemaAnnouncementEvent extends DiagnosticEvent
{
    private final SchemaAnnouncementEventType type;
    @Nullable
    private final Set<InetAddressAndPort> schemaDestinationEndpoints;
    @Nullable
    private final Set<InetAddressAndPort> schemaEndpointsIgnored;
    @Nullable
    private final CQLStatement statement;
    @Nullable
    private final InetAddressAndPort sender;

    enum SchemaAnnouncementEventType
    {
        SCHEMA_MUTATIONS_ANNOUNCED,
        SCHEMA_TRANSFORMATION_ANNOUNCED,
        SCHEMA_MUTATIONS_RECEIVED
    }

    SchemaAnnouncementEvent(SchemaAnnouncementEventType type,
                            @Nullable Set<InetAddressAndPort> schemaDestinationEndpoints,
                            @Nullable Set<InetAddressAndPort> schemaEndpointsIgnored,
                            @Nullable SchemaTransformation transformation,
                            @Nullable InetAddressAndPort sender)
    {
        this.type = type;
        this.schemaDestinationEndpoints = schemaDestinationEndpoints;
        this.schemaEndpointsIgnored = schemaEndpointsIgnored;
        if (transformation instanceof CQLStatement) this.statement = (CQLStatement) transformation;
        else this.statement = null;
        this.sender = sender;
    }

    public Enum<?> getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (schemaDestinationEndpoints != null)
        {
            Set<String> eps = schemaDestinationEndpoints.stream().map(Object::toString).collect(Collectors.toSet());
            ret.put("endpointDestinations", new HashSet<>(eps));
        }
        if (schemaEndpointsIgnored != null)
        {
            Set<String> eps = schemaEndpointsIgnored.stream().map(Object::toString).collect(Collectors.toSet());
            ret.put("endpointIgnored", new HashSet<>(eps));
        }
        if (statement != null)
        {
            AuditLogContext logContext = statement.getAuditLogContext();
            if (logContext != null)
            {
                HashMap<String, String> log = new HashMap<>();
                if (logContext.auditLogEntryType != null) log.put("type", logContext.auditLogEntryType.name());
                if (logContext.keyspace != null) log.put("keyspace", logContext.keyspace);
                if (logContext.scope != null) log.put("table", logContext.scope);
                ret.put("statement", log);
            }
        }
        if (sender != null) ret.put("sender", sender.toString());
        return ret;
    }
}
