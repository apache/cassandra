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
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;

/**
 * Internal events emitted by {@link MigrationManager}.
 */
final class SchemaMigrationEvent extends DiagnosticEvent
{
    private final MigrationManagerEventType type;
    @Nullable
    private final InetAddressAndPort endpoint;
    @Nullable
    private final UUID endpointSchemaVersion;
    private final UUID localSchemaVersion;
    private final Integer localMessagingVersion;
    private final SystemKeyspace.BootstrapState bootstrapState;
    @Nullable
    private Integer inflightTaskCount;
    @Nullable
    private Integer endpointMessagingVersion;
    @Nullable
    private Boolean endpointGossipOnlyMember;
    @Nullable
    private Boolean isAlive;

    enum MigrationManagerEventType
    {
        UNKNOWN_LOCAL_SCHEMA_VERSION,
        VERSION_MATCH,
        SKIP_PULL,
        RESET_LOCAL_SCHEMA,
        TASK_CREATED,
        TASK_SEND_ABORTED,
        TASK_REQUEST_SEND
    }

    SchemaMigrationEvent(MigrationManagerEventType type,
                         @Nullable InetAddressAndPort endpoint, @Nullable UUID endpointSchemaVersion)
    {
        this.type = type;
        this.endpoint = endpoint;
        this.endpointSchemaVersion = endpointSchemaVersion;

        localSchemaVersion = Schema.instance.getVersion();
        localMessagingVersion = MessagingService.current_version;

        Queue<CountDownLatch> inflightTasks = MigrationTask.getInflightTasks();
        if (inflightTasks != null)
            inflightTaskCount = inflightTasks.size();

        this.bootstrapState = SystemKeyspace.getBootstrapState();

        if (endpoint == null) return;

        if (MessagingService.instance().knowsVersion(endpoint))
            endpointMessagingVersion = MessagingService.instance().getRawVersion(endpoint);

        endpointGossipOnlyMember = Gossiper.instance.isGossipOnlyMember(endpoint);
        this.isAlive = FailureDetector.instance.isAlive(endpoint);
    }

    public Enum<?> getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (endpoint != null) ret.put("endpoint", endpoint.getHostAddress(true));
        ret.put("endpointSchemaVersion", Schema.schemaVersionToString(endpointSchemaVersion));
        ret.put("localSchemaVersion", Schema.schemaVersionToString(localSchemaVersion));
        if (endpointMessagingVersion != null) ret.put("endpointMessagingVersion", endpointMessagingVersion);
        if (localMessagingVersion != null) ret.put("localMessagingVersion", localMessagingVersion);
        if (endpointGossipOnlyMember != null) ret.put("endpointGossipOnlyMember", endpointGossipOnlyMember);
        if (isAlive != null) ret.put("endpointIsAlive", isAlive);
        if (bootstrapState != null) ret.put("bootstrapState", bootstrapState.name());
        if (inflightTaskCount != null) ret.put("inflightTaskCount", inflightTaskCount);
        return ret;
    }
}
