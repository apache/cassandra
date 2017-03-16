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

import java.util.UUID;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaMigrationEvent.MigrationManagerEventType;

final class SchemaMigrationDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private SchemaMigrationDiagnostics()
    {
    }

    static void unknownLocalSchemaVersion(InetAddressAndPort endpoint, UUID theirVersion)
    {
        if (isEnabled(MigrationManagerEventType.UNKNOWN_LOCAL_SCHEMA_VERSION))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.UNKNOWN_LOCAL_SCHEMA_VERSION, endpoint,
                                                     theirVersion));
    }

    static void versionMatch(InetAddressAndPort endpoint, UUID theirVersion)
    {
        if (isEnabled(MigrationManagerEventType.VERSION_MATCH))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.VERSION_MATCH, endpoint, theirVersion));
    }

    static void skipPull(InetAddressAndPort endpoint, UUID theirVersion)
    {
        if (isEnabled(MigrationManagerEventType.SKIP_PULL))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.SKIP_PULL, endpoint, theirVersion));
    }

    static void resetLocalSchema()
    {
        if (isEnabled(MigrationManagerEventType.RESET_LOCAL_SCHEMA))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.RESET_LOCAL_SCHEMA, null, null));
    }

    static void taskCreated(InetAddressAndPort endpoint)
    {
        if (isEnabled(MigrationManagerEventType.TASK_CREATED))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.TASK_CREATED, endpoint, null));
    }

    static void taskSendAborted(InetAddressAndPort endpoint)
    {
        if (isEnabled(MigrationManagerEventType.TASK_SEND_ABORTED))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.TASK_SEND_ABORTED, endpoint, null));
    }

    static void taskRequestSend(InetAddressAndPort endpoint)
    {
        if (isEnabled(MigrationManagerEventType.TASK_REQUEST_SEND))
            service.publish(new SchemaMigrationEvent(MigrationManagerEventType.TASK_REQUEST_SEND,
                                                     endpoint, null));
    }

    private static boolean isEnabled(MigrationManagerEventType type)
    {
        return service.isEnabled(SchemaMigrationEvent.class, type);
    }
}
