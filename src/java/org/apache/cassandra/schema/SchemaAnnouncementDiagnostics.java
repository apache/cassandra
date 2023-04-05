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

import java.util.Set;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaAnnouncementEvent.SchemaAnnouncementEventType;

final class SchemaAnnouncementDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private SchemaAnnouncementDiagnostics()
    {
    }

    static void schemaMutationsAnnounced(Set<InetAddressAndPort> schemaDestinationEndpoints, Set<InetAddressAndPort> schemaEndpointsIgnored)
    {
        if (isEnabled(SchemaAnnouncementEventType.SCHEMA_MUTATIONS_ANNOUNCED))
            service.publish(new SchemaAnnouncementEvent(SchemaAnnouncementEventType.SCHEMA_MUTATIONS_ANNOUNCED,
                                                        schemaDestinationEndpoints, schemaEndpointsIgnored, null, null));
    }

    public static void schemataMutationsReceived(InetAddressAndPort from)
    {
        if (isEnabled(SchemaAnnouncementEventType.SCHEMA_MUTATIONS_RECEIVED))
            service.publish(new SchemaAnnouncementEvent(SchemaAnnouncementEventType.SCHEMA_MUTATIONS_RECEIVED,
                                                        null, null, null, from));
    }

    static void schemaTransformationAnnounced(Set<InetAddressAndPort> schemaDestinationEndpoints, Set<InetAddressAndPort> schemaEndpointsIgnored, SchemaTransformation transformation)
    {
        if (isEnabled(SchemaAnnouncementEventType.SCHEMA_TRANSFORMATION_ANNOUNCED))
            service.publish(new SchemaAnnouncementEvent(SchemaAnnouncementEventType.SCHEMA_TRANSFORMATION_ANNOUNCED,
                                                        schemaDestinationEndpoints, schemaEndpointsIgnored, transformation, null));
    }

    private static boolean isEnabled(SchemaAnnouncementEventType type)
    {
        return service.isEnabled(SchemaAnnouncementEvent.class, type);
    }
}
