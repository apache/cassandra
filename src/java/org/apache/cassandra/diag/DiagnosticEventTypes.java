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

package org.apache.cassandra.diag;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.audit.AuditEvent;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.db.guardrails.GuardrailEvent;
import org.apache.cassandra.dht.BootstrapEvent;
import org.apache.cassandra.dht.tokenallocator.TokenAllocatorEvent;
import org.apache.cassandra.gms.GossiperEvent;
import org.apache.cassandra.hints.HintEvent;
import org.apache.cassandra.hints.HintsServiceEvent;
import org.apache.cassandra.schema.SchemaAnnouncementEvent;
import org.apache.cassandra.schema.SchemaEvent;
import org.apache.cassandra.service.reads.repair.PartitionRepairEvent;
import org.apache.cassandra.service.reads.repair.ReadRepairEvent;

public class DiagnosticEventTypes
{
    public static final Map<String, Set<Enum<?>>> ALL_EVENTS;

    static
    {
        ALL_EVENTS = new HashMap<>();
        ALL_EVENTS.put(AuditEvent.class.getName(), Set.of(AuditLogEntryType.values()));
        ALL_EVENTS.put(BootstrapEvent.class.getName(), Set.of(BootstrapEvent.BootstrapEventType.values()));
        ALL_EVENTS.put(GossiperEvent.class.getName(), Set.of(GossiperEvent.GossiperEventType.values()));
        ALL_EVENTS.put(GuardrailEvent.class.getName(), Set.of(GuardrailEvent.GuardrailEventType.values()));
        ALL_EVENTS.put(HintEvent.class.getName(), Set.of(HintEvent.HintEventType.values()));
        ALL_EVENTS.put(HintsServiceEvent.class.getName(), Set.of(HintsServiceEvent.HintsServiceEventType.values()));
        ALL_EVENTS.put(PartitionRepairEvent.class.getName(), Set.of(PartitionRepairEvent.PartitionRepairEventType.values()));
        ALL_EVENTS.put(ReadRepairEvent.class.getName(), Set.of(ReadRepairEvent.ReadRepairEventType.values()));
        ALL_EVENTS.put(SchemaAnnouncementEvent.class.getName(), Set.of(SchemaAnnouncementEvent.SchemaAnnouncementEventType.values()));
        ALL_EVENTS.put(SchemaEvent.class.getName(), Set.of(SchemaEvent.SchemaEventType.values()));
        ALL_EVENTS.put(TokenAllocatorEvent.class.getName(), Set.of(TokenAllocatorEvent.TokenAllocatorEventType.values()));
    }
}
