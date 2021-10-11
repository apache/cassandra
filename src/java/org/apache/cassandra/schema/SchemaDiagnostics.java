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

import com.google.common.collect.MapDifference;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.schema.SchemaEvent.SchemaEventType;

final class SchemaDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private SchemaDiagnostics()
    {
    }

    static void metadataInitialized(SchemaManager schemaManager, KeyspaceMetadata ksmUpdate)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_LOADED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_LOADED, schemaManager, ksmUpdate, null, null, null, null, null, null));
    }

    static void metadataReloaded(SchemaManager schemaManager, KeyspaceMetadata previous, KeyspaceMetadata ksmUpdate, Tables.TablesDiff tablesDiff, Views.ViewsDiff viewsDiff, MapDifference<String,TableMetadata> indexesDiff)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_RELOADED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_RELOADED, schemaManager, ksmUpdate, previous,
                                            null, null, tablesDiff, viewsDiff, indexesDiff));
    }

    static void metadataRemoved(SchemaManager schemaManager, KeyspaceMetadata ksmUpdate)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_REMOVED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_REMOVED, schemaManager, ksmUpdate,
                                            null, null, null, null, null, null));
    }

    static void versionUpdated(SchemaManager schemaManager)
    {
        if (isEnabled(SchemaEventType.VERSION_UPDATED))
            service.publish(new SchemaEvent(SchemaEventType.VERSION_UPDATED, schemaManager,
                                            null, null, null, null, null, null, null));
    }

    static void keyspaceCreating(SchemaManager schemaManager, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_CREATING))
            service.publish(new SchemaEvent(SchemaEventType.KS_CREATING, schemaManager, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceCreated(SchemaManager schemaManager, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_CREATED))
            service.publish(new SchemaEvent(SchemaEventType.KS_CREATED, schemaManager, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceAltering(SchemaManager schemaManager, KeyspaceMetadata.KeyspaceDiff delta)
    {
        if (isEnabled(SchemaEventType.KS_ALTERING))
            service.publish(new SchemaEvent(SchemaEventType.KS_ALTERING, schemaManager, delta.after,
                                            delta.before, delta, null, null, null, null));
    }

    static void keyspaceAltered(SchemaManager schemaManager, KeyspaceMetadata.KeyspaceDiff delta)
    {
        if (isEnabled(SchemaEventType.KS_ALTERED))
            service.publish(new SchemaEvent(SchemaEventType.KS_ALTERED, schemaManager, delta.after,
                                            delta.before, delta, null, null, null, null));
    }

    static void keyspaceDropping(SchemaManager schemaManager, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_DROPPING))
            service.publish(new SchemaEvent(SchemaEventType.KS_DROPPING, schemaManager, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceDropped(SchemaManager schemaManager, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_DROPPED))
            service.publish(new SchemaEvent(SchemaEventType.KS_DROPPED, schemaManager, keyspace,
                                            null, null, null, null, null, null));
    }

    static void schemaLoading(SchemaManager schemaManager)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_LOADING))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_LOADING, schemaManager, null,
                                            null, null, null, null, null, null));
    }

    static void schemaLoaded(SchemaManager schemaManager)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_LOADED))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_LOADED, schemaManager, null,
                                            null, null, null, null, null, null));
    }

    static void versionAnnounced(SchemaManager schemaManager)
    {
        if (isEnabled(SchemaEventType.VERSION_ANOUNCED))
            service.publish(new SchemaEvent(SchemaEventType.VERSION_ANOUNCED, schemaManager, null,
                                            null, null, null, null, null, null));
    }

    static void schemaCleared(SchemaManager schemaManager)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_CLEARED))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_CLEARED, schemaManager, null,
                                            null, null, null, null, null, null));
    }

    static void tableCreating(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_CREATING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_CREATING, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    static void tableCreated(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_CREATED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_CREATED, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    static void tableAltering(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_ALTERING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_ALTERING, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    static void tableAltered(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_ALTERED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_ALTERED, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    static void tableDropping(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_DROPPING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_DROPPING, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    static void tableDropped(SchemaManager schemaManager, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_DROPPED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_DROPPED, schemaManager, null,
                                            null, null, table, null, null, null));
    }

    private static boolean isEnabled(SchemaEventType type)
    {
        return service.isEnabled(SchemaEvent.class, type);
    }

}
