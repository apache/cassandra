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

    static void metadataInitialized(Schema schema, KeyspaceMetadata ksmUpdate)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_LOADED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_LOADED, schema, ksmUpdate, null, null, null, null, null, null));
    }

    static void metadataReloaded(Schema schema, KeyspaceMetadata previous, KeyspaceMetadata ksmUpdate, Tables.TablesDiff tablesDiff, Views.ViewsDiff viewsDiff, MapDifference<String,TableMetadata> indexesDiff)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_RELOADED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_RELOADED, schema, ksmUpdate, previous,
                                            null, null, tablesDiff, viewsDiff, indexesDiff));
    }

    static void metadataRemoved(Schema schema, KeyspaceMetadata ksmUpdate)
    {
        if (isEnabled(SchemaEventType.KS_METADATA_REMOVED))
            service.publish(new SchemaEvent(SchemaEventType.KS_METADATA_REMOVED, schema, ksmUpdate,
                                            null, null, null, null, null, null));
    }

    static void versionUpdated(Schema schema)
    {
        if (isEnabled(SchemaEventType.VERSION_UPDATED))
            service.publish(new SchemaEvent(SchemaEventType.VERSION_UPDATED, schema,
                                            null, null, null, null, null, null, null));
    }

    static void keyspaceCreating(Schema schema, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_CREATING))
            service.publish(new SchemaEvent(SchemaEventType.KS_CREATING, schema, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceCreated(Schema schema, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_CREATED))
            service.publish(new SchemaEvent(SchemaEventType.KS_CREATED, schema, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceAltering(Schema schema, KeyspaceMetadata.KeyspaceDiff delta)
    {
        if (isEnabled(SchemaEventType.KS_ALTERING))
            service.publish(new SchemaEvent(SchemaEventType.KS_ALTERING, schema, delta.after,
                                            delta.before, delta, null, null, null, null));
    }

    static void keyspaceAltered(Schema schema, KeyspaceMetadata.KeyspaceDiff delta)
    {
        if (isEnabled(SchemaEventType.KS_ALTERED))
            service.publish(new SchemaEvent(SchemaEventType.KS_ALTERED, schema, delta.after,
                                            delta.before, delta, null, null, null, null));
    }

    static void keyspaceDropping(Schema schema, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_DROPPING))
            service.publish(new SchemaEvent(SchemaEventType.KS_DROPPING, schema, keyspace,
                                            null, null, null, null, null, null));
    }

    static void keyspaceDropped(Schema schema, KeyspaceMetadata keyspace)
    {
        if (isEnabled(SchemaEventType.KS_DROPPED))
            service.publish(new SchemaEvent(SchemaEventType.KS_DROPPED, schema, keyspace,
                                            null, null, null, null, null, null));
    }

    static void schemaLoading(Schema schema)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_LOADING))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_LOADING, schema, null,
                                            null, null, null, null, null, null));
    }

    static void schemaLoaded(Schema schema)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_LOADED))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_LOADED, schema, null,
                                            null, null, null, null, null, null));
    }

    static void versionAnnounced(Schema schema)
    {
        if (isEnabled(SchemaEventType.VERSION_ANOUNCED))
            service.publish(new SchemaEvent(SchemaEventType.VERSION_ANOUNCED, schema, null,
                                            null, null, null, null, null, null));
    }

    static void schemaCleared(Schema schema)
    {
        if (isEnabled(SchemaEventType.SCHEMATA_CLEARED))
            service.publish(new SchemaEvent(SchemaEventType.SCHEMATA_CLEARED, schema, null,
                                            null, null, null, null, null, null));
    }

    static void tableCreating(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_CREATING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_CREATING, schema, null,
                                            null, null, table, null, null, null));
    }

    static void tableCreated(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_CREATED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_CREATED, schema, null,
                                            null, null, table, null, null, null));
    }

    static void tableAltering(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_ALTERING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_ALTERING, schema, null,
                                            null, null, table, null, null, null));
    }

    static void tableAltered(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_ALTERED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_ALTERED, schema, null,
                                            null, null, table, null, null, null));
    }

    static void tableDropping(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_DROPPING))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_DROPPING, schema, null,
                                            null, null, table, null, null, null));
    }

    static void tableDropped(Schema schema, TableMetadata table)
    {
        if (isEnabled(SchemaEventType.TABLE_DROPPED))
            service.publish(new SchemaEvent(SchemaEventType.TABLE_DROPPED, schema, null,
                                            null, null, table, null, null, null));
    }

    private static boolean isEnabled(SchemaEventType type)
    {
        return service.isEnabled(SchemaEvent.class, type);
    }

}
