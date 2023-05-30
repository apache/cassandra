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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;

/**
 * Registers schema change listeners and sends the notifications. The interface of this class just takes the high level
 * keyspace metadata changes. It iterates over all keyspaces elements and distributes appropriate notifications about
 * changes around those elements (tables, views, types, functions).
 */
public class SchemaChangeNotifier
{
    private final List<SchemaChangeListener> changeListeners = new CopyOnWriteArrayList<>();

    public void registerListener(SchemaChangeListener listener)
    {
        changeListeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        changeListeners.remove(listener);
    }

    public void notifyKeyspaceCreated(KeyspaceMetadata keyspace)
    {
        notifyCreateKeyspace(keyspace);
        keyspace.types.forEach(this::notifyCreateType);
        keyspace.tables.forEach(this::notifyCreateTable);
        keyspace.views.forEach(this::notifyCreateView);
        keyspace.userFunctions.udfs().forEach(this::notifyCreateFunction);
        keyspace.userFunctions.udas().forEach(this::notifyCreateAggregate);
    }

    public void notifyKeyspaceAltered(KeyspaceMetadata.KeyspaceDiff delta, boolean dropData)
    {
        // notify on everything dropped
        delta.udas.dropped.forEach(uda -> notifyDropAggregate((UDAggregate) uda));
        delta.udfs.dropped.forEach(udf -> notifyDropFunction((UDFunction) udf));
        delta.views.dropped.forEach(view -> notifyDropView(view, dropData));
        delta.tables.dropped.forEach(metadata -> notifyDropTable(metadata, dropData));
        delta.types.dropped.forEach(this::notifyDropType);

        // notify on everything created
        delta.types.created.forEach(this::notifyCreateType);
        delta.tables.created.forEach(this::notifyCreateTable);
        delta.views.created.forEach(this::notifyCreateView);
        delta.udfs.created.forEach(udf -> notifyCreateFunction((UDFunction) udf));
        delta.udas.created.forEach(uda -> notifyCreateAggregate((UDAggregate) uda));

        // notify on everything altered
        if (!delta.before.params.equals(delta.after.params))
            notifyAlterKeyspace(delta.before, delta.after);
        delta.types.altered.forEach(diff -> notifyAlterType(diff.before, diff.after));
        delta.tables.altered.forEach(diff -> notifyAlterTable(diff.before, diff.after));
        delta.views.altered.forEach(diff -> notifyAlterView(diff.before, diff.after));
        delta.udfs.altered.forEach(diff -> notifyAlterFunction(diff.before, diff.after));
        delta.udas.altered.forEach(diff -> notifyAlterAggregate(diff.before, diff.after));
    }

    public void notifyKeyspaceDropped(KeyspaceMetadata keyspace, boolean dropData)
    {
        keyspace.userFunctions.udas().forEach(this::notifyDropAggregate);
        keyspace.userFunctions.udfs().forEach(this::notifyDropFunction);
        keyspace.views.forEach(view -> notifyDropView(view, dropData));
        keyspace.tables.forEach(metadata -> notifyDropTable(metadata, dropData));
        keyspace.types.forEach(this::notifyDropType);
        notifyDropKeyspace(keyspace, dropData);
    }

    public void notifyPreChanges(SchemaTransformationResult transformationResult)
    {
        transformationResult.diff.altered.forEach(this::notifyPreAlterKeyspace);
    }

    private void notifyPreAlterKeyspace(KeyspaceMetadata.KeyspaceDiff keyspaceDiff)
    {
        keyspaceDiff.tables.altered.forEach(this::notifyPreAlterTable);
        keyspaceDiff.views.altered.forEach(this::notifyPreAlterView);
    }

    private void notifyPreAlterTable(Diff.Altered<TableMetadata> altered)
    {
        changeListeners.forEach(l -> l.onPreAlterTable(altered.before, altered.after));
    }

    private void notifyPreAlterView(Diff.Altered<ViewMetadata> altered)
    {
        changeListeners.forEach(l -> l.onPreAlterView(altered.before, altered.after));
    }

    private void notifyCreateKeyspace(KeyspaceMetadata ksm)
    {
        changeListeners.forEach(l -> l.onCreateKeyspace(ksm));
    }

    private void notifyCreateTable(TableMetadata metadata)
    {
        changeListeners.forEach(l -> l.onCreateTable(metadata));
    }

    private void notifyCreateView(ViewMetadata view)
    {
        changeListeners.forEach(l -> l.onCreateView(view));
    }

    private void notifyCreateType(UserType ut)
    {
        changeListeners.forEach(l -> l.onCreateType(ut));
    }

    private void notifyCreateFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onCreateFunction(udf));
    }

    private void notifyCreateAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onCreateAggregate(udf));
    }

    private void notifyAlterKeyspace(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        changeListeners.forEach(l -> l.onAlterKeyspace(before, after));
    }

    private void notifyAlterTable(TableMetadata before, TableMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.changeAffectsPreparedStatements(after);
        changeListeners.forEach(l -> l.onAlterTable(before, after, changeAffectedPreparedStatements));
    }

    private void notifyAlterView(ViewMetadata before, ViewMetadata after)
    {
        boolean changeAffectedPreparedStatements = before.metadata.changeAffectsPreparedStatements(after.metadata);
        changeListeners.forEach(l -> l.onAlterView(before, after, changeAffectedPreparedStatements));
    }

    private void notifyAlterType(UserType before, UserType after)
    {
        changeListeners.forEach(l -> l.onAlterType(before, after));
    }

    private void notifyAlterFunction(UDFunction before, UDFunction after)
    {
        changeListeners.forEach(l -> l.onAlterFunction(before, after));
    }

    private void notifyAlterAggregate(UDAggregate before, UDAggregate after)
    {
        changeListeners.forEach(l -> l.onAlterAggregate(before, after));
    }

    private void notifyDropKeyspace(KeyspaceMetadata ksm, boolean dropData)
    {
        changeListeners.forEach(l -> l.onDropKeyspace(ksm, dropData));
    }

    private void notifyDropTable(TableMetadata metadata, boolean dropData)
    {
        changeListeners.forEach(l -> l.onDropTable(metadata, dropData));
    }

    private void notifyDropView(ViewMetadata view, boolean dropData)
    {
        changeListeners.forEach(l -> l.onDropView(view, dropData));
    }

    private void notifyDropType(UserType ut)
    {
        changeListeners.forEach(l -> l.onDropType(ut));
    }

    private void notifyDropFunction(UDFunction udf)
    {
        changeListeners.forEach(l -> l.onDropFunction(udf));
    }

    private void notifyDropAggregate(UDAggregate udf)
    {
        changeListeners.forEach(l -> l.onDropAggregate(udf));
    }
}
