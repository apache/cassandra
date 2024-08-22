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

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Factory and utility methods to create simple schema transformation.
 */
public class SchemaTransformations
{
    public static SchemaTransformation fromCql(String cql)
    {
        CQLStatement statement = QueryProcessor.getStatement(cql, ClientState.forInternalCalls());
        if (!(statement instanceof SchemaTransformation))
            throw new IllegalArgumentException("Can not deserialize schema transformation");
        return (SchemaTransformation) statement;
    }

    /**
     * Creates a schema transformation that adds the provided keyspace.
     *
     * @param keyspace       the keyspace to add.
     * @param ignoreIfExists if {@code true}, the transformation is a no-op if a keyspace of the same name than
     *                       {@code keyspace} already exists in the schema the transformation is applied on. Otherwise,
     *                       the transformation throws an {@link AlreadyExistsException} in that case.
     * @return the created transformation.
     */
    public static SchemaTransformation addKeyspace(KeyspaceMetadata keyspace, boolean ignoreIfExists)
    {
        return (metadata) ->
        {
            Keyspaces schema = metadata.schema.getKeyspaces();
            KeyspaceMetadata existing = schema.getNullable(keyspace.name);
            if (existing != null)
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(keyspace.name);
            }

            return schema.withAddedOrUpdated(keyspace);
        };
    }

    /**
     * Creates a schema transformation that adds the provided table.
     *
     * @param table          the table to add.
     * @param ignoreIfExists if {@code true}, the transformation is a no-op if a table of the same name than
     *                       {@code table} already exists in the schema the transformation is applied on. Otherwise,
     *                       the transformation throws an {@link AlreadyExistsException} in that case.
     * @return the created transformation.
     */
    public static SchemaTransformation addTable(TableMetadata table, boolean ignoreIfExists)
    {
        return (metadata) ->
        {
            Keyspaces schema = metadata.schema.getKeyspaces();
            KeyspaceMetadata keyspace = schema.getNullable(table.keyspace);
            if (keyspace == null)
                throw invalidRequest("Keyspace '%s' doesn't exist", table.keyspace);

            if (keyspace.hasTable(table.name))
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(table.keyspace, table.name);
            }

            table.validate();

            return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.with(table)));
        };
    }

    public static SchemaTransformation addTypes(Types toAdd, boolean ignoreIfExists)
    {
        return (metadata) ->
        {
            Keyspaces schema = metadata.schema.getKeyspaces();
            if (toAdd.isEmpty())
                return schema;

            String keyspaceName = toAdd.iterator().next().keyspace;
            KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
            if (null == keyspace)
                throw invalidRequest("Keyspace '%s' doesn't exist", keyspaceName);

            Types types = keyspace.types;
            for (UserType type : toAdd)
            {
                if (types.containsType(type.name))
                {
                    if (ignoreIfExists)
                        continue;

                    throw new ConfigurationException("Type " + type + " already exists in " + keyspaceName);
                }

                types = types.with(type);
            }
            return schema.withAddedOrReplaced(keyspace.withSwapped(types));
        };
    }

    /**
     * Creates a schema transformation that adds the provided view.
     *
     * @param view           the view to add.
     * @param ignoreIfExists if {@code true}, the transformation is a no-op if a view of the same name than
     *                       {@code view} already exists in the schema the transformation is applied on. Otherwise,
     *                       the transformation throws an {@link AlreadyExistsException} in that case.
     * @return the created transformation.
     */
    public static SchemaTransformation addView(ViewMetadata view, boolean ignoreIfExists)
    {
        return (metadata) ->
        {
            Keyspaces schema = metadata.schema.getKeyspaces();
            KeyspaceMetadata keyspace = schema.getNullable(view.keyspace());
            if (keyspace == null)
                throw invalidRequest("Cannot add view to non existing keyspace '%s'", view.keyspace());

            if (keyspace.hasView(view.name()))
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(view.keyspace(), view.name());
            }

            return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.views.with(view)));
        };
    }
//
//    // Is it a bad idea to be
//    private static boolean columnsMatch(ColumnMetadata a, ColumnMetadata b)
//    {
//        return a.name.equals(b.name) && a.type.equals(b.type) && a.kind == b.kind;
//    }
//
//    private static boolean columnsMatch(List<ColumnMetadata> a, List<ColumnMetadata> b)
//    {
//        if (a.size() != b.size())
//            return false;
//
//        for (int i = 0; i < a.size(); i++)
//        {
//            if (!columnsMatch(a.get(i), b.get(i)))
//                return false;
//        }
//
//        return true;
//    }
//
//    /**
//     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
//     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
//     * and later evolved on major upgrades (sometimes minor too). This method compares the current known definitions
//     * of the tables (if the keyspace exists) to the expected, most modern ones expected by the running version of C*.
//     * If any changes have been detected, a schema transformation returned by this method should make cluster's view of
//     * that keyspace aligned with the expected modern definition.
//     *
//     * This doesn't drop columns or tables that aren't present in the new schema. It can also alter column types which
//     * isn't supported.
//     *
//     * @param keyspace   the metadata of the keyspace as it should be after application.
//     * @return the transformation.
//     */
//    @VisibleForTesting
//    public static SchemaTransformation updateSystemKeyspaceTransformation(KeyspaceMetadata keyspace)
//    {
//        return metadata -> {
//            Keyspaces schema = metadata.schema.getKeyspaces();
//            KeyspaceMetadata updatedKeyspace = keyspace;
//            KeyspaceMetadata curKeyspace = schema.getNullable(keyspace.name);
//            if (curKeyspace != null)
//            {
//                // If the keyspace already exists, we preserve whatever parameters it has.
//                updatedKeyspace = updatedKeyspace.withSwapped(curKeyspace.params);
//
//                for (TableMetadata curTable : curKeyspace.tables)
//                {
//                    TableMetadata desiredTable = updatedKeyspace.tables.getNullable(curTable.name);
//                    if (desiredTable == null)
//                    {
//                        // preserve existing tables which are missing in the new keyspace definition
//                        updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.with(curTable));
//                    }
//                    else
//                    {
//                        checkState(columnsMatch(curTable.partitionKeyColumns, desiredTable.partitionKeyColumns), "Can't alter partition key columns");
//                        checkState(columnsMatch(curTable.clusteringColumns, desiredTable.clusteringColumns), "Can't alter clustering columns");
//
//                        // Avoid updating the table epoch on no change
//                        if (columnsMatch(curTable.regularColumns(), desiredTable.regularColumns()) && columnsMatch(curTable.staticColumns(), desiredTable.staticColumns()))
//                            continue;
//
//                        updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.without(desiredTable));
//                        TableMetadata.Builder updatedBuilder = desiredTable.unbuild().epoch(metadata.nextEpoch());
//                        for (ColumnMetadata columnMetadata : curTable.regularAndStaticColumns())
//                        {
//                            ColumnMetadata desiredColumnMetadata = desiredTable.getColumn(columnMetadata.name);
//                            if (desiredColumnMetadata == null)
//                                updatedBuilder.addColumn(columnMetadata);
//                            else
//                                checkState(columnsMatch(columnMetadata, desiredColumnMetadata), "Can't alter regular or static columns");
//                        }
//
//                        updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.with(updatedBuilder.build()));
//                    }
//                }
//            }
//            return schema.withAddedOrReplaced(updatedKeyspace);
//        };
//    }
//
//    public static void maybeUpdateSystemKeyspace(KeyspaceMetadata keyspace)
//    {
//        ClusterMetadata cm = ClusterMetadata.current();
//        SchemaTransformation transformation = updateSystemKeyspaceTransformation(keyspace);
//        KeyspaceMetadata existingKeyspace = cm.schema.getKeyspaceMetadata(keyspace.name);
//        if (existingKeyspace == null)
//            return;
//
//        Keyspaces existingKeyspaces = Keyspaces.of(existingKeyspace);
//        Keyspaces keyspacees = transformation.apply(cm);
//        KeyspacesDiff diff = Keyspaces.diff(existingKeyspaces, keyspacees);
//        if (diff.isEmpty())
//            return;
//        Schema.instance.submit(transformation);
//    }
//
//    public static class ColumnDescription
//    {
//        private final String name;
//        private final CQL3Type type;
//
//        public ColumnDescription(String name, CQL3Type type)
//        {
//            this.name = name;
//            this.type = type;
//        }
//
//        private String name()
//        {
//            return name;
//        }
//
//        @Override
//        public String toString()
//        {
//            return name + " "  + type;
//        }
//    }
//    private static final String ADD_COLUMNS_CQL = "ALTER TABLE %s.%s ADD %s;";
//
//    // Add columns to a system table that should not contain any of the columns
//    public static void addColumnsToSystemTable(String keyspace, String table, List<ColumnDescription> columnsToAdd)
//    {
//        checkArgument(columnsToAdd.size() > 0, "No columns to add");
//
//        Optional<TableMetadata> maybeExistingTableMetadata = ClusterMetadata.current().schema.getKeyspaceMetadata(keyspace).tables.get(table);
//        if (!maybeExistingTableMetadata.isPresent())
//            throw new IllegalStateException(keyspace + "." + table + " should exist already");
//
//        List<String> newColumnNames = columnsToAdd.stream().map(ColumnDescription::name).collect(Collectors.toList());
//        Columns existingColumns = maybeExistingTableMetadata.get().regularColumns();
//
//        boolean allColumnsExist = newColumnNames.stream().allMatch(existingColumns::contains);
//        if (allColumnsExist)
//            return;
//
//        List<String> alreadyExistingColumns = newColumnNames.stream().filter(existingColumns::contains).collect(Collectors.toList());
//        checkState(alreadyExistingColumns.isEmpty(), keyspace + "." + table + " contains some, but not all of the new columns: " + alreadyExistingColumns);
//
//        String columnsCQL = columnsToAdd.stream().map(Object::toString).collect(Collectors.joining(", "));
//        String alterCQL = String.format(ADD_COLUMNS_CQL, keyspace, table, columnsCQL);
//        SchemaTransformation schemaTransformation = fromCql(alterCQL);
//        Schema.instance.submit(schemaTransformation);
//    }

}
