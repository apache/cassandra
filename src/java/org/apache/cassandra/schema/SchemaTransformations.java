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

import java.util.Optional;

import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Factory and utility methods to create simple schema transformation.
 */
public class SchemaTransformations
{
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
        return schema ->
        {
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
        return schema ->
        {
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
        return schema ->
        {
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
        return schema ->
        {
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

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
     * and later evolved on major upgrades (sometimes minor too). This method compares the current known definitions
     * of the tables (if the keyspace exists) to the expected, most modern ones expected by the running version of C*.
     * If any changes have been detected, a schema transformation returned by this method should make cluster's view of
     * that keyspace aligned with the expected modern definition.
     *
     * @param keyspace   the metadata of the keyspace as it should be after application.
     * @param generation timestamp to use for the table changes in the schema mutation
     * @return the transformation.
     */
    public static SchemaTransformation updateSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        return new SchemaTransformation()
        {
            @Override
            public Optional<Long> fixedTimestampMicros()
            {
                return Optional.of(generation);
            }

            @Override
            public Keyspaces apply(Keyspaces schema)
            {
                KeyspaceMetadata updatedKeyspace = keyspace;
                KeyspaceMetadata curKeyspace = schema.getNullable(keyspace.name);
                if (curKeyspace != null)
                {
                    // If the keyspace already exists, we preserve whatever parameters it has.
                    updatedKeyspace = updatedKeyspace.withSwapped(curKeyspace.params);

                    for (TableMetadata curTable : curKeyspace.tables)
                    {
                        TableMetadata desiredTable = updatedKeyspace.tables.getNullable(curTable.name);
                        if (desiredTable == null)
                        {
                            // preserve exsiting tables which are missing in the new keyspace definition
                            updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.with(curTable));
                        }
                        else
                        {
                            updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.without(desiredTable));

                            TableMetadata.Builder updatedBuilder = desiredTable.unbuild();

                            for (ColumnMetadata column : curTable.regularAndStaticColumns())
                            {
                                if (!desiredTable.regularAndStaticColumns().contains(column))
                                    updatedBuilder.addColumn(column);
                            }

                            updatedKeyspace = updatedKeyspace.withSwapped(updatedKeyspace.tables.with(updatedBuilder.build()));
                        }
                    }
                }
                return schema.withAddedOrReplaced(updatedKeyspace);
            }
        };
    }

}