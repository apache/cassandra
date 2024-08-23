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
}
