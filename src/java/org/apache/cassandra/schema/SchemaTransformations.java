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


import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;

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

    /**
     * Creates a schema transformation that either add the provided type, or "update" (replace really) it to be the
     * provided type.
     *
     * <p>Please note that this usually <b>unsafe</b>: if the type exists, this replace it without any particular check
     * and so could replace it with an incompatible version. This is used internally however for hard-coded tables
     * (System ones, including DSE ones) to force the "last version".
     *
     * @param type the type to add/update.
     * @return the created transformation.
     */
    public static SchemaTransformation addOrUpdateType(UserType type)
    {
        return schema ->
        {
            KeyspaceMetadata keyspace = schema.getNullable(type.keyspace);
            if (null == keyspace)
                throw invalidRequest("Keyspace '%s' doesn't exist", type.keyspace);

            Types newTypes = keyspace.types.get(type.name).isPresent()
                             ? keyspace.types.withUpdatedUserType(type)
                             : keyspace.types.with(type);
            return schema.withAddedOrUpdated(keyspace.withSwapped(newTypes));
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
}