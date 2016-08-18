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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * An immutable representation of keyspace metadata (name, params, tables, types, and functions).
 */
public final class KeyspaceMetadata
{
    public final String name;
    public final KeyspaceParams params;
    public final Tables tables;
    public final Views views;
    public final Types types;
    public final Functions functions;

    private KeyspaceMetadata(String name, KeyspaceParams params, Tables tables, Views views, Types types, Functions functions)
    {
        this.name = name;
        this.params = params;
        this.tables = tables;
        this.views = views;
        this.types = types;
        this.functions = functions;
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params)
    {
        return new KeyspaceMetadata(name, params, Tables.none(), Views.none(), Types.none(), Functions.none());
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables)
    {
        return new KeyspaceMetadata(name, params, tables, Views.none(), Types.none(), Functions.none());
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables, Views views, Types types, Functions functions)
    {
        return new KeyspaceMetadata(name, params, tables, views, types, functions);
    }

    public KeyspaceMetadata withSwapped(KeyspaceParams params)
    {
        return new KeyspaceMetadata(name, params, tables, views, types, functions);
    }

    public KeyspaceMetadata withSwapped(Tables regular)
    {
        return new KeyspaceMetadata(name, params, regular, views, types, functions);
    }

    public KeyspaceMetadata withSwapped(Views views)
    {
        return new KeyspaceMetadata(name, params, tables, views, types, functions);
    }

    public KeyspaceMetadata withSwapped(Types types)
    {
        return new KeyspaceMetadata(name, params, tables, views, types, functions);
    }

    public KeyspaceMetadata withSwapped(Functions functions)
    {
        return new KeyspaceMetadata(name, params, tables, views, types, functions);
    }

    public Iterable<CFMetaData> tablesAndViews()
    {
        return Iterables.concat(tables, views.metadatas());
    }

    @Nullable
    public CFMetaData getTableOrViewNullable(String tableOrViewName)
    {
        ViewDefinition view = views.getNullable(tableOrViewName);
        return view == null
             ? tables.getNullable(tableOrViewName)
             : view.metadata;
    }

    public Set<String> existingIndexNames(String cfToExclude)
    {
        Set<String> indexNames = new HashSet<>();
        for (CFMetaData table : tables)
            if (cfToExclude == null || !table.cfName.equals(cfToExclude))
                for (IndexMetadata index : table.getIndexes())
                    indexNames.add(index.name);
        return indexNames;
    }

    public Optional<CFMetaData> findIndexedTable(String indexName)
    {
        for (CFMetaData cfm : tablesAndViews())
            if (cfm.getIndexes().has(indexName))
                return Optional.of(cfm);

        return Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, params, tables, views, functions, types);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KeyspaceMetadata))
            return false;

        KeyspaceMetadata other = (KeyspaceMetadata) o;

        return name.equals(other.name)
            && params.equals(other.params)
            && tables.equals(other.tables)
            && views.equals(other.views)
            && functions.equals(other.functions)
            && types.equals(other.types);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("params", params)
                          .add("tables", tables)
                          .add("views", views)
                          .add("functions", functions)
                          .add("types", types)
                          .toString();
    }

    public void validate()
    {
        if (!CFMetaData.isNameValid(name))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, "
                                                           + "or contain non-alphanumeric-underscore characters (got \"%s\")",
                                                           SchemaConstants.NAME_LENGTH,
                                                           name));
        params.validate(name);
        tablesAndViews().forEach(CFMetaData::validate);
    }
}
