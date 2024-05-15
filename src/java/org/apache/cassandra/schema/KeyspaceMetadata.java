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

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.SchemaElement;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.UserFunctions.FunctionsDiff;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.schema.Tables.TablesDiff;
import org.apache.cassandra.schema.Types.TypesDiff;
import org.apache.cassandra.schema.Views.ViewsDiff;

import static com.google.common.collect.Iterables.any;
import static java.lang.String.format;
import static org.apache.cassandra.db.TypeSizes.sizeof;

/**
 * An immutable representation of keyspace metadata (name, params, tables, types, and functions).
 */
public final class KeyspaceMetadata implements SchemaElement
{
    public static final Serializer serializer = new Serializer();

    public enum Kind
    {
        REGULAR, VIRTUAL
    }

    public final String name;
    public final Kind kind;
    public final AbstractReplicationStrategy replicationStrategy;
    public final KeyspaceParams params;
    public final Tables tables;
    public final Views views;
    public final Types types;
    public final UserFunctions userFunctions;

    private KeyspaceMetadata(String keyspaceName, Kind kind, KeyspaceParams params, Tables tables, Views views, Types types, UserFunctions functions)
    {
        this.name = keyspaceName;
        this.kind = kind;
        this.params = params;
        this.tables = tables;
        this.views = views;
        this.types = types;
        this.userFunctions = functions;
        this.replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(keyspaceName, params.replication);
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params)
    {
        return new KeyspaceMetadata(name, Kind.REGULAR, params, Tables.none(), Views.none(), Types.none(), UserFunctions.none());
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables)
    {
        return new KeyspaceMetadata(name, Kind.REGULAR, params, tables, Views.none(), Types.none(), UserFunctions.none());
    }

    public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables, Views views, Types types, UserFunctions functions)
    {
        return new KeyspaceMetadata(name, Kind.REGULAR, params, tables, views, types, functions);
    }

    public static KeyspaceMetadata virtual(String name, Tables tables)
    {
        return new KeyspaceMetadata(name, Kind.VIRTUAL, KeyspaceParams.local(), tables, Views.none(), Types.none(), UserFunctions.none());
    }

    public KeyspaceMetadata withSwapped(KeyspaceParams params)
    {
        return new KeyspaceMetadata(name, kind, params, tables, views, types, userFunctions);
    }

    public KeyspaceMetadata withSwapped(Tables regular)
    {
        return new KeyspaceMetadata(name, kind, params, regular, views, types, userFunctions);
    }

    public KeyspaceMetadata withSwapped(Views views)
    {
        return new KeyspaceMetadata(name, kind, params, tables, views, types, userFunctions);
    }

    public KeyspaceMetadata withSwapped(Types types)
    {
        return new KeyspaceMetadata(name, kind, params, tables, views, types, userFunctions);
    }

    public KeyspaceMetadata withSwapped(UserFunctions functions)
    {
        return new KeyspaceMetadata(name, kind, params, tables, views, types, functions);
    }

    public KeyspaceMetadata empty()
    {
        return new KeyspaceMetadata(this.name, this.kind, this.params, Tables.none(), Views.none(), Types.none(), UserFunctions.none());
    }

    public boolean isVirtual()
    {
        return kind == Kind.VIRTUAL;
    }

    /**
     * Returns a new KeyspaceMetadata with all instances of old UDT replaced with the updated version.
     * Replaces all instances in tables, views, types, and functions.
     */
    public KeyspaceMetadata withUpdatedUserType(UserType udt)
    {
        return new KeyspaceMetadata(name,
                                    kind,
                                    params,
                                    tables.withUpdatedUserType(udt),
                                    views.withUpdatedUserTypes(udt),
                                    types.withUpdatedUserType(udt),
                                    userFunctions.withUpdatedUserType(udt));
    }

    public Iterable<TableMetadata> tablesAndViews()
    {
        return Iterables.concat(tables, views.allTableMetadata());
    }

    @Nullable
    public TableMetadata getTableOrViewNullable(String tableOrViewName)
    {
        ViewMetadata view = views.getNullable(tableOrViewName);
        return view == null
             ? tables.getNullable(tableOrViewName)
             : view.metadata;
    }

    @Nullable
    public TableMetadata getTableNullable(String tableName)
    {
        return tables.getNullable(tableName);
    }

    public boolean hasTable(String tableName)
    {
        return tables.get(tableName).isPresent();
    }

    public boolean hasView(String viewName)
    {
        return views.get(viewName).isPresent();
    }

    public boolean hasIndex(String indexName)
    {
        return any(tables, t -> t.indexes.has(indexName));
    }

    /**
     * @param function a user function
     * @return a stream of tables within this keyspace that have column masks using the specified user function
     */
    public Stream<TableMetadata> tablesUsingFunction(Function function)
    {
        return tables.stream().filter(table -> table.dependsOn(function));
    }

    public String findAvailableIndexName(String baseName)
    {
        if (!hasIndex(baseName))
            return baseName;

        int i = 1;
        do
        {
            String name = baseName + '_' + i++;
            if (!hasIndex(name))
                return name;
        }
        while (true);
    }

    public Optional<TableMetadata> findIndexedTable(String indexName)
    {
        for (TableMetadata table : tablesAndViews())
            if (table.indexes.has(indexName))
                return Optional.of(table);

        return Optional.empty();
    }

    public Optional<TableMetadata> getIndexMetadata(String indexName)
    {
        TableMetadata metadata = tables.indexTables().get(indexName);
        if (metadata != null)
            return Optional.of(metadata);

        return Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, kind, params, tables, views, userFunctions, types);
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
               && kind == other.kind
               && params.equals(other.params)
               && tables.equals(other.tables)
               && views.equals(other.views)
               && userFunctions.equals(other.userFunctions)
               && types.equals(other.types);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("kind", kind)
                          .add("params", params)
                          .add("tables", tables)
                          .add("views", views)
                          .add("functions", userFunctions)
                          .add("types", types)
                          .toString();
    }

    @Override
    public SchemaElementType elementType()
    {
        return SchemaElementType.KEYSPACE;
    }

    @Override
    public String elementKeyspace()
    {
        return name;
    }

    @Override
    public String elementName()
    {
        return name;
    }

    @Override
    public String toCqlString(boolean withWarnings, boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        if (isVirtual() && withWarnings)
        {
            builder.append("/*")
                   .newLine()
                   .append("Warning: Keyspace ")
                   .appendQuotingIfNeeded(name)
                   .append(" is a virtual keyspace and cannot be recreated with CQL.")
                   .newLine()
                   .append("Structure, for reference:")
                   .newLine()
                   .append("VIRTUAL KEYSPACE ")
                   .appendQuotingIfNeeded(name)
                   .append(';')
                   .newLine()
                   .append("*/")
                   .toString();
        }
        else
        {
            builder.append("CREATE KEYSPACE ");

            if (ifNotExists)
            {
                builder.append("IF NOT EXISTS ");
            }

            builder.appendQuotingIfNeeded(name)
                   .append(" WITH replication = ");

            params.replication.appendCqlTo(builder);

            builder.append("  AND durable_writes = ")
                   .append(params.durableWrites)
                   .append(';')
                   .toString();
        }
        return builder.toString();
    }

    public void validate(ClusterMetadata metadata)
    {
        if (!SchemaConstants.isValidName(name))
        {
            throw new ConfigurationException(format("Keyspace name must not be empty, more than %s characters long, "
                                                    + "or contain non-alphanumeric-underscore characters (got \"%s\")",
                                                    SchemaConstants.NAME_LENGTH,
                                                    name));
        }

        params.validate(name, null, metadata);
        tablesAndViews().forEach(TableMetadata::validate);

        Set<String> indexNames = new HashSet<>();
        for (TableMetadata table : tables)
        {
            for (IndexMetadata index : table.indexes)
            {
                if (indexNames.contains(index.name))
                    throw new ConfigurationException(format("Duplicate index name %s in keyspace %s", index.name, name));

                indexNames.add(index.name);
            }
        }
    }

    static Optional<KeyspaceDiff> diff(KeyspaceMetadata before, KeyspaceMetadata after)
    {
        return KeyspaceDiff.diff(before, after);
    }

    public static final class KeyspaceDiff
    {
        public final KeyspaceMetadata before;
        public final KeyspaceMetadata after;

        public final TablesDiff tables;
        public final ViewsDiff views;
        public final TypesDiff types;

        public final FunctionsDiff<UDFunction> udfs;
        public final FunctionsDiff<UDAggregate> udas;

        private KeyspaceDiff(KeyspaceMetadata before,
                             KeyspaceMetadata after,
                             TablesDiff tables,
                             ViewsDiff views,
                             TypesDiff types,
                             FunctionsDiff<UDFunction> udfs,
                             FunctionsDiff<UDAggregate> udas)
        {
            this.before = before;
            this.after = after;
            this.tables = tables;
            this.views = views;
            this.types = types;
            this.udfs = udfs;
            this.udas = udas;
        }

        private static Optional<KeyspaceDiff> diff(KeyspaceMetadata before, KeyspaceMetadata after)
        {
            if (before == after)
                return Optional.empty();

            if (!before.name.equals(after.name))
            {
                String msg = String.format("Attempting to diff two keyspaces with different names ('%s' and '%s')", before.name, after.name);
                throw new IllegalArgumentException(msg);
            }

            TablesDiff tables = Tables.diff(before.tables, after.tables);
            ViewsDiff views = Views.diff(before.views, after.views);
            TypesDiff types = Types.diff(before.types, after.types);

            @SuppressWarnings("unchecked") FunctionsDiff<UDFunction>  udfs = FunctionsDiff.NONE;
            @SuppressWarnings("unchecked") FunctionsDiff<UDAggregate> udas = FunctionsDiff.NONE;
            if (before.userFunctions != after.userFunctions)
            {
                udfs = UserFunctions.udfsDiff(before.userFunctions, after.userFunctions);
                udas = UserFunctions.udasDiff(before.userFunctions, after.userFunctions);
            }

            if (before.params.equals(after.params) && tables.isEmpty() && views.isEmpty() && types.isEmpty() && udfs.isEmpty() && udas.isEmpty())
                return Optional.empty();

            return Optional.of(new KeyspaceDiff(before, after, tables, views, types, udfs, udas));
        }

        @Override
        public String toString()
        {
            return "KeyspaceDiff{" +
                   "before=" + before +
                   ", after=" + after +
                   ", tables=" + tables +
                   ", views=" + views +
                   ", types=" + types +
                   ", udfs=" + udfs +
                   ", udas=" + udas +
                   '}';
        }
    }

    public static class Serializer implements MetadataSerializer<KeyspaceMetadata>
    {
        public void serialize(KeyspaceMetadata t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.name);
            Types.serializer.serialize(t.types, out, version);
            KeyspaceParams.serializer.serialize(t.params, out, version);
            UserFunctions.serializer.serialize(t.userFunctions, out, version);
            Tables.serializer.serialize(t.tables, out, version);
            Views.serializer.serialize(t.views, out, version);
        }

        public KeyspaceMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            String name = in.readUTF();
            Types types = Types.serializer.deserialize(name, in, version);
            KeyspaceParams params = KeyspaceParams.serializer.deserialize(in, version);
            UserFunctions functions = UserFunctions.serializer.deserialize(in, types, version);
            Tables tables = Tables.serializer.deserialize(in, types, functions, version);
            Views views = Views.serializer.deserialize(in, types, functions, version);
            return KeyspaceMetadata.create(name, params, tables, views, types, functions);
        }

        public long serializedSize(KeyspaceMetadata t, Version version)
        {
            return sizeof(t.name)
                   + Types.serializer.serializedSize(t.types, version)
                   + KeyspaceParams.serializer.serializedSize(t.params, version)
                   + UserFunctions.serializer.serializedSize(t.userFunctions, version)
                   + Tables.serializer.serializedSize(t.tables, version)
                   + Views.serializer.serializedSize(t.views, version);
        }
    }
}
