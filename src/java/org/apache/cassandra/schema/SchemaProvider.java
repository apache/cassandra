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

import java.util.*;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;

public interface SchemaProvider
{
    Set<String> getKeyspaces();
    int getNumberOfTables();

    ClusterMetadata submit(SchemaTransformation transformation);

    default UUID getVersion()
    {
        return ClusterMetadata.current().schema.getVersion();
    }

    Keyspaces localKeyspaces();
    Keyspaces distributedKeyspaces();
    Keyspaces distributedAndLocalKeyspaces();
    Keyspaces getUserKeyspaces();

    void registerListener(SchemaChangeListener listener);
    void unregisterListener(SchemaChangeListener listener);

    SchemaChangeNotifier schemaChangeNotifier();

    Optional<TableMetadata> getIndexMetadata(String keyspace, String index);

    default TableMetadata getTableMetadata(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname);
    }

    default TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname).ref;
    }

    default ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = distributedKeyspaces().getNullable(keyspaceName);
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    default Keyspaces getNonLocalStrategyKeyspaces()
    {
        return distributedKeyspaces().filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class);
    }

    default TableMetadata validateTable(String keyspaceName, String tableName)
    {
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        KeyspaceMetadata keyspace = getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException(String.format("keyspace %s does not exist", keyspaceName));

        TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
        if (metadata == null)
            throw new InvalidRequestException(String.format("table %s does not exist", tableName));

        return metadata;
    }

    default ColumnFamilyStore getColumnFamilyStoreInstance(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace instance = getKeyspaceInstance(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
               ? instance.getColumnFamilyStore(metadata.id)
               : null;
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    default Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        Keyspace ks = getKeyspaceInstance(keyspaceName);
        Preconditions.checkNotNull(ks, "Keyspace %s not found", keyspaceName);
        return ks.getMetadata().tablesAndViews();
    }

    @Nullable
    Keyspace getKeyspaceInstance(String keyspaceName);

    @Nullable
    KeyspaceMetadata getKeyspaceMetadata(String keyspaceName);

    @Nullable
    TableMetadata getTableMetadata(TableId id);

    @Nullable
    default TableMetadataRef getTableMetadataRef(TableId id)
    {
        return getTableMetadata(id).ref;
    }

    @Nullable
    TableMetadata getTableMetadata(String keyspace, String table);

    default TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        return getTableMetadata(keyspace, table).ref;
    }

    default TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        // TODO this should not even be possible anymore.
        String message =
            String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema"
                          + "not being fully propagated.  Please wait for schema agreement on table creation.",
                          id);
        throw new UnknownTableException(message, id);
    }

    /* Function helpers */

    /**
     * Get all function overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the keyspace or the function name are not found;
     *         a non-empty collection of {@link Function} otherwise
     */
    default Collection<UserFunction> getUserFunctions(FunctionName name)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
               ? Collections.emptyList()
               : ksm.userFunctions.get(name);
    }

    /**
     * Find the function with the specified name
     *
     * @param name     fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    default Optional<UserFunction> findFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
               ? Optional.empty()
               : ksm.userFunctions.find(name, argTypes);
    }

    /**
     * Compute the largest gc grace seconds amongst all the tables
     * @return the largest gcgs.
     */
    default int largestGcgs()
    {
        return distributedAndLocalKeyspaces().stream()
                                        .flatMap(ksm -> ksm.tables.stream())
                                        .mapToInt(tm -> tm.params.gcGraceSeconds)
                                        .max()
                                        .orElse(Integer.MIN_VALUE);
    }

    // TODO: remove?
    public abstract void saveSystemKeyspace();

    /**
     * Find the function with the specified name and arguments.
     *
     * @param name     fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    default Optional<UserFunction> findUserFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        return Optional.ofNullable(getKeyspaceMetadata(name.keyspace))
                       .flatMap(ksm -> ksm.userFunctions.find(name, argTypes));
    }
}
