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
package org.apache.cassandra.auth;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.schema.Schema;

/**
 * The primary type of resource in Cassandra.
 *
 * Used to represent a table or a keyspace or the root level "data" resource.
 * "data"                                 - the root level data resource.
 * "data/keyspace_name"                   - keyspace-level data resource.
 * "data/keyspace_name/*"                 - all tables-level data resource.
 * "data/keyspace_name/table_name"        - table-level data resource.
 */
public class DataResource implements IResource
{
    enum Level
    {
        ROOT, KEYSPACE, ALL_TABLES, TABLE
    }

    // permissions which may be granted on tables
    private static final Set<Permission> TABLE_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.ALTER,
                                                                                         Permission.DROP,
                                                                                         Permission.SELECT,
                                                                                         Permission.MODIFY,
                                                                                         Permission.AUTHORIZE,
                                                                                         Permission.UNMASK,
                                                                                         Permission.SELECT_MASKED);

    // permissions which may be granted on all tables of a given keyspace
    private static final Set<Permission> ALL_TABLES_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                              Permission.ALTER,
                                                                                              Permission.DROP,
                                                                                              Permission.SELECT,
                                                                                              Permission.MODIFY,
                                                                                              Permission.AUTHORIZE,
                                                                                              Permission.UNMASK,
                                                                                              Permission.SELECT_MASKED);

    // permissions which may be granted on one or all keyspaces
    private static final Set<Permission> KEYSPACE_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                            Permission.ALTER,
                                                                                            Permission.DROP,
                                                                                            Permission.SELECT,
                                                                                            Permission.MODIFY,
                                                                                            Permission.AUTHORIZE,
                                                                                            Permission.UNMASK,
                                                                                            Permission.SELECT_MASKED);
    private static final String ROOT_NAME = "data";
    private static final DataResource ROOT_RESOURCE = new DataResource(Level.ROOT, null, null);

    private final Level level;
    private final String keyspace;
    private final String table;

    // memoized hashcode since DataRessource is immutable and used in hashmaps often
    private final transient int hash;

    private DataResource(Level level, String keyspace, String table)
    {
        this.level = level;
        this.keyspace = keyspace;
        this.table = table;

        this.hash = Objects.hashCode(level, keyspace, table);
    }

    /**
     * @return the root-level resource.
     */
    public static DataResource root()
    {
        return ROOT_RESOURCE;
    }

    /**
     * Creates a DataResource representing a keyspace.
     *
     * @param keyspace Name of the keyspace.
     * @return DataResource instance representing the keyspace.
     */
    public static DataResource keyspace(String keyspace)
    {
        return new DataResource(Level.KEYSPACE, keyspace, null);
    }

    /**
     * Creates a DataResource representing all tables of a keyspace.
     *
     * @param keyspace Name of the keyspace.
     * @return DataResource instance representing the keyspace.
     */
    public static DataResource allTables(String keyspace)
    {
        return new DataResource(Level.ALL_TABLES, keyspace, null);
    }

    /**
     * Creates a DataResource instance representing a table.
     *
     * @param keyspace Name of the keyspace.
     * @param table Name of the table.
     * @return DataResource instance representing the column family.
     */
    public static DataResource table(String keyspace, String table)
    {
        return new DataResource(Level.TABLE, keyspace, table);
    }

    /**
     * Parses a data resource name into a DataResource instance.
     *
     * @param name Name of the data resource.
     * @return DataResource instance matching the name.
     */
    public static DataResource fromName(String name)
    {
        String[] parts = StringUtils.split(name, '/');

        if (!parts[0].equals(ROOT_NAME) || parts.length > 3)
            throw new IllegalArgumentException(String.format("%s is not a valid data resource name", name));

        if (parts.length == 1)
            return root();

        if (parts.length == 2)
            return keyspace(parts[1]);

        if ("*".equals(parts[2]))
            return allTables(parts[1]);

        return table(parts[1], parts[2]);
    }

    /**
     * @return Printable name of the resource.
     */
    public String getName()
    {
        switch (level)
        {
            case ROOT:
                return ROOT_NAME;
            case KEYSPACE:
                return String.format("%s/%s", ROOT_NAME, keyspace);
            case ALL_TABLES:
                return String.format("%s/%s/*", ROOT_NAME, keyspace);
            case TABLE:
                return String.format("%s/%s/%s", ROOT_NAME, keyspace, table);
        }
        throw new AssertionError();
    }

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    public IResource getParent()
    {
        switch (level)
        {
            case KEYSPACE:
                return root();
            case ALL_TABLES:
                return keyspace(keyspace);
            case TABLE:
                return allTables(keyspace);
        }
        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    public boolean isRootLevel()
    {
        return level == Level.ROOT;
    }

    public boolean isKeyspaceLevel()
    {
        return level == Level.KEYSPACE;
    }

    public boolean isAllTablesLevel()
    {
        return level == Level.ALL_TABLES;
    }

    public boolean isTableLevel()
    {
        return level == Level.TABLE;
    }
    /**
     * @return keyspace of the resource. Throws IllegalStateException if it's the root-level resource.
     */
    public String getKeyspace()
    {
        if (isRootLevel())
            throw new IllegalStateException("ROOT data resource has no keyspace");
        return keyspace;
    }

    /**
     * @return column family of the resource. Throws IllegalStateException if it's not a table-level resource.
     */
    public String getTable()
    {
        if (!isTableLevel())
            throw new IllegalStateException(String.format("%s data resource has no table", level));
        return table;
    }

    /**
     * @return Whether or not the resource has a parent in the hierarchy.
     */
    public boolean hasParent()
    {
        return level != Level.ROOT;
    }

    /**
     * @return Whether or not the resource exists in Cassandra.
     */
    public boolean exists()
    {
        switch (level)
        {
            case ROOT:
                return true;
            case KEYSPACE:
            case ALL_TABLES:
                return Schema.instance.getKeyspaces().contains(keyspace);
            case TABLE:
                return Schema.instance.getTableMetadata(keyspace, table) != null;
        }
        throw new AssertionError();
    }

    public Set<Permission> applicablePermissions()
    {
        switch (level)
        {
            case ROOT:
            case KEYSPACE:
                return KEYSPACE_LEVEL_PERMISSIONS;
            case ALL_TABLES:
                return ALL_TABLES_LEVEL_PERMISSIONS;
            case TABLE:
                return TABLE_LEVEL_PERMISSIONS;
        }
        throw new AssertionError();
    }

    @Override
    public String toString()
    {
        switch (level)
        {
            case ROOT:
                return "<all keyspaces>";
            case KEYSPACE:
                return String.format("<keyspace %s>", keyspace);
            case ALL_TABLES:
                return String.format("<all tables in %s>", keyspace);
            case TABLE:
                return String.format("<table %s.%s>", keyspace, table);
        }
        throw new AssertionError();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof DataResource))
            return false;

        DataResource ds = (DataResource) o;

        return Objects.equal(level, ds.level)
            && Objects.equal(keyspace, ds.keyspace)
            && Objects.equal(table, ds.table);
    }

    @Override
    public int hashCode()
    {
        return hash;
    }
}
