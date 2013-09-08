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

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.Schema;

/**
 * The primary type of resource in Cassandra.
 *
 * Used to represent a column family or a keyspace or the root level "data" resource.
 * "data"                                 - the root level data resource.
 * "data/keyspace_name"                   - keyspace-level data resource.
 * "data/keyspace_name/column_family_name" - cf-level data resource.
 */
public class DataResource implements IResource
{
    enum Level
    {
        ROOT, KEYSPACE, COLUMN_FAMILY
    }

    private static final String ROOT_NAME = "data";
    private static final DataResource ROOT_RESOURCE = new DataResource();

    private final Level level;
    private final String keyspace;
    private final String columnFamily;

    private DataResource()
    {
        level = Level.ROOT;
        keyspace = null;
        columnFamily = null;
    }

    private DataResource(String keyspace)
    {
        level = Level.KEYSPACE;
        this.keyspace = keyspace;
        columnFamily = null;
    }

    private DataResource(String keyspace, String columnFamily)
    {
        level = Level.COLUMN_FAMILY;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
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
        return new DataResource(keyspace);
    }

    /**
     * Creates a DataResource instance representing a column family.
     *
     * @param keyspace Name of the keyspace.
     * @param columnFamily Name of the column family.
     * @return DataResource instance representing the column family.
     */
    public static DataResource columnFamily(String keyspace, String columnFamily)
    {
        return new DataResource(keyspace, columnFamily);
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

        return columnFamily(parts[1], parts[2]);
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
            case COLUMN_FAMILY:
                return String.format("%s/%s/%s", ROOT_NAME, keyspace, columnFamily);
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
            case COLUMN_FAMILY:
                return keyspace(keyspace);
        }
        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    public boolean isRootLevel()
    {
        return level.equals(Level.ROOT);
    }

    public boolean isKeyspaceLevel()
    {
        return level.equals(Level.KEYSPACE);
    }

    public boolean isColumnFamilyLevel()
    {
        return level.equals(Level.COLUMN_FAMILY);
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
     * @return column family of the resource. Throws IllegalStateException if it's not a cf-level resource.
     */
    public String getColumnFamily()
    {
        if (!isColumnFamilyLevel())
            throw new IllegalStateException(String.format("%s data resource has no column family", level));
        return columnFamily;
    }

    /**
     * @return Whether or not the resource has a parent in the hierarchy.
     */
    public boolean hasParent()
    {
        return !level.equals(Level.ROOT);
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
                return Schema.instance.getKeyspaces().contains(keyspace);
            case COLUMN_FAMILY:
                return Schema.instance.getCFMetaData(keyspace, columnFamily) != null;
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
            case COLUMN_FAMILY:
                return String.format("<table %s.%s>", keyspace, columnFamily);
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
            && Objects.equal(columnFamily, ds.columnFamily);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level, keyspace, columnFamily);
    }
}
