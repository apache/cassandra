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
package org.apache.cassandra.db;

import static java.lang.String.format;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Lists;

/**
 * Base requirements for a VirtualTable. This is required to provide metadata about the virtual table, such as the
 * partition and clustering keys, and provide a ReadQuery for a SelectStatement.
 */
public abstract class VirtualTable
{
    public final static ConcurrentHashMap<String, VirtualSchema> schemas = new ConcurrentHashMap<>();
    protected final TableMetadata metadata;
    protected String keyspace;
    protected String name;

    public VirtualTable(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    public String getTableName()
    {
        return this.metadata.name;
    }

    /**
     * Is this table writable?
     *
     * @return True if UPDATE is supported
     */
    public boolean writable()
    {
        return false;
    }

    /**
     * If the table allows unrestricted queries (ie filter on clustering key with no partition). Since These tables are
     * not backed by the C* data model, this restriction isnt always necessary.
     */
    public boolean allowFiltering()
    {
        return true;
    }

    /**
     * Return some implementation of a ReadQuery for a given select statement and query options.
     * 
     * @param selectStatement
     * @param options
     * @param limits
     * @param nowInSec
     * @return ReadQuery
     */
    public abstract ReadQuery getQuery(SelectStatement selectStatement, QueryOptions options, DataLimits limits,
            int nowInSec);

    /**
     * Execute an update operation.
     *
     * @param partitionKey
     *            partition key for the update.
     * @param params
     *            parameters of the update.
     */
    public void mutate(DecoratedKey partitionKey, Row row) throws CassandraException
    {
        // this should not be called unless writable is overridden
        throw new InvalidRequestException("Not Implemented");
    }

    public static String getFullClassName(String name)
    {
        return name.contains(".")
                ? name
                : "org.apache.cassandra.db.virtual." + name;
    }

    public static Class<? extends VirtualTable> classFromName(String name)
    {
        String className = getFullClassName(name);
        Class<VirtualTable> strategyClass = FBUtilities.classForName(className, "virtual table");

        if (!VirtualTable.class.isAssignableFrom(strategyClass))
        {
            throw new ConfigurationException(format("Compaction strategy class %s is not derived from VirtualTable",
                    className));
        }

        return strategyClass;
    }

    public static VirtualSchema getSchema(String className)
    {
        return schemas.get(getFullClassName(className));
    }

    public static SchemaBuilder schemaBuilder(Map<String, CQL3Type> definitions)
    {
        return new SchemaBuilder(new Exception().getStackTrace()[1].getClassName(), definitions);
    }

    public static class SchemaBuilder
    {
        public final List<String> key = Lists.newArrayList();
        public final List<String> clustering = Lists.newArrayList();
        private final String className;
        private final Map<String, CQL3Type> definitions;

        public SchemaBuilder(String className, Map<String, CQL3Type> definitions)
        {
            this.className = className;
            this.definitions = definitions;
        }
        public SchemaBuilder addKey(String k)
        {
            key.add(k);
            return this;
        }

        public SchemaBuilder addClustering(String c)
        {
            clustering.add(c);
            return this;
        }

        public void register()
        {
            if (key.isEmpty())
                throw new IllegalStateException("Must have at least one primary key");
            VirtualTable.schemas.putIfAbsent(className, new VirtualSchema(definitions, key, clustering));
        }
    }

    public static class VirtualSchema
    {
        public List<String> key = Lists.newArrayList();
        public List<String> clustering = Lists.newArrayList();
        public final Map<String, CQL3Type> definitions;

        private VirtualSchema(Map<String, CQL3Type> definitions, List<String> key, List<String> clustering)
        {
            this.definitions = definitions;
            this.key = key;
            this.clustering = clustering;
        }
    }
}
