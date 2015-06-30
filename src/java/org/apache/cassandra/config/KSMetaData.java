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
package org.apache.cassandra.config;

import java.util.*;

import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.service.StorageService;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    public final boolean durableWrites;

    public final Tables tables;
    public final Types types;
    public final Functions functions;

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites)
    {
        this(name, strategyClass, strategyOptions, durableWrites, Tables.none(), Types.none(), Functions.none());
    }

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites,
                      Tables tables)
    {
        this(name, strategyClass, strategyOptions, durableWrites, tables, Types.none(), Functions.none());
    }

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites,
                      Tables tables,
                      Functions functions)
    {
        this(name, strategyClass, strategyOptions, durableWrites, tables, Types.none(), functions);
    }

    private KSMetaData(String name,
                       Class<? extends AbstractReplicationStrategy> strategyClass,
                       Map<String, String> strategyOptions,
                       boolean durableWrites,
                       Tables tables,
                       Types types,
                       Functions functions)
    {
        this.name = name;
        this.strategyClass = strategyClass == null ? NetworkTopologyStrategy.class : strategyClass;
        this.strategyOptions = strategyOptions;
        this.durableWrites = durableWrites;
        this.tables = tables;
        this.types = types;
        this.functions = functions;
    }

    // For new user created keyspaces (through CQL)
    public static KSMetaData newKeyspace(String name, String strategyName, Map<String, String> options, boolean durableWrites) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return newKeyspace(name, cls, options, durableWrites, Tables.none());
    }

    public static KSMetaData newKeyspace(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> options, boolean durablesWrites, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(name, strategyClass, options, durablesWrites, Tables.of(cfDefs), Types.none(), Functions.none());
    }

    public KSMetaData cloneWith(Tables tables, Types types, Functions functions)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, durableWrites, tables, types, functions);
    }

    public KSMetaData cloneWith(Tables tables)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, durableWrites, tables, types, functions);
    }

    public KSMetaData cloneWith(Types types)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, durableWrites, tables, types, functions);
    }

    public KSMetaData cloneWith(Functions functions)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, durableWrites, tables, types, functions);
    }

    public static KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Tables.of(cfDefs));
    }

    public static KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Tables.of(cfDefs));
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, strategyClass, strategyOptions, durableWrites, tables, functions, types);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KSMetaData))
            return false;

        KSMetaData other = (KSMetaData) o;

        return Objects.equal(name, other.name)
            && Objects.equal(strategyClass, other.strategyClass)
            && Objects.equal(strategyOptions, other.strategyOptions)
            && Objects.equal(durableWrites, other.durableWrites)
            && Objects.equal(tables, other.tables)
            && Objects.equal(functions, other.functions)
            && Objects.equal(types, other.types);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("strategyClass", strategyClass.getSimpleName())
                      .add("strategyOptions", strategyOptions)
                      .add("durableWrites", durableWrites)
                      .add("tables", tables)
                      .add("functions", functions)
                      .add("types", types)
                      .toString();
    }

    public static Map<String,String> optsWithRF(final Integer rf)
    {
        return Collections.singletonMap("replication_factor", rf.toString());
    }

    public KSMetaData validate() throws ConfigurationException
    {
        if (!CFMetaData.isNameValid(name))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, name));

        // Attempt to instantiate the ARS, which will throw a ConfigException if the strategy_options aren't fully formed
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy.validateReplicationStrategy(name, strategyClass, tmd, eps, strategyOptions);

        tables.forEach(CFMetaData::validate);
        return this;
    }
}
