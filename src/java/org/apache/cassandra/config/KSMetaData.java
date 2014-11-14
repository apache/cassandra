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

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.utils.FBUtilities.*;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    private final Map<String, CFMetaData> cfMetaData;
    public final boolean durableWrites;

    public final UTMetaData userTypes;

    public KSMetaData(String name,
                      Class<? extends AbstractReplicationStrategy> strategyClass,
                      Map<String, String> strategyOptions,
                      boolean durableWrites,
                      Iterable<CFMetaData> cfDefs)
    {
        this(name, strategyClass, strategyOptions, durableWrites, cfDefs, new UTMetaData());
    }

    KSMetaData(String name,
               Class<? extends AbstractReplicationStrategy> strategyClass,
               Map<String, String> strategyOptions,
               boolean durableWrites,
               Iterable<CFMetaData> cfDefs,
               UTMetaData userTypes)
    {
        this.name = name;
        this.strategyClass = strategyClass == null ? NetworkTopologyStrategy.class : strategyClass;
        this.strategyOptions = strategyOptions;
        Map<String, CFMetaData> cfmap = new HashMap<>();
        for (CFMetaData cfm : cfDefs)
            cfmap.put(cfm.cfName, cfm);
        this.cfMetaData = Collections.unmodifiableMap(cfmap);
        this.durableWrites = durableWrites;
        this.userTypes = userTypes;
    }

    // For new user created keyspaces (through CQL)
    public static KSMetaData newKeyspace(String name, String strategyName, Map<String, String> options, boolean durableWrites) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return newKeyspace(name, cls, options, durableWrites, Collections.<CFMetaData>emptyList());
    }

    public static KSMetaData newKeyspace(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> options, boolean durablesWrites, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(name, strategyClass, options, durablesWrites, cfDefs, new UTMetaData());
    }

    public static KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs, ksm.userTypes);
    }

    public static KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs));
    }

    public static KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs));
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, strategyClass, strategyOptions, cfMetaData, durableWrites, userTypes);
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
            && Objects.equal(cfMetaData, other.cfMetaData)
            && Objects.equal(durableWrites, other.durableWrites)
            && Objects.equal(userTypes, other.userTypes);
    }

    public Map<String, CFMetaData> cfMetaData()
    {
        return cfMetaData;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", name)
                      .add("strategyClass", strategyClass.getSimpleName())
                      .add("strategyOptions", strategyOptions)
                      .add("cfMetaData", cfMetaData)
                      .add("durableWrites", durableWrites)
                      .add("userTypes", userTypes)
                      .toString();
    }

    public static Map<String,String> optsWithRF(final Integer rf)
    {
        return Collections.singletonMap("replication_factor", rf.toString());
    }

    public Mutation toSchemaUpdate(KSMetaData newState, long modificationTimestamp)
    {
        return newState.toSchema(modificationTimestamp);
    }

    public KSMetaData validate() throws ConfigurationException
    {
        if (!CFMetaData.isNameValid(name))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, name));

        // Attempt to instantiate the ARS, which will throw a ConfigException if the strategy_options aren't fully formed
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy.validateReplicationStrategy(name, strategyClass, tmd, eps, strategyOptions);

        for (CFMetaData cfm : cfMetaData.values())
            cfm.validate();

        return this;
    }

    public KSMetaData reloadAttributes()
    {
        Row ksDefRow = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_KEYSPACES_TABLE, name);

        if (ksDefRow.cf == null)
            throw new RuntimeException(String.format("%s not found in the schema definitions keyspaceName (%s).", name, SystemKeyspace.SCHEMA_KEYSPACES_TABLE));

        return fromSchema(ksDefRow, Collections.<CFMetaData>emptyList(), userTypes);
    }

    public Mutation dropFromSchema(long timestamp)
    {
        Mutation mutation = new Mutation(SystemKeyspace.NAME, SystemKeyspace.getSchemaKSKey(name));

        mutation.delete(SystemKeyspace.SCHEMA_KEYSPACES_TABLE, timestamp);
        mutation.delete(SystemKeyspace.SCHEMA_COLUMNFAMILIES_TABLE, timestamp);
        mutation.delete(SystemKeyspace.SCHEMA_COLUMNS_TABLE, timestamp);
        mutation.delete(SystemKeyspace.SCHEMA_TRIGGERS_TABLE, timestamp);
        mutation.delete(SystemKeyspace.SCHEMA_USER_TYPES_TABLE, timestamp);
        mutation.delete(SystemKeyspace.SCHEMA_FUNCTIONS_TABLE, timestamp);
        mutation.delete(SystemKeyspace.BUILT_INDEXES_TABLE, timestamp);

        return mutation;
    }

    public Mutation toSchema(long timestamp)
    {
        Mutation mutation = new Mutation(SystemKeyspace.NAME, SystemKeyspace.getSchemaKSKey(name));
        ColumnFamily cf = mutation.addOrGet(SystemKeyspace.SchemaKeyspacesTable);
        CFRowAdder adder = new CFRowAdder(cf, SystemKeyspace.SchemaKeyspacesTable.comparator.builder().build(), timestamp);

        adder.add("durable_writes", durableWrites);
        adder.add("strategy_class", strategyClass.getName());
        adder.add("strategy_options", json(strategyOptions));

        for (CFMetaData cfm : cfMetaData.values())
            cfm.toSchema(mutation, timestamp);

        userTypes.toSchema(mutation, timestamp);
        return mutation;
    }

    /**
     * Deserialize only Keyspace attributes without nested ColumnFamilies
     *
     * @param row Keyspace attributes in serialized form
     *
     * @return deserialized keyspace without cf_defs
     */
    public static KSMetaData fromSchema(Row row, Iterable<CFMetaData> cfms, UTMetaData userTypes)
    {
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_keyspaces", row).one();
        try
        {
            return new KSMetaData(result.getString("keyspace_name"),
                                  AbstractReplicationStrategy.getClass(result.getString("strategy_class")),
                                  fromJsonMap(result.getString("strategy_options")),
                                  result.getBoolean("durable_writes"),
                                  cfms,
                                  userTypes);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize Keyspace with nested ColumnFamilies
     *
     * @param serializedKs Keyspace in serialized form
     * @param serializedCFs Collection of the serialized ColumnFamilies
     *
     * @return deserialized keyspace with cf_defs
     */
    public static KSMetaData fromSchema(Row serializedKs, Row serializedCFs, Row serializedUserTypes)
    {
        Map<String, CFMetaData> cfs = deserializeColumnFamilies(serializedCFs);
        UTMetaData userTypes = new UTMetaData(UTMetaData.fromSchema(serializedUserTypes));
        return fromSchema(serializedKs, cfs.values(), userTypes);
    }

    /**
     * Deserialize ColumnFamilies from low-level schema representation, all of them belong to the same keyspace
     *
     * @return map containing name of the ColumnFamily and it's metadata for faster lookup
     */
    public static Map<String, CFMetaData> deserializeColumnFamilies(Row row)
    {
        if (row.cf == null)
            return Collections.emptyMap();

        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", row);
        Map<String, CFMetaData> cfms = new HashMap<>(results.size());
        for (UntypedResultSet.Row result : results)
        {
            CFMetaData cfm = CFMetaData.fromSchema(result);
            cfms.put(cfm.cfName, cfm);
        }
        return cfms;
    }
}
