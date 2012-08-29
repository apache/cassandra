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

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.utils.FBUtilities.*;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    private final Map<String, CFMetaData> cfMetaData;
    public final boolean durableWrites;

    KSMetaData(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, boolean durableWrites, Iterable<CFMetaData> cfDefs)
    {
        this.name = name;
        this.strategyClass = strategyClass == null ? NetworkTopologyStrategy.class : strategyClass;
        this.strategyOptions = strategyOptions;
        Map<String, CFMetaData> cfmap = new HashMap<String, CFMetaData>();
        for (CFMetaData cfm : cfDefs)
            cfmap.put(cfm.cfName, cfm);
        this.cfMetaData = Collections.unmodifiableMap(cfmap);
        this.durableWrites = durableWrites;
    }

    // For new user created keyspaces (through CQL)
    public static KSMetaData newKeyspace(String name, String strategyName, Map<String, String> options) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(strategyName);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return new KSMetaData(name, cls, options, true, Collections.<CFMetaData>emptyList());
    }

    public static KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs);
    }

    public static KSMetaData systemKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaData.LocalCf,
                                                CFMetaData.PeersCf,
                                                CFMetaData.HintsCf,
                                                CFMetaData.IndexCf,
                                                CFMetaData.NodeIdCf,
                                                CFMetaData.SchemaKeyspacesCf,
                                                CFMetaData.SchemaColumnFamiliesCf,
                                                CFMetaData.SchemaColumnsCf,
                                                CFMetaData.OldStatusCf,
                                                CFMetaData.OldHintsCf,
                                                CFMetaData.MigrationsCf,
                                                CFMetaData.SchemaCf);
        return new KSMetaData(Table.SYSTEM_KS, LocalStrategy.class, Collections.<String, String>emptyMap(), true, cfDefs);
    }

    public static KSMetaData traceKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaData.TraceSessionsCf, CFMetaData.TraceEventsCf);
        return new KSMetaData(Tracing.TRACE_KS, SimpleStrategy.class, ImmutableMap.of("replication_factor", "1"), true, cfDefs);
    }

    public static KSMetaData testMetadata(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, true, Arrays.asList(cfDefs));
    }

    public static KSMetaData testMetadataNotDurable(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, CFMetaData... cfDefs)
    {
        return new KSMetaData(name, strategyClass, strategyOptions, false, Arrays.asList(cfDefs));
    }

    public int hashCode()
    {
        return name.hashCode();
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof KSMetaData))
            return false;
        KSMetaData other = (KSMetaData)obj;
        return other.name.equals(name)
                && ObjectUtils.equals(other.strategyClass, strategyClass)
                && ObjectUtils.equals(other.strategyOptions, strategyOptions)
                && other.cfMetaData.equals(cfMetaData)
                && other.durableWrites == durableWrites;
    }

    public Map<String, CFMetaData> cfMetaData()
    {
        return cfMetaData;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name)
          .append(", rep strategy:")
          .append(strategyClass.getSimpleName())
          .append("{")
          .append(StringUtils.join(cfMetaData.values(), ", "))
          .append("}")
          .append(", strategy_options: ")
          .append(strategyOptions.toString())
          .append(", durable_writes: ")
          .append(durableWrites);
        return sb.toString();
    }

    public static String convertOldStrategyName(String name)
    {
        return name.replace("RackUnawareStrategy", "SimpleStrategy")
                   .replace("RackAwareStrategy", "OldNetworkTopologyStrategy");
    }

    public static Map<String,String> optsWithRF(final Integer rf)
    {
        Map<String, String> ret = new HashMap<String,String>();
        ret.put("replication_factor", rf.toString());
        return ret;
    }

    public static KSMetaData fromThrift(KsDef ksd, CFMetaData... cfDefs) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(ksd.strategy_class);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        return new KSMetaData(ksd.name,
                              cls,
                              ksd.strategy_options == null ? Collections.<String, String>emptyMap() : ksd.strategy_options,
                              ksd.durable_writes,
                              Arrays.asList(cfDefs));
    }

    public KsDef toThrift()
    {
        List<CfDef> cfDefs = new ArrayList<CfDef>(cfMetaData.size());
        for (CFMetaData cfm : cfMetaData().values())
            cfDefs.add(cfm.toThrift());
        KsDef ksdef = new KsDef(name, strategyClass.getName(), cfDefs);
        ksdef.setStrategy_options(strategyOptions);
        ksdef.setDurable_writes(durableWrites);

        return ksdef;
    }

    public RowMutation toSchemaUpdate(KSMetaData newState, long modificationTimestamp)
    {
        return newState.toSchema(modificationTimestamp);
    }

    public KSMetaData validate() throws ConfigurationException
    {
        if (!CFMetaData.isNameValid(name))
            throw new ConfigurationException(String.format("Invalid keyspace name: shouldn't be empty nor more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, name));

        // Attempt to instantiate the ARS, which will throw a ConfigException if the strategy_options aren't fully formed
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy.createReplicationStrategy(name, strategyClass, tmd, eps, strategyOptions);

        for (CFMetaData cfm : cfMetaData.values())
            cfm.validate();

        return this;
    }


    public KSMetaData reloadAttributes()
    {
        Row ksDefRow = SystemTable.readSchemaRow(name);

        if (ksDefRow.cf == null)
            throw new RuntimeException(String.format("%s not found in the schema definitions table (%s).", name, SystemTable.SCHEMA_KEYSPACES_CF));

        return fromSchema(ksDefRow, Collections.<CFMetaData>emptyList());
    }

    public RowMutation dropFromSchema(long timestamp)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, SystemTable.getSchemaKSKey(name));
        rm.delete(new QueryPath(SystemTable.SCHEMA_KEYSPACES_CF), timestamp);
        rm.delete(new QueryPath(SystemTable.SCHEMA_COLUMNFAMILIES_CF), timestamp);
        rm.delete(new QueryPath(SystemTable.SCHEMA_COLUMNS_CF), timestamp);

        return rm;
    }

    public RowMutation toSchema(long timestamp)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, SystemTable.getSchemaKSKey(name));
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_KEYSPACES_CF);

        cf.addColumn(Column.create(durableWrites, timestamp, "durable_writes"));
        cf.addColumn(Column.create(strategyClass.getName(), timestamp, "strategy_class"));
        cf.addColumn(Column.create(json(strategyOptions), timestamp, "strategy_options"));

        for (CFMetaData cfm : cfMetaData.values())
            cfm.toSchema(rm, timestamp);

        return rm;
    }

    /**
     * Deserialize only Keyspace attributes without nested ColumnFamilies
     *
     * @param row Keyspace attributes in serialized form
     *
     * @return deserialized keyspace without cf_defs
     */
    public static KSMetaData fromSchema(Row row, Iterable<CFMetaData> cfms)
    {
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_keyspaces", row).one();
        try
        {
            return new KSMetaData(result.getString("keyspace_name"),
                                  AbstractReplicationStrategy.getClass(result.getString("strategy_class")),
                                  fromJsonMap(result.getString("strategy_options")),
                                  result.getBoolean("durable_writes"),
                                  cfms);
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
    public static KSMetaData fromSchema(Row serializedKs, Row serializedCFs)
    {
        Map<String, CFMetaData> cfs = deserializeColumnFamilies(serializedCFs);
        return fromSchema(serializedKs, cfs.values());
    }

    /**
     * Deserialize ColumnFamilies from low-level schema representation, all of them belong to the same keyspace
     *
     * @param row
     * @return map containing name of the ColumnFamily and it's metadata for faster lookup
     */
    public static Map<String, CFMetaData> deserializeColumnFamilies(Row row)
    {
        if (row.cf == null)
            return Collections.emptyMap();

        Map<String, CFMetaData> cfms = new HashMap<String, CFMetaData>();
        UntypedResultSet results = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", row);
        for (UntypedResultSet.Row result : results)
        {
            CFMetaData cfm = CFMetaData.fromSchema(result);
            cfms.put(cfm.cfName, cfm);
        }

        for (CFMetaData cfm : cfms.values())
        {
            Row columnRow = ColumnDefinition.readSchema(cfm.ksName, cfm.cfName);
            for (ColumnDefinition cd : ColumnDefinition.fromSchema(columnRow, cfm))
                cfm.column_metadata.put(cd.name, cd);
        }

        return cfms;
    }
}
