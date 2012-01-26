/**
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

import java.io.IOException;
import java.util.*;

import com.google.common.base.Objects;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.ColumnDef;

import static org.apache.cassandra.db.migration.MigrationHelper.*;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    private final Map<String, CFMetaData> cfMetaData;
    public final boolean durableWrites;

    private KSMetaData(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, boolean durableWrites, Iterable<CFMetaData> cfDefs)
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

    public static KSMetaData cloneWith(KSMetaData ksm, Iterable<CFMetaData> cfDefs)
    {
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.durableWrites, cfDefs);
    }

    public static KSMetaData systemKeyspace()
    {
        List<CFMetaData> cfDefs = Arrays.asList(CFMetaData.StatusCf,
                                                CFMetaData.HintsCf,
                                                CFMetaData.MigrationsCf,
                                                CFMetaData.SchemaCf,
                                                CFMetaData.IndexCf,
                                                CFMetaData.NodeIdCf,
                                                CFMetaData.VersionCf,
                                                CFMetaData.SchemaKeyspacesCf,
                                                CFMetaData.SchemaColumnFamiliesCf,
                                                CFMetaData.SchemaColumnsCf);
        return new KSMetaData(Table.SYSTEM_TABLE, LocalStrategy.class, optsWithRF(1), true, cfDefs);
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
          .append(", durable_writes: ").append(durableWrites);
        return sb.toString();
    }

    @Deprecated
    public static KSMetaData fromAvro(org.apache.cassandra.db.migration.avro.KsDef ks)
    {
        Class<? extends AbstractReplicationStrategy> repStratClass;
        try
        {
            String strategyClassName = convertOldStrategyName(ks.strategy_class.toString());
            repStratClass = (Class<AbstractReplicationStrategy>)Class.forName(strategyClassName);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Could not create ReplicationStrategy of type " + ks.strategy_class, ex);
        }

        Map<String, String> strategyOptions = new HashMap<String, String>();
        if (ks.strategy_options != null)
        {
            for (Map.Entry<CharSequence, CharSequence> e : ks.strategy_options.entrySet())
            {
                String name = e.getKey().toString();
                // Silently discard a replication_factor option to NTS.
                // The history is, people were creating CFs with the default settings (which in the CLI is NTS) and then
                // giving it a replication_factor option, which is nonsensical.  Initially our strategy was to silently
                // ignore this option, but that turned out to confuse people more.  So in 0.8.2 we switched to throwing
                // an exception in the NTS constructor, which would be turned into an InvalidRequestException for the
                // client.  But, it also prevented startup for anyone upgrading without first cleaning that option out.
                if (repStratClass == NetworkTopologyStrategy.class && name.trim().toLowerCase().equals("replication_factor"))
                    continue;
                strategyOptions.put(name, e.getValue().toString());
            }
        }

        int cfsz = ks.cf_defs.size();
        List<CFMetaData> cfMetaData = new ArrayList<CFMetaData>(cfsz);
        Iterator<org.apache.cassandra.db.migration.avro.CfDef> cfiter = ks.cf_defs.iterator();
        for (int i = 0; i < cfsz; i++)
            cfMetaData.add(CFMetaData.fromAvro(cfiter.next()));

        return new KSMetaData(ks.name.toString(), repStratClass, strategyOptions, ks.durable_writes, cfMetaData);
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
        return new KSMetaData(ksd.name,
                              AbstractReplicationStrategy.getClass(ksd.strategy_class),
                              ksd.strategy_options == null ? Collections.<String, String>emptyMap() : ksd.strategy_options,
                              ksd.durable_writes,
                              Arrays.asList(cfDefs));
    }

    public KsDef toThrift()
    {
        List<CfDef> cfDefs = new ArrayList<CfDef>();
        for (CFMetaData cfm : cfMetaData().values())
            cfDefs.add(cfm.toThrift());
        KsDef ksdef = new KsDef(name, strategyClass.getName(), cfDefs);
        ksdef.setStrategy_options(strategyOptions);
        ksdef.setDurable_writes(durableWrites);

        return ksdef;
    }

    public RowMutation diff(KsDef newState, long modificationTimestamp)
    {
        KsDef curState = toThrift();
        RowMutation m = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(name));

        for (KsDef._Fields field : KsDef._Fields.values())
        {
            if (field.equals(KsDef._Fields.CF_DEFS))
                continue;

            Object curValue = curState.getFieldValue(field);
            Object newValue = newState.getFieldValue(field);

            if (Objects.equal(curValue, newValue))
                continue;

            m.add(new QueryPath(SystemTable.SCHEMA_KEYSPACES_CF, null, AsciiType.instance.fromString(field.getFieldName())),
                  valueAsBytes(newValue),
                  modificationTimestamp);
        }

        return m;
    }

    public KSMetaData reloadAttributes() throws IOException
    {
        Row ksDefRow = SystemTable.readSchemaRow(name);

        if (ksDefRow.cf == null || ksDefRow.cf.isEmpty())
            throw new IOException(String.format("%s not found in the schema definitions table (%s).", name, SystemTable.SCHEMA_KEYSPACES_CF));

        return fromSchema(ksDefRow.cf, null);
    }

    public List<RowMutation> dropFromSchema(long timestamp)
    {
        List<RowMutation> mutations = new ArrayList<RowMutation>();

        RowMutation ksMutation = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(name));
        ksMutation.delete(new QueryPath(SystemTable.SCHEMA_KEYSPACES_CF), timestamp);
        mutations.add(ksMutation);

        for (CFMetaData cfm : cfMetaData.values())
            mutations.add(cfm.dropFromSchema(timestamp));

        return mutations;
    }

    public static RowMutation toSchema(KsDef ksDef, long timestamp) throws IOException
    {
        RowMutation mutation = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksDef.name));

        for (KsDef._Fields field : KsDef._Fields.values())
        {
            if (field.equals(KsDef._Fields.CF_DEFS))
                continue;

            mutation.add(new QueryPath(SystemTable.SCHEMA_KEYSPACES_CF,
                                       null,
                                       AsciiType.instance.fromString(field.getFieldName())),
                         valueAsBytes(ksDef.getFieldValue(field)),
                         timestamp);
        }

        if (!ksDef.isSetCf_defs())
            return mutation;

        for (CfDef cf : ksDef.cf_defs)
        {
            try
            {
                CFMetaData.toSchema(mutation, cf, timestamp);
            }
            catch (ConfigurationException e)
            {
                throw new IOException(e);
            }
        }

        return mutation;
    }

    public RowMutation toSchema(long timestamp) throws IOException
    {
        return toSchema(toThrift(), timestamp);
    }

    /**
     * Deserialize only Keyspace attributes without nested ColumnFamilies
     *
     * @param serializedKsDef Keyspace attributes in serialized form
     *
     * @return deserialized keyspace without cf_defs
     *
     * @throws IOException if deserialization failed
     */
    public static KsDef fromSchema(ColumnFamily serializedKsDef) throws IOException
    {
        KsDef ksDef = new KsDef();

        AbstractType comparator = serializedKsDef.getComparator();

        for (IColumn ksAttr : serializedKsDef.getSortedColumns())
        {
            if (ksAttr == null || ksAttr.isMarkedForDelete())
                continue;

            KsDef._Fields field = KsDef._Fields.findByName(comparator.getString(ksAttr.name()));
            ksDef.setFieldValue(field, deserializeValue(ksAttr.value(), getValueClass(KsDef.class, field.getFieldName())));
        }

        return ksDef.name == null ? null : ksDef;
    }

    /**
     * Deserialize Keyspace with nested ColumnFamilies
     *
     * @param serializedKsDef Keyspace in serialized form
     * @param serializedCFs Collection of the serialized ColumnFamilies
     *
     * @return deserialized keyspace with cf_defs
     *
     * @throws IOException if deserialization failed
     */
    public static KSMetaData fromSchema(ColumnFamily serializedKsDef, ColumnFamily serializedCFs) throws IOException
    {
        KsDef ksDef = fromSchema(serializedKsDef);

        assert ksDef != null;

        Map<String, CfDef> cfs = deserializeColumnFamilies(serializedCFs);

        try
        {
            CFMetaData[] cfms = new CFMetaData[cfs.size()];

            int index = 0;
            for (CfDef cfDef : cfs.values())
                cfms[index++] = CFMetaData.fromThrift(cfDef);

            return fromThrift(ksDef, cfms);
        }
        catch (Exception e)
        {
            // this is critical because indicates that something is wrong with serialized schema
            throw new IOException(e);
        }
    }

    /**
     * Deserialize ColumnFamilies from low-level schema representation, all of them belong to the same keyspace
     *
     * @param serializedColumnFamilies ColumnFamilies in the serialized form
     *
     * @return map containing name of the ColumnFamily and it's metadata for faster lookup
     */
    public static Map<String, CfDef> deserializeColumnFamilies(ColumnFamily serializedColumnFamilies)
    {
        Map<String, CfDef> cfs = new HashMap<String, CfDef>();

        if (serializedColumnFamilies == null)
            return cfs;

        AbstractType<?> comparator = serializedColumnFamilies.getComparator();

        for (IColumn column : serializedColumnFamilies.getSortedColumns())
        {
            if (column == null || column.isMarkedForDelete())
                continue;

            String[] attr = comparator.getString(column.name()).split(":");
            assert attr.length == 2;

            CfDef cfDef = cfs.get(attr[0]);

            if (cfDef == null)
            {
                cfDef = new CfDef();
                cfs.put(attr[0], cfDef);
            }

            CfDef._Fields field = CfDef._Fields.findByName(attr[1]);
            cfDef.setFieldValue(field, deserializeValue(column.value(), getValueClass(CfDef.class, field.getFieldName())));
        }

        for (CfDef cfDef : cfs.values())
        {
            for (ColumnDef columnDef : ColumnDefinition.fromSchema(ColumnDefinition.readSchema(cfDef.keyspace, cfDef.name)))
                cfDef.addToColumn_metadata(columnDef);
        }

        return cfs;
    }
}
