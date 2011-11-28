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

import java.util.*;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;

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
                                                CFMetaData.VersionCf);
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

    public static Map<String, String> forwardsCompatibleOptions(KsDef ks_def)
    {
        Map<String, String> options;
        options = ks_def.strategy_options == null
                ? new HashMap<String, String>()
                : new HashMap<String, String>(ks_def.strategy_options);
        maybeAddReplicationFactor(options, ks_def.strategy_class, ks_def.isSetReplication_factor() ? ks_def.replication_factor : null);
        return options;
    }

    // TODO remove this for 1.0
    private static void maybeAddReplicationFactor(Map<String, String> options, String cls, Integer rf)
    {
        if (rf != null && (cls.endsWith("SimpleStrategy") || cls.endsWith("OldNetworkTopologyStrategy")))
            options.put("replication_factor", rf.toString());
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
        
    public org.apache.cassandra.db.migration.avro.KsDef toAvro()
    {
        org.apache.cassandra.db.migration.avro.KsDef ks = new org.apache.cassandra.db.migration.avro.KsDef();
        ks.name = new Utf8(name);
        ks.strategy_class = new Utf8(strategyClass.getName());
        if (strategyOptions != null)
        {
            ks.strategy_options = new HashMap<CharSequence, CharSequence>();
            for (Map.Entry<String, String> e : strategyOptions.entrySet())
            {
                ks.strategy_options.put(new Utf8(e.getKey()), new Utf8(e.getValue()));
            }
        }
        ks.cf_defs = SerDeUtils.createArray(cfMetaData.size(), org.apache.cassandra.db.migration.avro.CfDef.SCHEMA$);
        for (CFMetaData cfm : cfMetaData.values())
            ks.cf_defs.add(cfm.toAvro());
        
        ks.durable_writes = durableWrites;
        
        return ks;
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
        maybeAddReplicationFactor(strategyOptions, ks.strategy_class.toString(), ks.replication_factor);

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
                              forwardsCompatibleOptions(ksd),
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
        if (strategyOptions != null && strategyOptions.containsKey("replication_factor"))
            ksdef.setReplication_factor(Integer.parseInt(strategyOptions.get("replication_factor")));
        ksdef.setDurable_writes(durableWrites);

        return ksdef;
    }
}
