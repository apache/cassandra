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

import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.io.SerDeUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.util.Utf8;

import org.apache.commons.lang.ObjectUtils;

public final class KSMetaData
{
    public final String name;
    public final Class<? extends AbstractReplicationStrategy> strategyClass;
    public final Map<String, String> strategyOptions;
    public final int replicationFactor;
    private final Map<String, CFMetaData> cfMetaData;

    public KSMetaData(String name, Class<? extends AbstractReplicationStrategy> strategyClass, Map<String, String> strategyOptions, int replicationFactor, CFMetaData... cfDefs)
    {
        this.name = name;
        this.strategyClass = strategyClass == null ? SimpleStrategy.class : strategyClass;
        this.strategyOptions = strategyOptions;
        this.replicationFactor = replicationFactor;
        Map<String, CFMetaData> cfmap = new HashMap<String, CFMetaData>();
        for (CFMetaData cfm : cfDefs)
            cfmap.put(cfm.cfName, cfm);
        this.cfMetaData = Collections.unmodifiableMap(cfmap);
    }
    
    public boolean equals(Object obj)
    {
        if (obj == null)
            return false;
        if (!(obj instanceof KSMetaData))
            return false;
        KSMetaData other = (KSMetaData)obj;
        return other.name.equals(name)
                && ObjectUtils.equals(other.strategyClass, strategyClass)
                && ObjectUtils.equals(other.strategyOptions, strategyOptions)
                && other.replicationFactor == replicationFactor
                && other.cfMetaData.size() == cfMetaData.size()
                && other.cfMetaData.equals(cfMetaData);
    }

    public Map<String, CFMetaData> cfMetaData()
    {
        return cfMetaData;
    }
        
    public org.apache.cassandra.avro.KsDef deflate()
    {
        org.apache.cassandra.avro.KsDef ks = new org.apache.cassandra.avro.KsDef();
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
        ks.replication_factor = replicationFactor;
        ks.cf_defs = SerDeUtils.createArray(cfMetaData.size(), org.apache.cassandra.avro.CfDef.SCHEMA$);
        for (CFMetaData cfm : cfMetaData.values())
            ks.cf_defs.add(cfm.deflate());
        return ks;
    }

    public static KSMetaData inflate(org.apache.cassandra.avro.KsDef ks)
    {
        Class<AbstractReplicationStrategy> repStratClass;
        try
        {
            String strategyClassName = convertOldStrategyName(ks.strategy_class.toString());
            repStratClass = (Class<AbstractReplicationStrategy>)Class.forName(strategyClassName);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Could not create ReplicationStrategy of type " + ks.strategy_class, ex);
        }
        Map<String, String> strategyOptions = null;
        if (ks.strategy_options != null)
        {
            strategyOptions = new HashMap<String, String>();
            for (Map.Entry<CharSequence, CharSequence> e : ks.strategy_options.entrySet())
            {
                strategyOptions.put(e.getKey().toString(), e.getValue().toString());
            }
        }
        int cfsz = (int)ks.cf_defs.size();
        CFMetaData[] cfMetaData = new CFMetaData[cfsz];
        Iterator<org.apache.cassandra.avro.CfDef> cfiter = ks.cf_defs.iterator();
        for (int i = 0; i < cfsz; i++)
            cfMetaData[i] = CFMetaData.inflate(cfiter.next());

        return new KSMetaData(ks.name.toString(), repStratClass, strategyOptions, ks.replication_factor, cfMetaData);
    }

    public static String convertOldStrategyName(String name)
    {
        return name.replace("RackUnawareStrategy", "SimpleStrategy")
                   .replace("RackAwareStrategy", "OldNetworkTopologyStrategy")
                   .replace("DatacenterShardStrategy", "NetworkTopologyStrategy");
    }
}
