/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.config;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.config.CFMetaData.Caching;

/**
 * methods to load schema definitions from old-style Avro serialization
 */
public class Avro
{
    @Deprecated
    public static KSMetaData ksFromAvro(org.apache.cassandra.db.migration.avro.KsDef ks)
    {
        Class<? extends AbstractReplicationStrategy> repStratClass;
        try
        {
            String strategyClassName = KSMetaData.convertOldStrategyName(ks.strategy_class.toString());
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

        for (CfDef cf_def : ks.cf_defs)
        {
            double keysCached = cf_def.key_cache_size == null ? -1 : cf_def.key_cache_size;
            double rowsCached = cf_def.row_cache_size == null ? -1 : cf_def.row_cache_size;

            if (keysCached > 0 && rowsCached > 0)
                cf_def.caching = Caching.ALL.name();
            else if (keysCached <= 0 && rowsCached <= 0)
                cf_def.caching = Caching.NONE.name();
            else if (keysCached > 0 && rowsCached <= 0)
                cf_def.caching = Caching.KEYS_ONLY.name();
            else
                cf_def.caching = Caching.ROWS_ONLY.name();

            cfMetaData.add(cfFromAvro(cf_def));
        }

        return new KSMetaData(ks.name.toString(), repStratClass, strategyOptions, ks.durable_writes, cfMetaData);
    }

    @Deprecated
    private static void maybeAddReplicationFactor(Map<String, String> options, String cls, Integer rf)
    {
        if (rf != null && (cls.endsWith("SimpleStrategy") || cls.endsWith("OldNetworkTopologyStrategy")))
            options.put("replication_factor", rf.toString());
    }

    @Deprecated
    public static CFMetaData cfFromAvro(CfDef cf)
    {
        AbstractType<?> comparator;
        AbstractType<?> subcolumnComparator = null;
        AbstractType<?> validator;
        AbstractType<?> keyValidator;

        try
        {
            comparator = TypeParser.parse(cf.comparator_type.toString());
            if (cf.subcomparator_type != null)
                subcolumnComparator = TypeParser.parse(cf.subcomparator_type);
            validator = TypeParser.parse(cf.default_validation_class);
            keyValidator = TypeParser.parse(cf.key_validation_class);
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Could not inflate CFMetaData for " + cf, ex);
        }
        Map<ByteBuffer, ColumnDefinition> column_metadata = new TreeMap<ByteBuffer, ColumnDefinition>(BytesType.instance);
        for (org.apache.cassandra.db.migration.avro.ColumnDef aColumn_metadata : cf.column_metadata)
        {
            ColumnDefinition cd = columnFromAvro(aColumn_metadata);
            if (cd.getIndexType() != null && cd.getIndexName() == null)
                cd.setIndexName(CFMetaData.getDefaultIndexName(cf.name.toString(), comparator, cd.name));
            column_metadata.put(cd.name, cd);
        }

        CFMetaData newCFMD = new CFMetaData(cf.keyspace.toString(),
                                            cf.name.toString(),
                                            ColumnFamilyType.create(cf.column_type.toString()),
                                            comparator,
                                            subcolumnComparator);

        // When we pull up an old avro CfDef which doesn't have these arguments,
        //  it doesn't default them correctly. Without explicit defaulting,
        //  grandfathered metadata becomes wrong or causes crashes.
        //  Isn't AVRO supposed to handle stuff like this?
        if (cf.min_compaction_threshold != null) { newCFMD.minCompactionThreshold(cf.min_compaction_threshold); }
        if (cf.max_compaction_threshold != null) { newCFMD.maxCompactionThreshold(cf.max_compaction_threshold); }
        if (cf.key_alias != null) { newCFMD.keyAliases(Collections.<ByteBuffer>singletonList(cf.key_alias)); }
        if (cf.column_aliases != null)
            newCFMD.columnAliases(new ArrayList<ByteBuffer>(cf.column_aliases)); // fix avro stupidity
        if (cf.value_alias != null) { newCFMD.valueAlias(cf.value_alias); }
        if (cf.compaction_strategy != null)
        {
            try
            {
                newCFMD.compactionStrategyClass = CFMetaData.createCompactionStrategy(cf.compaction_strategy.toString());
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
        if (cf.compaction_strategy_options != null)
        {
            for (Map.Entry<CharSequence, CharSequence> e : cf.compaction_strategy_options.entrySet())
                newCFMD.compactionStrategyOptions.put(e.getKey().toString(), e.getValue().toString());
        }

        CompressionParameters cp;
        try
        {
            cp = CompressionParameters.create(cf.compression_options);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

        CFMetaData.Caching caching;

        try
        {
            caching = cf.caching == null ? CFMetaData.DEFAULT_CACHING_STRATEGY : CFMetaData.Caching.fromString(cf.caching.toString());
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

        // adding old -> new style ID mapping to support backward compatibility
        Schema.instance.addOldCfIdMapping(cf.id, newCFMD.cfId);

        return newCFMD.comment(cf.comment.toString())
                      .readRepairChance(cf.read_repair_chance)
                      .dcLocalReadRepairChance(cf.dclocal_read_repair_chance)
                      .replicateOnWrite(cf.replicate_on_write)
                      .gcGraceSeconds(cf.gc_grace_seconds)
                      .defaultValidator(validator)
                      .keyValidator(keyValidator)
                      .columnMetadata(column_metadata)
                      .compressionParameters(cp)
                      .bloomFilterFpChance(cf.bloom_filter_fp_chance)
                      .caching(caching);
    }

    @Deprecated
    public static ColumnDefinition columnFromAvro(org.apache.cassandra.db.migration.avro.ColumnDef cd)
    {
        IndexType index_type = cd.index_type == null ? null : Enum.valueOf(IndexType.class, cd.index_type.name());
        String index_name = cd.index_name == null ? null : cd.index_name.toString();
        try
        {
            AbstractType<?> validatorType = TypeParser.parse(cd.validation_class);
            return new ColumnDefinition(ByteBufferUtil.clone(cd.name), validatorType, index_type, ColumnDefinition.getStringMap(cd.index_options), index_name, null);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }
}
