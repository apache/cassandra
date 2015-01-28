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
package org.apache.cassandra.thrift;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.schema.LegacySchemaTables;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Static utility methods to convert internal structure to and from thrift ones.
 */
public class ThriftConversion
{
    public static org.apache.cassandra.db.ConsistencyLevel fromThrift(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return org.apache.cassandra.db.ConsistencyLevel.ANY;
            case ONE: return org.apache.cassandra.db.ConsistencyLevel.ONE;
            case TWO: return org.apache.cassandra.db.ConsistencyLevel.TWO;
            case THREE: return org.apache.cassandra.db.ConsistencyLevel.THREE;
            case QUORUM: return org.apache.cassandra.db.ConsistencyLevel.QUORUM;
            case ALL: return org.apache.cassandra.db.ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
            case SERIAL: return org.apache.cassandra.db.ConsistencyLevel.SERIAL;
            case LOCAL_SERIAL: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
            case LOCAL_ONE: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }

    public static ConsistencyLevel toThrift(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return ConsistencyLevel.ANY;
            case ONE: return ConsistencyLevel.ONE;
            case TWO: return ConsistencyLevel.TWO;
            case THREE: return ConsistencyLevel.THREE;
            case QUORUM: return ConsistencyLevel.QUORUM;
            case ALL: return ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return ConsistencyLevel.EACH_QUORUM;
            case SERIAL: return ConsistencyLevel.SERIAL;
            case LOCAL_SERIAL: return ConsistencyLevel.LOCAL_SERIAL;
            case LOCAL_ONE: return ConsistencyLevel.LOCAL_ONE;
        }
        throw new AssertionError();
    }

    // We never return, but returning a RuntimeException allows to write "throw rethrow(e)" without java complaining
    // for methods that have a return value.
    public static RuntimeException rethrow(RequestExecutionException e) throws UnavailableException, TimedOutException
    {
        if (e instanceof RequestFailureException)
            throw toThrift((RequestFailureException)e);
        else if (e instanceof RequestTimeoutException)
            throw toThrift((RequestTimeoutException)e);
        else
            throw new UnavailableException();
    }

    public static InvalidRequestException toThrift(RequestValidationException e)
    {
        return new InvalidRequestException(e.getMessage());
    }

    public static UnavailableException toThrift(org.apache.cassandra.exceptions.UnavailableException e)
    {
        return new UnavailableException();
    }

    public static AuthenticationException toThrift(org.apache.cassandra.exceptions.AuthenticationException e)
    {
        return new AuthenticationException(e.getMessage());
    }

    public static TimedOutException toThrift(RequestTimeoutException e)
    {
        TimedOutException toe = new TimedOutException();
        if (e instanceof WriteTimeoutException)
        {
            WriteTimeoutException wte = (WriteTimeoutException)e;
            toe.setAcknowledged_by(wte.received);
            if (wte.writeType == WriteType.BATCH_LOG)
                toe.setAcknowledged_by_batchlog(false);
            else if (wte.writeType == WriteType.BATCH)
                toe.setAcknowledged_by_batchlog(true);
            else if (wte.writeType == WriteType.CAS)
                toe.setPaxos_in_progress(true);
        }
        return toe;
    }

    // Thrift does not support RequestFailureExceptions, so we translate them into timeouts
    public static TimedOutException toThrift(RequestFailureException e)
    {
        return new TimedOutException();
    }

    public static List<org.apache.cassandra.db.IndexExpression> indexExpressionsFromThrift(List<IndexExpression> exprs)
    {
        if (exprs == null)
            return null;

        if (exprs.isEmpty())
            return Collections.emptyList();

        List<org.apache.cassandra.db.IndexExpression> converted = new ArrayList<>(exprs.size());
        for (IndexExpression expr : exprs)
        {
            converted.add(new org.apache.cassandra.db.IndexExpression(expr.column_name,
                                                                      Operator.valueOf(expr.op.name()),
                                                                      expr.value));
        }
        return converted;
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

    public static KsDef toThrift(KSMetaData ksm)
    {
        List<CfDef> cfDefs = new ArrayList<>(ksm.cfMetaData().size());
        for (CFMetaData cfm : ksm.cfMetaData().values())
            if (cfm.isThriftCompatible()) // Don't expose CF that cannot be correctly handle by thrift; see CASSANDRA-4377 for further details
                cfDefs.add(toThrift(cfm));

        KsDef ksdef = new KsDef(ksm.name, ksm.strategyClass.getName(), cfDefs);
        ksdef.setStrategy_options(ksm.strategyOptions);
        ksdef.setDurable_writes(ksm.durableWrites);

        return ksdef;
    }

    public static CFMetaData fromThrift(CfDef cf_def)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, Collections.<ColumnDefinition>emptyList());
    }

    public static CFMetaData fromThriftForUpdate(CfDef cf_def, CFMetaData toUpdate)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, toUpdate.allColumns());
    }

    // Convert a thrift CfDef, given a list of ColumnDefinitions to copy over to the created CFMetadata before the CQL metadata are rebuild
    private static CFMetaData internalFromThrift(CfDef cf_def, Collection<ColumnDefinition> previousCQLMetadata)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
        if (cfType == null)
            throw new org.apache.cassandra.exceptions.InvalidRequestException("Invalid column type " + cf_def.column_type);

        applyImplicitDefaults(cf_def);

        try
        {
            AbstractType<?> rawComparator = TypeParser.parse(cf_def.comparator_type);
            AbstractType<?> subComparator = cfType == ColumnFamilyType.Standard
                    ? null
                    : cf_def.subcomparator_type == null ? BytesType.instance : TypeParser.parse(cf_def.subcomparator_type);

            AbstractType<?> fullRawComparator = CFMetaData.makeRawAbstractType(rawComparator, subComparator);

            AbstractType<?> keyValidator = cf_def.isSetKey_validation_class() ? TypeParser.parse(cf_def.key_validation_class) : null;

            // Convert the REGULAR definitions from the input CfDef
            List<ColumnDefinition> defs = fromThrift(cf_def.keyspace, cf_def.name, rawComparator, subComparator, cf_def.column_metadata);

            // Add the keyAlias if there is one, since that's on CQL metadata that thrift can actually change (for
            // historical reasons)
            boolean hasKeyAlias = cf_def.isSetKey_alias() && keyValidator != null && !(keyValidator instanceof CompositeType);
            if (hasKeyAlias)
                defs.add(ColumnDefinition.partitionKeyDef(cf_def.keyspace, cf_def.name, cf_def.key_alias, keyValidator, null));

            // Now add any CQL metadata that we want to copy, skipping the keyAlias if there was one
            for (ColumnDefinition def : previousCQLMetadata)
            {
                // isPartOfCellName basically means 'is not just a CQL metadata'
                if (def.isPartOfCellName())
                    continue;

                if (def.kind == ColumnDefinition.Kind.PARTITION_KEY && hasKeyAlias)
                    continue;

                defs.add(def);
            }

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, CFMetaData.calculateIsDense(fullRawComparator, defs));

            UUID cfId = Schema.instance.getId(cf_def.keyspace, cf_def.name);
            if (cfId == null)
                cfId = UUIDGen.getTimeUUID();

            CFMetaData newCFMD = new CFMetaData(cf_def.keyspace, cf_def.name, cfType, comparator, cfId);

            newCFMD.addAllColumnDefinitions(defs);

            if (keyValidator != null)
                newCFMD.keyValidator(keyValidator);
            if (cf_def.isSetGc_grace_seconds())
                newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds);
            if (cf_def.isSetMin_compaction_threshold())
                newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold);
            if (cf_def.isSetMax_compaction_threshold())
                newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold);
            if (cf_def.isSetCompaction_strategy())
                newCFMD.compactionStrategyClass(CFMetaData.createCompactionStrategy(cf_def.compaction_strategy));
            if (cf_def.isSetCompaction_strategy_options())
                newCFMD.compactionStrategyOptions(new HashMap<>(cf_def.compaction_strategy_options));
            if (cf_def.isSetBloom_filter_fp_chance())
                newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
            if (cf_def.isSetMemtable_flush_period_in_ms())
                newCFMD.memtableFlushPeriod(cf_def.memtable_flush_period_in_ms);
            if (cf_def.isSetCaching() || cf_def.isSetCells_per_row_to_cache())
                newCFMD.caching(CachingOptions.fromThrift(cf_def.caching, cf_def.cells_per_row_to_cache));
            if (cf_def.isSetRead_repair_chance())
                newCFMD.readRepairChance(cf_def.read_repair_chance);
            if (cf_def.isSetDefault_time_to_live())
                newCFMD.defaultTimeToLive(cf_def.default_time_to_live);
            if (cf_def.isSetDclocal_read_repair_chance())
                newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);
            if (cf_def.isSetMin_index_interval())
                newCFMD.minIndexInterval(cf_def.min_index_interval);
            if (cf_def.isSetMax_index_interval())
                newCFMD.maxIndexInterval(cf_def.max_index_interval);
            if (cf_def.isSetSpeculative_retry())
                newCFMD.speculativeRetry(CFMetaData.SpeculativeRetry.fromString(cf_def.speculative_retry));
            if (cf_def.isSetTriggers())
                newCFMD.triggers(triggerDefinitionsFromThrift(cf_def.triggers));

            return newCFMD.comment(cf_def.comment)
                          .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                          .compressionParameters(CompressionParameters.create(cf_def.compression_options))
                          .rebuild();
        }
        catch (SyntaxException | MarshalException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    /** applies implicit defaults to cf definition. useful in updates */
    private static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def)
    {
        if (!cf_def.isSetComment())
            cf_def.setComment("");
        if (!cf_def.isSetMin_compaction_threshold())
            cf_def.setMin_compaction_threshold(CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD);
        if (!cf_def.isSetMax_compaction_threshold())
            cf_def.setMax_compaction_threshold(CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD);
        if (cf_def.compaction_strategy == null)
            cf_def.compaction_strategy = CFMetaData.DEFAULT_COMPACTION_STRATEGY_CLASS.getSimpleName();
        if (cf_def.compaction_strategy_options == null)
            cf_def.compaction_strategy_options = Collections.emptyMap();
        if (!cf_def.isSetCompression_options())
            cf_def.setCompression_options(Collections.singletonMap(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR));
        if (!cf_def.isSetDefault_time_to_live())
            cf_def.setDefault_time_to_live(CFMetaData.DEFAULT_DEFAULT_TIME_TO_LIVE);
        if (!cf_def.isSetDclocal_read_repair_chance())
            cf_def.setDclocal_read_repair_chance(CFMetaData.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE);

        // if index_interval was set, use that for the min_index_interval default
        if (!cf_def.isSetMin_index_interval())
        {
            if (cf_def.isSetIndex_interval())
                cf_def.setMin_index_interval(cf_def.getIndex_interval());
            else
                cf_def.setMin_index_interval(CFMetaData.DEFAULT_MIN_INDEX_INTERVAL);
        }

        if (!cf_def.isSetMax_index_interval())
        {
            // ensure the max is at least as large as the min
            cf_def.setMax_index_interval(Math.max(cf_def.min_index_interval, CFMetaData.DEFAULT_MAX_INDEX_INTERVAL));
        }
    }

    /**
     * Create CFMetaData from thrift {@link CqlRow} that contains columns from schema_columnfamilies.
     *
     * @param columnsRes CqlRow containing columns from schema_columnfamilies.
     * @return CFMetaData derived from CqlRow
     */
    public static CFMetaData fromThriftCqlRow(CqlRow cf, CqlResult columnsRes)
    {
        UntypedResultSet.Row cfRow = new UntypedResultSet.Row(convertThriftCqlRow(cf));

        List<Map<String, ByteBuffer>> cols = new ArrayList<>(columnsRes.rows.size());
        for (CqlRow row : columnsRes.rows)
            cols.add(convertThriftCqlRow(row));
        UntypedResultSet colsRows = UntypedResultSet.create(cols);

        return LegacySchemaTables.createTableFromTableRowAndColumnRows(cfRow, colsRows);
    }

    private static Map<String, ByteBuffer> convertThriftCqlRow(CqlRow row)
    {
        Map<String, ByteBuffer> m = new HashMap<>();
        for (org.apache.cassandra.thrift.Column column : row.getColumns())
            m.put(UTF8Type.instance.getString(column.bufferForName()), column.value);
        return m;
    }

    public static CfDef toThrift(CFMetaData cfm)
    {
        CfDef def = new CfDef(cfm.ksName, cfm.cfName);
        def.setColumn_type(cfm.cfType.name());

        if (cfm.isSuper())
        {
            def.setComparator_type(cfm.comparator.subtype(0).toString());
            def.setSubcomparator_type(cfm.comparator.subtype(1).toString());
        }
        else
        {
            def.setComparator_type(cfm.comparator.toString());
        }

        def.setComment(Strings.nullToEmpty(cfm.getComment()));
        def.setRead_repair_chance(cfm.getReadRepairChance());
        def.setDclocal_read_repair_chance(cfm.getDcLocalReadRepairChance());
        def.setGc_grace_seconds(cfm.getGcGraceSeconds());
        def.setDefault_validation_class(cfm.getDefaultValidator().toString());
        def.setKey_validation_class(cfm.getKeyValidator().toString());
        def.setMin_compaction_threshold(cfm.getMinCompactionThreshold());
        def.setMax_compaction_threshold(cfm.getMaxCompactionThreshold());
        // We only return the alias if only one is set since thrift don't know about multiple key aliases
        if (cfm.partitionKeyColumns().size() == 1)
            def.setKey_alias(cfm.partitionKeyColumns().get(0).name.bytes);
        def.setColumn_metadata(columnDefinitionsToThrift(cfm.allColumns()));
        def.setCompaction_strategy(cfm.compactionStrategyClass.getName());
        def.setCompaction_strategy_options(new HashMap<>(cfm.compactionStrategyOptions));
        def.setCompression_options(cfm.compressionParameters.asThriftOptions());
        def.setBloom_filter_fp_chance(cfm.getBloomFilterFpChance());
        def.setMin_index_interval(cfm.getMinIndexInterval());
        def.setMax_index_interval(cfm.getMaxIndexInterval());
        def.setMemtable_flush_period_in_ms(cfm.getMemtableFlushPeriod());
        def.setCaching(cfm.getCaching().toThriftCaching());
        def.setCells_per_row_to_cache(cfm.getCaching().toThriftCellsPerRow());
        def.setDefault_time_to_live(cfm.getDefaultTimeToLive());
        def.setSpeculative_retry(cfm.getSpeculativeRetry().toString());
        def.setTriggers(triggerDefinitionsToThrift(cfm.getTriggers().values()));

        return def;
    }

    public static ColumnDefinition fromThrift(String ksName,
                                              String cfName,
                                              AbstractType<?> thriftComparator,
                                              AbstractType<?> thriftSubcomparator,
                                              ColumnDef thriftColumnDef)
    throws SyntaxException, ConfigurationException
    {
        // For super columns, the componentIndex is 1 because the ColumnDefinition applies to the column component.
        Integer componentIndex = thriftSubcomparator != null ? 1 : null;
        AbstractType<?> comparator = thriftSubcomparator == null ? thriftComparator : thriftSubcomparator;
        try
        {
            comparator.validate(thriftColumnDef.name);
        }
        catch (MarshalException e)
        {
            throw new ConfigurationException(String.format("Column name %s is not valid for comparator %s", ByteBufferUtil.bytesToHex(thriftColumnDef.name), comparator));
        }

        return new ColumnDefinition(ksName,
                                    cfName,
                                    new ColumnIdentifier(ByteBufferUtil.clone(thriftColumnDef.name), comparator),
                                    TypeParser.parse(thriftColumnDef.validation_class),
                                    thriftColumnDef.index_type == null ? null : org.apache.cassandra.config.IndexType.valueOf(thriftColumnDef.index_type.name()),
                                    thriftColumnDef.index_options,
                                    thriftColumnDef.index_name,
                                    componentIndex,
                                    ColumnDefinition.Kind.REGULAR);
    }

    private static List<ColumnDefinition> fromThrift(String ksName,
                                                     String cfName,
                                                     AbstractType<?> thriftComparator,
                                                     AbstractType<?> thriftSubcomparator,
                                                     List<ColumnDef> thriftDefs)
    throws SyntaxException, ConfigurationException
    {
        if (thriftDefs == null)
            return new ArrayList<>();

        List<ColumnDefinition> defs = new ArrayList<>(thriftDefs.size());
        for (ColumnDef thriftColumnDef : thriftDefs)
            defs.add(fromThrift(ksName, cfName, thriftComparator, thriftSubcomparator, thriftColumnDef));

        return defs;
    }

    @VisibleForTesting
    public static ColumnDef toThrift(ColumnDefinition column)
    {
        ColumnDef cd = new ColumnDef();

        cd.setName(ByteBufferUtil.clone(column.name.bytes));
        cd.setValidation_class(column.type.toString());
        cd.setIndex_type(column.getIndexType() == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(column.getIndexType().name()));
        cd.setIndex_name(column.getIndexName());
        cd.setIndex_options(column.getIndexOptions() == null ? null : Maps.newHashMap(column.getIndexOptions()));

        return cd;
    }

    private static List<ColumnDef> columnDefinitionsToThrift(Collection<ColumnDefinition> columns)
    {
        List<ColumnDef> thriftDefs = new ArrayList<>(columns.size());
        for (ColumnDefinition def : columns)
            if (def.kind == ColumnDefinition.Kind.REGULAR)
                thriftDefs.add(ThriftConversion.toThrift(def));
        return thriftDefs;
    }

    private static Map<String, TriggerDefinition> triggerDefinitionsFromThrift(List<TriggerDef> thriftDefs)
    {
        Map<String, TriggerDefinition> triggerDefinitions = new HashMap<>();
        for (TriggerDef thriftDef : thriftDefs)
            triggerDefinitions.put(thriftDef.getName(),
                                   new TriggerDefinition(thriftDef.getName(), thriftDef.getOptions().get(TriggerDefinition.CLASS)));
        return triggerDefinitions;
    }

    private static List<TriggerDef> triggerDefinitionsToThrift(Collection<TriggerDefinition> triggers)
    {
        List<TriggerDef> thriftDefs = new ArrayList<>(triggers.size());
        for (TriggerDefinition def : triggers)
        {
            TriggerDef td = new TriggerDef();
            td.setName(def.name);
            td.setOptions(Collections.singletonMap(TriggerDefinition.CLASS, def.classOption));
            thriftDefs.add(td);
        }
        return thriftDefs;
    }
}
