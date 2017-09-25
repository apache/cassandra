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

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.LegacyLayout;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
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

    public static RowFilter rowFilterFromThrift(CFMetaData metadata, List<IndexExpression> exprs)
    {
        if (exprs == null || exprs.isEmpty())
            return RowFilter.NONE;

        RowFilter converted = RowFilter.forThrift(exprs.size());
        for (IndexExpression expr : exprs)
            converted.addThriftExpression(metadata, expr.column_name, Operator.valueOf(expr.op.name()), expr.value);
        return converted;
    }

    public static KeyspaceMetadata fromThrift(KsDef ksd, CFMetaData... cfDefs) throws ConfigurationException
    {
        Class<? extends AbstractReplicationStrategy> cls = AbstractReplicationStrategy.getClass(ksd.strategy_class);
        if (cls.equals(LocalStrategy.class))
            throw new ConfigurationException("Unable to use given strategy class: LocalStrategy is reserved for internal use.");

        Map<String, String> replicationMap = new HashMap<>();
        if (ksd.strategy_options != null)
            replicationMap.putAll(ksd.strategy_options);
        replicationMap.put(ReplicationParams.CLASS, cls.getName());

        return KeyspaceMetadata.create(ksd.name, KeyspaceParams.create(ksd.durable_writes, replicationMap), Tables.of(cfDefs));
    }

    public static KsDef toThrift(KeyspaceMetadata ksm)
    {
        List<CfDef> cfDefs = new ArrayList<>();
        for (CFMetaData cfm : ksm.tables) // do not include views
            if (cfm.isThriftCompatible()) // Don't expose CF that cannot be correctly handle by thrift; see CASSANDRA-4377 for further details
                cfDefs.add(toThrift(cfm));

        KsDef ksdef = new KsDef(ksm.name, ksm.params.replication.klass.getName(), cfDefs);
        ksdef.setStrategy_options(ksm.params.replication.options);
        ksdef.setDurable_writes(ksm.params.durableWrites);

        return ksdef;
    }

    public static CFMetaData fromThrift(CfDef cf_def)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        // This is a creation: the table is dense if it doesn't define any column_metadata
        boolean isDense = cf_def.column_metadata == null || cf_def.column_metadata.isEmpty();
        return internalFromThrift(cf_def, true, Collections.<ColumnDefinition>emptyList(), isDense);
    }

    public static CFMetaData fromThriftForUpdate(CfDef cf_def, CFMetaData toUpdate)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, false, toUpdate.allColumns(), toUpdate.isDense());
    }

    private static boolean isSuper(String thriftColumnType)
    throws org.apache.cassandra.exceptions.InvalidRequestException
    {
        switch (thriftColumnType.toLowerCase(Locale.ENGLISH))
        {
            case "standard": return false;
            case "super": return true;
            default: throw new org.apache.cassandra.exceptions.InvalidRequestException("Invalid column type " + thriftColumnType);
        }
    }

    /**
     * Convert a thrift CfDef.
     * <p>,
     * This is used both for creation and update of CF.
     *
     * @param cf_def the thrift CfDef to convert.
     * @param isCreation whether that is a new table creation or not.
     * @param previousCQLMetadata if it is not a table creation, the previous
     * definitions of the tables (which we use to preserve the CQL metadata).
     * If it is a table creation, this will be empty.
     * @param isDense whether the table is dense or not.
     *
     * @return the converted table definition.
     */
    private static CFMetaData internalFromThrift(CfDef cf_def,
                                                 boolean isCreation,
                                                 Collection<ColumnDefinition> previousCQLMetadata,
                                                 boolean isDense)
    throws org.apache.cassandra.exceptions.InvalidRequestException, ConfigurationException
    {
        applyImplicitDefaults(cf_def);

        try
        {
            boolean isSuper = isSuper(cf_def.column_type);
            AbstractType<?> rawComparator = TypeParser.parse(cf_def.comparator_type);
            AbstractType<?> subComparator = isSuper
                                          ? cf_def.subcomparator_type == null ? BytesType.instance : TypeParser.parse(cf_def.subcomparator_type)
                                          : null;

            AbstractType<?> keyValidator = cf_def.isSetKey_validation_class() ? TypeParser.parse(cf_def.key_validation_class) : BytesType.instance;
            AbstractType<?> defaultValidator = TypeParser.parse(cf_def.default_validation_class);

            // Convert the definitions from the input CfDef
            List<ColumnDefinition> defs = fromThrift(cf_def.keyspace, cf_def.name, rawComparator, subComparator, cf_def.column_metadata);

            // Add the keyAlias if there is one, since that's a CQL metadata that thrift can actually change (for
            // historical reasons)
            boolean hasKeyAlias = cf_def.isSetKey_alias() && keyValidator != null && !(keyValidator instanceof CompositeType);
            if (hasKeyAlias)
                defs.add(ColumnDefinition.partitionKeyDef(cf_def.keyspace, cf_def.name, UTF8Type.instance.getString(cf_def.key_alias), keyValidator, 0));

            // Now add any CQL metadata that we want to copy, skipping the keyAlias if there was one
            for (ColumnDefinition def : previousCQLMetadata)
            {
                // isPartOfCellName basically means 'is not just a CQL metadata'
                if (def.isPartOfCellName(false, isSuper))
                    continue;

                if (def.kind == ColumnDefinition.Kind.PARTITION_KEY && hasKeyAlias)
                    continue;

                defs.add(def);
            }

            UUID cfId = Schema.instance.getId(cf_def.keyspace, cf_def.name);
            if (cfId == null)
                cfId = UUIDGen.getTimeUUID();

            boolean isCompound = !isSuper && (rawComparator instanceof CompositeType);
            boolean isCounter = defaultValidator instanceof CounterColumnType;

            // If it's a thrift table creation, adds the default CQL metadata for the new table
            if (isCreation)
            {
                addDefaultCQLMetadata(defs,
                                      cf_def.keyspace,
                                      cf_def.name,
                                      hasKeyAlias ? null : keyValidator,
                                      rawComparator,
                                      subComparator,
                                      defaultValidator,
                                      isDense);
            }

            // We do not allow Thrift views, so we always set it to false
            boolean isView = false;

            CFMetaData newCFMD = CFMetaData.create(cf_def.keyspace,
                                                   cf_def.name,
                                                   cfId,
                                                   isDense,
                                                   isCompound,
                                                   isSuper,
                                                   isCounter,
                                                   isView,
                                                   defs,
                                                   DatabaseDescriptor.getPartitioner());

            // Convert any secondary indexes defined in the thrift column_metadata
            newCFMD.indexes(indexDefsFromThrift(newCFMD,
                                                cf_def.keyspace,
                                                cf_def.name,
                                                rawComparator,
                                                subComparator,
                                                cf_def.column_metadata));

            if (cf_def.isSetGc_grace_seconds())
                newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds);

            newCFMD.compaction(compactionParamsFromThrift(cf_def));

            if (cf_def.isSetBloom_filter_fp_chance())
                newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
            if (cf_def.isSetMemtable_flush_period_in_ms())
                newCFMD.memtableFlushPeriod(cf_def.memtable_flush_period_in_ms);
            if (cf_def.isSetCaching() || cf_def.isSetCells_per_row_to_cache())
                newCFMD.caching(cachingFromThrift(cf_def.caching, cf_def.cells_per_row_to_cache));
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
                newCFMD.speculativeRetry(SpeculativeRetryParam.fromString(cf_def.speculative_retry));
            if (cf_def.isSetTriggers())
                newCFMD.triggers(triggerDefinitionsFromThrift(cf_def.triggers));
            if (cf_def.isSetComment())
                newCFMD.comment(cf_def.comment);
            if (cf_def.isSetCompression_options())
                newCFMD.compression(compressionParametersFromThrift(cf_def.compression_options));

            return newCFMD;
        }
        catch (SyntaxException | MarshalException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static CompactionParams compactionParamsFromThrift(CfDef cf_def)
    {
        Class<? extends AbstractCompactionStrategy> klass =
            CFMetaData.createCompactionStrategy(cf_def.compaction_strategy);
        Map<String, String> options = new HashMap<>(cf_def.compaction_strategy_options);

        int minThreshold = cf_def.min_compaction_threshold;
        int maxThreshold = cf_def.max_compaction_threshold;

        if (CompactionParams.supportsThresholdParams(klass))
        {
            options.putIfAbsent(CompactionParams.Option.MIN_THRESHOLD.toString(), Integer.toString(minThreshold));
            options.putIfAbsent(CompactionParams.Option.MAX_THRESHOLD.toString(), Integer.toString(maxThreshold));
        }

        return CompactionParams.create(klass, options);
    }

    private static CompressionParams compressionParametersFromThrift(Map<String, String> compression_options)
    {
        CompressionParams compressionParameter = CompressionParams.fromMap(compression_options);
        compressionParameter.validate();
        return compressionParameter;
    }

    private static void addDefaultCQLMetadata(Collection<ColumnDefinition> defs,
                                              String ks,
                                              String cf,
                                              AbstractType<?> keyValidator,
                                              AbstractType<?> comparator,
                                              AbstractType<?> subComparator,
                                              AbstractType<?> defaultValidator,
                                              boolean isDense)
    {
        CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(defs);
        if (keyValidator != null)
        {
            if (keyValidator instanceof CompositeType)
            {
                List<AbstractType<?>> subTypes = ((CompositeType)keyValidator).types;
                for (int i = 0; i < subTypes.size(); i++)
                    defs.add(ColumnDefinition.partitionKeyDef(ks, cf, names.defaultPartitionKeyName(), subTypes.get(i), i));
            }
            else
            {
                defs.add(ColumnDefinition.partitionKeyDef(ks, cf, names.defaultPartitionKeyName(), keyValidator, 0));
            }
        }

        if (subComparator != null)
        {
            // SuperColumn tables: we use a special map to hold dynamic values within a given super column
            defs.add(ColumnDefinition.clusteringDef(ks, cf, names.defaultClusteringName(), comparator, 0));
            defs.add(ColumnDefinition.regularDef(ks, cf, SuperColumnCompatibility.SUPER_COLUMN_MAP_COLUMN_STR, MapType.getInstance(subComparator, defaultValidator, true)));
            if (isDense)
            {
                defs.add(ColumnDefinition.clusteringDef(ks, cf, names.defaultClusteringName(), subComparator, 1));
                defs.add(ColumnDefinition.regularDef(ks, cf, names.defaultCompactValueName(), defaultValidator));
            }
        }
        else
        {
            List<AbstractType<?>> subTypes = comparator instanceof CompositeType
                                           ? ((CompositeType)comparator).types
                                           : Collections.<AbstractType<?>>singletonList(comparator);

            for (int i = 0; i < subTypes.size(); i++)
                defs.add(ColumnDefinition.clusteringDef(ks, cf, names.defaultClusteringName(), subTypes.get(i), i));

            defs.add(ColumnDefinition.regularDef(ks, cf, names.defaultCompactValueName(), defaultValidator));
        }
    }

    /* applies implicit defaults to cf definition. useful in updates */
    @SuppressWarnings("deprecation")
    private static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def)
    {
        if (!cf_def.isSetComment())
            cf_def.setComment("");
        if (!cf_def.isSetMin_compaction_threshold())
            cf_def.setMin_compaction_threshold(CompactionParams.DEFAULT_MIN_THRESHOLD);
        if (!cf_def.isSetMax_compaction_threshold())
            cf_def.setMax_compaction_threshold(CompactionParams.DEFAULT_MAX_THRESHOLD);
        if (!cf_def.isSetCompaction_strategy())
            cf_def.setCompaction_strategy(CompactionParams.DEFAULT.klass().getSimpleName());
        if (!cf_def.isSetCompaction_strategy_options())
            cf_def.setCompaction_strategy_options(Collections.emptyMap());
        if (!cf_def.isSetCompression_options())
            cf_def.setCompression_options(Collections.singletonMap(CompressionParams.SSTABLE_COMPRESSION, CompressionParams.DEFAULT.klass().getCanonicalName()));
        if (!cf_def.isSetDefault_time_to_live())
            cf_def.setDefault_time_to_live(TableParams.DEFAULT_DEFAULT_TIME_TO_LIVE);
        if (!cf_def.isSetDclocal_read_repair_chance())
            cf_def.setDclocal_read_repair_chance(TableParams.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE);

        // if index_interval was set, use that for the min_index_interval default
        if (!cf_def.isSetMin_index_interval())
        {
            if (cf_def.isSetIndex_interval())
                cf_def.setMin_index_interval(cf_def.getIndex_interval());
            else
                cf_def.setMin_index_interval(TableParams.DEFAULT_MIN_INDEX_INTERVAL);
        }

        if (!cf_def.isSetMax_index_interval())
        {
            // ensure the max is at least as large as the min
            cf_def.setMax_index_interval(Math.max(cf_def.min_index_interval, TableParams.DEFAULT_MAX_INDEX_INTERVAL));
        }
    }

    public static CfDef toThrift(CFMetaData cfm)
    {
        CfDef def = new CfDef(cfm.ksName, cfm.cfName);
        def.setColumn_type(cfm.isSuper() ? "Super" : "Standard");

        if (cfm.isSuper())
        {
            def.setComparator_type(cfm.comparator.subtype(0).toString());
            def.setSubcomparator_type(cfm.thriftColumnNameType().toString());
        }
        else
        {
            def.setComparator_type(LegacyLayout.makeLegacyComparator(cfm).toString());
        }

        def.setComment(cfm.params.comment);
        def.setRead_repair_chance(cfm.params.readRepairChance);
        def.setDclocal_read_repair_chance(cfm.params.dcLocalReadRepairChance);
        def.setGc_grace_seconds(cfm.params.gcGraceSeconds);
        def.setDefault_validation_class(cfm.makeLegacyDefaultValidator().toString());
        def.setKey_validation_class(cfm.getKeyValidator().toString());
        def.setMin_compaction_threshold(cfm.params.compaction.minCompactionThreshold());
        def.setMax_compaction_threshold(cfm.params.compaction.maxCompactionThreshold());
        // We only return the alias if only one is set since thrift don't know about multiple key aliases
        if (cfm.partitionKeyColumns().size() == 1)
            def.setKey_alias(cfm.partitionKeyColumns().get(0).name.bytes);
        def.setColumn_metadata(columnDefinitionsToThrift(cfm, cfm.allColumns()));
        def.setCompaction_strategy(cfm.params.compaction.klass().getName());
        def.setCompaction_strategy_options(cfm.params.compaction.options());
        def.setCompression_options(compressionParametersToThrift(cfm.params.compression));
        def.setBloom_filter_fp_chance(cfm.params.bloomFilterFpChance);
        def.setMin_index_interval(cfm.params.minIndexInterval);
        def.setMax_index_interval(cfm.params.maxIndexInterval);
        def.setMemtable_flush_period_in_ms(cfm.params.memtableFlushPeriodInMs);
        def.setCaching(toThrift(cfm.params.caching));
        def.setCells_per_row_to_cache(toThriftCellsPerRow(cfm.params.caching));
        def.setDefault_time_to_live(cfm.params.defaultTimeToLive);
        def.setSpeculative_retry(cfm.params.speculativeRetry.toString());
        def.setTriggers(triggerDefinitionsToThrift(cfm.getTriggers()));

        return def;
    }

    public static ColumnDefinition fromThrift(String ksName,
                                              String cfName,
                                              AbstractType<?> thriftComparator,
                                              AbstractType<?> thriftSubcomparator,
                                              ColumnDef thriftColumnDef)
    throws SyntaxException, ConfigurationException
    {
        boolean isSuper = thriftSubcomparator != null;
        // For super columns, the componentIndex is 1 because the ColumnDefinition applies to the column component.
        AbstractType<?> comparator = thriftSubcomparator == null ? thriftComparator : thriftSubcomparator;
        try
        {
            comparator.validate(thriftColumnDef.name);
        }
        catch (MarshalException e)
        {
            throw new ConfigurationException(String.format("Column name %s is not valid for comparator %s", ByteBufferUtil.bytesToHex(thriftColumnDef.name), comparator));
        }

        // In our generic layout, we store thrift defined columns as static, but this doesn't work for super columns so we
        // use a regular definition (and "dynamic" columns are handled in a map).
        ColumnDefinition.Kind kind = isSuper ? ColumnDefinition.Kind.REGULAR : ColumnDefinition.Kind.STATIC;
        return new ColumnDefinition(ksName,
                                    cfName,
                                    ColumnIdentifier.getInterned(ByteBufferUtil.clone(thriftColumnDef.name), comparator),
                                    TypeParser.parse(thriftColumnDef.validation_class),
                                    ColumnDefinition.NO_POSITION,
                                    kind);
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

    private static Indexes indexDefsFromThrift(CFMetaData cfm,
                                               String ksName,
                                               String cfName,
                                               AbstractType<?> thriftComparator,
                                               AbstractType<?> thriftSubComparator,
                                               List<ColumnDef> thriftDefs)
    {
        if (thriftDefs == null)
            return Indexes.none();

        Set<String> indexNames = new HashSet<>();
        Indexes.Builder indexes = Indexes.builder();
        for (ColumnDef def : thriftDefs)
        {
            if (def.isSetIndex_type())
            {
                ColumnDefinition column = fromThrift(ksName, cfName, thriftComparator, thriftSubComparator, def);

                String indexName = def.getIndex_name();
                // add a generated index name if none was supplied
                if (Strings.isNullOrEmpty(indexName))
                    indexName = Indexes.getAvailableIndexName(ksName, cfName, column.name.toString());

                if (indexNames.contains(indexName))
                    throw new ConfigurationException("Duplicate index name " + indexName);

                indexNames.add(indexName);

                Map<String, String> indexOptions = def.getIndex_options();
                if (indexOptions != null && indexOptions.containsKey(IndexTarget.TARGET_OPTION_NAME))
                        throw new ConfigurationException("Reserved index option 'target' cannot be used");

                IndexMetadata.Kind kind = IndexMetadata.Kind.valueOf(def.index_type.name());

                indexes.add(IndexMetadata.fromLegacyMetadata(cfm, column, indexName, kind, indexOptions));
            }
        }
        return indexes.build();
    }

    @VisibleForTesting
    public static ColumnDef toThrift(CFMetaData cfMetaData, ColumnDefinition column)
    {
        ColumnDef cd = new ColumnDef();

        cd.setName(ByteBufferUtil.clone(column.name.bytes));
        cd.setValidation_class(column.type.toString());

        // we include the index in the ColumnDef iff its targets are compatible with
        // pre-3.0 indexes AND it is the only index defined on the given column, that is:
        //   * it is the only index on the column (i.e. with this column as its target)
        //   * it has only a single target, which matches the pattern for pre-3.0 indexes
        //     i.e. keys/values/entries/full, with exactly 1 argument that matches the
        //     column name OR a simple column name (for indexes on non-collection columns)
        // n.b. it's a guess that using a pre-compiled regex and checking the group is
        // cheaper than compiling a new regex for each column, but as this isn't on
        // any hot path this hasn't been verified yet.
        IndexMetadata matchedIndex = null;
        for (IndexMetadata index : cfMetaData.getIndexes())
        {
            Pair<ColumnDefinition, IndexTarget.Type> target  = TargetParser.parse(cfMetaData, index);
            if (target.left.equals(column))
            {
                // we already found an index for this column, we've no option but to
                // ignore both of them (and any others we've yet to find)
                if (matchedIndex != null)
                    return cd;

                matchedIndex = index;
            }
        }

        if (matchedIndex != null)
        {
            cd.setIndex_type(org.apache.cassandra.thrift.IndexType.valueOf(matchedIndex.kind.name()));
            cd.setIndex_name(matchedIndex.name);
            Map<String, String> filteredOptions = Maps.filterKeys(matchedIndex.options,
                                                                  s -> !IndexTarget.TARGET_OPTION_NAME.equals(s));
            cd.setIndex_options(filteredOptions.isEmpty()
                                ? null
                                : Maps.newHashMap(filteredOptions));
        }

        return cd;
    }

    private static List<ColumnDef> columnDefinitionsToThrift(CFMetaData metadata, Collection<ColumnDefinition> columns)
    {
        List<ColumnDef> thriftDefs = new ArrayList<>(columns.size());
        for (ColumnDefinition def : columns)
            if (def.isPartOfCellName(metadata.isCQLTable(), metadata.isSuper()))
                thriftDefs.add(ThriftConversion.toThrift(metadata, def));
        return thriftDefs;
    }

    private static Triggers triggerDefinitionsFromThrift(List<TriggerDef> thriftDefs)
    {
        Triggers.Builder triggers = Triggers.builder();
        for (TriggerDef thriftDef : thriftDefs)
            triggers.add(new TriggerMetadata(thriftDef.getName(), thriftDef.getOptions().get(TriggerMetadata.CLASS)));
        return triggers.build();
    }

    private static List<TriggerDef> triggerDefinitionsToThrift(Triggers triggers)
    {
        List<TriggerDef> thriftDefs = new ArrayList<>();
        for (TriggerMetadata def : triggers)
        {
            TriggerDef td = new TriggerDef();
            td.setName(def.name);
            td.setOptions(Collections.singletonMap(TriggerMetadata.CLASS, def.classOption));
            thriftDefs.add(td);
        }
        return thriftDefs;
    }

    @SuppressWarnings("deprecation")
    public static Map<String, String> compressionParametersToThrift(CompressionParams parameters)
    {
        if (!parameters.isEnabled())
            return Collections.emptyMap();

        Map<String, String> options = new HashMap<>(parameters.getOtherOptions());
        Class<? extends ICompressor> klass = parameters.getSstableCompressor().getClass();
        options.put(CompressionParams.SSTABLE_COMPRESSION, klass.getName());
        options.put(CompressionParams.CHUNK_LENGTH_KB, parameters.chunkLengthInKB());
        return options;
    }

    private static String toThrift(CachingParams caching)
    {
        if (caching.cacheRows() && caching.cacheKeys())
            return "ALL";

        if (caching.cacheRows())
            return "ROWS_ONLY";

        if (caching.cacheKeys())
            return "KEYS_ONLY";

        return "NONE";
    }

    private static CachingParams cachingFromTrhfit(String caching)
    {
        switch (caching.toUpperCase(Locale.ENGLISH))
        {
            case "ALL":
                return CachingParams.CACHE_EVERYTHING;
            case "ROWS_ONLY":
                return new CachingParams(false, Integer.MAX_VALUE);
            case "KEYS_ONLY":
                return CachingParams.CACHE_KEYS;
            case "NONE":
                return CachingParams.CACHE_NOTHING;
            default:
                throw new ConfigurationException(String.format("Invalid value %s for caching parameter", caching));
        }
    }

    private static String toThriftCellsPerRow(CachingParams caching)
    {
        return caching.cacheAllRows()
             ? "ALL"
             : String.valueOf(caching.rowsPerPartitionToCache());
    }

    private static int fromThriftCellsPerRow(String value)
    {
        return "ALL".equals(value)
             ? Integer.MAX_VALUE
             : Integer.parseInt(value);
    }

    public static CachingParams cachingFromThrift(String caching, String cellsPerRow)
    {
        boolean cacheKeys = true;
        int rowsPerPartitionToCache = 0;

        // if we get a caching string from thrift it is legacy, "ALL", "KEYS_ONLY" etc
        if (caching != null)
        {
            CachingParams parsed = cachingFromTrhfit(caching);
            cacheKeys = parsed.cacheKeys();
            rowsPerPartitionToCache = parsed.rowsPerPartitionToCache();
        }

        // if we get cells_per_row from thrift, it is either "ALL" or "<number of cells to cache>".
        if (cellsPerRow != null && rowsPerPartitionToCache > 0)
            rowsPerPartitionToCache = fromThriftCellsPerRow(cellsPerRow);

        return new CachingParams(cacheKeys, rowsPerPartitionToCache);
    }
}
