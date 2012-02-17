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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.db.migration.MigrationHelper.*;

public final class CFMetaData
{
    //
    // !! Important !!
    // This class can be tricky to modify.  Please read http://wiki.apache.org/cassandra/ConfigurationNotes
    // for how to do so safely.
    //

    private static Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.1;
    public final static double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.0;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = true;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static double DEFAULT_MERGE_SHARDS_CHANCE = 0.1;
    public final static String DEFAULT_COMPACTION_STRATEGY_CLASS = "SizeTieredCompactionStrategy";
    public final static ByteBuffer DEFAULT_KEY_NAME = ByteBufferUtil.bytes("KEY");

    public static final CFMetaData StatusCf = newSystemMetadata(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null);
    public static final CFMetaData HintsCf = newSystemMetadata(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance);
    @Deprecated
    public static final CFMetaData MigrationsCf = newSystemMetadata(Migration.MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null);
    @Deprecated
    public static final CFMetaData SchemaCf = newSystemMetadata(Migration.SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null);
    public static final CFMetaData IndexCf = newSystemMetadata(SystemTable.INDEX_CF, 5, "indexes that have been completed", UTF8Type.instance, null);
    public static final CFMetaData NodeIdCf = newSystemMetadata(SystemTable.NODE_ID_CF, 6, "nodeId and their metadata", TimeUUIDType.instance, null);
    public static final CFMetaData VersionCf =
            newSystemMetadata(SystemTable.VERSION_CF, 7, "server version information", UTF8Type.instance, null)
            .keyAlias(ByteBufferUtil.bytes("component"))
            .keyValidator(UTF8Type.instance)
            .columnMetadata(Collections.singletonMap(ByteBufferUtil.bytes("version"),
                                                     new ColumnDefinition(ByteBufferUtil.bytes("version"),
                                                                          UTF8Type.instance,
                                                                          null,
                                                                          null,
                                                                          null)));
    public static final CFMetaData SchemaKeyspacesCf = schemaCFDefinition(SystemTable.SCHEMA_KEYSPACES_CF, 8, "keyspace attributes of the schema", AsciiType.instance, 1);
    public static final CFMetaData SchemaColumnFamiliesCf = schemaCFDefinition(SystemTable.SCHEMA_COLUMNFAMILIES_CF, 9, "ColumnFamily attributes of the schema", AsciiType.instance, 2);
    public static final CFMetaData SchemaColumnsCf = schemaCFDefinition(SystemTable.SCHEMA_COLUMNS_CF, 10, "ColumnFamily column attributes of the schema", AsciiType.instance, 3);

    private static CFMetaData schemaCFDefinition(String name, int index, String comment, AbstractType<?> comp, int nestingLevel)
    {
        AbstractType<?> comparator;

        if (nestingLevel == 1)
        {
            comparator = comp;
        }
        else
        {
            List<AbstractType<?>> composite = new ArrayList<AbstractType<?>>(nestingLevel);

            for (int i = 0; i < nestingLevel; i++)
                composite.add(comp);

            comparator = CompositeType.getInstance(composite);
        }

        return newSystemMetadata(name,
                                 index,
                                 comment,
                                 comparator,
                                 null)
                                 .keyValidator(AsciiType.instance)
                                 .defaultValidator(UTF8Type.instance);
    }

    public enum Caching
    {
        ALL, KEYS_ONLY, ROWS_ONLY, NONE;

        public static Caching fromString(String cache) throws ConfigurationException
        {
            try
            {
                return valueOf(cache.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(String.format("%s not found, available types: %s.", cache, StringUtils.join(values(), ", ")));
            }
        }
    }

    //REQUIRED
    public final Integer cfId;                        // internal id, never exposed to user
    public final String ksName;                       // name of keyspace
    public final String cfName;                       // name of this column family
    public final ColumnFamilyType cfType;             // standard, super
    public AbstractType<?> comparator;          // bytes, long, timeuuid, utf8, etc.
    public AbstractType<?> subcolumnComparator; // like comparator, for supercolumns

    //OPTIONAL
    private String comment;                           // default none, for humans only
    private double readRepairChance;                  // default 1.0 (always), chance [0.0,1.0] of read repair
    private double dcLocalReadRepairChance;           // default 0.0
    private boolean replicateOnWrite;                 // default false
    private int gcGraceSeconds;                       // default 864000 (ten days)
    private AbstractType<?> defaultValidator;         // default BytesType (no-op), use comparator types
    private AbstractType<?> keyValidator;             // default BytesType (no-op), use comparator types
    private int minCompactionThreshold;               // default 4
    private int maxCompactionThreshold;               // default 32
    // mergeShardsChance is now obsolete, but left here so as to not break
    // thrift compatibility
    private double mergeShardsChance;                 // default 0.1, chance [0.0, 1.0] of merging old shards during replication
    private ByteBuffer keyAlias;                      // default NULL
    private List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;                    // default NULL
    private Double bloomFilterFpChance;               // default NULL
    private Caching caching;                          // default KEYS_ONLY (possible: all, key_only, row_only, none)

    private Map<ByteBuffer, ColumnDefinition> column_metadata;
    public Class<? extends AbstractCompactionStrategy> compactionStrategyClass;
    public Map<String, String> compactionStrategyOptions;

    public CompressionParameters compressionParameters;

    // Processed infos used by CQL. This can be fully reconstructed from the CFMedata,
    // so it's not saved on disk. It is however costlyish to recreate for each query
    // so we cache it here (and update on each relevant CFMetadata change)
    private CFDefinition cqlCfDef;

    public CFMetaData comment(String prop) { comment = enforceCommentNotNull(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dclocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData replicateOnWrite(boolean prop) {replicateOnWrite = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType<?> prop) {defaultValidator = prop; updateCfDef(); return this;}
    public CFMetaData keyValidator(AbstractType<?> prop) {keyValidator = prop; updateCfDef(); return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData mergeShardsChance(double prop) {mergeShardsChance = prop; return this;}
    public CFMetaData keyAlias(ByteBuffer prop) {keyAlias = prop; updateCfDef(); return this;}
    public CFMetaData columnAliases(List<ByteBuffer> prop) {columnAliases = prop; updateCfDef(); return this;}
    public CFMetaData valueAlias(ByteBuffer prop) {valueAlias = prop; updateCfDef(); return this;}
    public CFMetaData columnMetadata(Map<ByteBuffer,ColumnDefinition> prop) {column_metadata = prop; updateCfDef(); return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(Double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(Caching prop) {caching = prop; return this;}

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc)
    {
        this(keyspace, name, type, comp, subcc, Schema.instance.nextCFId());
    }

    private CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc, int id)
    {
        // Final fields must be set in constructor
        ksName = keyspace;
        cfName = name;
        cfType = type;
        comparator = comp;
        subcolumnComparator = enforceSubccDefault(type, subcc);

        // System cfs have specific ids, and copies of old CFMDs need
        //  to copy over the old id.
        cfId = id;
        caching = Caching.KEYS_ONLY;

        this.init();
    }

    private AbstractType<?> enforceSubccDefault(ColumnFamilyType cftype, AbstractType<?> subcc)
    {
        return (subcc == null) && (cftype == ColumnFamilyType.Super) ? BytesType.instance : subcc;
    }

    private static String enforceCommentNotNull (CharSequence comment)
    {
        return (comment == null) ? "" : comment.toString();
    }

    private void init()
    {
        // Set a bunch of defaults
        readRepairChance             = DEFAULT_READ_REPAIR_CHANCE;
        dcLocalReadRepairChance      = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
        replicateOnWrite             = DEFAULT_REPLICATE_ON_WRITE;
        gcGraceSeconds               = DEFAULT_GC_GRACE_SECONDS;
        minCompactionThreshold       = DEFAULT_MIN_COMPACTION_THRESHOLD;
        maxCompactionThreshold       = DEFAULT_MAX_COMPACTION_THRESHOLD;
        mergeShardsChance            = DEFAULT_MERGE_SHARDS_CHANCE;

        // Defaults strange or simple enough to not need a DEFAULT_T for
        defaultValidator = BytesType.instance;
        keyValidator = BytesType.instance;
        comment = "";
        keyAlias = null; // This qualifies as a 'strange default'.
        valueAlias = null;
        column_metadata = new HashMap<ByteBuffer,ColumnDefinition>();

        try
        {
            compactionStrategyClass = createCompactionStrategy(DEFAULT_COMPACTION_STRATEGY_CLASS);
        }
        catch (ConfigurationException e)
        {
            throw new AssertionError(e);
        }
        compactionStrategyOptions = new HashMap<String, String>();

        compressionParameters = new CompressionParameters(null);
        updateCfDef(); // init cqlCfDef
    }

    private static CFMetaData newSystemMetadata(String cfName, int cfId, String comment, AbstractType<?> comparator, AbstractType<?> subcc)
    {
        ColumnFamilyType type = subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super;
        CFMetaData newCFMD = new CFMetaData(Table.SYSTEM_TABLE, cfName, type, comparator,  subcc, cfId);

        return newCFMD.comment(comment)
                      .readRepairChance(0)
                      .dclocalReadRepairChance(0)
                      .gcGraceSeconds(0)
                      .mergeShardsChance(0.0);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType<?> columnComparator)
    {
        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, null)
                             .keyValidator(info.getValidator())
                             .readRepairChance(0.0)
                             .dclocalReadRepairChance(0.0)
                             .reloadSecondaryIndexMetadata(parent);
    }

    public CFMetaData reloadSecondaryIndexMetadata(CFMetaData parent)
    {
        gcGraceSeconds(parent.gcGraceSeconds);
        minCompactionThreshold(parent.minCompactionThreshold);
        maxCompactionThreshold(parent.maxCompactionThreshold);
        compactionStrategyClass(parent.compactionStrategyClass);
        compactionStrategyOptions(parent.compactionStrategyOptions);
        compressionParameters(parent.compressionParameters);;
        return this;
    }

    // Create a new CFMD by changing just the cfName
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        return copyOpts(new CFMetaData(cfm.ksName, newName, cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.cfId), cfm);
    }

    // Create a new CFMD by changing just the ksName
    public static CFMetaData renameTable(CFMetaData cfm, String ksName)
    {
        return copyOpts(new CFMetaData(ksName, cfm.cfName, cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.cfId), cfm);
    }

    private static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        return newCFMD.comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dclocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .replicateOnWrite(oldCFMD.replicateOnWrite)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .columnMetadata(oldCFMD.column_metadata)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(oldCFMD.compactionStrategyOptions)
                      .compressionParameters(oldCFMD.compressionParameters)
                      .bloomFilterFpChance(oldCFMD.bloomFilterFpChance)
                      .caching(oldCFMD.caching);
    }
    
    /**
     * generate a column family name for an index corresponding to the given column.
     * This is NOT the same as the index's name! This is only used in sstable filenames and is not exposed to users.
     *
     * @param info A definition of the column with index
     *
     * @return name of the index ColumnFamily
     */
    public String indexColumnFamilyName(ColumnDefinition info)
    {
        // TODO simplify this when info.index_name is guaranteed to be set
        return cfName + Directories.SECONDARY_INDEX_NAME_SEPARATOR + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name) : info.getIndexName());
    }

    @Deprecated
    public static CFMetaData fromAvro(org.apache.cassandra.db.migration.avro.CfDef cf)
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
            ColumnDefinition cd = ColumnDefinition.fromAvro(aColumn_metadata);
            if (cd.getIndexType() != null && cd.getIndexName() == null)
                cd.setIndexName(getDefaultIndexName(cf.name.toString(), comparator, cd.name));
            column_metadata.put(cd.name, cd);
        }

        CFMetaData newCFMD = new CFMetaData(cf.keyspace.toString(),
                                            cf.name.toString(),
                                            ColumnFamilyType.create(cf.column_type.toString()),
                                            comparator,
                                            subcolumnComparator,
                                            cf.id);

        // When we pull up an old avro CfDef which doesn't have these arguments,
        //  it doesn't default them correctly. Without explicit defaulting,
        //  grandfathered metadata becomes wrong or causes crashes.
        //  Isn't AVRO supposed to handle stuff like this?
        if (cf.min_compaction_threshold != null) { newCFMD.minCompactionThreshold(cf.min_compaction_threshold); }
        if (cf.max_compaction_threshold != null) { newCFMD.maxCompactionThreshold(cf.max_compaction_threshold); }
        if (cf.merge_shards_chance != null) { newCFMD.mergeShardsChance(cf.merge_shards_chance); }
        if (cf.key_alias != null) { newCFMD.keyAlias(cf.key_alias); }
        if (cf.column_aliases != null) { newCFMD.columnAliases(fixAvroRetardation(cf.column_aliases)); }
        if (cf.value_alias != null) { newCFMD.valueAlias(cf.value_alias); }
        if (cf.compaction_strategy != null)
        {
            try
            {
                newCFMD.compactionStrategyClass = createCompactionStrategy(cf.compaction_strategy.toString());
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

        Caching caching;

        try
        {
            caching = cf.caching == null ? Caching.KEYS_ONLY : Caching.fromString(cf.caching.toString());
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }

        return newCFMD.comment(cf.comment.toString())
                      .readRepairChance(cf.read_repair_chance)
                      .dclocalReadRepairChance(cf.dclocal_read_repair_chance)
                      .replicateOnWrite(cf.replicate_on_write)
                      .gcGraceSeconds(cf.gc_grace_seconds)
                      .defaultValidator(validator)
                      .keyValidator(keyValidator)
                      .columnMetadata(column_metadata)
                      .compressionParameters(cp)
                      .bloomFilterFpChance(cf.bloom_filter_fp_chance)
                      .caching(caching);
    }

    /*
     * Avro handles array with it's own class, GenericArray, that extends
     * AbstractList but redefine equals() in a way that violate List.equals()
     * specification (basically only a GenericArray can ever be equal to a
     * GenericArray).
     * (Concretely, keeping the list returned by avro breaks DefsTest.saveAndRestore())
     */
    private static <T> List<T> fixAvroRetardation(List<T> array)
    {
        return new ArrayList<T>(array);
    }
    
    public String getComment()
    {
        return comment;
    }

    public double getReadRepairChance()
    {
        return readRepairChance;
    }

    public double getDcLocalReadRepair()
    {
        return dcLocalReadRepairChance;
    }

    public double getMergeShardsChance()
    {
        return mergeShardsChance;
    }

    public boolean getReplicateOnWrite()
    {
        return replicateOnWrite;
    }
    
    public int getGcGraceSeconds()
    {
        return gcGraceSeconds;
    }

    public AbstractType<?> getDefaultValidator()
    {
        return defaultValidator;
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public Integer getMinCompactionThreshold()
    {
        return minCompactionThreshold;
    }

    public Integer getMaxCompactionThreshold()
    {
        return maxCompactionThreshold;
    }

    public ByteBuffer getKeyName()
    {
        return keyAlias == null ? DEFAULT_KEY_NAME : keyAlias;
    }

    public ByteBuffer getKeyAlias()
    {
        return keyAlias;
    }

    public List<ByteBuffer> getColumnAliases()
    {
        return columnAliases;
    }

    public ByteBuffer getValueAlias()
    {
        return valueAlias;
    }

    public CompressionParameters compressionParameters()
    {
        return compressionParameters;
    }

    public Map<ByteBuffer, ColumnDefinition> getColumn_metadata()
    {
        return Collections.unmodifiableMap(column_metadata);
    }

    public AbstractType<?> getComparatorFor(ByteBuffer superColumnName)
    {
        return superColumnName == null ? comparator : subcolumnComparator;
    }

    public Double getBloomFilterFpChance()
    {
        return bloomFilterFpChance;
    }

    public Caching getCaching()
    {
        return caching;
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }

        CFMetaData rhs = (CFMetaData) obj;
        return new EqualsBuilder()
            .append(ksName, rhs.ksName)
            .append(cfName, rhs.cfName)
            .append(cfType, rhs.cfType)
            .append(comparator, rhs.comparator)
            .append(subcolumnComparator, rhs.subcolumnComparator)
            .append(comment, rhs.comment)
            .append(readRepairChance, rhs.readRepairChance)
            .append(dcLocalReadRepairChance, rhs.dcLocalReadRepairChance)
            .append(replicateOnWrite, rhs.replicateOnWrite)
            .append(gcGraceSeconds, rhs.gcGraceSeconds)
            .append(defaultValidator, rhs.defaultValidator)
            .append(keyValidator, rhs.keyValidator)
            .append(minCompactionThreshold, rhs.minCompactionThreshold)
            .append(maxCompactionThreshold, rhs.maxCompactionThreshold)
            .append(cfId.intValue(), rhs.cfId.intValue())
            .append(column_metadata, rhs.column_metadata)
            .append(mergeShardsChance, rhs.mergeShardsChance)
            .append(keyAlias, rhs.keyAlias)
            .append(columnAliases, rhs.columnAliases)
            .append(valueAlias, rhs.valueAlias)
            .append(compactionStrategyClass, rhs.compactionStrategyClass)
            .append(compactionStrategyOptions, rhs.compactionStrategyOptions)
            .append(compressionParameters, rhs.compressionParameters)
            .append(bloomFilterFpChance, rhs.bloomFilterFpChance)
            .append(caching, rhs.caching)
            .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(ksName)
            .append(cfName)
            .append(cfType)
            .append(comparator)
            .append(subcolumnComparator)
            .append(comment)
            .append(readRepairChance)
            .append(dcLocalReadRepairChance)
            .append(replicateOnWrite)
            .append(gcGraceSeconds)
            .append(defaultValidator)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(cfId)
            .append(column_metadata)
            .append(mergeShardsChance)
            .append(keyAlias)
            .append(columnAliases)
            .append(valueAlias)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .append(bloomFilterFpChance)
            .append(caching)
            .toHashCode();
    }

    public AbstractType<?> getValueValidator(ByteBuffer column)
    {
        return getValueValidator(column_metadata.get(column));
    }

    public AbstractType<?> getValueValidator(ColumnDefinition columnDefinition)
    {
        return columnDefinition == null
               ? defaultValidator
               : columnDefinition.getValidator();
    }

    /** applies implicit defaults to cf definition. useful in updates */
    public static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def) 
    {
        if (!cf_def.isSetComment())
            cf_def.setComment("");
        if (!cf_def.isSetReplicate_on_write())
            cf_def.setReplicate_on_write(CFMetaData.DEFAULT_REPLICATE_ON_WRITE);
        if (!cf_def.isSetMin_compaction_threshold())
            cf_def.setMin_compaction_threshold(CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD);
        if (!cf_def.isSetMax_compaction_threshold())
            cf_def.setMax_compaction_threshold(CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD);
        if (!cf_def.isSetMerge_shards_chance())
            cf_def.setMerge_shards_chance(CFMetaData.DEFAULT_MERGE_SHARDS_CHANCE);
        if (null == cf_def.compaction_strategy)
            cf_def.compaction_strategy = DEFAULT_COMPACTION_STRATEGY_CLASS;
        if (null == cf_def.compaction_strategy_options)
            cf_def.compaction_strategy_options = Collections.emptyMap();
        if (!cf_def.isSetCompression_options())
        {
            cf_def.setCompression_options(new HashMap<String, String>()
            {{
                put(CompressionParameters.SSTABLE_COMPRESSION, SnappyCompressor.class.getCanonicalName());
            }});
        }
        if (!cf_def.isSetDclocal_read_repair_chance())
            cf_def.setDclocal_read_repair_chance(CFMetaData.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE);
    }

    public static CFMetaData fromThrift(org.apache.cassandra.thrift.CfDef cf_def) throws InvalidRequestException, ConfigurationException
    {
        ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
        if (cfType == null)
        {
          throw new InvalidRequestException("Invalid column type " + cf_def.column_type);
        }

        applyImplicitDefaults(cf_def);

        CFMetaData newCFMD = new CFMetaData(cf_def.keyspace,
                                            cf_def.name,
                                            cfType,
                                            TypeParser.parse(cf_def.comparator_type),
                                            cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type),
                                            cf_def.isSetId() ? cf_def.id : Schema.instance.nextCFId());

        if (cf_def.isSetGc_grace_seconds()) { newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds); }
        if (cf_def.isSetMin_compaction_threshold()) { newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold); }
        if (cf_def.isSetMax_compaction_threshold()) { newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold); }
        if (cf_def.isSetMerge_shards_chance()) { newCFMD.mergeShardsChance(cf_def.merge_shards_chance); }
        if (cf_def.isSetKey_alias()) { newCFMD.keyAlias(cf_def.key_alias); }
        if (cf_def.isSetColumn_aliases() && cf_def.column_aliases != null) { newCFMD.columnAliases(cf_def.column_aliases); }
        if (cf_def.isSetValue_alias()) { newCFMD.valueAlias(cf_def.value_alias); }
        if (cf_def.isSetKey_validation_class()) { newCFMD.keyValidator(TypeParser.parse(cf_def.key_validation_class)); }
        if (cf_def.isSetCompaction_strategy())
            newCFMD.compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy);
        if (cf_def.isSetCompaction_strategy_options())
            newCFMD.compactionStrategyOptions(new HashMap<String, String>(cf_def.compaction_strategy_options));
        if (cf_def.isSetBloom_filter_fp_chance())
            newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
        if (cf_def.isSetCaching())
            newCFMD.caching(Caching.fromString(cf_def.caching));
        if (cf_def.isSetDclocal_read_repair_chance())
            newCFMD.dclocalReadRepairChance(cf_def.dclocal_read_repair_chance);

        CompressionParameters cp = CompressionParameters.create(cf_def.compression_options);

        return newCFMD.comment(cf_def.comment)
                      .readRepairChance(cf_def.read_repair_chance)
                      .replicateOnWrite(cf_def.replicate_on_write)
                      .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                      .keyValidator(TypeParser.parse(cf_def.key_validation_class))
                      .columnMetadata(ColumnDefinition.fromThrift(cf_def.column_metadata))
                      .compressionParameters(cp)
                      .validate();
    }

    public void reload() throws IOException
    {
        Row cfDefRow = SystemTable.readSchemaRow(ksName, cfName);

        if (cfDefRow.cf == null || cfDefRow.cf.isEmpty())
            throw new IOException(String.format("%s not found in the schema definitions table.", ksName + ":" + cfName));

        try
        {
            apply(fromSchema(cfDefRow.cf));
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
    }

    /**
     * Updates CFMetaData in-place to match cf_def
     *
     * *Note*: This method left public only for DefsTest, don't use directly!
     *
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    public void apply(CfDef cf_def) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cf_def, this);
        // validate
        if (!cf_def.keyspace.equals(ksName))
            throw new ConfigurationException(String.format("Keyspace mismatch (found %s; expected %s)",
                                                           cf_def.keyspace, ksName));
        if (!cf_def.name.equals(cfName))
            throw new ConfigurationException(String.format("Column family mismatch (found %s; expected %s)",
                                                           cf_def.name, cfName));
        if (cf_def.id != cfId)
            throw new ConfigurationException(String.format("Column family ID mismatch (found %s; expected %s)",
                                                           cf_def.id, cfId));

        if (!cf_def.column_type.equals(cfType.name()))
            throw new ConfigurationException("types do not match.");

        AbstractType<?> newComparator = TypeParser.parse(cf_def.comparator_type);
        AbstractType<?> newSubComparator = (cf_def.subcomparator_type == null || cf_def.subcomparator_type.equals(""))
                                         ? null
                                         : TypeParser.parse(cf_def.subcomparator_type);

        if (!newComparator.isCompatibleWith(comparator))
            throw new ConfigurationException("comparators do not match or are not compatible.");
        if (newSubComparator == null)
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException("subcolumncomparators do not match.");
            // else, it's null and we're good.
        }
        else if (!newSubComparator.isCompatibleWith(subcolumnComparator))
            throw new ConfigurationException("subcolumncomparators do not match or are note compatible.");

        // TODO: this method should probably return a new CFMetaData so that
        // 1) we can keep comparator and subcolumnComparator final
        // 2) updates are applied atomically
        comparator = newComparator;
        subcolumnComparator = newSubComparator;

        validateMinMaxCompactionThresholds(cf_def);

        comment = enforceCommentNotNull(cf_def.comment);
        readRepairChance = cf_def.read_repair_chance;
        if (cf_def.isSetDclocal_read_repair_chance())
            dcLocalReadRepairChance = cf_def.dclocal_read_repair_chance;
        replicateOnWrite = cf_def.replicate_on_write;
        gcGraceSeconds = cf_def.gc_grace_seconds;
        defaultValidator = TypeParser.parse(cf_def.default_validation_class);
        keyValidator = TypeParser.parse(cf_def.key_validation_class);
        minCompactionThreshold = cf_def.min_compaction_threshold;
        maxCompactionThreshold = cf_def.max_compaction_threshold;
        mergeShardsChance = cf_def.merge_shards_chance;
        keyAlias = cf_def.key_alias;
        columnAliases = cf_def.column_aliases;
        valueAlias = cf_def.value_alias;
        if (cf_def.isSetBloom_filter_fp_chance())
            bloomFilterFpChance = cf_def.bloom_filter_fp_chance;
        caching = Caching.fromString(cf_def.caching);

        if (!cf_def.isSetColumn_metadata())
            cf_def.setColumn_metadata(new ArrayList<ColumnDef>());

        // adjust column definitions. figure out who is coming and going.
        Set<ByteBuffer> toRemove = new HashSet<ByteBuffer>();
        Set<ByteBuffer> newColumns = new HashSet<ByteBuffer>();
        Set<ColumnDef> toAdd = new HashSet<ColumnDef>();
        for (ColumnDef def : cf_def.column_metadata)
        {
            newColumns.add(def.name);
            if (!column_metadata.containsKey(def.name))
                toAdd.add(def);
        }
        for (ByteBuffer name : column_metadata.keySet())
            if (!newColumns.contains(name))
                toRemove.add(name);
        
        // remove the ones leaving.
        for (ByteBuffer indexName : toRemove)
        {
            column_metadata.remove(indexName);
        }
        // update the ones staying
        for (ColumnDef def : cf_def.column_metadata)
        {
            ColumnDefinition oldDef = column_metadata.get(def.name);
            if (oldDef == null)
                continue;
            oldDef.setValidator(TypeParser.parse(def.validation_class));
            oldDef.setIndexType(def.index_type == null ? null : IndexType.valueOf(def.index_type.name()),
                                def.index_options);
            oldDef.setIndexName(def.index_name == null ? null : def.index_name);
        }
        // add the new ones coming in.
        for (ColumnDef def : toAdd)
        {
            AbstractType<?> dValidClass = TypeParser.parse(def.validation_class);
            ColumnDefinition cd = new ColumnDefinition(def.name, 
                                                       dValidClass,
                                                       def.index_type == null ? null : IndexType.valueOf(def.index_type.name()),
                                                       def.index_options,
                                                       def.index_name == null ? null : def.index_name);
            column_metadata.put(cd.name, cd);
        }

        if (cf_def.compaction_strategy != null)
            compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy);

        if (null != cf_def.compaction_strategy_options)
        {
            compactionStrategyOptions = new HashMap<String, String>();
            for (Map.Entry<String, String> e : cf_def.compaction_strategy_options.entrySet())
                compactionStrategyOptions.put(e.getKey(), e.getValue());
        }

        compressionParameters = CompressionParameters.create(cf_def.compression_options);

        logger.debug("application result is {}", this);
    }

    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        try
        {
            return (Class<? extends AbstractCompactionStrategy>) Class.forName(className);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Could not create Compaction Strategy of type " + className, e);
        }
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor = compactionStrategyClass.getConstructor(new Class[] {
                ColumnFamilyStore.class,
                Map.class // options
            });
            return (AbstractCompactionStrategy)constructor.newInstance(cfs, compactionStrategyOptions);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    // converts CFM to thrift CfDef
    public org.apache.cassandra.thrift.CfDef toThrift()
    {
        org.apache.cassandra.thrift.CfDef def = new org.apache.cassandra.thrift.CfDef(ksName, cfName);
        def.setId(cfId);
        def.setColumn_type(cfType.name());
        def.setComparator_type(comparator.toString());
        if (subcolumnComparator != null)
        {
            assert cfType == ColumnFamilyType.Super
                   : String.format("%s CF %s should not have subcomparator %s defined", cfType, cfName, subcolumnComparator);
            def.setSubcomparator_type(subcolumnComparator.toString());
        }
        def.setComment(enforceCommentNotNull(comment));
        def.setRead_repair_chance(readRepairChance);
        def.setDclocal_read_repair_chance(dcLocalReadRepairChance);
        def.setReplicate_on_write(replicateOnWrite);
        def.setGc_grace_seconds(gcGraceSeconds);
        def.setDefault_validation_class(defaultValidator == null ? null : defaultValidator.toString());
        def.setKey_validation_class(keyValidator.toString());
        def.setMin_compaction_threshold(minCompactionThreshold);
        def.setMax_compaction_threshold(maxCompactionThreshold);
        def.setMerge_shards_chance(mergeShardsChance);
        def.setKey_alias(keyAlias);
        List<org.apache.cassandra.thrift.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.thrift.ColumnDef>(column_metadata.size());
        for (ColumnDefinition cd : column_metadata.values())
        {
            org.apache.cassandra.thrift.ColumnDef tcd = new org.apache.cassandra.thrift.ColumnDef();
            tcd.setIndex_name(cd.getIndexName());
            tcd.setIndex_type(cd.getIndexType());
            tcd.setIndex_options(cd.getIndexOptions());
            tcd.setName(cd.name);
            tcd.setValidation_class(cd.getValidator().toString());
            column_meta.add(tcd);
        }
        def.setColumn_metadata(column_meta);
        def.setCompaction_strategy(compactionStrategyClass.getName());
        def.setCompaction_strategy_options(new HashMap<String, String>(compactionStrategyOptions));
        def.setCompression_options(compressionParameters.asThriftOptions());
        if (bloomFilterFpChance != null)
            def.setBloom_filter_fp_chance(bloomFilterFpChance);
        def.setCaching(caching.toString());
        return def;
    }

    public static void validateMinMaxCompactionThresholds(CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.isSetMin_compaction_threshold() && cf_def.isSetMax_compaction_threshold())
        {
            if ((cf_def.min_compaction_threshold > cf_def.max_compaction_threshold) &&
                    cf_def.max_compaction_threshold != 0)
            {
                throw new ConfigurationException("min_compaction_threshold cannot be greater than max_compaction_threshold");
            }
        }
        else if (cf_def.isSetMin_compaction_threshold())
        {
            if (cf_def.min_compaction_threshold > DEFAULT_MAX_COMPACTION_THRESHOLD)
            {
                throw new ConfigurationException("min_compaction_threshold cannot be greater than max_compaction_threshold (default " +
                                                  DEFAULT_MAX_COMPACTION_THRESHOLD + ")");
            }
        }
        else if (cf_def.isSetMax_compaction_threshold())
        {
            if (cf_def.max_compaction_threshold < DEFAULT_MIN_COMPACTION_THRESHOLD && cf_def.max_compaction_threshold != 0) {
                throw new ConfigurationException("max_compaction_threshold cannot be less than min_compaction_threshold");
            }
        }
        else
        {
            //Defaults are valid.
        }
    }

    public ColumnDefinition getColumnDefinition(ByteBuffer name)
    {
        return column_metadata.get(name);
    }

    public ColumnDefinition getColumnDefinitionForIndex(String indexName)
    {
        for (ColumnDefinition def : column_metadata.values())
        {
            if (indexName.equals(def.getIndexName()))
                return def;
        }
        return null;
    }

    /**
     * Convert a null index_name to appropriate default name according to column status
     * @param cf_def Thrift ColumnFamily Definition
     */
    public static void addDefaultIndexNames(org.apache.cassandra.thrift.CfDef cf_def) throws InvalidRequestException
    {
        if (cf_def.column_metadata == null)
            return;

        try
        {
            AbstractType<?> comparator = TypeParser.parse(cf_def.comparator_type);
            
            for (org.apache.cassandra.thrift.ColumnDef column : cf_def.column_metadata)
            {
                if (column.index_type != null && column.index_name == null)
                    column.index_name = getDefaultIndexName(cf_def.name, comparator, column.name);
            }
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    public static String getDefaultIndexName(String cfName, AbstractType<?> comparator, ByteBuffer columnName)
    {
        return (cfName + "_" + comparator.getString(columnName) + "_idx").replaceAll("\\W", "");
    }

    public IColumnSerializer getColumnSerializer()
    {
        if (cfType == ColumnFamilyType.Standard)
            return Column.serializer();
        return SuperColumn.serializer(subcolumnComparator);
    }

    public CFMetaData validate() throws ConfigurationException
    {
        if (comparator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid comparator");
        if (subcolumnComparator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid sub-column comparator");
        if (keyValidator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid key validator");

        // Mixing counter with non counter columns is not supported (#2614)
        if (defaultValidator instanceof CounterColumnType)
        {
            for (ColumnDefinition def : column_metadata.values())
                if (!(def.getValidator() instanceof CounterColumnType))
                    throw new ConfigurationException("Cannot add a non counter column (" + comparator.getString(def.name) + ") in a counter column family");
        }
        else
        {
            for (ColumnDefinition def : column_metadata.values())
                if (def.getValidator() instanceof CounterColumnType)
                    throw new ConfigurationException("Cannot add a counter column (" + comparator.getString(def.name) + ") in a non counter column family");
        }

        return this;
    }

    /**
     * Calculate the difference between current metadata and given and serialize it as schema RowMutation
     *
     * @param newState The new metadata (for the same CF)
     * @param modificationTimestamp Timestamp to use for mutation
     *
     * @return Difference between attributes in form of schema mutation
     *
     * @throws ConfigurationException if any of the attributes didn't pass validation
     */
    public RowMutation diff(CfDef newState, long modificationTimestamp) throws ConfigurationException
    {
        CfDef curState = toThrift();
        RowMutation m = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));

        for (CfDef._Fields field : CfDef._Fields.values())
        {
            if (field.equals(CfDef._Fields.COLUMN_METADATA))
                continue; // deal with columns after main attributes

            Object curValue = curState.isSet(field) ? curState.getFieldValue(field) : null;
            Object newValue = newState.isSet(field) ? newState.getFieldValue(field) : null;

            if (Objects.equal(curValue, newValue))
                continue;

            m.add(new QueryPath(SystemTable.SCHEMA_COLUMNFAMILIES_CF, null, compositeNameFor(curState.name, field.getFieldName())),
                  valueAsBytes(newValue),
                  modificationTimestamp);
        }

        AbstractType nameComparator = cfType.equals(ColumnFamilyType.Super)
                                        ? subcolumnComparator
                                        : comparator;

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, ColumnDefinition.fromThrift(newState.column_metadata));
        Map<ByteBuffer, ColumnDef> columnDefMap = ColumnDefinition.toMap(newState.column_metadata);

        // columns that are no longer needed
        for (ByteBuffer name : columnDiff.entriesOnlyOnLeft().keySet())
            ColumnDefinition.deleteFromSchema(m, curState.name, nameComparator, name, modificationTimestamp);

        // newly added columns
        for (ByteBuffer name : columnDiff.entriesOnlyOnRight().keySet())
            ColumnDefinition.addToSchema(m, curState.name, nameComparator, columnDefMap.get(name), modificationTimestamp);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
            ColumnDefinition.addToSchema(m, curState.name, nameComparator, columnDefMap.get(name), modificationTimestamp);

        return m;
    }

    /**
     * Remove all CF attributes from schema
     *
     * @param timestamp Timestamp to use
     *
     * @return RowMutation to use to completely remove cf from schema
     */
    public RowMutation dropFromSchema(long timestamp)
    {
        RowMutation m = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));

        for (CfDef._Fields field : CfDef._Fields.values())
            m.delete(new QueryPath(SystemTable.SCHEMA_COLUMNFAMILIES_CF, null, compositeNameFor(cfName, field.getFieldName())), timestamp);

        for (ColumnDefinition columnDefinition : column_metadata.values())
            ColumnDefinition.deleteFromSchema(m, cfName, comparator, columnDefinition.name, timestamp);

        return m;
    }

    /**
     * Convert current metadata into schema mutation
     *
     * @param timestamp Timestamp to use
     *
     * @return Low-level representation of the CF
     *
     * @throws ConfigurationException if any of the attributes didn't pass validation
     */
    public RowMutation toSchema(long timestamp) throws ConfigurationException
    {
        RowMutation mutation = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));

        toSchema(mutation, toThrift(), timestamp);

        return mutation;
    }

    /**
     * Convert given Thrift-serialized metadata into schema mutation
     *
     * @param mutation The mutation to include ColumnFamily attributes into (can contain keyspace attributes already)
     * @param cfDef Thrift-serialized metadata to use as source for schema mutation
     * @param timestamp Timestamp to use
     *
     * @throws ConfigurationException if any of the attributes didn't pass validation
     */
    public static void toSchema(RowMutation mutation, CfDef cfDef, long timestamp) throws ConfigurationException
    {
        applyImplicitDefaults(cfDef);

        for (CfDef._Fields field : CfDef._Fields.values())
        {
            if (field.equals(CfDef._Fields.COLUMN_METADATA))
                continue;

            Object value = cfDef.isSet(field) ? cfDef.getFieldValue(field) : null;
            mutation.add(new QueryPath(SystemTable.SCHEMA_COLUMNFAMILIES_CF, null, compositeNameFor(cfDef.name, field.getFieldName())),
                         valueAsBytes(value),
                         timestamp);
        }

        if (!cfDef.isSetColumn_metadata())
            return;

        AbstractType comparator = getColumnDefinitionComparator(cfDef);

        for (ColumnDef columnDef : cfDef.column_metadata)
            ColumnDefinition.addToSchema(mutation, cfDef.name, comparator, columnDef, timestamp);
    }

    public static AbstractType<?> getColumnDefinitionComparator(CfDef cfDef) throws ConfigurationException
    {
        AbstractType<?> cfComparator = TypeParser.parse(cfDef.column_type.equals("Super")
                                     ? cfDef.subcomparator_type
                                     : cfDef.comparator_type);

        if (cfComparator instanceof CompositeType)
        {
            List<AbstractType<?>> types = ((CompositeType)cfComparator).types;
            return types.get(types.size() - 1);
        }
        else
        {
            return cfComparator;
        }
    }

    /**
     * Deserialize CF metadata from low-level representation
     *
     * @param serializedCfDef The data to use for deserialization
     *
     * @return Thrift-based metadata deserialized from schema
     *
     * @throws IOException on any I/O related error
     */
    public static CfDef fromSchema(ColumnFamily serializedCfDef) throws IOException
    {
        CfDef cfDef = fromSchemaNoColumnDefinition(serializedCfDef);

        ColumnFamily serializedColumnDefinitions = ColumnDefinition.readSchema(cfDef.keyspace, cfDef.name);
        return addColumnDefinitionSchema(cfDef, serializedColumnDefinitions);
    }

    // Package protected for use by tests
    static CfDef fromSchemaNoColumnDefinition(ColumnFamily serializedCfDef)
    {
        assert serializedCfDef != null;

        CfDef cfDef = new CfDef();

        AbstractType sysComparator = serializedCfDef.getComparator();

        for (IColumn cfAttr : serializedCfDef.getSortedColumns())
        {
            if (cfAttr == null || cfAttr.isMarkedForDelete())
                continue;

            // column name format is <cf>:<attribute name>
            String[] attr = sysComparator.getString(cfAttr.name()).split(":");
            assert attr.length == 2;

            CfDef._Fields field = CfDef._Fields.findByName(attr[1]);
            cfDef.setFieldValue(field, deserializeValue(cfAttr.value(), getValueClass(CfDef.class, field.getFieldName())));
        }
        return cfDef;
    }

    // Package protected for use by tests
    static CfDef addColumnDefinitionSchema(CfDef cfDef, ColumnFamily serializedColumnDefinitions)
    {
        for (ColumnDef columnDef : ColumnDefinition.fromSchema(serializedColumnDefinitions))
            cfDef.addToColumn_metadata(columnDef);
        return cfDef;
    }

    private void updateCfDef()
    {
        cqlCfDef = new CFDefinition(this);
    }

    public CFDefinition getCfDef()
    {
        assert cqlCfDef != null;
        return cqlCfDef;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("ksName", ksName)
            .append("cfName", cfName)
            .append("cfType", cfType)
            .append("comparator", comparator)
            .append("subcolumncomparator", subcolumnComparator)
            .append("comment", comment)
            .append("readRepairChance", readRepairChance)
            .append("dclocalReadRepairChance", dcLocalReadRepairChance)
            .append("replicateOnWrite", replicateOnWrite)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("mergeShardsChance", mergeShardsChance)
            .append("keyAlias", keyAlias)
            .append("columnAliases", columnAliases)
            .append("valueAlias", keyAlias)
            .append("column_metadata", column_metadata)
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionOptions", compressionParameters.asThriftOptions())
            .append("bloomFilterFpChance", bloomFilterFpChance)
            .append("caching", caching)
            .toString();
    }
}
