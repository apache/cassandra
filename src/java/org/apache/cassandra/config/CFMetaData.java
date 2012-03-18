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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.*;

public final class CFMetaData
{
    //
    // !! Important !!
    // This class can be tricky to modify.  Please read http://wiki.apache.org/cassandra/ConfigurationNotes
    // for how to do so safely.
    //

    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.1;
    public final static double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.0;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = true;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static String DEFAULT_COMPACTION_STRATEGY_CLASS = "SizeTieredCompactionStrategy";
    public final static ByteBuffer DEFAULT_KEY_NAME = ByteBufferUtil.bytes("KEY");

    public static final CFMetaData StatusCf = newSystemMetadata(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null);
    public static final CFMetaData HintsCf = newSystemMetadata(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance);
    @Deprecated
    public static final CFMetaData MigrationsCf = newSystemMetadata(DefsTable.OLD_MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null);
    @Deprecated
    public static final CFMetaData SchemaCf = newSystemMetadata(DefsTable.OLD_SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null);
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

    // new-style schema
    public static final CFMetaData SchemaKeyspacesCf;
    public static final CFMetaData SchemaColumnFamiliesCf;
    public static final CFMetaData SchemaColumnsCf;
    static
    {
        SchemaKeyspacesCf = newSchemaMetadata(SystemTable.SCHEMA_KEYSPACES_CF,
                                              8,
                                              "Keyspace definitions",
                                              AsciiType.instance,
                                              null)
                            .keyValidator(AsciiType.instance)
                            .keyAlias("keyspace");
        SchemaKeyspacesCf.columnMetadata(ColumnDefinition.utf8("name"),
                                         ColumnDefinition.bool("durable_writes"),
                                         ColumnDefinition.ascii("strategy_class"),
                                         ColumnDefinition.ascii("strategy_options"));

        SchemaColumnFamiliesCf = newSchemaMetadata(SystemTable.SCHEMA_COLUMNFAMILIES_CF,
                                                   9,
                                                   "ColumnFamily definitions",
                                                   CompositeType.getInstance(Arrays.<AbstractType<?>>asList(AsciiType.instance, AsciiType.instance)),
                                                   null)
                                 .keyValidator(AsciiType.instance)
                                 .keyAlias("keyspace")
                                 .columnAliases(Arrays.asList(ByteBufferUtil.bytes("columnfamily")))
                                 .columnMetadata(ColumnDefinition.int32("id"),
                                                 ColumnDefinition.ascii("type"),
                                                 ColumnDefinition.ascii("comparator"),
                                                 ColumnDefinition.ascii("subcomparator"),
                                                 ColumnDefinition.utf8("comment"),
                                                 ColumnDefinition.double_("read_repair_chance"),
                                                 ColumnDefinition.double_("local_read_repair_chance"),
                                                 ColumnDefinition.bool("replicate_on_write"),
                                                 ColumnDefinition.int32("gc_grace_seconds"),
                                                 ColumnDefinition.ascii("default_validator"),
                                                 ColumnDefinition.ascii("key_validator"),
                                                 ColumnDefinition.int32("min_compaction_threshold"),
                                                 ColumnDefinition.int32("max_compaction_threshold"),
                                                 ColumnDefinition.ascii("key_alias"),
                                                 ColumnDefinition.double_("bloom_filter_fp_chance"),
                                                 ColumnDefinition.ascii("caching"),
                                                 ColumnDefinition.ascii("compaction_strategy_class"),
                                                 ColumnDefinition.ascii("compression_parameters"),
                                                 ColumnDefinition.utf8("value_alias"),
                                                 ColumnDefinition.utf8("column_aliases"),
                                                 ColumnDefinition.ascii("compaction_strategy_options"));

        SchemaColumnsCf = newSchemaMetadata(SystemTable.SCHEMA_COLUMNS_CF,
                                            10,
                                            "ColumnFamily column attributes",
                                            CompositeType.getInstance(Arrays.<AbstractType<?>>asList(AsciiType.instance,
                                                                                                     AsciiType.instance,
                                                                                                     UTF8Type.instance)),
                                            null)
                          .keyValidator(AsciiType.instance)
                          .keyAlias("keyspace")
                          .columnAliases(Arrays.asList(ByteBufferUtil.bytes("columnfamily"), ByteBufferUtil.bytes("column")))
                          .columnMetadata(ColumnDefinition.ascii("validator"),
                                          ColumnDefinition.ascii("index_type"),
                                          ColumnDefinition.ascii("index_options"),
                                          ColumnDefinition.ascii("index_name"));
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
    private ByteBuffer keyAlias;                      // default NULL
    private List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;                    // default NULL
    private Double bloomFilterFpChance;               // default NULL
    private Caching caching;                          // default KEYS_ONLY (possible: all, key_only, row_only, none)

    Map<ByteBuffer, ColumnDefinition> column_metadata;
    public Class<? extends AbstractCompactionStrategy> compactionStrategyClass;
    public Map<String, String> compactionStrategyOptions;

    public CompressionParameters compressionParameters;

    // Processed infos used by CQL. This can be fully reconstructed from the CFMedata,
    // so it's not saved on disk. It is however costlyish to recreate for each query
    // so we cache it here (and update on each relevant CFMetadata change)
    private CFDefinition cqlCfDef;

    public CFMetaData comment(String prop) { comment = enforceCommentNotNull(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dcLocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData replicateOnWrite(boolean prop) {replicateOnWrite = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType<?> prop) {defaultValidator = prop; updateCfDef(); return this;}
    public CFMetaData keyValidator(AbstractType<?> prop) {keyValidator = prop; updateCfDef(); return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData keyAlias(ByteBuffer prop) {keyAlias = prop; updateCfDef(); return this;}
    public CFMetaData keyAlias(String alias) { return keyAlias(ByteBufferUtil.bytes(alias)); }
    public CFMetaData columnAliases(List<ByteBuffer> prop) {columnAliases = prop; updateCfDef(); return this;}
    public CFMetaData valueAlias(ByteBuffer prop) {valueAlias = prop; updateCfDef(); return this;}
    public CFMetaData columnMetadata(Map<ByteBuffer,ColumnDefinition> prop) {column_metadata = prop; updateCfDef(); return this;}
    private CFMetaData columnMetadata(ColumnDefinition... cds)
    {
        Map<ByteBuffer, ColumnDefinition> map = new HashMap<ByteBuffer, ColumnDefinition>();
        for (ColumnDefinition cd : cds)
            map.put(cd.name, cd);

        return columnMetadata(map);
    }
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(Double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(Caching prop) {caching = prop; return this;}

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc)
    {
        this(keyspace, name, type, comp, subcc, Schema.instance.nextCFId());
    }

    CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc, int id)
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
                      .dcLocalReadRepairChance(0)
                      .gcGraceSeconds(0);
    }

    private static CFMetaData newSchemaMetadata(String cfName, int cfId, String comment, AbstractType<?> comparator, AbstractType<?> subcc)
    {
        /*
         * Schema column families needs a gc_grace (since they are replicated
         * on every node). That gc_grace should be high enough that no node
         * could be dead for that long a time.
         */
        int gcGrace = 120 * 24 * 3600; // 3 months
        return newSystemMetadata(cfName, cfId, comment, comparator, subcc).gcGraceSeconds(gcGrace);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType<?> columnComparator)
    {
        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, null)
                             .keyValidator(info.getValidator())
                             .readRepairChance(0.0)
                             .dcLocalReadRepairChance(0.0)
                             .caching(Caching.NONE)
                             .reloadSecondaryIndexMetadata(parent);
    }

    public CFMetaData reloadSecondaryIndexMetadata(CFMetaData parent)
    {
        gcGraceSeconds(parent.gcGraceSeconds);
        minCompactionThreshold(parent.minCompactionThreshold);
        maxCompactionThreshold(parent.maxCompactionThreshold);
        compactionStrategyClass(parent.compactionStrategyClass);
        compactionStrategyOptions(parent.compactionStrategyOptions);
        compressionParameters(parent.compressionParameters);
        return this;
    }

    public CFMetaData clone()
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, subcolumnComparator, cfId), this);
    }

    // Create a new CFMD by changing just the cfName
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        return copyOpts(new CFMetaData(cfm.ksName, newName, cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.cfId), cfm);
    }

    static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        return newCFMD.comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
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
            newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);

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
            apply(fromSchema(cfDefRow));
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
    public void apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);
        // validate
        if (!cfm.ksName.equals(ksName))
            throw new ConfigurationException(String.format("Keyspace mismatch (found %s; expected %s)",
                                                           cfm.ksName, ksName));
        if (!cfm.cfName.equals(cfName))
            throw new ConfigurationException(String.format("Column family mismatch (found %s; expected %s)",
                                                           cfm.cfName, cfName));
        if (!cfm.cfId.equals(cfId))
            throw new ConfigurationException(String.format("Column family ID mismatch (found %s; expected %s)",
                                                           cfm.cfId, cfId));

        if (!cfm.cfType.equals(cfType))
            throw new ConfigurationException("types do not match.");

        if (!cfm.comparator.isCompatibleWith(comparator))
            throw new ConfigurationException("comparators do not match or are not compatible.");
        if (cfm.subcolumnComparator == null)
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException("subcolumncomparators do not match.");
            // else, it's null and we're good.
        }
        else if (!cfm.subcolumnComparator.isCompatibleWith(subcolumnComparator))
            throw new ConfigurationException("subcolumncomparators do not match or are note compatible.");

        // TODO: this method should probably return a new CFMetaData so that
        // 1) we can keep comparator and subcolumnComparator final
        // 2) updates are applied atomically
        comparator = cfm.comparator;
        subcolumnComparator = cfm.subcolumnComparator;

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        comment = enforceCommentNotNull(cfm.comment);
        readRepairChance = cfm.readRepairChance;
        dcLocalReadRepairChance = cfm.dcLocalReadRepairChance;
        replicateOnWrite = cfm.replicateOnWrite;
        gcGraceSeconds = cfm.gcGraceSeconds;
        defaultValidator = cfm.defaultValidator;
        keyValidator = cfm.keyValidator;
        minCompactionThreshold = cfm.minCompactionThreshold;
        maxCompactionThreshold = cfm.maxCompactionThreshold;
        keyAlias = cfm.keyAlias;
        columnAliases = cfm.columnAliases;
        valueAlias = cfm.valueAlias;
        bloomFilterFpChance = cfm.bloomFilterFpChance;
        caching = cfm.caching;

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, cfm.column_metadata);
        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            column_metadata.remove(cd.name);
        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            column_metadata.put(cd.name, cd);
        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition oldDef = column_metadata.get(name);
            ColumnDefinition def = cfm.column_metadata.get(name);
            oldDef.apply(def, getColumnDefinitionComparator());
        }

        compactionStrategyClass = cfm.compactionStrategyClass;
        compactionStrategyOptions = cfm.compactionStrategyOptions;

        compressionParameters = cfm.compressionParameters();

        updateCfDef();
        logger.debug("application result is {}", this);
    }

    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        return FBUtilities.classForName(className, "compaction strategy");
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor = compactionStrategyClass.getConstructor(new Class[] {
                ColumnFamilyStore.class,
                Map.class // options
            });
            return constructor.newInstance(cfs, compactionStrategyOptions);
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
     * Create schema mutations to update this metadata to provided new state.
     *
     * @param newState The new metadata (for the same CF)
     * @param modificationTimestamp Timestamp to use for mutation
     *
     * @return Difference between attributes in form of schema mutation
     */
    public RowMutation toSchemaUpdate(CFMetaData newState, long modificationTimestamp)
    {
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));

        newState.toSchemaNoColumns(rm, modificationTimestamp);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, newState.column_metadata);

        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(), modificationTimestamp);

        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(), modificationTimestamp);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition cd = newState.getColumnDefinition(name);
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(), modificationTimestamp);
        }

        return rm;
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
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "id"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "type"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "comparator"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "subcomparator"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "comment"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "read_repair_chance"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "local_read_repair_chance"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "replicate_on_write"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "gc_grace_seconds"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "default_validator"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "key_validator"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "min_compaction_threshold"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "max_compaction_threshold"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "key_alias"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "bloom_filter_fp_chance"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "caching"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compaction_strategy_class"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compression_parameters"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "value_alias"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "column_aliases"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compaction_strategy_options"));

        for (ColumnDefinition cd : column_metadata.values())
            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(), timestamp);

        return rm;
    }

    public void toSchema(RowMutation rm, long timestamp)
    {
        toSchemaNoColumns(rm, timestamp);

        for (ColumnDefinition cd : column_metadata.values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(), timestamp);
    }

    private void toSchemaNoColumns(RowMutation rm, long timestamp)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(Column.create(cfId, timestamp, cfName, "id"));
        cf.addColumn(Column.create(cfType.toString(), timestamp, cfName, "type"));
        cf.addColumn(Column.create(comparator.toString(), timestamp, cfName, "comparator"));
        if (subcolumnComparator != null)
            cf.addColumn(Column.create(subcolumnComparator.toString(), timestamp, cfName, "subcomparator"));
        cf.addColumn(comment == null ? DeletedColumn.create(ldt, timestamp, cfName, "comment")
                                     : Column.create(comment, timestamp, cfName, "comment"));
        cf.addColumn(Column.create(readRepairChance, timestamp, cfName, "read_repair_chance"));
        cf.addColumn(Column.create(dcLocalReadRepairChance, timestamp, cfName, "local_read_repair_chance"));
        cf.addColumn(Column.create(replicateOnWrite, timestamp, cfName, "replicate_on_write"));
        cf.addColumn(Column.create(gcGraceSeconds, timestamp, cfName, "gc_grace_seconds"));
        cf.addColumn(Column.create(defaultValidator.toString(), timestamp, cfName, "default_validator"));
        cf.addColumn(Column.create(keyValidator.toString(), timestamp, cfName, "key_validator"));
        cf.addColumn(Column.create(minCompactionThreshold, timestamp, cfName, "min_compaction_threshold"));
        cf.addColumn(Column.create(maxCompactionThreshold, timestamp, cfName, "max_compaction_threshold"));
        cf.addColumn(keyAlias == null ? DeletedColumn.create(ldt, timestamp, cfName, "key_alias")
                                      : Column.create(keyAlias, timestamp, cfName, "key_alias"));
        cf.addColumn(bloomFilterFpChance == null ? DeletedColumn.create(ldt, timestamp, cfName, "bloomFilterFpChance")
                                                 : Column.create(bloomFilterFpChance, timestamp, cfName, "bloom_filter_fp_chance"));
        cf.addColumn(Column.create(caching.toString(), timestamp, cfName, "caching"));
        cf.addColumn(Column.create(compactionStrategyClass.getName(), timestamp, cfName, "compaction_strategy_class"));
        cf.addColumn(Column.create(json(compressionParameters.asThriftOptions()), timestamp, cfName, "compression_parameters"));
        cf.addColumn(valueAlias == null ? DeletedColumn.create(ldt, timestamp, cfName, "value_alias")
                                        : Column.create(valueAlias, timestamp, cfName, "value_alias"));
        cf.addColumn(Column.create(json(columnAliasesAsStrings()), timestamp, cfName, "column_aliases"));
        cf.addColumn(Column.create(json(compactionStrategyOptions), timestamp, cfName, "compaction_strategy_options"));
    }

    // Package protected for use by tests
    static CFMetaData fromSchemaNoColumns(UntypedResultSet.Row result)
    {
        try
        {
            CFMetaData cfm = new CFMetaData(result.getString("keyspace"),
                                            result.getString("columnfamily"),
                                            ColumnFamilyType.valueOf(result.getString("type")),
                                            TypeParser.parse(result.getString("comparator")),
                                            result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null,
                                            result.getInt("id"));
            cfm.readRepairChance(result.getDouble("read_repair_chance"));
            cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
            cfm.replicateOnWrite(result.getBoolean("replicate_on_write"));
            cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
            cfm.defaultValidator(TypeParser.parse(result.getString("default_validator")));
            cfm.keyValidator(TypeParser.parse(result.getString("key_validator")));
            cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
            cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
            if (result.has("comment"))
                cfm.comment(result.getString("comment"));
            if (result.has("key_alias"))
                cfm.keyAlias(result.getBytes("key_alias"));
            if (result.has("bloom_filter_fp_chance"))
                cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
            cfm.caching(Caching.valueOf(result.getString("caching")));
            cfm.compactionStrategyClass(createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
            if (result.has("value_alias"))
                cfm.valueAlias(result.getBytes("value_alias"));
            cfm.columnAliases(columnAliasesFromStrings(fromJsonList(result.getString("column_aliases"))));
            cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));

            return cfm;
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserialize CF metadata from low-level representation
     *
     * @return Thrift-based metadata deserialized from schema
     *
     * @throws IOException on any I/O related error
     */
    public static CFMetaData fromSchema(UntypedResultSet.Row result)
    {
        CFMetaData cfDef = fromSchemaNoColumns(result);

        Row serializedColumnDefinitions = ColumnDefinition.readSchema(cfDef.ksName, cfDef.cfName);
        return addColumnDefinitionSchema(cfDef, serializedColumnDefinitions).updateCfDef();
    }

    private static CFMetaData fromSchema(Row row)
    {
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", row).one();
        return fromSchema(result);
    }

    private List<String> columnAliasesAsStrings()
    {
        List<String> aliases = new ArrayList<String>(columnAliases.size());
        for (ByteBuffer rawAlias : columnAliases)
            aliases.add(UTF8Type.instance.compose(rawAlias));
        return aliases;
    }

    private static List<ByteBuffer> columnAliasesFromStrings(List<String> aliases)
    {
        List<ByteBuffer> rawAliases = new ArrayList<ByteBuffer>(aliases.size());
        for (String alias : aliases)
            rawAliases.add(UTF8Type.instance.decompose(alias));
        return rawAliases;
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
        RowMutation rm = new RowMutation(Table.SYSTEM_TABLE, SystemTable.getSchemaKSKey(ksName));
        toSchema(rm, timestamp);
        return rm;
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

    public AbstractType<?> getColumnDefinitionComparator()
    {
        AbstractType<?> cfComparator = cfType == ColumnFamilyType.Super ? subcolumnComparator : comparator;
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

    // Package protected for use by tests
    static CFMetaData addColumnDefinitionSchema(CFMetaData cfDef, Row serializedColumnDefinitions)
    {
        for (ColumnDefinition cd : ColumnDefinition.fromSchema(serializedColumnDefinitions, cfDef.getColumnDefinitionComparator()))
            cfDef.column_metadata.put(cd.name, cd);
        return cfDef;
    }

    public void addColumnDefinition(ColumnDefinition def)
    {
        column_metadata.put(def.name, def);
    }

    public boolean removeColumnDefinition(ColumnDefinition def)
    {
        return column_metadata.remove(def.name) != null;
    }

    private CFMetaData updateCfDef()
    {
        cqlCfDef = new CFDefinition(this);
        return this;
    }

    public CFDefinition getCfDef()
    {
        assert cqlCfDef != null;
        return cqlCfDef;
    }

    public static boolean isNameValid(String name)
    {
        return name.matches("\\w+");
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
            .append("keyAlias", keyAlias)
            .append("columnAliases", columnAliases)
            .append("valueAlias", valueAlias)
            .append("column_metadata", column_metadata)
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionOptions", compressionParameters.asThriftOptions())
            .append("bloomFilterFpChance", bloomFilterFpChance)
            .append("caching", caching)
            .toString();
    }
}
