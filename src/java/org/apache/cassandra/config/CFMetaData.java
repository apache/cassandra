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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.tracing.Tracing;
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
    public final static Caching DEFAULT_CACHING_STRATEGY = Caching.KEYS_ONLY;
    public final static Double DEFAULT_BF_FP_CHANCE = 0.01;

    // Note that this is the default only for user created tables
    public final static String DEFAULT_COMPRESSOR = SnappyCompressor.isAvailable() ? SnappyCompressor.class.getCanonicalName() : null;

    @Deprecated
    public static final CFMetaData OldStatusCf = newSystemMetadata(Table.SYSTEM_KS, SystemTable.OLD_STATUS_CF, 0, "unused", BytesType.instance, null);
    @Deprecated
    public static final CFMetaData OldHintsCf = newSystemMetadata(Table.SYSTEM_KS, SystemTable.OLD_HINTS_CF, 1, "unused", BytesType.instance, BytesType.instance);
    @Deprecated
    public static final CFMetaData MigrationsCf = newSystemMetadata(Table.SYSTEM_KS, DefsTable.OLD_MIGRATIONS_CF, 2, "unused", TimeUUIDType.instance, null);
    @Deprecated
    public static final CFMetaData SchemaCf = newSystemMetadata(Table.SYSTEM_KS, DefsTable.OLD_SCHEMA_CF, 3, "unused", UTF8Type.instance, null);

    public static final CFMetaData IndexCf = compile(5, "CREATE TABLE \"" + SystemTable.INDEX_CF + "\" ("
                                                        + "table_name text,"
                                                        + "index_name text,"
                                                        + "PRIMARY KEY (table_name, index_name)"
                                                        + ") WITH COMPACT STORAGE AND COMMENT='indexes that have been completed'");

    public static final CFMetaData NodeIdCf = compile(6, "CREATE TABLE \"" + SystemTable.NODE_ID_CF + "\" ("
                                                         + "key text,"
                                                         + "id timeuuid,"
                                                         + "PRIMARY KEY (key, id)"
                                                         + ") WITH COMPACT STORAGE AND COMMENT='counter node IDs'");

    // new-style schema
    public static final CFMetaData SchemaKeyspacesCf = compile(8, "CREATE TABLE " + SystemTable.SCHEMA_KEYSPACES_CF + "("
                                                                  + "keyspace_name text PRIMARY KEY,"
                                                                  + "durable_writes boolean,"
                                                                  + "strategy_class text,"
                                                                  + "strategy_options text"
                                                                  + ") WITH COMPACT STORAGE AND COMMENT='keyspace definitions' AND gc_grace_seconds=8640");
    public static final CFMetaData SchemaColumnFamiliesCf = compile(9, "CREATE TABLE " + SystemTable.SCHEMA_COLUMNFAMILIES_CF + "("
                                                                     + "keyspace_name text,"
                                                                     + "columnfamily_name text,"
                                                                     + "id int,"
                                                                     + "type text,"
                                                                     + "comparator text,"
                                                                     + "subcomparator text,"
                                                                     + "comment text,"
                                                                     + "read_repair_chance double,"
                                                                     + "local_read_repair_chance double,"
                                                                     + "replicate_on_write boolean,"
                                                                     + "gc_grace_seconds int,"
                                                                     + "default_validator text,"
                                                                     + "key_validator text,"
                                                                     + "min_compaction_threshold int,"
                                                                     + "max_compaction_threshold int,"
                                                                     + "key_alias text," // that one is kept for compatibility sake
                                                                     + "key_aliases text,"
                                                                     + "bloom_filter_fp_chance double,"
                                                                     + "caching text,"
                                                                     + "compaction_strategy_class text,"
                                                                     + "compression_parameters text,"
                                                                     + "value_alias text,"
                                                                     + "column_aliases text,"
                                                                     + "compaction_strategy_options text,"
                                                                     + "PRIMARY KEY (keyspace_name, columnfamily_name)"
                                                                     + ") WITH COMMENT='ColumnFamily definitions' AND gc_grace_seconds=8640");
    public static final CFMetaData SchemaColumnsCf = compile(10, "CREATE TABLE " + SystemTable.SCHEMA_COLUMNS_CF + "("
                                                               + "keyspace_name text,"
                                                               + "columnfamily_name text,"
                                                               + "column_name text,"
                                                               + "validator text,"
                                                               + "index_type text,"
                                                               + "index_options text,"
                                                               + "index_name text,"
                                                               + "component_index int,"
                                                               + "PRIMARY KEY(keyspace_name, columnfamily_name, column_name)"
                                                               + ") WITH COMMENT='ColumnFamily column attributes' AND gc_grace_seconds=8640");

    public static final CFMetaData HintsCf = compile(11, "CREATE TABLE " + SystemTable.HINTS_CF + " ("
                                                         + "target_id uuid,"
                                                         + "hint_id timeuuid,"
                                                         + "message_version int,"
                                                         + "mutation blob,"
                                                         + "PRIMARY KEY (target_id, hint_id, message_version)"
                                                         + ") WITH COMPACT STORAGE "
                                                         + "AND COMPACTION_STRATEGY_OPTIONS:MIN_COMPACTION_THRESHOLD=0 "
                                                         + "AND COMPACTION_STRATEGY_OPTIONS:MAX_COMPACTION_THRESHOLD=0 "
                                                         + "AND COMMENT='hints awaiting delivery'");

    public static final CFMetaData PeersCf = compile(12, "CREATE TABLE " + SystemTable.PEERS_CF + " ("
                                                         + "token_bytes blob PRIMARY KEY,"
                                                         + "peer inet"
                                                         + ") WITH COMMENT='known peers in the cluster'");

    public static final CFMetaData LocalCf = compile(13, "CREATE TABLE " + SystemTable.LOCAL_CF + " ("
                                                         + "key text PRIMARY KEY,"
                                                         + "token_bytes blob,"
                                                         + "cluster_name text,"
                                                         + "gossip_generation int,"
                                                         + "bootstrapped text,"
                                                         + "ring_id uuid,"
                                                         + "release_version text,"
                                                         + "thrift_version text,"
                                                         + "cql_version text"
                                                         + ") WITH COMMENT='information about the local node'");

    public static final CFMetaData TraceSessionsCf = compile(14, "CREATE TABLE " + Tracing.SESSIONS_CF + " ("
                                                               + "  session_id uuid PRIMARY KEY,"
                                                               + "  coordinator inet,"
                                                               + "  request text,"
                                                               + "  started_at timestamp,"
                                                               + "  finished_at timestamp,"
                                                               + "  parameters map<text, text>"
                                                               + ") WITH COMMENT='traced sessions'", Tracing.TRACE_KS);

    public static final CFMetaData TraceEventsCf = compile(15, "CREATE TABLE " + Tracing.EVENTS_CF + " ("
                                                               + "  session_id uuid,"
                                                               + "  event_id timeuuid,"
                                                               + "  source inet,"
                                                               + "  thread text,"
                                                               + "  activity text,"
                                                               + "  happened_at timestamp,"
                                                               + "  source_elapsed int,"
                                                               + "  PRIMARY KEY (session_id, event_id)"
                                                               + ");", Tracing.TRACE_KS);

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
    public final UUID cfId;                           // internal id, never exposed to user
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
    private List<ByteBuffer> keyAliases = new ArrayList<ByteBuffer>();
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
    public CFMetaData keyAliases(List<ByteBuffer> prop) {keyAliases = prop; updateCfDef(); return this;}
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
        this(keyspace, name, type, comp, subcc, getId(keyspace, name));
    }

    CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc, UUID id)
    {
        // Final fields must be set in constructor
        ksName = keyspace;
        cfName = name;
        cfType = type;
        comparator = comp;
        subcolumnComparator = enforceSubccDefault(type, subcc);
        cfId = id;

        this.init();
    }

    private static CFMetaData compile(int id, String cql, String keyspace)
    {
        try
        {
            CreateColumnFamilyStatement statement = (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql).prepare().statement;
            CFMetaData cfmd = newSystemMetadata(keyspace, statement.columnFamily(), id, "", statement.comparator, null);
            statement.applyPropertiesTo(cfmd);
            return cfmd;
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static CFMetaData compile(int id, String cql)
    {
        return compile(id, cql, Table.SYSTEM_KS);
    }

    private AbstractType<?> enforceSubccDefault(ColumnFamilyType cftype, AbstractType<?> subcc)
    {
        return (subcc == null) && (cftype == ColumnFamilyType.Super) ? BytesType.instance : subcc;
    }

    private static String enforceCommentNotNull (CharSequence comment)
    {
        return (comment == null) ? "" : comment.toString();
    }

    static UUID getId(String ksName, String cfName)
    {
        return UUID.nameUUIDFromBytes(ArrayUtils.addAll(ksName.getBytes(), cfName.getBytes()));
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
        caching                      = DEFAULT_CACHING_STRATEGY;

        // Defaults strange or simple enough to not need a DEFAULT_T for
        defaultValidator = BytesType.instance;
        keyValidator = BytesType.instance;
        comment = "";
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

    private static CFMetaData newSystemMetadata(String keyspace, String cfName, int oldCfId, String comment, AbstractType<?> comparator, AbstractType<?> subcc)
    {
        ColumnFamilyType type = subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super;
        CFMetaData newCFMD = new CFMetaData(keyspace, cfName, type, comparator,  subcc);

        // adding old -> new style ID mapping to support backward compatibility
        Schema.instance.addOldCfIdMapping(oldCfId, newCFMD.cfId);

        return newCFMD.comment(comment)
                .readRepairChance(0)
                .dcLocalReadRepairChance(0)
                .gcGraceSeconds(0);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType<?> columnComparator)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Here, only key cache is enabled, but later (in KeysIndex) row cache will be turned on depending on cardinality.
        Caching indexCaching = parent.getCaching() == Caching.ALL || parent.getCaching() == Caching.KEYS_ONLY
                             ? Caching.KEYS_ONLY
                             : Caching.NONE;

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, null)
                             .keyValidator(info.getValidator())
                             .readRepairChance(0.0)
                             .dcLocalReadRepairChance(0.0)
                             .gcGraceSeconds(0)
                             .caching(indexCaching)
                             .compactionStrategyClass(parent.compactionStrategyClass)
                             .compactionStrategyOptions(parent.compactionStrategyOptions)
                             .reloadSecondaryIndexMetadata(parent);
    }

    public CFMetaData reloadSecondaryIndexMetadata(CFMetaData parent)
    {
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
        Map<ByteBuffer, ColumnDefinition> clonedColumns = new HashMap<ByteBuffer, ColumnDefinition>();
        for (ColumnDefinition cd : oldCFMD.column_metadata.values())
        {
            ColumnDefinition cloned = cd.clone();
            clonedColumns.put(cloned.name, cloned);
        }
        return newCFMD.comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .replicateOnWrite(oldCFMD.replicateOnWrite)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .keyValidator(oldCFMD.keyValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .keyAliases(new ArrayList<ByteBuffer>(oldCFMD.keyAliases))
                      .columnAliases(new ArrayList<ByteBuffer>(oldCFMD.columnAliases))
                      .valueAlias(oldCFMD.valueAlias)
                      .columnMetadata(clonedColumns)
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

    // Used by CQL2 only.
    public ByteBuffer getKeyName()
    {
        if (keyAliases.size() > 1)
            throw new IllegalStateException("Cannot acces column family with composite key from CQL < 3.0.0");

        return keyAliases.isEmpty() ? DEFAULT_KEY_NAME : keyAliases.get(0);
    }

    public List<ByteBuffer> getKeyAliases()
    {
        return keyAliases;
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
            .append(cfId, rhs.cfId)
            .append(column_metadata, rhs.column_metadata)
            .append(keyAliases, rhs.keyAliases)
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
            .append(keyAliases)
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
        if (cf_def.compaction_strategy == null)
            cf_def.compaction_strategy = DEFAULT_COMPACTION_STRATEGY_CLASS;
        if (cf_def.compaction_strategy_options == null)
            cf_def.compaction_strategy_options = Collections.emptyMap();
        if (!cf_def.isSetCompression_options())
        {
            cf_def.setCompression_options(new HashMap<String, String>()
            {{
                if (DEFAULT_COMPRESSOR != null)
                    put(CompressionParameters.SSTABLE_COMPRESSION, DEFAULT_COMPRESSOR);
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
                                            cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type));

        if (cf_def.isSetGc_grace_seconds()) { newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds); }
        if (cf_def.isSetMin_compaction_threshold()) { newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold); }
        if (cf_def.isSetMax_compaction_threshold()) { newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold); }
        if (cf_def.isSetKey_alias()) { newCFMD.keyAliases(Collections.<ByteBuffer>singletonList(cf_def.key_alias)); }
        if (cf_def.isSetKey_validation_class()) { newCFMD.keyValidator(TypeParser.parse(cf_def.key_validation_class)); }
        if (cf_def.isSetCompaction_strategy())
            newCFMD.compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy);
        if (cf_def.isSetCompaction_strategy_options())
            newCFMD.compactionStrategyOptions(new HashMap<String, String>(cf_def.compaction_strategy_options));
        if (cf_def.isSetBloom_filter_fp_chance())
            newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
        if (cf_def.isSetCaching())
            newCFMD.caching(Caching.fromString(cf_def.caching));
        if (cf_def.isSetRead_repair_chance())
            newCFMD.readRepairChance(cf_def.read_repair_chance);
        if (cf_def.isSetDclocal_read_repair_chance())
            newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);

        CompressionParameters cp = CompressionParameters.create(cf_def.compression_options);

        try
        {
            return newCFMD.comment(cf_def.comment)
                          .replicateOnWrite(cf_def.replicate_on_write)
                          .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                          .keyValidator(TypeParser.parse(cf_def.key_validation_class))
                          .columnMetadata(ColumnDefinition.fromThrift(cf_def.column_metadata))
                          .compressionParameters(cp);
        }
        catch (MarshalException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    public void reload()
    {
        Row cfDefRow = SystemTable.readSchemaRow(ksName, cfName);

        if (cfDefRow.cf == null || cfDefRow.cf.isEmpty())
            throw new RuntimeException(String.format("%s not found in the schema definitions table.", ksName + ":" + cfName));

        try
        {
            apply(fromSchema(cfDefRow));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
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

        /*
         * We don't allow changing the number of aliases (removal would be plain wrong and we've decided to no support addition since it would
         * only make sense in very few cases).
         * However, since thrift doesn't know about aliases (expect for the key aliases, but even then it doesn't support composite ones), we
         * don't want to reject update that don't set the aliases at all.
         */
        if (!cfm.keyAliases.isEmpty())
        {
            if (keyAliases.size() != cfm.keyAliases.size())
                throw new ConfigurationException("Cannot change the number of key aliases");
            keyAliases = cfm.keyAliases;
        }
        if (!cfm.columnAliases.isEmpty())
        {
            if (columnAliases.size() != cfm.columnAliases.size())
                throw new ConfigurationException("Cannot change the number of column aliases");
            columnAliases = cfm.columnAliases;
        }
        if (cfm.valueAlias != null)
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
            oldDef.apply(def, getColumnDefinitionComparator(oldDef));
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
        // We only return the alias if only one is set since thrift don't know about multiple key aliases
        if (keyAliases.size() == 1)
            def.setKey_alias(keyAliases.get(0));
        List<org.apache.cassandra.thrift.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.thrift.ColumnDef>(column_metadata.size());
        for (ColumnDefinition cd : column_metadata.values())
        {
            // Non-null componentIndex are only used by CQL (so far) so we don't expose
            // them through thrift
            if (cd.componentIndex == null)
                column_meta.add(cd.toThrift());
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
     */
    public void addDefaultIndexNames() throws ConfigurationException
    {
        // if this is ColumnFamily update we need to add previously defined index names to the existing columns first
        UUID cfId = Schema.instance.getId(ksName, cfName);
        if (cfId != null)
        {
            CFMetaData cfm = Schema.instance.getCFMetaData(cfId);

            for (Map.Entry<ByteBuffer, ColumnDefinition> entry : column_metadata.entrySet())
            {
                ColumnDefinition newDef = entry.getValue();

                if (!cfm.column_metadata.containsKey(entry.getKey()) || newDef.getIndexType() == null)
                    continue;

                String oldIndexName = cfm.column_metadata.get(entry.getKey()).getIndexName();

                if (oldIndexName == null)
                    continue;

                if (newDef.getIndexName() != null && !oldIndexName.equals(newDef.getIndexName()))
                    throw new ConfigurationException("Can't modify index name: was '" + oldIndexName + "' changed to '" + newDef.getIndexName() + "'.");

                newDef.setIndexName(oldIndexName);
            }
        }

        Set<String> existingNames = existingIndexNames(null);
        for (ColumnDefinition column : column_metadata.values())
        {
            if (column.getIndexType() != null && column.getIndexName() == null)
            {
                String baseName = getDefaultIndexName(cfName, getColumnDefinitionComparator(column), column.name);
                String indexName = baseName;
                int i = 0;
                while (existingNames.contains(indexName))
                    indexName = baseName + '_' + (++i);
                column.setIndexName(indexName);
            }
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

    public OnDiskAtom.Serializer getOnDiskSerializer()
    {
        if (cfType == ColumnFamilyType.Standard)
            return Column.onDiskSerializer();
        return SuperColumn.onDiskSerializer(subcolumnComparator);
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.length() <= Schema.NAME_LENGTH && name.matches("\\w+");
    }

    public static boolean isIndexNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.matches("\\w+");
    }

    public CFMetaData validate() throws ConfigurationException
    {
        if (!isNameValid(ksName))
            throw new ConfigurationException(String.format("Invalid keyspace name: shouldn't be empty nor more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, ksName));
        if (!isNameValid(cfName))
            throw new ConfigurationException(String.format("Invalid keyspace name: shouldn't be empty nor more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, cfName));

        if (cfType == null)
            throw new ConfigurationException(String.format("Invalid column family type for %s", cfName));

        if (cfType == ColumnFamilyType.Super)
        {
            if (subcolumnComparator == null)
                throw new ConfigurationException(String.format("Missing subcolumn comparator for super column family %s", cfName));
        }
        else
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException(String.format("Subcolumn comparator (%s) is invalid for standard column family %s", subcolumnComparator, cfName));
        }


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

        // check if any of the columns has name equal to the cf.key_alias
        for (ColumnDefinition columndef : column_metadata.values())
        {
            for (ByteBuffer alias : keyAliases)
                if (alias.equals(columndef.name))
                    throw new ConfigurationException("Cannot have key alias equals to a column name: " + UTF8Type.instance.compose(alias));

            for (ByteBuffer alias : columnAliases)
                if (alias.equals(columndef.name))
                    throw new ConfigurationException("Cannot have column alias equals to a column name: " + UTF8Type.instance.compose(alias));

            if (valueAlias != null && valueAlias.equals(columndef.name))
                throw new ConfigurationException("Cannot have value alias equals to a column name: " + UTF8Type.instance.compose(valueAlias));
        }

        for (ByteBuffer alias : keyAliases)
            validateAlias(alias, "Key");
        for (ByteBuffer alias : columnAliases)
            validateAlias(alias, "Column");
        validateAlias(valueAlias, "Value");

        // initialize a set of names NOT in the CF under consideration
        Set<String> indexNames = existingIndexNames(cfName);
        for (ColumnDefinition c : column_metadata.values())
        {
            AbstractType<?> comparator = getColumnDefinitionComparator(c);

            try
            {
                comparator.validate(c.name);
            }
            catch (MarshalException e)
            {
                throw new ConfigurationException(String.format("Column name %s is not valid for comparator %s",
                                                               ByteBufferUtil.bytesToHex(c.name), comparator));
            }

            if (c.getIndexType() == null)
            {
                if (c.getIndexName() != null)
                    throw new ConfigurationException("Index name cannot be set without index type");
            }
            else
            {
                if (cfType == ColumnFamilyType.Super)
                    throw new ConfigurationException("Secondary indexes are not supported on super column families");
                if (!isIndexNameValid(c.getIndexName()))
                    throw new ConfigurationException("Illegal index name " + c.getIndexName());
                // check index names against this CF _and_ globally
                if (indexNames.contains(c.getIndexName()))
                    throw new ConfigurationException("Duplicate index name " + c.getIndexName());
                indexNames.add(c.getIndexName());

                if (c.getIndexType() == IndexType.CUSTOM)
                {
                    if (c.getIndexOptions() == null || !c.getIndexOptions().containsKey(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
                        throw new ConfigurationException("Required index option missing: " + SecondaryIndex.CUSTOM_INDEX_OPTION_NAME);
                }

                // This method validates the column metadata but does not intialize the index
                SecondaryIndex.createInstance(null, c);
            }
        }

        validateCompactionThresholds();

        return this;
    }

    private static Set<String> existingIndexNames(String cfToExclude)
    {
        Set<String> indexNames = new HashSet<String>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfToExclude == null || !cfs.getColumnFamilyName().equals(cfToExclude))
                for (ColumnDefinition cd : cfs.metadata.getColumn_metadata().values())
                    indexNames.add(cd.getIndexName());
        }
        return indexNames;
    }

    private static void validateAlias(ByteBuffer alias, String msg) throws ConfigurationException
    {
        if (alias != null)
        {
            try
            {
                UTF8Type.instance.validate(alias);
            }
            catch (MarshalException e)
            {
                throw new ConfigurationException(msg + " alias must be UTF8");
            }
        }
    }

    private void validateCompactionThresholds() throws ConfigurationException
    {
        if (maxCompactionThreshold == 0)
            return;

        if (minCompactionThreshold <= 1)
            throw new ConfigurationException(String.format("Min compaction threshold cannot be less than 2 (got %d).", minCompactionThreshold));

        if (minCompactionThreshold > maxCompactionThreshold)
            throw new ConfigurationException(String.format("Min compaction threshold (got %d) cannot be greater than max compaction threshold (got %d)",
                                                            minCompactionThreshold, maxCompactionThreshold));
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
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, SystemTable.getSchemaKSKey(ksName));

        newState.toSchemaNoColumns(rm, modificationTimestamp);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, newState.column_metadata);

        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);

        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition cd = newState.getColumnDefinition(name);
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);
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
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, SystemTable.getSchemaKSKey(ksName));
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
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "key_aliases"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "bloom_filter_fp_chance"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "caching"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compaction_strategy_class"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compression_parameters"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "value_alias"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "column_aliases"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, "compaction_strategy_options"));

        for (ColumnDefinition cd : column_metadata.values())
            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(cd), timestamp);

        return rm;
    }

    public void toSchema(RowMutation rm, long timestamp)
    {
        toSchemaNoColumns(rm, timestamp);

        for (ColumnDefinition cd : column_metadata.values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), timestamp);
    }

    private void toSchemaNoColumns(RowMutation rm, long timestamp)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Integer oldId = Schema.instance.convertNewCfId(cfId);

        if (oldId != null) // keep old ids (see CASSANDRA-3794 for details)
            cf.addColumn(Column.create(oldId, timestamp, cfName, "id"));

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
        cf.addColumn(Column.create(json(aliasesAsStrings(keyAliases)), timestamp, cfName, "key_aliases"));
        cf.addColumn(bloomFilterFpChance == null ? DeletedColumn.create(ldt, timestamp, cfName, "bloomFilterFpChance")
                                                 : Column.create(bloomFilterFpChance, timestamp, cfName, "bloom_filter_fp_chance"));
        cf.addColumn(Column.create(caching.toString(), timestamp, cfName, "caching"));
        cf.addColumn(Column.create(compactionStrategyClass.getName(), timestamp, cfName, "compaction_strategy_class"));
        cf.addColumn(Column.create(json(compressionParameters.asThriftOptions()), timestamp, cfName, "compression_parameters"));
        cf.addColumn(valueAlias == null ? DeletedColumn.create(ldt, timestamp, cfName, "value_alias")
                                        : Column.create(valueAlias, timestamp, cfName, "value_alias"));
        cf.addColumn(Column.create(json(aliasesAsStrings(columnAliases)), timestamp, cfName, "column_aliases"));
        cf.addColumn(Column.create(json(compactionStrategyOptions), timestamp, cfName, "compaction_strategy_options"));
    }

    // Package protected for use by tests
    static CFMetaData fromSchemaNoColumns(UntypedResultSet.Row result)
    {
        try
        {
            CFMetaData cfm = new CFMetaData(result.getString("keyspace_name"),
                                            result.getString("columnfamily_name"),
                                            ColumnFamilyType.valueOf(result.getString("type")),
                                            TypeParser.parse(result.getString("comparator")),
                                            result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null);

            if (result.has("id"))// try to identify if ColumnFamily Id is old style (before C* 1.2) and add old -> new mapping if so
                Schema.instance.addOldCfIdMapping(result.getInt("id"), cfm.cfId);

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
            // We need support the old key_alias for compatibility sake
            if (result.has("key_aliases"))
            {
                cfm.keyAliases(aliasesFromStrings(fromJsonList(result.getString("key_aliases"))));
            }
            else if (result.has("key_alias"))
            {
                cfm.keyAliases(Collections.<ByteBuffer>singletonList(result.getBytes("key_alias")));
            }
            if (result.has("bloom_filter_fp_chance"))
                cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
            cfm.caching(Caching.valueOf(result.getString("caching")));
            cfm.compactionStrategyClass(createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
            cfm.columnAliases(aliasesFromStrings(fromJsonList(result.getString("column_aliases"))));
            if (result.has("value_alias"))
                cfm.valueAlias(result.getBytes("value_alias"));
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

    private List<String> aliasesAsStrings(List<ByteBuffer> rawAliases)
    {
        List<String> aliases = new ArrayList<String>(rawAliases.size());
        for (ByteBuffer rawAlias : rawAliases)
            aliases.add(UTF8Type.instance.compose(rawAlias));
        return aliases;
    }

    private static List<ByteBuffer> aliasesFromStrings(List<String> aliases)
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
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, SystemTable.getSchemaKSKey(ksName));
        toSchema(rm, timestamp);
        return rm;
    }

    public AbstractType<?> getColumnDefinitionComparator(ColumnDefinition def)
    {
        return getColumnDefinitionComparator(def.componentIndex);
    }

    public AbstractType<?> getColumnDefinitionComparator(Integer componentIndex)
    {
        AbstractType<?> cfComparator = cfType == ColumnFamilyType.Super ? subcolumnComparator : comparator;
        if (cfComparator instanceof CompositeType)
        {
            if (componentIndex == null)
                return cfComparator;

            List<AbstractType<?>> types = ((CompositeType)cfComparator).types;
            AbstractType<?> t = types.get(componentIndex);
            assert t != null : "Non-sensical component index";
            return t;
        }
        else
        {
            return cfComparator;
        }
    }

    // Package protected for use by tests
    static CFMetaData addColumnDefinitionSchema(CFMetaData cfDef, Row serializedColumnDefinitions)
    {
        for (ColumnDefinition cd : ColumnDefinition.fromSchema(serializedColumnDefinitions, cfDef))
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
            .append("keyAliases", keyAliases)
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
