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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.cache.IRowCacheProvider;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.cache.ConcurrentLinkedHashCacheProvider;
import org.apache.cassandra.cache.SerializingCacheProvider;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CFMetaData
{
    //
    // !! Important !!
    // This class can be tricky to modify.  Please read http://wiki.apache.org/cassandra/ConfigurationNotes
    // for how to do so safely.
    //

    private static Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.1;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = true;
    public final static int DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS = 0;
    public final static int DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS = 4 * 3600;
    public final static int DEFAULT_ROW_CACHE_KEYS_TO_SAVE = Integer.MAX_VALUE;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static double DEFAULT_MERGE_SHARDS_CHANCE = 0.1;
    public final static IRowCacheProvider DEFAULT_ROW_CACHE_PROVIDER = initDefaultRowCacheProvider();
    public final static String DEFAULT_COMPACTION_STRATEGY_CLASS = "SizeTieredCompactionStrategy";
    public final static ByteBuffer DEFAULT_KEY_NAME = ByteBufferUtil.bytes("KEY");

    public static final CFMetaData StatusCf = newSystemMetadata(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null);
    public static final CFMetaData HintsCf = newSystemMetadata(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance);
    public static final CFMetaData MigrationsCf = newSystemMetadata(Migration.MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null);
    public static final CFMetaData SchemaCf = newSystemMetadata(Migration.SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null);
    public static final CFMetaData IndexCf = newSystemMetadata(SystemTable.INDEX_CF, 5, "indexes that have been completed", UTF8Type.instance, null);
    public static final CFMetaData NodeIdCf = newSystemMetadata(SystemTable.NODE_ID_CF, 6, "nodeId and their metadata", TimeUUIDType.instance, null);
    public static final CFMetaData VersionCf = newSystemMetadata(SystemTable.VERSION_CF, 7, "server version information", UTF8Type.instance, null);
    static
    {
        try
        {
            VersionCf.keyAlias(ByteBufferUtil.bytes("component"))
                     .keyValidator(UTF8Type.instance)
                     .columnMetadata(Collections.singletonMap(ByteBufferUtil.bytes("version"),
                                                              new ColumnDefinition(ByteBufferUtil.bytes("version"),
                                                                                   UTF8Type.instance,
                                                                                   null,
                                                                                   null,
                                                                                   null)));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static IRowCacheProvider initDefaultRowCacheProvider()
    {
        try
        {
            return new SerializingCacheProvider();
        }
        catch (ConfigurationException e)
        {
            return new ConcurrentLinkedHashCacheProvider();
        }
    }

    //REQUIRED
    public final Integer cfId;                        // internal id, never exposed to user
    public final String ksName;                       // name of keyspace
    public final String cfName;                       // name of this column family
    public final ColumnFamilyType cfType;             // standard, super
    public final AbstractType comparator;             // bytes, long, timeuuid, utf8, etc.
    public final AbstractType subcolumnComparator;    // like comparator, for supercolumns

    //OPTIONAL
    private String comment;                           // default none, for humans only
    private double rowCacheSize;                      // default 0
    private double keyCacheSize;                      // default 0.01
    private double readRepairChance;                  // default 1.0 (always), chance [0.0,1.0] of read repair
    private boolean replicateOnWrite;                 // default false
    private int gcGraceSeconds;                       // default 864000 (ten days)
    private AbstractType defaultValidator;            // default BytesType (no-op), use comparator types
    private AbstractType keyValidator;                // default BytesType (no-op), use comparator types
    private int minCompactionThreshold;               // default 4
    private int maxCompactionThreshold;               // default 32
    private int rowCacheSavePeriodInSeconds;          // default 0 (off)
    private int keyCacheSavePeriodInSeconds;          // default 3600 (1 hour)
    private int rowCacheKeysToSave;                   // default max int (aka feature is off)
    // mergeShardsChance is now obsolete, but left here so as to not break
    // thrift compatibility
    private double mergeShardsChance;                 // default 0.1, chance [0.0, 1.0] of merging old shards during replication
    private IRowCacheProvider rowCacheProvider;
    private ByteBuffer keyAlias;                      // default NULL

    private Map<ByteBuffer, ColumnDefinition> column_metadata;
    public Class<? extends AbstractCompactionStrategy> compactionStrategyClass;
    public Map<String, String> compactionStrategyOptions;

    private CompressionParameters compressionParameters;

    public CFMetaData comment(String prop) { comment = enforceCommentNotNull(prop); return this;}
    public CFMetaData rowCacheSize(double prop) {rowCacheSize = prop; return this;}
    public CFMetaData keyCacheSize(double prop) {keyCacheSize = prop; return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData replicateOnWrite(boolean prop) {replicateOnWrite = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType prop) {defaultValidator = prop; return this;}
    public CFMetaData keyValidator(AbstractType prop) {keyValidator = prop; return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData rowCacheSavePeriod(int prop) {rowCacheSavePeriodInSeconds = prop; return this;}
    public CFMetaData keyCacheSavePeriod(int prop) {keyCacheSavePeriodInSeconds = prop; return this;}
    public CFMetaData rowCacheKeysToSave(int prop) {rowCacheKeysToSave = prop; return this;}
    public CFMetaData mergeShardsChance(double prop) {mergeShardsChance = prop; return this;}
    public CFMetaData keyAlias(ByteBuffer prop) {keyAlias = prop; return this;}
    public CFMetaData columnMetadata(Map<ByteBuffer,ColumnDefinition> prop) {column_metadata = prop; return this;}
    public CFMetaData rowCacheProvider(IRowCacheProvider prop) { rowCacheProvider = prop; return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType comp, AbstractType subcc)
    {
        this(keyspace, name, type, comp, subcc, Schema.instance.nextCFId());
    }

    private CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType comp, AbstractType subcc, int id)
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

        this.init();
    }

    private AbstractType enforceSubccDefault(ColumnFamilyType cftype, AbstractType subcc)
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
        rowCacheSize                 = DEFAULT_ROW_CACHE_SIZE;
        keyCacheSize                 = DEFAULT_KEY_CACHE_SIZE;
        rowCacheKeysToSave           = DEFAULT_ROW_CACHE_KEYS_TO_SAVE;
        readRepairChance             = DEFAULT_READ_REPAIR_CHANCE;
        replicateOnWrite             = DEFAULT_REPLICATE_ON_WRITE;
        gcGraceSeconds               = DEFAULT_GC_GRACE_SECONDS;
        minCompactionThreshold       = DEFAULT_MIN_COMPACTION_THRESHOLD;
        maxCompactionThreshold       = DEFAULT_MAX_COMPACTION_THRESHOLD;
        mergeShardsChance            = DEFAULT_MERGE_SHARDS_CHANCE;
        rowCacheProvider             = DEFAULT_ROW_CACHE_PROVIDER;

        // Defaults strange or simple enough to not need a DEFAULT_T for
        defaultValidator = BytesType.instance;
        keyValidator = BytesType.instance;
        comment = "";
        keyAlias = null; // This qualifies as a 'strange default'.
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
    }

    private static CFMetaData newSystemMetadata(String cfName, int cfId, String comment, AbstractType comparator, AbstractType subcc)
    {
        ColumnFamilyType type = subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super;
        CFMetaData newCFMD = new CFMetaData(Table.SYSTEM_TABLE, cfName, type, comparator,  subcc, cfId);

        return newCFMD.comment(comment)
                      .keyCacheSize(0.01)
                      .readRepairChance(0)
                      .gcGraceSeconds(0)
                      .mergeShardsChance(0.0)
                      .rowCacheSavePeriod(0)
                      .keyCacheSavePeriod(0);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType columnComparator)
    {
        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, null)
                             .keyValidator(info.getValidator())
                             .keyCacheSize(0.0)
                             .readRepairChance(0.0)
                             .gcGraceSeconds(parent.gcGraceSeconds)
                             .minCompactionThreshold(parent.minCompactionThreshold)
                             .maxCompactionThreshold(parent.maxCompactionThreshold);
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
                      .rowCacheSize(oldCFMD.rowCacheSize)
                      .keyCacheSize(oldCFMD.keyCacheSize)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .replicateOnWrite(oldCFMD.replicateOnWrite)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .rowCacheSavePeriod(oldCFMD.rowCacheSavePeriodInSeconds)
                      .keyCacheSavePeriod(oldCFMD.keyCacheSavePeriodInSeconds)
                      .rowCacheKeysToSave(oldCFMD.rowCacheKeysToSave)
                      .columnMetadata(oldCFMD.column_metadata)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(oldCFMD.compactionStrategyOptions)
                      .compressionParameters(oldCFMD.compressionParameters);
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
        return cfName + "." + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name) : info.getIndexName());
    }

    // converts CFM to avro CfDef
    public org.apache.cassandra.db.migration.avro.CfDef toAvro()
    {
        org.apache.cassandra.db.migration.avro.CfDef cf = new org.apache.cassandra.db.migration.avro.CfDef();
        cf.id = cfId;
        cf.keyspace = new Utf8(ksName);
        cf.name = new Utf8(cfName);
        cf.column_type = new Utf8(cfType.name());
        cf.comparator_type = new Utf8(comparator.toString());
        if (subcolumnComparator != null)
        {
            assert cfType == ColumnFamilyType.Super
                   : String.format("%s CF %s should not have subcomparator %s defined", cfType, cfName, subcolumnComparator);
            cf.subcomparator_type = new Utf8(subcolumnComparator.toString());
        }
        cf.comment = new Utf8(enforceCommentNotNull(comment));
        cf.row_cache_size = rowCacheSize;
        cf.key_cache_size = keyCacheSize;
        cf.read_repair_chance = readRepairChance;
        cf.replicate_on_write = replicateOnWrite;
        cf.gc_grace_seconds = gcGraceSeconds;
        cf.default_validation_class = defaultValidator == null ? null : new Utf8(defaultValidator.toString());
        cf.key_validation_class = new Utf8(keyValidator.toString());
        cf.min_compaction_threshold = minCompactionThreshold;
        cf.max_compaction_threshold = maxCompactionThreshold;
        cf.row_cache_save_period_in_seconds = rowCacheSavePeriodInSeconds;
        cf.key_cache_save_period_in_seconds = keyCacheSavePeriodInSeconds;
        cf.row_cache_keys_to_save = rowCacheKeysToSave;
        cf.merge_shards_chance = mergeShardsChance;
        cf.key_alias = keyAlias;
        cf.column_metadata = new ArrayList<ColumnDef>(column_metadata.size());
        for (ColumnDefinition cd : column_metadata.values())
            cf.column_metadata.add(cd.toAvro());
        cf.row_cache_provider = new Utf8(rowCacheProvider.getClass().getName());
        cf.compaction_strategy = new Utf8(compactionStrategyClass.getName());
        if (compactionStrategyOptions != null)
        {
            cf.compaction_strategy_options = new HashMap<CharSequence, CharSequence>();
            for (Map.Entry<String, String> e : compactionStrategyOptions.entrySet())
                cf.compaction_strategy_options.put(new Utf8(e.getKey()), new Utf8(e.getValue()));
        }
        cf.compression_options = compressionParameters.asAvroOptions();
        return cf;
    }

    public static CFMetaData fromAvro(org.apache.cassandra.db.migration.avro.CfDef cf)
    {
        AbstractType comparator;
        AbstractType subcolumnComparator = null;
        AbstractType validator;
        AbstractType keyValidator;

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
        for (ColumnDef aColumn_metadata : cf.column_metadata)
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
        if (cf.row_cache_save_period_in_seconds != null) { newCFMD.rowCacheSavePeriod(cf.row_cache_save_period_in_seconds); }
        if (cf.key_cache_save_period_in_seconds != null) { newCFMD.keyCacheSavePeriod(cf.key_cache_save_period_in_seconds); }
        if (cf.row_cache_keys_to_save != null) { newCFMD.rowCacheKeysToSave(cf.row_cache_keys_to_save); }
        if (cf.merge_shards_chance != null) { newCFMD.mergeShardsChance(cf.merge_shards_chance); }
        if (cf.row_cache_provider != null)
        {
            try
            {
                newCFMD.rowCacheProvider(FBUtilities.newCacheProvider(cf.row_cache_provider.toString()));
            }
            catch (ConfigurationException e)
            {
                // default was already set upon newCFMD init
                logger.warn("Unable to instantiate cache provider {}; using default {} instead",
                            cf.row_cache_provider,
                            DEFAULT_ROW_CACHE_PROVIDER);
            }
        }
        if (cf.key_alias != null) { newCFMD.keyAlias(cf.key_alias); }
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

        return newCFMD.comment(cf.comment.toString())
                      .rowCacheSize(cf.row_cache_size)
                      .keyCacheSize(cf.key_cache_size)
                      .readRepairChance(cf.read_repair_chance)
                      .replicateOnWrite(cf.replicate_on_write)
                      .gcGraceSeconds(cf.gc_grace_seconds)
                      .defaultValidator(validator)
                      .keyValidator(keyValidator)
                      .columnMetadata(column_metadata)
                      .compressionParameters(cp);
    }
    
    public String getComment()
    {
        return comment;
    }
    
    public double getRowCacheSize()
    {
        return rowCacheSize;
    }
    
    public double getKeyCacheSize()
    {
        return keyCacheSize;
    }
    
    public double getReadRepairChance()
    {
        return readRepairChance;
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

    public AbstractType getDefaultValidator()
    {
        return defaultValidator;
    }

    public AbstractType getKeyValidator()
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

    public int getRowCacheSavePeriodInSeconds()
    {
        return rowCacheSavePeriodInSeconds;
    }

    public int getKeyCacheSavePeriodInSeconds()
    {
        return keyCacheSavePeriodInSeconds;
    }

    public int getRowCacheKeysToSave()
    {
        return rowCacheKeysToSave;
    }

    public IRowCacheProvider getRowCacheProvider()
    {
        return rowCacheProvider;
    }

    public ByteBuffer getKeyName()
    {
        return keyAlias == null ? DEFAULT_KEY_NAME : keyAlias;
    }

    public CompressionParameters compressionParameters()
    {
        return compressionParameters;
    }

    public Map<ByteBuffer, ColumnDefinition> getColumn_metadata()
    {
        return Collections.unmodifiableMap(column_metadata);
    }

    public AbstractType getComparatorFor(ByteBuffer superColumnName)
    {
        return superColumnName == null ? comparator : subcolumnComparator;
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
            .append(rowCacheSize, rhs.rowCacheSize)
            .append(keyCacheSize, rhs.keyCacheSize)
            .append(readRepairChance, rhs.readRepairChance)
            .append(replicateOnWrite, rhs.replicateOnWrite)
            .append(gcGraceSeconds, rhs.gcGraceSeconds)
            .append(defaultValidator, rhs.defaultValidator)
            .append(keyValidator, rhs.keyValidator)
            .append(minCompactionThreshold, rhs.minCompactionThreshold)
            .append(maxCompactionThreshold, rhs.maxCompactionThreshold)
            .append(cfId.intValue(), rhs.cfId.intValue())
            .append(column_metadata, rhs.column_metadata)
            .append(rowCacheSavePeriodInSeconds, rhs.rowCacheSavePeriodInSeconds)
            .append(keyCacheSavePeriodInSeconds, rhs.keyCacheSavePeriodInSeconds)
            .append(rowCacheKeysToSave, rhs.rowCacheKeysToSave)
            .append(mergeShardsChance, rhs.mergeShardsChance)
            .append(keyAlias, rhs.keyAlias)
            .append(compactionStrategyClass, rhs.compactionStrategyClass)
            .append(compactionStrategyOptions, rhs.compactionStrategyOptions)
            .append(compressionParameters, rhs.compressionParameters)
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
            .append(rowCacheSize)
            .append(keyCacheSize)
            .append(readRepairChance)
            .append(replicateOnWrite)
            .append(gcGraceSeconds)
            .append(defaultValidator)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(cfId)
            .append(column_metadata)
            .append(rowCacheSavePeriodInSeconds)
            .append(keyCacheSavePeriodInSeconds)
            .append(rowCacheKeysToSave)
            .append(mergeShardsChance)
            .append(keyAlias)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .toHashCode();
    }

    public AbstractType getValueValidator(ByteBuffer column)
    {
        return getValueValidator(column_metadata.get(column));
    }

    public AbstractType getValueValidator(ColumnDefinition columnDefinition)
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
        if (!cf_def.isSetRow_cache_save_period_in_seconds())
            cf_def.setRow_cache_save_period_in_seconds(CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS);
        if (!cf_def.isSetKey_cache_save_period_in_seconds())
            cf_def.setKey_cache_save_period_in_seconds(CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS);
        if (!cf_def.isSetRow_cache_keys_to_save())
            cf_def.setRow_cache_keys_to_save(CFMetaData.DEFAULT_ROW_CACHE_KEYS_TO_SAVE);
        if (!cf_def.isSetMerge_shards_chance())
            cf_def.setMerge_shards_chance(CFMetaData.DEFAULT_MERGE_SHARDS_CHANCE);
        if (null == cf_def.compaction_strategy)
            cf_def.compaction_strategy = DEFAULT_COMPACTION_STRATEGY_CLASS;
        if (null == cf_def.compaction_strategy_options)
            cf_def.compaction_strategy_options = Collections.emptyMap();
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
        if (cf_def.isSetRow_cache_save_period_in_seconds()) { newCFMD.rowCacheSavePeriod(cf_def.row_cache_save_period_in_seconds); }
        if (cf_def.isSetKey_cache_save_period_in_seconds()) { newCFMD.keyCacheSavePeriod(cf_def.key_cache_save_period_in_seconds); }
        if (cf_def.isSetRow_cache_keys_to_save()) { newCFMD.rowCacheKeysToSave(cf_def.row_cache_keys_to_save); }
        if (cf_def.isSetMerge_shards_chance()) { newCFMD.mergeShardsChance(cf_def.merge_shards_chance); }
        if (cf_def.isSetRow_cache_provider()) { newCFMD.rowCacheProvider(FBUtilities.newCacheProvider(cf_def.row_cache_provider)); }
        if (cf_def.isSetKey_alias()) { newCFMD.keyAlias(cf_def.key_alias); }
        if (cf_def.isSetKey_validation_class()) { newCFMD.keyValidator(TypeParser.parse(cf_def.key_validation_class)); }
        if (cf_def.isSetCompaction_strategy())
            newCFMD.compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy);
        if (cf_def.isSetCompaction_strategy_options())
            newCFMD.compactionStrategyOptions(new HashMap<String, String>(cf_def.compaction_strategy_options));

        CompressionParameters cp = CompressionParameters.create(cf_def.compression_options);

        return newCFMD.comment(cf_def.comment)
                      .rowCacheSize(cf_def.row_cache_size)
                      .keyCacheSize(cf_def.key_cache_size)
                      .readRepairChance(cf_def.read_repair_chance)
                      .replicateOnWrite(cf_def.replicate_on_write)
                      .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                      .keyValidator(TypeParser.parse(cf_def.key_validation_class))
                      .columnMetadata(ColumnDefinition.fromThrift(cf_def.column_metadata))
                      .compressionParameters(cp)
                      .validate();
    }

    /** updates CFMetaData in-place to match cf_def */
    public void apply(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cf_def, this);
        // validate
        if (!cf_def.keyspace.toString().equals(ksName))
            throw new ConfigurationException(String.format("Keyspace mismatch (found %s; expected %s)",
                                                           cf_def.keyspace, ksName));
        if (!cf_def.name.toString().equals(cfName))
            throw new ConfigurationException(String.format("Column family mismatch (found %s; expected %s)",
                                                           cf_def.name, cfName));
        if (!cf_def.id.equals(cfId))
            throw new ConfigurationException(String.format("Column family ID mismatch (found %s; expected %s)",
                                                           cf_def.id, cfId));

        if (!cf_def.column_type.toString().equals(cfType.name()))
            throw new ConfigurationException("types do not match.");
        if (comparator != TypeParser.parse(cf_def.comparator_type))
            throw new ConfigurationException("comparators do not match.");
        if (cf_def.subcomparator_type == null || cf_def.subcomparator_type.equals(""))
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException("subcolumncomparators do not match.");
            // else, it's null and we're good.
        }
        else if (subcolumnComparator != TypeParser.parse(cf_def.subcomparator_type))
            throw new ConfigurationException("subcolumncomparators do not match.");

        validateMinMaxCompactionThresholds(cf_def);

        comment = enforceCommentNotNull(cf_def.comment);
        rowCacheSize = cf_def.row_cache_size;
        keyCacheSize = cf_def.key_cache_size;
        readRepairChance = cf_def.read_repair_chance;
        replicateOnWrite = cf_def.replicate_on_write;
        gcGraceSeconds = cf_def.gc_grace_seconds;
        defaultValidator = TypeParser.parse(cf_def.default_validation_class);
        keyValidator = TypeParser.parse(cf_def.key_validation_class);
        minCompactionThreshold = cf_def.min_compaction_threshold;
        maxCompactionThreshold = cf_def.max_compaction_threshold;
        rowCacheSavePeriodInSeconds = cf_def.row_cache_save_period_in_seconds;
        keyCacheSavePeriodInSeconds = cf_def.key_cache_save_period_in_seconds;
        rowCacheKeysToSave = cf_def.row_cache_keys_to_save;
        mergeShardsChance = cf_def.merge_shards_chance;
        if (cf_def.row_cache_provider != null)
            rowCacheProvider = FBUtilities.newCacheProvider(cf_def.row_cache_provider.toString());
        keyAlias = cf_def.key_alias;

        // adjust column definitions. figure out who is coming and going.
        Set<ByteBuffer> toRemove = new HashSet<ByteBuffer>();
        Set<ByteBuffer> newColumns = new HashSet<ByteBuffer>();
        Set<org.apache.cassandra.db.migration.avro.ColumnDef> toAdd = new HashSet<org.apache.cassandra.db.migration.avro.ColumnDef>();
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : cf_def.column_metadata)
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
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : cf_def.column_metadata)
        {
            ColumnDefinition oldDef = column_metadata.get(def.name);
            if (oldDef == null)
                continue;
            oldDef.setValidator(TypeParser.parse(def.validation_class));
            oldDef.setIndexType(def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.name()),
                                ColumnDefinition.getStringMap(def.index_options));
            oldDef.setIndexName(def.index_name == null ? null : def.index_name.toString());
        }
        // add the new ones coming in.
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : toAdd)
        {
            AbstractType dValidClass = TypeParser.parse(def.validation_class);
            ColumnDefinition cd = new ColumnDefinition(def.name, 
                                                       dValidClass,
                                                       def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.toString()), 
                                                       ColumnDefinition.getStringMap(def.index_options),
                                                       def.index_name == null ? null : def.index_name.toString());
            column_metadata.put(cd.name, cd);
        }

        if (cf_def.compaction_strategy != null)
            compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy.toString());

        if (null != cf_def.compaction_strategy_options)
        {
            compactionStrategyOptions = new HashMap<String, String>();
            for (Map.Entry<CharSequence, CharSequence> e : cf_def.compaction_strategy_options.entrySet())
                compactionStrategyOptions.put(e.getKey().toString(), e.getValue().toString());
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
            Constructor constructor = compactionStrategyClass.getConstructor(new Class[] {
                ColumnFamilyStore.class,
                Map.class // options
            });
            return (AbstractCompactionStrategy)constructor.newInstance(new Object[] {
                cfs,
                compactionStrategyOptions});
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
        def.setRow_cache_size(rowCacheSize);
        def.setKey_cache_size(keyCacheSize);
        def.setRead_repair_chance(readRepairChance);
        def.setReplicate_on_write(replicateOnWrite);
        def.setGc_grace_seconds(gcGraceSeconds);
        def.setDefault_validation_class(defaultValidator.toString());
        def.setKey_validation_class(keyValidator.toString());
        def.setMin_compaction_threshold(minCompactionThreshold);
        def.setMax_compaction_threshold(maxCompactionThreshold);
        def.setRow_cache_save_period_in_seconds(rowCacheSavePeriodInSeconds);
        def.setKey_cache_save_period_in_seconds(keyCacheSavePeriodInSeconds);
        def.setRow_cache_keys_to_save(rowCacheKeysToSave);
        def.setRow_cache_provider(rowCacheProvider.getClass().getName());
        def.setMerge_shards_chance(mergeShardsChance);
        def.setKey_alias(getKeyName());
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
        return def;
    }

    public static void validateMinMaxCompactionThresholds(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.min_compaction_threshold != null && cf_def.max_compaction_threshold != null)
        {
            if ((cf_def.min_compaction_threshold > cf_def.max_compaction_threshold) &&
                    cf_def.max_compaction_threshold != 0)
            {
                throw new ConfigurationException("min_compaction_threshold cannot be greater than max_compaction_threshold");
            }
        }
        else if (cf_def.min_compaction_threshold != null)
        {
            if (cf_def.min_compaction_threshold > DEFAULT_MAX_COMPACTION_THRESHOLD)
            {
                throw new ConfigurationException("min_compaction_threshold cannot be greather than max_compaction_threshold (default " +
                                                  DEFAULT_MAX_COMPACTION_THRESHOLD + ")");
            }
        }
        else if (cf_def.max_compaction_threshold != null)
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

        AbstractType comparator;
        try
        {
            comparator = TypeParser.parse(cf_def.comparator_type);
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        for (org.apache.cassandra.thrift.ColumnDef column : cf_def.column_metadata)
        {
            if (column.index_type != null && column.index_name == null)
                column.index_name = getDefaultIndexName(cf_def.name, comparator, column.name);
        }
    }

    public static String getDefaultIndexName(String cfName, AbstractType comparator, ByteBuffer columnName)
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
            .append("rowCacheSize", rowCacheSize)
            .append("keyCacheSize", keyCacheSize)
            .append("readRepairChance", readRepairChance)
            .append("replicateOnWrite", replicateOnWrite)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("rowCacheSavePeriodInSeconds", rowCacheSavePeriodInSeconds)
            .append("keyCacheSavePeriodInSeconds", keyCacheSavePeriodInSeconds)
            .append("rowCacheKeysToSave", rowCacheKeysToSave)
            .append("rowCacheProvider", rowCacheProvider)
            .append("mergeShardsChance", mergeShardsChance)
            .append("keyAlias", keyAlias)
            .append("column_metadata", column_metadata)
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionOptions", compressionParameters.asThriftOptions())
            .toString();
    }
}
