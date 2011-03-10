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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;


public final class CFMetaData
{

    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = false;
    public final static int DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB = 8;
    public final static int DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS = 0;
    public final static int DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS = 4 * 3600;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static int DEFAULT_MEMTABLE_LIFETIME_IN_MINS = 60 * 24;
    public final static int DEFAULT_MEMTABLE_THROUGHPUT_IN_MB = sizeMemtableThroughput();
    public final static double DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS = sizeMemtableOperations(DEFAULT_MEMTABLE_THROUGHPUT_IN_MB);
    public final static double DEFAULT_MERGE_SHARDS_CHANCE = 0.1;

    private static final int MIN_CF_ID = 1000;

    private static final AtomicInteger idGen = new AtomicInteger(MIN_CF_ID);
    
    private static final BiMap<Pair<String, String>, Integer> cfIdMap = HashBiMap.create();
    
    public static final CFMetaData StatusCf = newSystemTable(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData HintsCf = newSystemTable(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance, Math.min(256, Math.max(32, DEFAULT_MEMTABLE_THROUGHPUT_IN_MB / 2)));
    public static final CFMetaData MigrationsCf = newSystemTable(Migration.MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData SchemaCf = newSystemTable(Migration.SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData IndexCf = newSystemTable(SystemTable.INDEX_CF, 5, "indexes that have been completed", UTF8Type.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData NodeIdCf = newSystemTable(SystemTable.NODE_ID_CF, 6, "nodeId and their metadata", TimeUUIDType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);

    private static CFMetaData newSystemTable(String cfName, int cfId, String comment, AbstractType comparator, AbstractType subComparator, int memtableThroughPutInMB)
    {
        return new CFMetaData(Table.SYSTEM_TABLE,
                              cfName,
                              subComparator == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super,
                              comparator,
                              subComparator,
                              comment,
                              0,
                              0.01,
                              0,
                              false,
                              0,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_MEMTABLE_LIFETIME_IN_MINS,
                              memtableThroughPutInMB,
                              sizeMemtableOperations(memtableThroughPutInMB),
                              0,
                              cfId,
                              Collections.<ByteBuffer, ColumnDefinition>emptyMap());
    }

    /**
     * @return A calculated memtable throughput size for this machine.
     */
    public static int sizeMemtableThroughput()
    {
        return (int) (Runtime.getRuntime().maxMemory() / (1048576 * 16));
    }

    /**
     * @return A calculated memtable operation count for this machine.
     */
    public static double sizeMemtableOperations(int mem_throughput)
    {
        return 0.3 * mem_throughput / 64.0;
    }

    /**
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public static Pair<String,String> getCF(Integer cfId)
    {
        return cfIdMap.inverse().get(cfId);
    }
    
    /**
     * @return The id for the given (ksname,cfname) pair, or null if it has been dropped.
     */
    public static Integer getId(String table, String cfName)
    {
        return cfIdMap.get(new Pair<String, String>(table, cfName));
    }
    
    // this gets called after initialization to make sure that id generation happens properly.
    public static void fixMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        idGen.set(cfIdMap.size() == 0 ? MIN_CF_ID : Math.max(Collections.max(cfIdMap.values()) + 1, MIN_CF_ID));
    }

    //REQUIRED
    public final Integer cfId;                        // internal id, never exposed to user
    public final String tableName;                    // name of keyspace
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
    private AbstractType defaultValidator;            // default none, use comparator types
    private Integer minCompactionThreshold;           // default 4
    private Integer maxCompactionThreshold;           // default 32
    private int rowCacheSavePeriodInSeconds;          // default 0 (off)
    private int keyCacheSavePeriodInSeconds;          // default 3600 (1 hour)
    private int memtableFlushAfterMins;               // default 60 
    private int memtableThroughputInMb;               // default based on heap size
    private double memtableOperationsInMillions;      // default based on throughput
    private double mergeShardsChance;                 // default 0.1, chance [0.0, 1.0] of merging old shards during replication
    // NOTE: if you find yourself adding members to this class, make sure you keep the convert methods in lockstep.

    private final Map<ByteBuffer, ColumnDefinition> column_metadata;

    private CFMetaData(String tableName,
                       String cfName,
                       ColumnFamilyType cfType,
                       AbstractType comparator,
                       AbstractType subcolumnComparator,
                       String comment,
                       double rowCacheSize,
                       double keyCacheSize,
                       double readRepairChance,
                       boolean replicateOnWrite,
                       int gcGraceSeconds,
                       AbstractType defaultValidator,
                       int minCompactionThreshold,
                       int maxCompactionThreshold,
                       int rowCacheSavePeriodInSeconds,
                       int keyCacheSavePeriodInSeconds,
                       int memtableFlushAfterMins,
                       Integer memtableThroughputInMb,
                       Double memtableOperationsInMillions,
                       double mergeShardsChance,
                       Integer cfId,
                       Map<ByteBuffer, ColumnDefinition> column_metadata)

    {
        assert column_metadata != null;
        this.tableName = tableName;
        this.cfName = cfName;
        this.cfType = cfType;
        this.comparator = comparator;
        // the default subcolumncomparator is null per thrift spec, but only should be null if cfType == Standard. If
        // cfType == Super, subcolumnComparator should default to BytesType if not set.
        this.subcolumnComparator = subcolumnComparator == null && cfType == ColumnFamilyType.Super
                                   ? BytesType.instance
                                   : subcolumnComparator;
        this.comment = comment == null ? "" : comment;
        this.rowCacheSize = rowCacheSize;
        this.keyCacheSize = keyCacheSize;
        this.readRepairChance = readRepairChance;
        this.replicateOnWrite = replicateOnWrite;
        this.gcGraceSeconds = gcGraceSeconds;
        this.defaultValidator = defaultValidator;
        this.minCompactionThreshold = minCompactionThreshold;
        this.maxCompactionThreshold = maxCompactionThreshold;
        this.rowCacheSavePeriodInSeconds = rowCacheSavePeriodInSeconds;
        this.keyCacheSavePeriodInSeconds = keyCacheSavePeriodInSeconds;
        this.memtableFlushAfterMins = memtableFlushAfterMins;
        this.memtableThroughputInMb = memtableThroughputInMb == null
                                      ? DEFAULT_MEMTABLE_THROUGHPUT_IN_MB
                                      : memtableThroughputInMb;

        this.memtableOperationsInMillions = memtableOperationsInMillions == null
                                            ? DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS
                                            : memtableOperationsInMillions;
        this.mergeShardsChance = mergeShardsChance;
        this.cfId = cfId;
        this.column_metadata = new HashMap<ByteBuffer, ColumnDefinition>(column_metadata);
    }
    
    /** adds this cfm to the map. */
    public static void map(CFMetaData cfm) throws ConfigurationException
    {
        Pair<String, String> key = new Pair<String, String>(cfm.tableName, cfm.cfName);
        if (cfIdMap.containsKey(key))
            throw new ConfigurationException("Attempt to assign id to existing column family.");
        else
        {
            cfIdMap.put(key, cfm.cfId);
        }
    }

    public CFMetaData(String tableName,
                      String cfName,
                      ColumnFamilyType cfType,
                      AbstractType comparator,
                      AbstractType subcolumnComparator,
                      String comment,
                      double rowCacheSize,
                      double keyCacheSize,
                      double readRepairChance,
                      boolean replicateOnWrite,
                      int gcGraceSeconds,
                      AbstractType defaultValidator,
                      int minCompactionThreshold,
                      int maxCompactionThreshold,
                      int rowCacheSavePeriodInSeconds,
                      int keyCacheSavePeriodInSeconds,
                      int memTime,
                      Integer memSize,
                      Double memOps,
                      double mergeShardsChance,
                      //This constructor generates the id!
                      Map<ByteBuffer, ColumnDefinition> column_metadata)
    {
        this(tableName,
             cfName,
             cfType,
             comparator,
             subcolumnComparator,
             comment,
             rowCacheSize,
             keyCacheSize,
             readRepairChance,
             replicateOnWrite,
             gcGraceSeconds,
             defaultValidator,
             minCompactionThreshold,
             maxCompactionThreshold,
             rowCacheSavePeriodInSeconds,
             keyCacheSavePeriodInSeconds,
             memTime,
             memSize,
             memOps,
             mergeShardsChance,
             nextId(),
             column_metadata);
    }
    
    public static CFMetaData newIndexMetadata(String table, String parentCf, ColumnDefinition info, AbstractType columnComparator)
    {
        return new CFMetaData(table,
                              indexName(parentCf, info),
                              ColumnFamilyType.Standard,
                              columnComparator,
                              null,
                              "",
                              0,
                              0,
                              0,
                              false,
                              DEFAULT_GC_GRACE_SECONDS,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_MEMTABLE_LIFETIME_IN_MINS,
                              DEFAULT_MEMTABLE_THROUGHPUT_IN_MB,
                              DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS,
                              0,
                              Collections.<ByteBuffer, ColumnDefinition>emptyMap());
    }

    /** clones an existing CFMetaData using the same id. */
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        return new CFMetaData(cfm.tableName,
                              newName,
                              cfm.cfType,
                              cfm.comparator,
                              cfm.subcolumnComparator,
                              cfm.comment,
                              cfm.rowCacheSize,
                              cfm.keyCacheSize,
                              cfm.readRepairChance,
                              cfm.replicateOnWrite,
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.rowCacheSavePeriodInSeconds,
                              cfm.keyCacheSavePeriodInSeconds,
                              cfm.memtableFlushAfterMins,
                              cfm.memtableThroughputInMb,
                              cfm.memtableOperationsInMillions,
                              cfm.mergeShardsChance,
                              cfm.cfId,
                              cfm.column_metadata);
    }
    
    /** clones existing CFMetaData. keeps the id but changes the table name.*/
    public static CFMetaData renameTable(CFMetaData cfm, String tableName)
    {
        return new CFMetaData(tableName,
                              cfm.cfName,
                              cfm.cfType,
                              cfm.comparator,
                              cfm.subcolumnComparator,
                              cfm.comment,
                              cfm.rowCacheSize,
                              cfm.keyCacheSize,
                              cfm.readRepairChance,
                              cfm.replicateOnWrite,
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.rowCacheSavePeriodInSeconds,
                              cfm.keyCacheSavePeriodInSeconds,
                              cfm.memtableFlushAfterMins,
                              cfm.memtableThroughputInMb,
                              cfm.memtableOperationsInMillions,
                              cfm.mergeShardsChance,
                              cfm.cfId,
                              cfm.column_metadata);
    }
    
    /** used for evicting cf data out of static tracking collections. */
    public static void purge(CFMetaData cfm)
    {
        cfIdMap.remove(new Pair<String, String>(cfm.tableName, cfm.cfName));
    }
    
    /** convention for nameing secondary indexes. */
    public static String indexName(String parentCf, ColumnDefinition info)
    {
        return parentCf + "." + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name) : info.getIndexName());
    }

    public org.apache.cassandra.db.migration.avro.CfDef deflate()
    {
        org.apache.cassandra.db.migration.avro.CfDef cf = new org.apache.cassandra.db.migration.avro.CfDef();
        cf.id = cfId;
        cf.keyspace = new Utf8(tableName);
        cf.name = new Utf8(cfName);
        cf.column_type = new Utf8(cfType.name());
        cf.comparator_type = new Utf8(comparator.getClass().getName());
        if (subcolumnComparator != null)
            cf.subcomparator_type = new Utf8(subcolumnComparator.getClass().getName());
        cf.comment = new Utf8(comment);
        cf.row_cache_size = rowCacheSize;
        cf.key_cache_size = keyCacheSize;
        cf.read_repair_chance = readRepairChance;
        cf.replicate_on_write = replicateOnWrite;
        cf.gc_grace_seconds = gcGraceSeconds;
        cf.default_validation_class = new Utf8(defaultValidator.getClass().getName());
        cf.min_compaction_threshold = minCompactionThreshold;
        cf.max_compaction_threshold = maxCompactionThreshold;
        cf.row_cache_save_period_in_seconds = rowCacheSavePeriodInSeconds;
        cf.key_cache_save_period_in_seconds = keyCacheSavePeriodInSeconds;
        cf.memtable_flush_after_mins = memtableFlushAfterMins;
        cf.memtable_throughput_in_mb = memtableThroughputInMb;
        cf.memtable_operations_in_millions = memtableOperationsInMillions;
        cf.merge_shards_chance = mergeShardsChance;
        cf.column_metadata = SerDeUtils.createArray(column_metadata.size(),
                                                    org.apache.cassandra.db.migration.avro.ColumnDef.SCHEMA$);
        for (ColumnDefinition cd : column_metadata.values())
            cf.column_metadata.add(cd.deflate());
        return cf;
    }

    public static CFMetaData inflate(org.apache.cassandra.db.migration.avro.CfDef cf)
    {
        AbstractType comparator;
        AbstractType subcolumnComparator = null;
        AbstractType validator;
        try
        {
            comparator = DatabaseDescriptor.getComparator(cf.comparator_type.toString());
            if (cf.subcomparator_type != null)
                subcolumnComparator = DatabaseDescriptor.getComparator(cf.subcomparator_type.toString());
            validator = cf.default_validation_class == null
                        ? BytesType.instance
                        : DatabaseDescriptor.getComparator(cf.default_validation_class.toString());
        }
        catch (Exception ex)
        {
            throw new RuntimeException("Could not inflate CFMetaData for " + cf, ex);
        }
        Map<ByteBuffer, ColumnDefinition> column_metadata = new TreeMap<ByteBuffer, ColumnDefinition>(BytesType.instance);
        for (ColumnDef aColumn_metadata : cf.column_metadata)
        {
            ColumnDefinition cd = ColumnDefinition.inflate(aColumn_metadata);
            column_metadata.put(cd.name, cd);
        }

        //isn't AVRO supposed to handle stuff like this?
        Integer minct = cf.min_compaction_threshold == null ? DEFAULT_MIN_COMPACTION_THRESHOLD : cf.min_compaction_threshold;
        Integer maxct = cf.max_compaction_threshold == null ? DEFAULT_MAX_COMPACTION_THRESHOLD : cf.max_compaction_threshold;
        Integer row_cache_save_period_in_seconds = cf.row_cache_save_period_in_seconds == null ? DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS : cf.row_cache_save_period_in_seconds;
        Integer key_cache_save_period_in_seconds = cf.key_cache_save_period_in_seconds == null ? DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS : cf.key_cache_save_period_in_seconds;
        Integer memtable_flush_after_mins = cf.memtable_flush_after_mins == null ? DEFAULT_MEMTABLE_LIFETIME_IN_MINS : cf.memtable_flush_after_mins;
        Integer memtable_throughput_in_mb = cf.memtable_throughput_in_mb == null ? DEFAULT_MEMTABLE_THROUGHPUT_IN_MB : cf.memtable_throughput_in_mb;
        Double memtable_operations_in_millions = cf.memtable_operations_in_millions == null ? DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS : cf.memtable_operations_in_millions;
        double merge_shards_chance = cf.merge_shards_chance == null ? DEFAULT_MERGE_SHARDS_CHANCE : cf.merge_shards_chance;

        return new CFMetaData(cf.keyspace.toString(),
                              cf.name.toString(),
                              ColumnFamilyType.create(cf.column_type.toString()),
                              comparator,
                              subcolumnComparator,
                              cf.comment.toString(),
                              cf.row_cache_size,
                              cf.key_cache_size,
                              cf.read_repair_chance,
                              cf.replicate_on_write,
                              cf.gc_grace_seconds,
                              validator,
                              minct,
                              maxct,
                              row_cache_save_period_in_seconds,
                              key_cache_save_period_in_seconds,
                              memtable_flush_after_mins,
                              memtable_throughput_in_mb,
                              memtable_operations_in_millions,
                              merge_shards_chance,
                              cf.id,
                              column_metadata);
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
        return readRepairChance;
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

    public int getMemtableFlushAfterMins()
    {
        return memtableFlushAfterMins;
    }

    public int getMemtableThroughputInMb()
    {
        return memtableThroughputInMb;
    }

    public double getMemtableOperationsInMillions()
    {
        return memtableOperationsInMillions;
    }

    public Map<ByteBuffer, ColumnDefinition> getColumn_metadata()
    {
        return Collections.unmodifiableMap(column_metadata);
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
            .append(tableName, rhs.tableName)
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
            .append(minCompactionThreshold, rhs.minCompactionThreshold)
            .append(maxCompactionThreshold, rhs.maxCompactionThreshold)
            .append(cfId.intValue(), rhs.cfId.intValue())
            .append(column_metadata, rhs.column_metadata)
            .append(rowCacheSavePeriodInSeconds, rhs.rowCacheSavePeriodInSeconds)
            .append(keyCacheSavePeriodInSeconds, rhs.keyCacheSavePeriodInSeconds)
            .append(memtableFlushAfterMins, rhs.memtableFlushAfterMins)
            .append(memtableThroughputInMb, rhs.memtableThroughputInMb)
            .append(memtableOperationsInMillions, rhs.memtableOperationsInMillions)
            .append(mergeShardsChance, rhs.mergeShardsChance)
            .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(tableName)
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
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(cfId)
            .append(column_metadata)
            .append(rowCacheSavePeriodInSeconds)
            .append(keyCacheSavePeriodInSeconds)
            .append(memtableFlushAfterMins)
            .append(memtableThroughputInMb)
            .append(memtableOperationsInMillions)
            .append(mergeShardsChance)
            .toHashCode();
    }

    private static int nextId()
    {
        return idGen.getAndIncrement();
    }

    public AbstractType getValueValidator(ByteBuffer column)
    {
        AbstractType validator = defaultValidator;
        ColumnDefinition columnDefinition = column_metadata.get(column);
        if (columnDefinition != null)
            validator = columnDefinition.validator;
        return validator;
    }
    
    /** applies implicit defaults to cf definition. useful in updates */
    public static void applyImplicitDefaults(org.apache.cassandra.db.migration.avro.CfDef cf_def)
    {
        if (cf_def.min_compaction_threshold == null)
            cf_def.min_compaction_threshold = CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD;
        if (cf_def.max_compaction_threshold == null)
            cf_def.max_compaction_threshold = CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD;
        if (cf_def.row_cache_save_period_in_seconds == null)
            cf_def.row_cache_save_period_in_seconds = CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS;
        if (cf_def.key_cache_save_period_in_seconds == null)
            cf_def.key_cache_save_period_in_seconds = CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS;
        if (cf_def.memtable_flush_after_mins == null)
            cf_def.memtable_flush_after_mins = CFMetaData.DEFAULT_MEMTABLE_LIFETIME_IN_MINS;
        if (cf_def.memtable_throughput_in_mb == null)
            cf_def.memtable_throughput_in_mb = CFMetaData.DEFAULT_MEMTABLE_THROUGHPUT_IN_MB;
        if (cf_def.memtable_operations_in_millions == null)
            cf_def.memtable_operations_in_millions = CFMetaData.DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS; 
        if (cf_def.merge_shards_chance == null)
            cf_def.merge_shards_chance = CFMetaData.DEFAULT_MERGE_SHARDS_CHANCE;
    }
    
    /** applies implicit defaults to cf definition. useful in updates */
    public static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def) 
    {
        if (!cf_def.isSetMin_compaction_threshold())
            cf_def.setMin_compaction_threshold(CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD);
        if (!cf_def.isSetMax_compaction_threshold())
            cf_def.setMax_compaction_threshold(CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD);
        if (!cf_def.isSetRow_cache_save_period_in_seconds())
            cf_def.setRow_cache_save_period_in_seconds(CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS);
        if (!cf_def.isSetKey_cache_save_period_in_seconds())
            cf_def.setKey_cache_save_period_in_seconds(CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS);
        if (!cf_def.isSetMemtable_flush_after_mins())
            cf_def.setMemtable_flush_after_mins(CFMetaData.DEFAULT_MEMTABLE_LIFETIME_IN_MINS);
        if (!cf_def.isSetMemtable_throughput_in_mb())
            cf_def.setMemtable_throughput_in_mb(CFMetaData.DEFAULT_MEMTABLE_THROUGHPUT_IN_MB);
        if (!cf_def.isSetMemtable_operations_in_millions())
            cf_def.setMemtable_operations_in_millions(CFMetaData.DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS);
        if (!cf_def.isSetMerge_shards_chance())
            cf_def.setMerge_shards_chance(CFMetaData.DEFAULT_MERGE_SHARDS_CHANCE);
    }
    
    // merges some final fields from this CFM with modifiable fields from CfDef into a new CFMetaData.
    public void apply(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
        // validate
        if (!cf_def.id.equals(cfId))
            throw new ConfigurationException("ids do not match.");
        if (!cf_def.keyspace.toString().equals(tableName))
            throw new ConfigurationException("keyspaces do not match.");
        if (!cf_def.name.toString().equals(cfName))
            throw new ConfigurationException("names do not match.");
        if (!cf_def.column_type.toString().equals(cfType.name()))
            throw new ConfigurationException("types do not match.");
        if (comparator != DatabaseDescriptor.getComparator(cf_def.comparator_type))
            throw new ConfigurationException("comparators do not match.");
        if (cf_def.subcomparator_type == null || cf_def.subcomparator_type.equals(""))
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException("subcolumncomparators do not match.");
            // else, it's null and we're good.
        }
        else if (subcolumnComparator != DatabaseDescriptor.getComparator(cf_def.subcomparator_type))
            throw new ConfigurationException("subcolumncomparators do not match.");

        validateMinMaxCompactionThresholds(cf_def);
        validateMemtableSettings(cf_def);

        comment = cf_def.comment == null ? "" : cf_def.comment.toString();
        rowCacheSize = cf_def.row_cache_size;
        keyCacheSize = cf_def.key_cache_size;
        readRepairChance = cf_def.read_repair_chance;
        replicateOnWrite = cf_def.replicate_on_write;
        gcGraceSeconds = cf_def.gc_grace_seconds;
        defaultValidator = DatabaseDescriptor.getComparator(cf_def.default_validation_class);
        minCompactionThreshold = cf_def.min_compaction_threshold;
        maxCompactionThreshold = cf_def.max_compaction_threshold;
        rowCacheSavePeriodInSeconds = cf_def.row_cache_save_period_in_seconds;
        keyCacheSavePeriodInSeconds = cf_def.key_cache_save_period_in_seconds;
        memtableFlushAfterMins = cf_def.memtable_flush_after_mins;
        memtableThroughputInMb = cf_def.memtable_throughput_in_mb;
        memtableOperationsInMillions = cf_def.memtable_operations_in_millions;
        mergeShardsChance = cf_def.merge_shards_chance;
        
        // adjust secondary indexes. figure out who is coming and going.
        Set<ByteBuffer> toRemove = new HashSet<ByteBuffer>();
        Set<ByteBuffer> newIndexNames = new HashSet<ByteBuffer>();
        Set<org.apache.cassandra.db.migration.avro.ColumnDef> toAdd = new HashSet<org.apache.cassandra.db.migration.avro.ColumnDef>();
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : cf_def.column_metadata)
        {
            newIndexNames.add(def.name);
            if (!column_metadata.containsKey(def.name))
                toAdd.add(def);
        }
        for (ByteBuffer indexName : column_metadata.keySet())
            if (!newIndexNames.contains(indexName))
                toRemove.add(indexName);
        
        // remove the ones leaving.
        for (ByteBuffer indexName : toRemove)
            column_metadata.remove(indexName);
        // update the ones staying
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : cf_def.column_metadata)
        {
            if (!column_metadata.containsKey(def.name))
                continue;
            column_metadata.get(def.name).setIndexType(def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.name()));
            column_metadata.get(def.name).setIndexName(def.index_name == null ? null : def.index_name.toString());
        }
        // add the new ones coming in.
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : toAdd)
        {
            ColumnDefinition cd = new ColumnDefinition(def.name, 
                                                       def.validation_class.toString(), 
                                                       def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.toString()), 
                                                       def.index_name == null ? null : def.index_name.toString());
            column_metadata.put(cd.name, cd);
        }
    }
    
    // converts CFM to thrift CfDef
    public static org.apache.cassandra.thrift.CfDef convertToThrift(CFMetaData cfm)
    {
        org.apache.cassandra.thrift.CfDef def = new org.apache.cassandra.thrift.CfDef(cfm.tableName, cfm.cfName);
        def.setId(cfm.cfId);
        def.setColumn_type(cfm.cfType.name());
        def.setComparator_type(cfm.comparator.getClass().getName());
        if (cfm.subcolumnComparator != null)
        {
            def.setSubcomparator_type(cfm.subcolumnComparator.getClass().getName());
            def.setColumn_type("Super");
        }
        def.setComment(cfm.comment == null ? "" : cfm.comment);
        def.setRow_cache_size(cfm.rowCacheSize);
        def.setKey_cache_size(cfm.keyCacheSize);
        def.setRead_repair_chance(cfm.readRepairChance);
        def.setReplicate_on_write(cfm.replicateOnWrite);
        def.setGc_grace_seconds(cfm.gcGraceSeconds);
        def.setDefault_validation_class(cfm.defaultValidator.getClass().getName());
        def.setMin_compaction_threshold(cfm.minCompactionThreshold);
        def.setMax_compaction_threshold(cfm.maxCompactionThreshold);
        def.setRow_cache_save_period_in_seconds(cfm.rowCacheSavePeriodInSeconds);
        def.setKey_cache_save_period_in_seconds(cfm.keyCacheSavePeriodInSeconds);
        def.setMemtable_flush_after_mins(cfm.memtableFlushAfterMins);
        def.setMemtable_throughput_in_mb(cfm.memtableThroughputInMb);
        def.setMemtable_operations_in_millions(cfm.memtableOperationsInMillions);
        def.setMerge_shards_chance(cfm.mergeShardsChance);
        List<org.apache.cassandra.thrift.ColumnDef> column_meta = new ArrayList< org.apache.cassandra.thrift.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.thrift.ColumnDef tcd = new org.apache.cassandra.thrift.ColumnDef();
            tcd.setIndex_name(cd.getIndexName());
            tcd.setIndex_type(cd.getIndexType());
            tcd.setName(cd.name);
            tcd.setValidation_class(cd.validator.getClass().getName());
            column_meta.add(tcd);
        }
        def.setColumn_metadata(column_meta);
        return def;
    }
    
    // converts CFM to avro CfDef
    public static org.apache.cassandra.db.migration.avro.CfDef convertToAvro(CFMetaData cfm)
    {
        org.apache.cassandra.db.migration.avro.CfDef def = new org.apache.cassandra.db.migration.avro.CfDef();
        def.name = cfm.cfName;
        def.keyspace = cfm.tableName;
        def.id = cfm.cfId;
        def.column_type = cfm.cfType.name();
        def.comparator_type = cfm.comparator.getClass().getName();
        if (cfm.subcolumnComparator != null)
        {
            def.subcomparator_type = cfm.subcolumnComparator.getClass().getName();
            def.column_type = "Super";
        }
        def.comment = cfm.comment == null ? "" : cfm.comment;
        def.row_cache_size = cfm.rowCacheSize;
        def.key_cache_size = cfm.keyCacheSize;
        def.read_repair_chance = cfm.readRepairChance;
        def.replicate_on_write = cfm.replicateOnWrite;
        def.gc_grace_seconds = cfm.gcGraceSeconds;
        def.default_validation_class = cfm.defaultValidator == null ? null : cfm.defaultValidator.getClass().getName();
        def.min_compaction_threshold = cfm.minCompactionThreshold;
        def.max_compaction_threshold = cfm.maxCompactionThreshold;
        def.row_cache_save_period_in_seconds = cfm.rowCacheSavePeriodInSeconds;
        def.key_cache_save_period_in_seconds = cfm.keyCacheSavePeriodInSeconds;
        def.memtable_flush_after_mins = cfm.memtableFlushAfterMins;
        def.memtable_throughput_in_mb = cfm.memtableThroughputInMb;
        def.memtable_operations_in_millions = cfm.memtableOperationsInMillions;
        def.merge_shards_chance = cfm.mergeShardsChance;
        List<org.apache.cassandra.db.migration.avro.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.db.migration.avro.ColumnDef tcd = new org.apache.cassandra.db.migration.avro.ColumnDef();
            tcd.index_name = cd.getIndexName();
            tcd.index_type = cd.getIndexType() == null ? null : org.apache.cassandra.db.migration.avro.IndexType.valueOf(cd.getIndexType().name());
            tcd.name = ByteBufferUtil.clone(cd.name);
            tcd.validation_class = cd.validator.getClass().getName();
            column_meta.add(tcd);
        }
        def.column_metadata = column_meta;   
        return def;
    }
    
    public static org.apache.cassandra.db.migration.avro.CfDef convertToAvro(org.apache.cassandra.thrift.CfDef def)
    {
        org.apache.cassandra.db.migration.avro.CfDef newDef = new org.apache.cassandra.db.migration.avro.CfDef();
        newDef.keyspace = def.getKeyspace();
        newDef.name = def.getName();
        newDef.column_type = def.getColumn_type();
        newDef.comment = def.getComment();
        newDef.comparator_type = def.getComparator_type();
        newDef.default_validation_class = def.getDefault_validation_class();
        newDef.gc_grace_seconds = def.getGc_grace_seconds();
        newDef.id = def.getId();
        newDef.key_cache_save_period_in_seconds = def.getKey_cache_save_period_in_seconds();
        newDef.key_cache_size = def.getKey_cache_size();
        newDef.max_compaction_threshold = def.getMax_compaction_threshold();
        newDef.memtable_flush_after_mins = def.getMemtable_flush_after_mins();
        newDef.memtable_operations_in_millions = def.getMemtable_operations_in_millions();
        newDef.memtable_throughput_in_mb = def.getMemtable_throughput_in_mb();
        newDef.min_compaction_threshold = def.getMin_compaction_threshold();
        newDef.read_repair_chance = def.getRead_repair_chance();
        newDef.replicate_on_write = def.isReplicate_on_write();
        newDef.row_cache_save_period_in_seconds = def.getRow_cache_save_period_in_seconds();
        newDef.row_cache_size = def.getRow_cache_size();
        newDef.subcomparator_type = def.getSubcomparator_type();
        newDef.merge_shards_chance = def.getMerge_shards_chance();

        List<org.apache.cassandra.db.migration.avro.ColumnDef> columnMeta = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>();
        if (def.isSetColumn_metadata())
        {
            for (org.apache.cassandra.thrift.ColumnDef cdef : def.getColumn_metadata())
            {
                org.apache.cassandra.db.migration.avro.ColumnDef tdef = new org.apache.cassandra.db.migration.avro.ColumnDef();
                tdef.name = ByteBufferUtil.clone(cdef.BufferForName());
                tdef.validation_class = cdef.getValidation_class();
                tdef.index_name = cdef.getIndex_name();
                tdef.index_type = cdef.getIndex_type() == null ? null : org.apache.cassandra.db.migration.avro.IndexType.valueOf(cdef.getIndex_type().name());
                columnMeta.add(tdef);
            }
        }
        newDef.column_metadata = columnMeta;
        return newDef;
    }

    public static void validateMinMaxCompactionThresholds(org.apache.cassandra.thrift.CfDef cf_def) throws ConfigurationException
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
                throw new ConfigurationException("min_compaction_threshold cannot be greather than max_compaction_threshold (default " +
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

    public static void validateMemtableSettings(org.apache.cassandra.thrift.CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.isSetMemtable_flush_after_mins())
            DatabaseDescriptor.validateMemtableFlushPeriod(cf_def.memtable_flush_after_mins);
        if (cf_def.isSetMemtable_throughput_in_mb())
            DatabaseDescriptor.validateMemtableThroughput(cf_def.memtable_throughput_in_mb);
        if (cf_def.isSetMemtable_operations_in_millions())
            DatabaseDescriptor.validateMemtableOperations(cf_def.memtable_operations_in_millions);
    }

    public static void validateMemtableSettings(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.memtable_flush_after_mins != null)
            DatabaseDescriptor.validateMemtableFlushPeriod(cf_def.memtable_flush_after_mins);
        if (cf_def.memtable_throughput_in_mb != null)
            DatabaseDescriptor.validateMemtableThroughput(cf_def.memtable_throughput_in_mb);
        if (cf_def.memtable_operations_in_millions != null)
            DatabaseDescriptor.validateMemtableOperations(cf_def.memtable_operations_in_millions);
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("tableName", tableName)
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
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("rowCacheSavePeriodInSeconds", rowCacheSavePeriodInSeconds)
            .append("keyCacheSavePeriodInSeconds", keyCacheSavePeriodInSeconds)
            .append("memtableFlushAfterMins", memtableFlushAfterMins)
            .append("memtableThroughputInMb", memtableThroughputInMb)
            .append("memtableOperationsInMillions", memtableOperationsInMillions)
            .append("mergeShardsChance", mergeShardsChance)
            .append("column_metadata", column_metadata)
            .toString();
    }
}
