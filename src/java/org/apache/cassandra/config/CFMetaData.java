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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.avro.ColumnDef;
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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;


public final class CFMetaData
{
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static int DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS = 0;
    public final static int DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS = 3600;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static int DEFAULT_MEMTABLE_LIFETIME_IN_MINS = 60;
    public final static int DEFAULT_MEMTABLE_THROUGHPUT_IN_MB = sizeMemtableThroughput();
    public final static double DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS = sizeMemtableOperations(DEFAULT_MEMTABLE_THROUGHPUT_IN_MB);

    private static final int MIN_CF_ID = 1000;

    private static final AtomicInteger idGen = new AtomicInteger(MIN_CF_ID);
    
    private static final BiMap<Pair<String, String>, Integer> cfIdMap = HashBiMap.create();
    
    public static final CFMetaData StatusCf = newSystemTable(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null);
    public static final CFMetaData HintsCf = newSystemTable(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance);
    public static final CFMetaData MigrationsCf = newSystemTable(Migration.MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null);
    public static final CFMetaData SchemaCf = newSystemTable(Migration.SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null);
    public static final CFMetaData IndexCf = newSystemTable(SystemTable.INDEX_CF, 5, "indexes that have been completed", UTF8Type.instance, null);

    private static CFMetaData newSystemTable(String cfName, int cfId, String comment, AbstractType comparator, AbstractType subComparator)
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
                              0,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_MEMTABLE_LIFETIME_IN_MINS,
                              DEFAULT_MEMTABLE_THROUGHPUT_IN_MB,
                              DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS,
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
    public final String comment;                      // default none, for humans only
    public final double rowCacheSize;                 // default 0
    public final double keyCacheSize;                 // default 0.01
    public final double readRepairChance;             // default 1.0 (always), chance [0.0,1.0] of read repair
    public final int gcGraceSeconds;                  // default 864000 (ten days)
    public final AbstractType defaultValidator;       // default none, use comparator types
    public final Integer minCompactionThreshold;      // default 4
    public final Integer maxCompactionThreshold;      // default 32
    public final int rowCacheSavePeriodInSeconds;     // default 0 (off)
    public final int keyCacheSavePeriodInSeconds;     // default 3600 (1 hour)
    public final int memtableFlushAfterMins;          // default 60 
    public final int memtableThroughputInMb;          // default based on heap size
    public final double memtableOperationsInMillions; // default based on throughput
    // NOTE: if you find yourself adding members to this class, make sure you keep the convert methods in lockstep.

    public final Map<ByteBuffer, ColumnDefinition> column_metadata;

    private CFMetaData(String tableName,
                       String cfName,
                       ColumnFamilyType cfType,
                       AbstractType comparator,
                       AbstractType subcolumnComparator,
                       String comment,
                       double rowCacheSize,
                       double keyCacheSize,
                       double readRepairChance,
                       int gcGraceSeconds,
                       AbstractType defaultValidator,
                       int minCompactionThreshold,
                       int maxCompactionThreshold,
                       int rowCacheSavePeriodInSeconds,
                       int keyCacheSavePeriodInSeconds,
                       int memtableFlushAfterMins,
                       Integer memtableThroughputInMb,
                       Double memtableOperationsInMillions,
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
        this.cfId = cfId;
        this.column_metadata = Collections.unmodifiableMap(column_metadata);
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
                      int gcGraceSeconds,
                      AbstractType defaultValidator,
                      int minCompactionThreshold,
                      int maxCompactionThreshold,
                      int rowCacheSavePeriodInSeconds,
                      int keyCacheSavePeriodInSeconds,
                      int memTime,
                      Integer memSize,
                      Double memOps,
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
             gcGraceSeconds,
             defaultValidator,
             minCompactionThreshold,
             maxCompactionThreshold,
             rowCacheSavePeriodInSeconds,
             keyCacheSavePeriodInSeconds,
             memTime,
             memSize,
             memOps,
             nextId(),
             column_metadata);
    }

    public static CFMetaData newIndexMetadata(String table, String parentCf, ColumnDefinition info, AbstractType columnComparator)
    {
        return new CFMetaData(table,
                              parentCf + "." + (info.index_name == null ? FBUtilities.bytesToHex(info.name) : info.index_name),
                              ColumnFamilyType.Standard,
                              columnComparator,
                              null,
                              "",
                              0,
                              0,
                              0,
                              DEFAULT_GC_GRACE_SECONDS,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS,
                              DEFAULT_MEMTABLE_LIFETIME_IN_MINS,
                              DEFAULT_MEMTABLE_THROUGHPUT_IN_MB,
                              DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS,
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
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.rowCacheSavePeriodInSeconds,
                              cfm.keyCacheSavePeriodInSeconds,
                              cfm.memtableFlushAfterMins,
                              cfm.memtableThroughputInMb,
                              cfm.memtableOperationsInMillions,
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
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.rowCacheSavePeriodInSeconds,
                              cfm.keyCacheSavePeriodInSeconds,
                              cfm.memtableFlushAfterMins,
                              cfm.memtableThroughputInMb,
                              cfm.memtableOperationsInMillions,
                              cfm.cfId,
                              cfm.column_metadata);
    }
    
    /** used for evicting cf data out of static tracking collections. */
    public static void purge(CFMetaData cfm)
    {
        cfIdMap.remove(new Pair<String, String>(cfm.tableName, cfm.cfName));
    }

    public org.apache.cassandra.avro.CfDef deflate()
    {
        org.apache.cassandra.avro.CfDef cf = new org.apache.cassandra.avro.CfDef();
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
        cf.gc_grace_seconds = gcGraceSeconds;
        cf.default_validation_class = new Utf8(defaultValidator.getClass().getName());
        cf.min_compaction_threshold = minCompactionThreshold;
        cf.max_compaction_threshold = maxCompactionThreshold;
        cf.row_cache_save_period_in_seconds = rowCacheSavePeriodInSeconds;
        cf.key_cache_save_period_in_seconds = keyCacheSavePeriodInSeconds;
        cf.memtable_flush_after_mins = memtableFlushAfterMins;
        cf.memtable_throughput_in_mb = memtableThroughputInMb;
        cf.memtable_operations_in_millions = memtableOperationsInMillions;
        cf.column_metadata = SerDeUtils.createArray(column_metadata.size(),
                                                    org.apache.cassandra.avro.ColumnDef.SCHEMA$);
        for (ColumnDefinition cd : column_metadata.values())
            cf.column_metadata.add(cd.deflate());
        return cf;
    }

    public static CFMetaData inflate(org.apache.cassandra.avro.CfDef cf)
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

        return new CFMetaData(cf.keyspace.toString(),
                              cf.name.toString(),
                              ColumnFamilyType.create(cf.column_type.toString()),
                              comparator,
                              subcolumnComparator,
                              cf.comment.toString(),
                              cf.row_cache_size,
                              cf.key_cache_size,
                              cf.read_repair_chance,
                              cf.gc_grace_seconds,
                              validator,
                              minct,
                              maxct,
                              row_cache_save_period_in_seconds,
                              key_cache_save_period_in_seconds,
                              memtable_flush_after_mins,
                              memtable_throughput_in_mb,
                              memtable_operations_in_millions,
                              cf.id,
                              column_metadata);
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
    
    public CFMetaData apply(org.apache.cassandra.avro.CfDef cf_def) throws ConfigurationException
    {
        // validate.
        if (!cf_def.id.equals(cfId))
            throw new ConfigurationException(String.format("ids do not match. %d, %d", cf_def.id, cfId));
        if (!cf_def.keyspace.toString().equals(tableName))
            throw new ConfigurationException(String.format("keyspaces do not match. %s, %s", cf_def.keyspace, tableName));
        if (!cf_def.name.toString().equals(cfName))
            throw new ConfigurationException("names do not match.");
        if (!cf_def.column_type.toString().equals(cfType.name()))
            throw new ConfigurationException("types do not match.");
        if (comparator != DatabaseDescriptor.getComparator(cf_def.comparator_type.toString()))
            throw new ConfigurationException("comparators do not match.");
        if (cf_def.subcomparator_type == null || cf_def.subcomparator_type.equals(""))
        {
            if (subcolumnComparator != null)
                throw new ConfigurationException("subcolumncomparators do not match.");
            // else, it's null and we're good.
        }
        else if (subcolumnComparator != DatabaseDescriptor.getComparator(cf_def.subcomparator_type.toString()))
            throw new ConfigurationException("subcolumncomparators do not match.");

        validateMinMaxCompactionThresholds(cf_def);
        validateMemtableSettings(cf_def);
        
        return new CFMetaData(tableName, 
                              cfName, 
                              cfType, 
                              comparator, 
                              subcolumnComparator, 
                              cf_def.comment == null ? "" : cf_def.comment.toString(), 
                              cf_def.row_cache_size, 
                              cf_def.key_cache_size,
                              cf_def.read_repair_chance, 
                              cf_def.gc_grace_seconds, 
                              DatabaseDescriptor.getComparator(cf_def.default_validation_class == null ? null : cf_def.default_validation_class.toString()),
                              cf_def.min_compaction_threshold,
                              cf_def.max_compaction_threshold,
                              cf_def.row_cache_save_period_in_seconds,
                              cf_def.key_cache_save_period_in_seconds,
                              cf_def.memtable_flush_after_mins,
                              cf_def.memtable_throughput_in_mb,
                              cf_def.memtable_operations_in_millions,
                              cfId,
                              column_metadata);
    }
    
    // merges some final fields from this CFM with modifiable fields from CfDef into a new CFMetaData.
    public CFMetaData apply(org.apache.cassandra.thrift.CfDef cf_def) throws ConfigurationException
    {
        // validate
        if (cf_def.id != cfId)
            throw new ConfigurationException("ids do not match.");
        if (!cf_def.keyspace.equals(tableName))
            throw new ConfigurationException("keyspaces do not match.");
        if (!cf_def.name.equals(cfName))
            throw new ConfigurationException("names do not match.");
        if (!cf_def.column_type.equals(cfType.name()))
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

        Map<ByteBuffer, ColumnDefinition> metadata = new HashMap<ByteBuffer, ColumnDefinition>();
        if (cf_def.column_metadata == null)
        {
            metadata = column_metadata;
        }
        else
        {
            for (org.apache.cassandra.thrift.ColumnDef def : cf_def.column_metadata)
            {
                ColumnDefinition cd = new ColumnDefinition(def.name, def.validation_class, def.index_type, def.index_name);
                metadata.put(cd.name, cd);
            }
        }

        return new CFMetaData(tableName, 
                              cfName, 
                              cfType, 
                              comparator, 
                              subcolumnComparator, 
                              cf_def.comment, 
                              cf_def.row_cache_size, 
                              cf_def.key_cache_size,
                              cf_def.read_repair_chance, 
                              cf_def.gc_grace_seconds, 
                              DatabaseDescriptor.getComparator(cf_def.default_validation_class == null ? null : cf_def.default_validation_class),
                              cf_def.min_compaction_threshold,
                              cf_def.max_compaction_threshold,
                              cf_def.row_cache_save_period_in_seconds,
                              cf_def.key_cache_save_period_in_seconds,
                              cf_def.memtable_flush_after_mins,
                              cf_def.memtable_throughput_in_mb,
                              cf_def.memtable_operations_in_millions,
                              cfId,
                              metadata);
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
        def.setGc_grace_seconds(cfm.gcGraceSeconds);
        def.setDefault_validation_class(cfm.defaultValidator.getClass().getName());
        def.setMin_compaction_threshold(cfm.minCompactionThreshold);
        def.setMax_compaction_threshold(cfm.maxCompactionThreshold);
        def.setRow_cache_save_period_in_seconds(cfm.rowCacheSavePeriodInSeconds);
        def.setKey_cache_save_period_in_seconds(cfm.keyCacheSavePeriodInSeconds);
        def.setMemtable_flush_after_mins(cfm.memtableFlushAfterMins);
        def.setMemtable_throughput_in_mb(cfm.memtableThroughputInMb);
        def.setMemtable_operations_in_millions(cfm.memtableOperationsInMillions);
        List<org.apache.cassandra.thrift.ColumnDef> column_meta = new ArrayList< org.apache.cassandra.thrift.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.thrift.ColumnDef tcd = new org.apache.cassandra.thrift.ColumnDef();
            tcd.setIndex_name(cd.index_name);
            tcd.setIndex_type(cd.index_type);
            tcd.setName(cd.name);
            tcd.setValidation_class(cd.validator.getClass().getName());
            column_meta.add(tcd);
        }
        def.setColumn_metadata(column_meta);
        return def;
    }
    
    // converts CFM to avro CfDef
    public static org.apache.cassandra.avro.CfDef convertToAvro(CFMetaData cfm)
    {
        org.apache.cassandra.avro.CfDef def = new org.apache.cassandra.avro.CfDef();
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
        def.gc_grace_seconds = cfm.gcGraceSeconds;
        def.default_validation_class = cfm.defaultValidator.getClass().getName();
        def.min_compaction_threshold = cfm.minCompactionThreshold;
        def.max_compaction_threshold = cfm.maxCompactionThreshold;
        def.row_cache_save_period_in_seconds = cfm.rowCacheSavePeriodInSeconds;
        def.key_cache_save_period_in_seconds = cfm.keyCacheSavePeriodInSeconds;
        def.memtable_flush_after_mins = cfm.memtableFlushAfterMins;
        def.memtable_throughput_in_mb = cfm.memtableThroughputInMb;
        def.memtable_operations_in_millions = cfm.memtableOperationsInMillions;
        List<org.apache.cassandra.avro.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.avro.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.avro.ColumnDef tcd = new org.apache.cassandra.avro.ColumnDef();
            tcd.index_name = cd.index_name;
            tcd.index_type = org.apache.cassandra.avro.IndexType.valueOf(cd.index_type.name());
            tcd.name = cd.name;
            tcd.validation_class = cd.validator.getClass().getName();
            column_meta.add(tcd);
        }
        def.column_metadata = column_meta;   
        return def;
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

    public static void validateMinMaxCompactionThresholds(org.apache.cassandra.avro.CfDef cf_def) throws ConfigurationException
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
        if (cf_def.isSetMemtable_flush_after_mins() && cf_def.memtable_flush_after_mins <= 0) {
            throw new ConfigurationException("memtable_flush_after_mins cannot be non-positive");
        }
        if (cf_def.isSetMemtable_throughput_in_mb() && cf_def.memtable_throughput_in_mb <= 0) {
            throw new ConfigurationException("memtable_throughput_in_mb cannot be non-positive.");
        }
        if (cf_def.isSetMemtable_operations_in_millions() && cf_def.memtable_operations_in_millions <= 0) {
            throw new ConfigurationException("memtable_operations_in_millions cannot be non-positive");
        }
    }

    public static void validateMemtableSettings(org.apache.cassandra.avro.CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.memtable_flush_after_mins != null && cf_def.memtable_flush_after_mins <= 0) {
            throw new ConfigurationException("memtable_flush_after_mins cannot be non-positive");
        }
        if (cf_def.memtable_throughput_in_mb != null && cf_def.memtable_throughput_in_mb <= 0) {
            throw new ConfigurationException("memtable_throughput_in_mb cannot be non-positive.");
        }
        if (cf_def.memtable_operations_in_millions != null && cf_def.memtable_operations_in_millions <= 0) {
            throw new ConfigurationException("memtable_operations_in_millions cannot be non-positive");
        }
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
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("rowCacheSavePeriodInSeconds", rowCacheSavePeriodInSeconds)
            .append("keyCacheSavePeriodInSeconds", keyCacheSavePeriodInSeconds)
            .append("memtableFlushAfterMins", memtableFlushAfterMins)
            .append("memtableThroughputInMb", memtableThroughputInMb)
            .append("memtableOperationsInMillions", memtableOperationsInMillions)
            .append("column_metadata", column_metadata)
            .toString();
    }
}
