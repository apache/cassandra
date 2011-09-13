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
import org.apache.cassandra.cache.IRowCacheProvider;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;


public final class CFMetaData
{
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = true;
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
    public final static String DEFAULT_ROW_CACHE_PROVIDER = "org.apache.cassandra.cache.ConcurrentLinkedHashCacheProvider";
    public final static ByteBuffer DEFAULT_KEY_NAME = ByteBufferUtil.bytes("KEY");

    private static final int MIN_CF_ID = 1000;
    private static final AtomicInteger idGen = new AtomicInteger(MIN_CF_ID);
    
    private static final BiMap<Pair<String, String>, Integer> cfIdMap = HashBiMap.create();
    
    public static final CFMetaData StatusCf = newSystemMetadata(SystemTable.STATUS_CF, 0, "persistent metadata for the local node", BytesType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData HintsCf = newSystemMetadata(HintedHandOffManager.HINTS_CF, 1, "hinted handoff data", BytesType.instance, BytesType.instance, Math.min(256, Math.max(32, DEFAULT_MEMTABLE_THROUGHPUT_IN_MB / 2)));
    public static final CFMetaData MigrationsCf = newSystemMetadata(Migration.MIGRATIONS_CF, 2, "individual schema mutations", TimeUUIDType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData SchemaCf = newSystemMetadata(Migration.SCHEMA_CF, 3, "current state of the schema", UTF8Type.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData IndexCf = newSystemMetadata(SystemTable.INDEX_CF, 5, "indexes that have been completed", UTF8Type.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);
    public static final CFMetaData NodeIdCf = newSystemMetadata(SystemTable.NODE_ID_CF, 6, "nodeId and their metadata", TimeUUIDType.instance, null, DEFAULT_SYSTEM_MEMTABLE_THROUGHPUT_IN_MB);

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
    public static Integer getId(String ksName, String cfName)
    {
        return cfIdMap.get(new Pair<String, String>(ksName, cfName));
    }
    
    // this gets called after initialization to make sure that id generation happens properly.
    public static void fixMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        idGen.set(cfIdMap.size() == 0 ? MIN_CF_ID : Math.max(Collections.max(cfIdMap.values()) + 1, MIN_CF_ID));
    }

    /** adds this cfm to the map. */
    public static void map(CFMetaData cfm) throws ConfigurationException
    {
        Pair<String, String> key = new Pair<String, String>(cfm.ksName, cfm.cfName);
        if (cfIdMap.containsKey(key))
            throw new ConfigurationException("Attempt to assign id to existing column family.");
        else
        {
            cfIdMap.put(key, cfm.cfId);
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
    private int memtableFlushAfterMins;               // default 60 
    private int memtableThroughputInMb;               // default based on heap size
    private double memtableOperationsInMillions;      // default based on throughput
    private double mergeShardsChance;                 // default 0.1, chance [0.0, 1.0] of merging old shards during replication
    private IRowCacheProvider rowCacheProvider;
    private ByteBuffer keyAlias;                      // default NULL
    // NOTE: if you find yourself adding members to this class, make sure you keep the convert methods in lockstep.

    private Map<ByteBuffer, ColumnDefinition> column_metadata;

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
    public CFMetaData memTime(int prop) {memtableFlushAfterMins = prop; return this;}
    public CFMetaData memSize(int prop) {memtableThroughputInMb = prop; return this;}
    public CFMetaData memOps(double prop) {memtableOperationsInMillions = prop; return this;}
    public CFMetaData mergeShardsChance(double prop) {mergeShardsChance = prop; return this;}
    public CFMetaData keyAlias(ByteBuffer prop) {keyAlias = prop; return this;}
    public CFMetaData columnMetadata(Map<ByteBuffer,ColumnDefinition> prop) {column_metadata = prop; return this;}
    public CFMetaData rowCacheProvider(IRowCacheProvider prop) { rowCacheProvider = prop; return this;}

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType comp, AbstractType subcc)
    {
        this(keyspace, name, type, comp, subcc, nextId());
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
        readRepairChance             = DEFAULT_READ_REPAIR_CHANCE;
        replicateOnWrite             = DEFAULT_REPLICATE_ON_WRITE;
        gcGraceSeconds               = DEFAULT_GC_GRACE_SECONDS;
        minCompactionThreshold       = DEFAULT_MIN_COMPACTION_THRESHOLD;
        maxCompactionThreshold       = DEFAULT_MAX_COMPACTION_THRESHOLD;
        memtableFlushAfterMins       = DEFAULT_MEMTABLE_LIFETIME_IN_MINS;
        memtableThroughputInMb       = DEFAULT_MEMTABLE_THROUGHPUT_IN_MB;
        memtableOperationsInMillions = DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS;
        mergeShardsChance            = DEFAULT_MERGE_SHARDS_CHANCE;
        try
        {
            rowCacheProvider             = FBUtilities.newCacheProvider(DEFAULT_ROW_CACHE_PROVIDER);
        }
        catch (ConfigurationException e)
        {
            throw new AssertionError(e); // the default provider should not error out
        }

        // Defaults strange or simple enough to not need a DEFAULT_T for
        defaultValidator = BytesType.instance;
        keyValidator = BytesType.instance;
        comment = "";
        keyAlias = null; // This qualifies as a 'strange default'.
        column_metadata = new HashMap<ByteBuffer,ColumnDefinition>();
    }

    private static CFMetaData newSystemMetadata(String cfName, int cfId, String comment, AbstractType comparator, AbstractType subcc, int memtableThroughPutInMB)
    {
        ColumnFamilyType type = subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super;
        CFMetaData newCFMD = new CFMetaData(Table.SYSTEM_TABLE, cfName, type, comparator,  subcc, cfId);

        return newCFMD.comment(comment)
                      .keyCacheSize(0.01)
                      .readRepairChance(0)
                      .gcGraceSeconds(0)
                      .memSize(memtableThroughPutInMB)
                      .memOps(sizeMemtableOperations(memtableThroughPutInMB))
                      .mergeShardsChance(0.0)
                      .rowCacheSavePeriod(0)
                      .keyCacheSavePeriod(0);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType columnComparator)
    {
        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, null)
                             .keyCacheSize(0.0)
                             .readRepairChance(0.0)
                             .gcGraceSeconds(parent.gcGraceSeconds)
                             .minCompactionThreshold(parent.minCompactionThreshold)
                             .maxCompactionThreshold(parent.maxCompactionThreshold)
                             .memTime(parent.memtableFlushAfterMins)
                             .memSize(parent.memtableThroughputInMb)
                             .memOps(parent.memtableOperationsInMillions);
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
                      .memTime(oldCFMD.memtableFlushAfterMins)
                      .memSize(oldCFMD.memtableThroughputInMb)
                      .memOps(oldCFMD.memtableOperationsInMillions)
                      .columnMetadata(oldCFMD.column_metadata);
    }

    /** used for evicting cf data out of static tracking collections. */
    public static void purge(CFMetaData cfm)
    {
        cfIdMap.remove(new Pair<String, String>(cfm.ksName, cfm.cfName));
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

    public org.apache.cassandra.db.migration.avro.CfDef deflate()
    {
        org.apache.cassandra.db.migration.avro.CfDef cf = new org.apache.cassandra.db.migration.avro.CfDef();
        cf.id = cfId;
        cf.keyspace = new Utf8(ksName);
        cf.name = new Utf8(cfName);
        cf.column_type = new Utf8(cfType.name());
        cf.comparator_type = new Utf8(comparator.toString());
        if (subcolumnComparator != null)
            cf.subcomparator_type = new Utf8(subcolumnComparator.toString());
        cf.comment = new Utf8(comment);
        cf.row_cache_size = rowCacheSize;
        cf.key_cache_size = keyCacheSize;
        cf.read_repair_chance = readRepairChance;
        cf.replicate_on_write = replicateOnWrite;
        cf.gc_grace_seconds = gcGraceSeconds;
        cf.default_validation_class = new Utf8(defaultValidator.toString());
        cf.key_validation_class = new Utf8(keyValidator.toString());
        cf.min_compaction_threshold = minCompactionThreshold;
        cf.max_compaction_threshold = maxCompactionThreshold;
        cf.row_cache_save_period_in_seconds = rowCacheSavePeriodInSeconds;
        cf.key_cache_save_period_in_seconds = keyCacheSavePeriodInSeconds;
        cf.memtable_flush_after_mins = memtableFlushAfterMins;
        cf.memtable_throughput_in_mb = memtableThroughputInMb;
        cf.memtable_operations_in_millions = memtableOperationsInMillions;
        cf.merge_shards_chance = mergeShardsChance;
        cf.key_alias = keyAlias;
        cf.column_metadata = SerDeUtils.createArray(column_metadata.size(),
                                                    org.apache.cassandra.db.migration.avro.ColumnDef.SCHEMA$);
        for (ColumnDefinition cd : column_metadata.values())
            cf.column_metadata.add(cd.deflate());
        cf.row_cache_provider = new Utf8(rowCacheProvider.getClass().getName());
        return cf;
    }

    public static CFMetaData inflate(org.apache.cassandra.db.migration.avro.CfDef cf)
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
            ColumnDefinition cd = ColumnDefinition.inflate(aColumn_metadata);
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
        if (cf.memtable_flush_after_mins != null) { newCFMD.memTime(cf.memtable_flush_after_mins); }
        if (cf.memtable_throughput_in_mb != null) { newCFMD.memSize(cf.memtable_throughput_in_mb); }
        if (cf.memtable_operations_in_millions != null) { newCFMD.memOps(cf.memtable_operations_in_millions); }
        if (cf.merge_shards_chance != null) { newCFMD.mergeShardsChance(cf.merge_shards_chance); }
        if (cf.row_cache_provider != null)
        {
            try
            {
                newCFMD.rowCacheProvider(FBUtilities.newCacheProvider(cf.row_cache_provider.toString()));
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }
        if (cf.key_alias != null) { newCFMD.keyAlias(cf.key_alias); }

        return newCFMD.comment(cf.comment.toString())
                      .rowCacheSize(cf.row_cache_size)
                      .keyCacheSize(cf.key_cache_size)
                      .readRepairChance(cf.read_repair_chance)
                      .replicateOnWrite(cf.replicate_on_write)
                      .gcGraceSeconds(cf.gc_grace_seconds)
                      .defaultValidator(validator)
                      .keyValidator(keyValidator)
                      .columnMetadata(column_metadata);
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

    public IRowCacheProvider getRowCacheProvider()
    {
        return rowCacheProvider;
    }

    public ByteBuffer getKeyName()
    {
        return keyAlias == null ? DEFAULT_KEY_NAME : keyAlias;
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
            .append(memtableFlushAfterMins, rhs.memtableFlushAfterMins)
            .append(memtableThroughputInMb, rhs.memtableThroughputInMb)
            .append(memtableOperationsInMillions, rhs.memtableOperationsInMillions)
            .append(mergeShardsChance, rhs.mergeShardsChance)
            .append(keyAlias, rhs.keyAlias)
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
            .append(memtableFlushAfterMins)
            .append(memtableThroughputInMb)
            .append(memtableOperationsInMillions)
            .append(mergeShardsChance)
            .append(keyAlias)
            .toHashCode();
    }

    private static int nextId()
    {
        return idGen.getAndIncrement();
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
        if (!cf_def.isSetMemtable_flush_after_mins())
            cf_def.setMemtable_flush_after_mins(CFMetaData.DEFAULT_MEMTABLE_LIFETIME_IN_MINS);
        if (!cf_def.isSetMemtable_throughput_in_mb())
            cf_def.setMemtable_throughput_in_mb(CFMetaData.DEFAULT_MEMTABLE_THROUGHPUT_IN_MB);
        if (!cf_def.isSetMemtable_operations_in_millions())
            cf_def.setMemtable_operations_in_millions(CFMetaData.DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS);
        if (!cf_def.isSetMerge_shards_chance())
            cf_def.setMerge_shards_chance(CFMetaData.DEFAULT_MERGE_SHARDS_CHANCE);
        if (!cf_def.isSetRow_cache_provider())
            cf_def.setRow_cache_provider(CFMetaData.DEFAULT_ROW_CACHE_PROVIDER);
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
        if (cf_def.isSetRow_cache_save_period_in_seconds()) { newCFMD.rowCacheSavePeriod(cf_def.row_cache_save_period_in_seconds); }
        if (cf_def.isSetKey_cache_save_period_in_seconds()) { newCFMD.keyCacheSavePeriod(cf_def.key_cache_save_period_in_seconds); }
        if (cf_def.isSetMemtable_flush_after_mins()) { newCFMD.memTime(cf_def.memtable_flush_after_mins); }
        if (cf_def.isSetMemtable_throughput_in_mb()) { newCFMD.memSize(cf_def.memtable_throughput_in_mb); }
        if (cf_def.isSetMemtable_operations_in_millions()) { newCFMD.memOps(cf_def.memtable_operations_in_millions); }
        if (cf_def.isSetMerge_shards_chance()) { newCFMD.mergeShardsChance(cf_def.merge_shards_chance); }
        if (cf_def.isSetRow_cache_provider()) { newCFMD.rowCacheProvider(FBUtilities.newCacheProvider(cf_def.row_cache_provider)); }
        if (cf_def.isSetKey_alias()) { newCFMD.keyAlias(cf_def.key_alias); }
        if (cf_def.isSetKey_validation_class()) { newCFMD.keyValidator(TypeParser.parse(cf_def.key_validation_class)); }

        return newCFMD.comment(cf_def.comment)
                      .rowCacheSize(cf_def.row_cache_size)
                      .keyCacheSize(cf_def.key_cache_size)
                      .readRepairChance(cf_def.read_repair_chance)
                      .replicateOnWrite(cf_def.replicate_on_write)
                      .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                      .keyValidator(TypeParser.parse(cf_def.key_validation_class))
                      .columnMetadata(ColumnDefinition.fromColumnDef(cf_def.column_metadata));
    }

    // merges some final fields from this CFM with modifiable fields from CfDef into a new CFMetaData.
    public void apply(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
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
        validateMemtableSettings(cf_def);

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
        memtableFlushAfterMins = cf_def.memtable_flush_after_mins;
        memtableThroughputInMb = cf_def.memtable_throughput_in_mb;
        memtableOperationsInMillions = cf_def.memtable_operations_in_millions;
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
            column_metadata.remove(indexName);
        // update the ones staying
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : cf_def.column_metadata)
        {
            ColumnDefinition oldDef = column_metadata.get(def.name);
            if (oldDef == null)
                continue;
            oldDef.setValidator(TypeParser.parse(def.validation_class));
            oldDef.setIndexType(def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.name()));
            oldDef.setIndexName(def.index_name == null ? null : def.index_name.toString());
        }
        // add the new ones coming in.
        for (org.apache.cassandra.db.migration.avro.ColumnDef def : toAdd)
        {
            AbstractType dValidClass = TypeParser.parse(def.validation_class);
            ColumnDefinition cd = new ColumnDefinition(def.name, 
                                                       dValidClass,
                                                       def.index_type == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(def.index_type.toString()), 
                                                       def.index_name == null ? null : def.index_name.toString());
            column_metadata.put(cd.name, cd);
        }
    }
    
    // converts CFM to thrift CfDef
    public static org.apache.cassandra.thrift.CfDef convertToThrift(CFMetaData cfm)
    {
        org.apache.cassandra.thrift.CfDef def = new org.apache.cassandra.thrift.CfDef(cfm.ksName, cfm.cfName);
        def.setId(cfm.cfId);
        def.setColumn_type(cfm.cfType.name());
        def.setComparator_type(cfm.comparator.toString());
        if (cfm.subcolumnComparator != null)
        {
            def.setSubcomparator_type(cfm.subcolumnComparator.toString());
            def.setColumn_type("Super");
        }
        def.setComment(enforceCommentNotNull(cfm.comment));
        def.setRow_cache_size(cfm.rowCacheSize);
        def.setKey_cache_size(cfm.keyCacheSize);
        def.setRead_repair_chance(cfm.readRepairChance);
        def.setReplicate_on_write(cfm.replicateOnWrite);
        def.setGc_grace_seconds(cfm.gcGraceSeconds);
        def.setDefault_validation_class(cfm.defaultValidator.toString());
        def.setKey_validation_class(cfm.keyValidator.toString());
        def.setMin_compaction_threshold(cfm.minCompactionThreshold);
        def.setMax_compaction_threshold(cfm.maxCompactionThreshold);
        def.setRow_cache_save_period_in_seconds(cfm.rowCacheSavePeriodInSeconds);
        def.setKey_cache_save_period_in_seconds(cfm.keyCacheSavePeriodInSeconds);
        def.setMemtable_flush_after_mins(cfm.memtableFlushAfterMins);
        def.setMemtable_throughput_in_mb(cfm.memtableThroughputInMb);
        def.setMemtable_operations_in_millions(cfm.memtableOperationsInMillions);
        def.setMerge_shards_chance(cfm.mergeShardsChance);
        def.setKey_alias(cfm.getKeyName());
        List<org.apache.cassandra.thrift.ColumnDef> column_meta = new ArrayList< org.apache.cassandra.thrift.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.thrift.ColumnDef tcd = new org.apache.cassandra.thrift.ColumnDef();
            tcd.setIndex_name(cd.getIndexName());
            tcd.setIndex_type(cd.getIndexType());
            tcd.setName(cd.name);
            tcd.setValidation_class(cd.getValidator().toString());
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
        def.keyspace = cfm.ksName;
        def.id = cfm.cfId;
        def.column_type = cfm.cfType.name();
        def.comparator_type = cfm.comparator.toString();
        if (cfm.subcolumnComparator != null)
        {
            def.subcomparator_type = cfm.subcolumnComparator.toString();
            def.column_type = "Super";
        }
        def.comment = enforceCommentNotNull(cfm.comment);
        def.row_cache_size = cfm.rowCacheSize;
        def.key_cache_size = cfm.keyCacheSize;
        def.read_repair_chance = cfm.readRepairChance;
        def.replicate_on_write = cfm.replicateOnWrite;
        def.gc_grace_seconds = cfm.gcGraceSeconds;
        def.default_validation_class = cfm.defaultValidator == null ? null : cfm.defaultValidator.toString();
        def.min_compaction_threshold = cfm.minCompactionThreshold;
        def.max_compaction_threshold = cfm.maxCompactionThreshold;
        def.row_cache_save_period_in_seconds = cfm.rowCacheSavePeriodInSeconds;
        def.key_cache_save_period_in_seconds = cfm.keyCacheSavePeriodInSeconds;
        def.memtable_flush_after_mins = cfm.memtableFlushAfterMins;
        def.memtable_throughput_in_mb = cfm.memtableThroughputInMb;
        def.memtable_operations_in_millions = cfm.memtableOperationsInMillions;
        def.merge_shards_chance = cfm.mergeShardsChance;
        def.key_validation_class = cfm.keyValidator.getClass().getName();
        def.key_alias = cfm.keyAlias;
        List<org.apache.cassandra.db.migration.avro.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.db.migration.avro.ColumnDef tcd = new org.apache.cassandra.db.migration.avro.ColumnDef();
            tcd.index_name = cd.getIndexName();
            tcd.index_type = cd.getIndexType() == null ? null : org.apache.cassandra.db.migration.avro.IndexType.valueOf(cd.getIndexType().name());
            tcd.name = ByteBufferUtil.clone(cd.name);
            tcd.validation_class = cd.getValidator().toString();
            column_meta.add(tcd);
        }
        def.column_metadata = column_meta; 
        def.row_cache_provider = new Utf8(cfm.rowCacheProvider.getClass().getName());
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
        newDef.key_validation_class = def.getKey_validation_class();
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
        newDef.key_alias = def.key_alias;

        List<org.apache.cassandra.db.migration.avro.ColumnDef> columnMeta = new ArrayList<org.apache.cassandra.db.migration.avro.ColumnDef>();
        if (def.isSetColumn_metadata())
        {
            for (org.apache.cassandra.thrift.ColumnDef cdef : def.getColumn_metadata())
            {
                org.apache.cassandra.db.migration.avro.ColumnDef tdef = new org.apache.cassandra.db.migration.avro.ColumnDef();
                tdef.name = ByteBufferUtil.clone(cdef.bufferForName());
                tdef.validation_class = cdef.getValidation_class();
                tdef.index_name = cdef.getIndex_name();
                tdef.index_type = cdef.getIndex_type() == null ? null : org.apache.cassandra.db.migration.avro.IndexType.valueOf(cdef.getIndex_type().name());
                columnMeta.add(tdef);
            }
        }
        newDef.column_metadata = columnMeta;
        return newDef;
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

    public static void validateMemtableSettings(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException
    {
        if (cf_def.memtable_flush_after_mins != null)
            DatabaseDescriptor.validateMemtableFlushPeriod(cf_def.memtable_flush_after_mins);
        if (cf_def.memtable_throughput_in_mb != null)
            DatabaseDescriptor.validateMemtableThroughput(cf_def.memtable_throughput_in_mb);
        if (cf_def.memtable_operations_in_millions != null)
            DatabaseDescriptor.validateMemtableOperations(cf_def.memtable_operations_in_millions);
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
            .append("memtableFlushAfterMins", memtableFlushAfterMins)
            .append("memtableThroughputInMb", memtableThroughputInMb)
            .append("memtableOperationsInMillions", memtableOperationsInMillions)
            .append("mergeShardsChance", mergeShardsChance)
            .append("keyAlias", keyAlias)
            .append("column_metadata", column_metadata)
            .toString();
    }
}
