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

import com.google.common.collect.*;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.config.avro.CfDef;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.config.avro.ColumnDef;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ClockType;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;


public final class CFMetaData
{
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static boolean DEFAULT_PRELOAD_ROW_CACHE = false;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;

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
                              ClockType.Timestamp,
                              comparator,
                              subComparator,
                              TimestampReconciler.instance,
                              comment,
                              0,
                              false,
                              0.01,
                              0,
                              0,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              cfId,
                              Collections.<byte[], ColumnDefinition>emptyMap());
    }

    /**
     * @return An immutable mapping of (ksname,cfname) to id.
     */
    public static final Map<Pair<String, String>, Integer> getCfToIdMap()
    {
        return Collections.unmodifiableMap(cfIdMap);
    }
    
    /**
     * @return The (ksname,cfname) pair for the given id, or null if it has been dropped.
     */
    public static final Pair<String,String> getCF(Integer cfId)
    {
        return cfIdMap.inverse().get(cfId);
    }
    
    /**
     * @return The id for the given (ksname,cfname) pair, or null if it has been dropped.
     */
    public static final Integer getId(String table, String cfName)
    {
        return cfIdMap.get(new Pair<String, String>(table, cfName));
    }
    
    // this gets called after initialization to make sure that id generation happens properly.
    public static final void fixMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        idGen.set(cfIdMap.size() == 0 ? MIN_CF_ID : Math.max(Collections.max(cfIdMap.values()) + 1, MIN_CF_ID));
    }
    
    public final Integer cfId;
    public final String tableName;                  // name of table which has this column family
    public final String cfName;                     // name of the column family
    public final ColumnFamilyType cfType;           // type: super, standard, etc.
    public final ClockType clockType;               // clock type: timestamp, etc.
    public final AbstractType comparator;           // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator;  // like comparator, for supercolumns
    public final AbstractReconciler reconciler;     // determine correct column from conflicting versions
    public final String comment;                    // for humans only

    public final double rowCacheSize;               // default 0
    public final double keyCacheSize;               // default 0.01
    public final double readRepairChance;           // default 1.0 (always), chance [0.0,1.0] of read repair
    public final boolean preloadRowCache;           // default false
    public final int gcGraceSeconds;                // default 864000 (ten days)
    public final AbstractType defaultValidator;     // default none, use comparator types
    public final Integer minCompactionThreshold;    // default 4
    public final Integer maxCompactionThreshold;    // default 32
    // NOTE: if you find yourself adding members to this class, make sure you keep the convert methods in lockstep.

    public final Map<byte[], ColumnDefinition> column_metadata;

    private CFMetaData(String tableName,
                       String cfName,
                       ColumnFamilyType cfType,
                       ClockType clockType,
                       AbstractType comparator,
                       AbstractType subcolumnComparator,
                       AbstractReconciler reconciler,
                       String comment,
                       double rowCacheSize,
                       boolean preloadRowCache,
                       double keyCacheSize,
                       double readRepairChance,
                       int gcGraceSeconds,
                       AbstractType defaultValidator,
                       int minCompactionThreshold,
                       int maxCompactionThreshold,
                       Integer cfId,
                       Map<byte[], ColumnDefinition> column_metadata)
    {
        assert column_metadata != null;
        this.tableName = tableName;
        this.cfName = cfName;
        this.cfType = cfType;
        this.clockType = clockType;
        this.comparator = comparator;
        // the default subcolumncomparator is null per thrift spec, but only should be null if cfType == Standard. If
        // cfType == Super, subcolumnComparator should default to BytesType if not set.
        this.subcolumnComparator = subcolumnComparator == null && cfType == ColumnFamilyType.Super ? BytesType.instance : subcolumnComparator;
        this.reconciler = reconciler;
        this.comment = comment == null ? "" : comment;
        this.rowCacheSize = rowCacheSize;
        this.preloadRowCache = preloadRowCache;
        this.keyCacheSize = keyCacheSize;
        this.readRepairChance = readRepairChance;
        this.gcGraceSeconds = gcGraceSeconds;
        this.defaultValidator = defaultValidator;
        this.minCompactionThreshold = minCompactionThreshold;
        this.maxCompactionThreshold = maxCompactionThreshold;
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
                      ClockType clockType,
                      AbstractType comparator,
                      AbstractType subcolumnComparator,
                      AbstractReconciler reconciler,
                      String comment,
                      double rowCacheSize,
                      boolean preloadRowCache,
                      double keyCacheSize,
                      double readRepairChance,
                      int gcGraceSeconds,
                      AbstractType defaultValidator,
                      int minCompactionThreshold,
                      int maxCompactionThreshold,
                      //This constructor generates the id!
                      Map<byte[], ColumnDefinition> column_metadata)
    {
        this(tableName,
             cfName,
             cfType,
             clockType,
             comparator,
             subcolumnComparator,
             reconciler,
             comment,
             rowCacheSize,
             preloadRowCache,
             keyCacheSize,
             readRepairChance,
             gcGraceSeconds,
             defaultValidator,
             minCompactionThreshold,
             maxCompactionThreshold,
             nextId(),
             column_metadata);
    }

    public static CFMetaData newIndexMetadata(String table, String parentCf, ColumnDefinition info, AbstractType columnComparator)
    {
        return new CFMetaData(table,
                              parentCf + "." + (info.index_name == null ? FBUtilities.bytesToHex(info.name) : info.index_name),
                              ColumnFamilyType.Standard,
                              ClockType.Timestamp,
                              columnComparator,
                              null,
                              TimestampReconciler.instance,
                              "",
                              0,
                              false,
                              0,
                              0,
                              DEFAULT_GC_GRACE_SECONDS,
                              BytesType.instance,
                              DEFAULT_MIN_COMPACTION_THRESHOLD,
                              DEFAULT_MAX_COMPACTION_THRESHOLD,
                              Collections.<byte[], ColumnDefinition>emptyMap());
    }

    /** clones an existing CFMetaData using the same id. */
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        return new CFMetaData(cfm.tableName,
                              newName,
                              cfm.cfType,
                              cfm.clockType,
                              cfm.comparator,
                              cfm.subcolumnComparator,
                              cfm.reconciler,
                              cfm.comment,
                              cfm.rowCacheSize,
                              cfm.preloadRowCache,
                              cfm.keyCacheSize,
                              cfm.readRepairChance,
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.cfId,
                              cfm.column_metadata);
    }
    
    /** clones existing CFMetaData. keeps the id but changes the table name.*/
    public static CFMetaData renameTable(CFMetaData cfm, String tableName)
    {
        return new CFMetaData(tableName,
                              cfm.cfName,
                              cfm.cfType,
                              cfm.clockType,
                              cfm.comparator,
                              cfm.subcolumnComparator,
                              cfm.reconciler,
                              cfm.comment,
                              cfm.rowCacheSize,
                              cfm.preloadRowCache,
                              cfm.keyCacheSize,
                              cfm.readRepairChance,
                              cfm.gcGraceSeconds,
                              cfm.defaultValidator,
                              cfm.minCompactionThreshold,
                              cfm.maxCompactionThreshold,
                              cfm.cfId,
                              cfm.column_metadata);
    }
    
    /** used for evicting cf data out of static tracking collections. */
    public static void purge(CFMetaData cfm)
    {
        cfIdMap.remove(new Pair<String, String>(cfm.tableName, cfm.cfName));
    }

    // a quick and dirty pretty printer for describing the column family...
    //TODO: Make it prettier, use it in the CLI
    public String pretty()
    {
        return tableName + "." + cfName + "\n"
               + "Column Family Type: " + cfType + "\n"
               + "Column Family Clock Type: " + clockType + "\n"
               + "Columns Sorted By: " + comparator + "\n";
    }

    public org.apache.cassandra.config.avro.CfDef deflate()
    {
        org.apache.cassandra.config.avro.CfDef cf = new org.apache.cassandra.config.avro.CfDef();
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
        cf.preload_row_cache = preloadRowCache;
        cf.read_repair_chance = readRepairChance;
        cf.gc_grace_seconds = gcGraceSeconds;
        cf.default_validation_class = new Utf8(defaultValidator.getClass().getName());
        cf.min_compaction_threshold = minCompactionThreshold;
        cf.max_compaction_threshold = maxCompactionThreshold;
        cf.column_metadata = SerDeUtils.createArray(column_metadata.size(),
                                                    org.apache.cassandra.config.avro.ColumnDef.SCHEMA$);
        for (ColumnDefinition cd : column_metadata.values())
            cf.column_metadata.add(cd.deflate());
        return cf;
    }

    public static CFMetaData inflate(org.apache.cassandra.config.avro.CfDef cf)
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
        Map<byte[], ColumnDefinition> column_metadata = new TreeMap<byte[], ColumnDefinition>(FBUtilities.byteArrayComparator);
        for (ColumnDef aColumn_metadata : cf.column_metadata)
        {
            ColumnDefinition cd = ColumnDefinition.inflate(aColumn_metadata);
            column_metadata.put(cd.name, cd);
        }
        return new CFMetaData(cf.keyspace.toString(),
                              cf.name.toString(),
                              ColumnFamilyType.create(cf.column_type.toString()),
                              ClockType.Timestamp,
                              comparator,
                              subcolumnComparator,
                              TimestampReconciler.instance,
                              cf.comment.toString(),
                              cf.row_cache_size,
                              cf.preload_row_cache,
                              cf.key_cache_size,
                              cf.read_repair_chance,
                              cf.gc_grace_seconds,
                              validator,
                              cf.min_compaction_threshold,
                              cf.max_compaction_threshold,
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
            .append(clockType, rhs.clockType)
            .append(comparator, rhs.comparator)
            .append(subcolumnComparator, rhs.subcolumnComparator)
            .append(reconciler, rhs.reconciler)
            .append(comment, rhs.comment)
            .append(rowCacheSize, rhs.rowCacheSize)
            .append(keyCacheSize, rhs.keyCacheSize)
            .append(readRepairChance, rhs.readRepairChance)
            .append(gcGraceSeconds, rhs.gcGraceSeconds)
            .append(minCompactionThreshold, rhs.minCompactionThreshold)
            .append(maxCompactionThreshold, rhs.maxCompactionThreshold)
            .append(cfId.intValue(), rhs.cfId.intValue())
            .append(column_metadata, rhs.column_metadata)
            .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(tableName)
            .append(cfName)
            .append(cfType)
            .append(clockType)
            .append(comparator)
            .append(subcolumnComparator)
            .append(reconciler)
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
            .toHashCode();
    }

    private static int nextId()
    {
        return idGen.getAndIncrement();
    }

    public AbstractType getValueValidator(byte[] column)
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
        if (cf_def.id != cfId)
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
        
        return new CFMetaData(tableName, 
                              cfName, 
                              cfType, 
                              clockType, 
                              comparator, 
                              subcolumnComparator, 
                              reconciler, 
                              cf_def.comment == null ? "" : cf_def.comment.toString(), 
                              cf_def.row_cache_size, 
                              cf_def.preload_row_cache, 
                              cf_def.key_cache_size, 
                              cf_def.read_repair_chance, 
                              cf_def.gc_grace_seconds, 
                              DatabaseDescriptor.getComparator(cf_def.default_validation_class == null ? (String)null : cf_def.default_validation_class.toString()),
                              cf_def.min_compaction_threshold,
                              cf_def.max_compaction_threshold,
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

        return new CFMetaData(tableName, 
                              cfName, 
                              cfType, 
                              clockType, 
                              comparator, 
                              subcolumnComparator, 
                              reconciler, 
                              cf_def.comment, 
                              cf_def.row_cache_size, 
                              cf_def.preload_row_cache, 
                              cf_def.key_cache_size, 
                              cf_def.read_repair_chance, 
                              cf_def.gc_grace_seconds, 
                              DatabaseDescriptor.getComparator(cf_def.default_validation_class == null ? null : cf_def.default_validation_class),
                              cf_def.min_compaction_threshold,
                              cf_def.max_compaction_threshold,
                              cfId,
                              column_metadata);
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
        def.setPreload_row_cache(cfm.preloadRowCache);
        def.setKey_cache_size(cfm.keyCacheSize);
        def.setRead_repair_chance(cfm.readRepairChance);
        def.setGc_grace_seconds(cfm.gcGraceSeconds);
        def.setDefault_validation_class(cfm.defaultValidator.getClass().getName());
        def.setMin_compaction_threshold(cfm.minCompactionThreshold);
        def.setMax_compaction_threshold(cfm.maxCompactionThreshold);
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
        def.preload_row_cache = cfm.preloadRowCache;
        def.key_cache_size = cfm.keyCacheSize;
        def.read_repair_chance = cfm.readRepairChance;
        def.gc_grace_seconds = cfm.gcGraceSeconds;
        def.default_validation_class = cfm.defaultValidator.getClass().getName();
        def.min_compaction_threshold = cfm.minCompactionThreshold;
        def.max_compaction_threshold = cfm.maxCompactionThreshold;
        List<org.apache.cassandra.avro.ColumnDef> column_meta = new ArrayList<org.apache.cassandra.avro.ColumnDef>(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            org.apache.cassandra.avro.ColumnDef tcd = new org.apache.cassandra.avro.ColumnDef();
            tcd.index_name = cd.index_name;
            tcd.index_type = org.apache.cassandra.avro.IndexType.valueOf(cd.index_type.name());
            tcd.name = ByteBuffer.wrap(cd.name);
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
}
