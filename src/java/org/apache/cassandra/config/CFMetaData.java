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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.locator.DatacenterShardStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;


public final class CFMetaData
{
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static boolean DEFAULT_PRELOAD_ROW_CACHE = false;
    private static final int MIN_CF_ID = 1000;

    private static final Logger logger = LoggerFactory.getLogger(DatacenterShardStrategy.class);

    private static final AtomicInteger idGen = new AtomicInteger(MIN_CF_ID);
    
    private static final Map<Integer, String> currentCfNames = new HashMap<Integer, String>();
    
    private static final BiMap<Pair<String, String>, Integer> cfIdMap = HashBiMap.<Pair<String, String>, Integer>create();
    
    public static final CFMetaData StatusCf = new CFMetaData(Table.SYSTEM_TABLE, SystemTable.STATUS_CF, ColumnFamilyType.Standard, ClockType.Timestamp, UTF8Type.instance, null, new TimestampReconciler(), "persistent metadata for the local node", 0, false, 0.01, 0, Collections.<byte[],ColumnDefinition>emptyMap());
    public static final CFMetaData HintsCf = new CFMetaData(Table.SYSTEM_TABLE, HintedHandOffManager.HINTS_CF, ColumnFamilyType.Super, ClockType.Timestamp, UTF8Type.instance, BytesType.instance, new TimestampReconciler(), "hinted handoff data", 0, false, 0.01, 1, Collections.<byte[], ColumnDefinition>emptyMap());
    public static final CFMetaData MigrationsCf = new CFMetaData(Table.SYSTEM_TABLE, Migration.MIGRATIONS_CF, ColumnFamilyType.Standard, ClockType.Timestamp, TimeUUIDType.instance, null, new TimestampReconciler(), "individual schema mutations", 0, false, 0.01, 2, Collections.<byte[], ColumnDefinition>emptyMap());
    public static final CFMetaData SchemaCf = new CFMetaData(Table.SYSTEM_TABLE, Migration.SCHEMA_CF, ColumnFamilyType.Standard, ClockType.Timestamp, UTF8Type.instance, null, new TimestampReconciler(), "current state of the schema", 0, false, 0.01, 3, Collections. <byte[], ColumnDefinition>emptyMap());

    /**
     * @return An immutable mapping of (ksname,cfname) to id.
     */
    public static final Map<Pair<String, String>, Integer> getCfToIdMap()
    {
        return Collections.unmodifiableMap(cfIdMap);
    }
    
    /**
     * @return An immutable mapping of id to (ksname,cfname).
     */
    public static final Map<Integer, Pair<String, String>> getIdToCfMap()
    {
        return Collections.unmodifiableMap(cfIdMap.inverse());
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
    
    public final String tableName;            // name of table which has this column family
    public final String cfName;               // name of the column family
    public final ColumnFamilyType cfType;     // type: super, standard, etc.
    public final ClockType clockType;         // clock type: timestamp, etc.
    public final AbstractType comparator;       // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator; // like comparator, for supercolumns
    public final AbstractReconciler reconciler; // determine correct column from conflicting versions
    public final String comment; // for humans only
    public final double rowCacheSize; // default 0
    public final double keyCacheSize; // default 0.01
    public final double readRepairChance; //chance 0 to 1, of doing a read repair; defaults 1.0 (always)
    public final Integer cfId;
    public boolean preloadRowCache;

    // BytesToken because byte[].hashCode|equals is inherited from Object.  gggrrr...
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
        this.comment = comment;
        this.rowCacheSize = rowCacheSize;
        this.preloadRowCache = preloadRowCache;
        this.keyCacheSize = keyCacheSize;
        this.readRepairChance = readRepairChance;
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
            currentCfNames.put(cfm.cfId, cfm.cfName);
        }
    }

    public CFMetaData(String tableName, String cfName, ColumnFamilyType cfType, ClockType clockType, AbstractType comparator, AbstractType subcolumnComparator, AbstractReconciler reconciler, String comment, double rowCacheSize, boolean preloadRowCache, double keyCacheSize, double readRepairChance, Map<byte[], ColumnDefinition> column_metadata)
    {
        this(tableName, cfName, cfType, clockType, comparator, subcolumnComparator, reconciler, comment, rowCacheSize, preloadRowCache, keyCacheSize, readRepairChance, nextId(), column_metadata);
    }

    /** clones an existing CFMetaData using the same id. */
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        CFMetaData newCfm = new CFMetaData(cfm.tableName, newName, cfm.cfType, cfm.clockType, cfm.comparator, cfm.subcolumnComparator, cfm.reconciler, cfm.comment, cfm.rowCacheSize, cfm.preloadRowCache, cfm.keyCacheSize, cfm.readRepairChance, cfm.cfId, cfm.column_metadata);
        return newCfm;
    }
    
    /** clones existing CFMetaData. keeps the id but changes the table name.*/
    public static CFMetaData renameTable(CFMetaData cfm, String tableName)
    {
        return new CFMetaData(tableName, cfm.cfName, cfm.cfType, cfm.clockType, cfm.comparator, cfm.subcolumnComparator, cfm.reconciler, cfm.comment, cfm.rowCacheSize, cfm.preloadRowCache, cfm.keyCacheSize, cfm.readRepairChance, cfm.cfId, cfm.column_metadata);
    }
    
    /** used for evicting cf data out of static tracking collections. */
    public static void purge(CFMetaData cfm)
    {
        cfIdMap.remove(new Pair<String, String>(cfm.tableName, cfm.cfName));
        currentCfNames.remove(cfm.cfId);
    }

    // a quick and dirty pretty printer for describing the column family...
    public String pretty()
    {
        return tableName + "." + cfName + "\n"
               + "Column Family Type: " + cfType + "\n"
               + "Column Family Clock Type: " + clockType + "\n"
               + "Columns Sorted By: " + comparator + "\n";
    }

    public static byte[] serialize(CFMetaData cfm) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(cfm.tableName);
        dout.writeUTF(cfm.cfName);
        dout.writeUTF(cfm.cfType.name());
        dout.writeUTF(cfm.clockType.name());
        dout.writeUTF(cfm.comparator.getClass().getName());
        dout.writeBoolean(cfm.subcolumnComparator != null);
        if (cfm.subcolumnComparator != null)
            dout.writeUTF(cfm.subcolumnComparator.getClass().getName());
        dout.writeUTF(cfm.reconciler.getClass().getName());
        dout.writeBoolean(cfm.comment != null);
        if (cfm.comment != null)
            dout.writeUTF(cfm.comment);
        dout.writeDouble(cfm.rowCacheSize);
        dout.writeBoolean(cfm.preloadRowCache);
        dout.writeDouble(cfm.keyCacheSize);
        dout.writeDouble(cfm.readRepairChance);
        dout.writeInt(cfm.cfId);
        dout.writeInt(cfm.column_metadata.size());
        for (ColumnDefinition cd : cfm.column_metadata.values())
        {
            byte[] cdBytes = ColumnDefinition.serialize(cd);
            dout.writeInt(cdBytes.length);
            dout.write(cdBytes);
        }
        dout.close();
        return bout.toByteArray();
    }

    public static CFMetaData deserialize(InputStream in) throws IOException, ConfigurationException
    {
        DataInputStream din = new DataInputStream(in);
        String tableName = din.readUTF();
        String cfName = din.readUTF();
        ColumnFamilyType cfType = ColumnFamilyType.create(din.readUTF());
        ClockType clockType = ClockType.create(din.readUTF());
        AbstractType comparator = DatabaseDescriptor.getComparator(din.readUTF());
        AbstractType subcolumnComparator = null;
        subcolumnComparator = din.readBoolean() ? DatabaseDescriptor.getComparator(din.readUTF()) : null;
        AbstractReconciler reconciler = null;
        try
        {
            reconciler = (AbstractReconciler)Class.forName(din.readUTF()).newInstance();
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        String comment = din.readBoolean() ? din.readUTF() : null;
        double rowCacheSize = din.readDouble();
        boolean preloadRowCache = din.readBoolean();
        double keyCacheSize = din.readDouble();
        double readRepairChance = din.readDouble();
        int cfId = din.readInt();
        int columnMetadataEntries = din.readInt();
        Map<byte[], ColumnDefinition> column_metadata = new TreeMap<byte[], ColumnDefinition>(FBUtilities.byteArrayComparator);
        for (int i = 0; i < columnMetadataEntries; i++)
        {
            int cdSize = din.readInt();
            byte[] cdBytes = new byte[cdSize];
            din.readFully(cdBytes);
            ColumnDefinition cd = ColumnDefinition.deserialize(cdBytes);
            column_metadata.put(cd.name, cd);
        }
        return new CFMetaData(tableName, cfName, cfType, clockType, comparator, subcolumnComparator, reconciler, comment, rowCacheSize, preloadRowCache, keyCacheSize, readRepairChance, cfId, column_metadata);
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
        ColumnDefinition columnDefinition = column_metadata.get(column);
        if (columnDefinition == null)
            return null;
        return columnDefinition.validator;
    }
}
