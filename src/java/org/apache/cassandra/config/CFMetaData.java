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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.Pair;

public final class CFMetaData
{
    public final static double DEFAULT_READ_REPAIR_CHANCE = 1.0;
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;
    public final static boolean DEFAULT_PRELOAD_ROW_CACHE = false;

    private static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static final Map<Integer, String> currentCfNames = new HashMap<Integer, String>();
    
    private static final Map<Pair<String, String>, Integer> cfIdMap = new HashMap<Pair<String, String>, Integer>();

    public static final Map<Pair<String, String>, Integer> getCfIdMap()
    {
        return Collections.unmodifiableMap(cfIdMap);    
    }
    
    public static final String getCurrentName(int id)
    {
        return currentCfNames.get(id);
    }
    
    public static final int getId(String table, String cfName)
    {
        Integer id = cfIdMap.get(new Pair<String, String>(table, cfName));
        if (id == null)
            throw new IllegalArgumentException(String.format("Illegal table/cf pair (%s.%s)", table, cfName));
        return id;
    }
    
    // this gets called after initialization to make sure that id generation happens properly.
    public static final void fixMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        idGen.set(cfIdMap.size() == 0 ? 1000 : Math.max(Collections.max(cfIdMap.values()) + 1, 1000));
    }
    
    public final String tableName;            // name of table which has this column family
    public final String cfName;               // name of the column family
    public final ColumnFamilyType cfType;     // type: super, standard, etc.
    public final AbstractType comparator;       // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator; // like comparator, for supercolumns
    public final String comment; // for humans only
    public final double rowCacheSize; // default 0
    public final double keyCacheSize; // default 0.01
    public final double readRepairChance; //chance 0 to 1, of doing a read repair; defaults 1.0 (always)
    public final int cfId;
    public boolean preloadRowCache;


    private CFMetaData(String tableName, String cfName, ColumnFamilyType cfType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, boolean preloadRowCache, double keyCacheSize, double readRepairChance, int cfId)
    {
        this.tableName = tableName;
        this.cfName = cfName;
        this.cfType = cfType;
        this.comparator = comparator;
        this.subcolumnComparator = subcolumnComparator;
        this.comment = comment;
        this.rowCacheSize = rowCacheSize;
        this.preloadRowCache = preloadRowCache;
        this.keyCacheSize = keyCacheSize;
        this.readRepairChance = readRepairChance;
        this.cfId = cfId;
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
    
    public CFMetaData(String tableName, String cfName, ColumnFamilyType cfType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, boolean preloadRowCache, double keyCacheSize)
    {
        this(tableName, cfName, cfType, comparator, subcolumnComparator, comment, rowCacheSize, preloadRowCache, keyCacheSize, DEFAULT_READ_REPAIR_CHANCE, nextId());
    }

    public CFMetaData(String tableName, String cfName, ColumnFamilyType cfType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, boolean preloadRowCache, double keyCacheSize, double readRepairChance)
    {
        this(tableName, cfName, cfType, comparator, subcolumnComparator, comment, rowCacheSize, preloadRowCache, keyCacheSize, readRepairChance, nextId());
    }

    /** clones an existing CFMetaData using the same id. */
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        CFMetaData newCfm = new CFMetaData(cfm.tableName, newName, cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.comment, cfm.rowCacheSize, cfm.preloadRowCache, cfm.keyCacheSize, cfm.readRepairChance, cfm.cfId);
        return newCfm;
    }
    
    /** clones existing CFMetaData. keeps the id but changes the table name.*/
    public static CFMetaData renameTable(CFMetaData cfm, String tableName)
    {
        return new CFMetaData(tableName, cfm.cfName, cfm.cfType, cfm.comparator, cfm.subcolumnComparator, cfm.comment, cfm.rowCacheSize, cfm.preloadRowCache, cfm.keyCacheSize, cfm.readRepairChance, cfm.cfId);
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
               + "Columns Sorted By: " + comparator + "\n";
    }

    public static byte[] serialize(CFMetaData cfm) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(cfm.tableName);
        dout.writeUTF(cfm.cfName);
        dout.writeUTF(cfm.cfType.name());
        dout.writeUTF(cfm.comparator.getClass().getName());
        dout.writeBoolean(cfm.subcolumnComparator != null);
        if (cfm.subcolumnComparator != null)
            dout.writeUTF(cfm.subcolumnComparator.getClass().getName());
        dout.writeBoolean(cfm.comment != null);
        if (cfm.comment != null)
            dout.writeUTF(cfm.comment);
        dout.writeDouble(cfm.rowCacheSize);
        dout.writeBoolean(cfm.preloadRowCache);
        dout.writeDouble(cfm.keyCacheSize);
        dout.writeDouble(cfm.readRepairChance);
        dout.writeInt(cfm.cfId);
        dout.close();
        return bout.toByteArray();
    }

    public static CFMetaData deserialize(InputStream in) throws IOException
    {
        DataInputStream din = new DataInputStream(in);
        String tableName = din.readUTF();
        String cfName = din.readUTF();
        ColumnFamilyType cfType = ColumnFamilyType.create(din.readUTF());
        AbstractType comparator = null;
        try
        {
            comparator = (AbstractType)Class.forName(din.readUTF()).newInstance();
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
        }
        AbstractType subcolumnComparator = null;
        try
        {
            subcolumnComparator = din.readBoolean() ? (AbstractType)Class.forName(din.readUTF()).newInstance() : null;
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
        return new CFMetaData(tableName, cfName, cfType, comparator, subcolumnComparator, comment, rowCacheSize, preloadRowCache, keyCacheSize, readRepairChance, cfId);
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
            .append(cfId, rhs.cfId)
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
            .append(cfId)
            .toHashCode();
    }

    private static int nextId() 
    {
        return idGen.getAndIncrement();
    }
}
