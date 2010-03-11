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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ObjectUtils;

public final class CFMetaData
{
    public final static double DEFAULT_KEY_CACHE_SIZE = 0.1;
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;

    private static final AtomicInteger idGen = new AtomicInteger(0);
    
    private static final Map<Pair<String, String>, Integer> cfIdMap = new HashMap<Pair<String, String>, Integer>();
    
    public static final Map<Pair<String, String>, Integer> getCfIdMap()
    {
        return Collections.unmodifiableMap(cfIdMap);    
    }
    
    // this gets called after initialization to make sure that id generation happens properly.
    public static final void fixMaxId()
    {
        // never set it to less than 1000. this ensures that we have enough system CFids for future use.
        idGen.set(Math.max(Collections.max(cfIdMap.values()) + 1, 1000));
    }
    
    public final String tableName;            // name of table which has this column family
    public final String cfName;               // name of the column family
    public final String columnType;           // type: super, standard, etc.
    public final AbstractType comparator;       // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator; // like comparator, for supercolumns
    public final String comment; // for humans only
    public final double rowCacheSize; // default 0
    public final double keyCacheSize; // default 0.01
    public final int cfId;

    private CFMetaData(String tableName, String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, double keyCacheSize, int cfId)
    {
        this.tableName = tableName;
        this.cfName = cfName;
        this.columnType = columnType;
        this.comparator = comparator;
        this.subcolumnComparator = subcolumnComparator;
        this.comment = comment;
        this.rowCacheSize = rowCacheSize;
        this.keyCacheSize = keyCacheSize;
        this.cfId = cfId;
    }
    
    public CFMetaData(String tableName, String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, double keyCacheSize)
    {
        this(tableName, cfName, columnType, comparator, subcolumnComparator, comment, rowCacheSize, keyCacheSize, nextId());
        cfIdMap.put(new Pair<String, String>(tableName, cfName), cfId);
    }

    // a quick and dirty pretty printer for describing the column family...
    public String pretty()
    {
        return tableName + "." + cfName + "\n"
               + "Column Family Type: " + columnType + "\n"
               + "Columns Sorted By: " + comparator + "\n";
    }

    public static byte[] serialize(CFMetaData cfm) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeUTF(cfm.tableName);
        dout.writeUTF(cfm.cfName);
        dout.writeUTF(cfm.columnType);
        dout.writeUTF(cfm.comparator.getClass().getName());
        dout.writeBoolean(cfm.subcolumnComparator != null);
        if (cfm.subcolumnComparator != null)
            dout.writeUTF(cfm.subcolumnComparator.getClass().getName());
        dout.writeBoolean(cfm.comment != null);
        if (cfm.comment != null)
            dout.writeUTF(cfm.comment);
        dout.writeDouble(cfm.rowCacheSize);
        dout.writeDouble(cfm.keyCacheSize);
        dout.writeInt(cfm.cfId);
        dout.close();
        return bout.toByteArray();
    }

    public static CFMetaData deserialize(InputStream in) throws IOException
    {

        DataInputStream din = new DataInputStream(in);
        String tableName = din.readUTF();
        String cfName = din.readUTF();
        String columnType = din.readUTF();
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
        double keyCacheSize = din.readDouble();
        int cfId = din.readInt();
        return new CFMetaData(tableName, cfName, columnType, comparator, subcolumnComparator, comment, rowCacheSize, keyCacheSize, cfId);
    }

    public boolean equals(Object obj)
    {
        if (!(obj instanceof CFMetaData))
            return false;
        CFMetaData other = (CFMetaData)obj;
        return other.tableName.equals(tableName)
                && other.cfName.equals(cfName)
                && other.columnType.equals(columnType)
                && other.comparator.equals(comparator)
                && ObjectUtils.equals(other.subcolumnComparator, subcolumnComparator)
                && ObjectUtils.equals(other.comment, comment)
                && other.rowCacheSize == rowCacheSize
                && other.keyCacheSize == keyCacheSize
                && other.cfId == cfId;
    }
    
    private static int nextId() 
    {
        return idGen.getAndIncrement();
    }
}
