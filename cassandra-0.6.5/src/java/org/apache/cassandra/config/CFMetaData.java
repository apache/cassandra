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

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.FBUtilities;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public final class CFMetaData
{
    public final static double DEFAULT_KEY_CACHE_SIZE = 200000;
    public final static double DEFAULT_ROW_CACHE_SIZE = 0.0;

    public final String tableName;            // name of table which has this column family
    public final String cfName;               // name of the column family
    public final String columnType;           // type: super, standard, etc.
    public final AbstractType comparator;       // name sorted, time stamp sorted etc.
    public final AbstractType subcolumnComparator; // like comparator, for supercolumns
    public final String comment; // for humans only
    public final double rowCacheSize; // default 0
    public final double keyCacheSize; // default 0.01

    CFMetaData(String tableName, String cfName, String columnType, AbstractType comparator, AbstractType subcolumnComparator, String comment, double rowCacheSize, double keyCacheSize)
    {
        this.tableName = tableName;
        this.cfName = cfName;
        this.columnType = columnType;
        this.comparator = comparator;
        this.subcolumnComparator = subcolumnComparator;
        this.comment = comment;
        this.rowCacheSize = rowCacheSize;
        this.keyCacheSize = keyCacheSize;
    }

    // a quick and dirty pretty printer for describing the column family...
    public String pretty()
    {
        return tableName + "." + cfName + "\n"
               + "Column Family Type: " + columnType + "\n"
               + "Columns Sorted By: " + comparator + "\n";
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
                && FBUtilities.equals(other.subcolumnComparator, subcolumnComparator)
                && FBUtilities.equals(other.comment, comment)
                && other.rowCacheSize == rowCacheSize
                && other.keyCacheSize == keyCacheSize;
    }
}
