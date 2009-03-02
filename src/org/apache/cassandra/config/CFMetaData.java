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

public class CFMetaData
{
    public String tableName;            // name of table which has this column family
    public String cfName;               // name of the column family
    public String columnType;           // type: super, standard, etc.
    public String indexProperty_;       // name sorted, time stamp sorted etc. 

    // The user chosen names (n_) for various parts of data in a column family.
    // CQL queries, for instance, will refer to/extract data within a column
    // family using these logical names.
    public String n_rowKey;               
    public String n_superColumnMap;     // only used if this is a super column family
    public String n_superColumnKey;     // only used if this is a super column family
    public String n_columnMap;
    public String n_columnKey;
    public String n_columnValue;
    public String n_columnTimestamp;
 
    // a quick and dirty pretty printer for describing the column family...
    public String pretty()
    {
        String desc;
        desc = n_columnMap + "(" + n_columnKey + ", " + n_columnValue + ", " + n_columnTimestamp + ")";
        if ("Super".equals(columnType))
        {
            desc = n_superColumnMap + "(" + n_superColumnKey + ", " + desc + ")"; 
        }
        desc = tableName + "." + cfName + "(" + n_rowKey + ", " + desc + ")\n";
        
        desc += "Column Family Type: " + columnType + "\n" +
                "Columns Sorted By: " + indexProperty_ + "\n";
        return desc;
    }
}
