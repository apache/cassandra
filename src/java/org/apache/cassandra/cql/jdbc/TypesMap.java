package org.apache.cassandra.cql.jdbc;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.HashMap;
import java.util.Map;

public class TypesMap
{
    private final static Map<String, AbstractJdbcType<?>> map = new HashMap<String, AbstractJdbcType<?>>();
    
    static
    {
        map.put("org.apache.cassandra.db.marshal.AsciiType", JdbcAscii.instance);
        map.put("org.apache.cassandra.db.marshal.BooleanType", JdbcBoolean.instance);
        map.put("org.apache.cassandra.db.marshal.BytesType", JdbcBytes.instance);
        map.put("org.apache.cassandra.db.marshal.CounterColumnType", JdbcCounterColumn.instance);
        map.put("org.apache.cassandra.db.marshal.DateType", JdbcDate.instance);
        map.put("org.apache.cassandra.db.marshal.DecimalType", JdbcDecimal.instance);
        map.put("org.apache.cassandra.db.marshal.DoubleType", JdbcDouble.instance);
        map.put("org.apache.cassandra.db.marshal.FloatType", JdbcFloat.instance);
        map.put("org.apache.cassandra.db.marshal.Int32Type", JdbcInt32.instance);
        map.put("org.apache.cassandra.db.marshal.IntegerType", JdbcInteger.instance);
        map.put("org.apache.cassandra.db.marshal.LexicalUUIDType", JdbcLexicalUUID.instance);
        map.put("org.apache.cassandra.db.marshal.LongType", JdbcLong.instance);
        map.put("org.apache.cassandra.db.marshal.TimeUUIDType", JdbcTimeUUID.instance);
        map.put("org.apache.cassandra.db.marshal.UTF8Type", JdbcUTF8.instance);
        map.put("org.apache.cassandra.db.marshal.UUIDType", JdbcUUID.instance);
    }
    
    public static AbstractJdbcType<?> getTypeForComparator(String comparator)
    {
        // If not fully qualified, assume it's the short name for a built-in.
        if ((comparator != null) && (!comparator.contains(".")))
            return map.get("org.apache.cassandra.db.marshal." + comparator);
        return map.get(comparator);
    }
}
