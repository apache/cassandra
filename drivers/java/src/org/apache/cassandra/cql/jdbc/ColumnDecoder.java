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


import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Decodes columns from bytes into instances of their respective expected types. */
class ColumnDecoder 
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnDecoder.class);
    private static final String MapFormatString = "%s.%s.%s";
    
    // basically denotes column or value.
    enum Specifier
    {
        Comparator,
        Validator,
        Key
    }
    
    private Map<String, CfDef> cfDefs = new HashMap<String, CfDef>();
    
    // cache the comparators for efficiency.
    private Map<String, AbstractType> comparators = new HashMap<String, AbstractType>();
    
    /** is specific per set of keyspace definitions. */
    public ColumnDecoder(List<KsDef> defs)
    {
        for (KsDef ks : defs) 
            for (CfDef cf : ks.getCf_defs())
                cfDefs.put(String.format("%s.%s", ks.getName(), cf.getName()), cf);
    }

    /**
     * @param keyspace ALWAYS specify
     * @param columnFamily ALWAYS specify
     * @param specifier ALWAYS specify
     * @param def avoids additional map lookup if specified. null is ok though.
     * @return
     */
    AbstractType getComparator(String keyspace, String columnFamily, Specifier specifier, CfDef def) 
    {
        // check cache first.
        String key = String.format(MapFormatString, keyspace, columnFamily, specifier.name());
        AbstractType comparator = comparators.get(key);

        // make and put in cache.
        if (comparator == null) 
        {
            if (def == null)
                def = cfDefs.get(String.format("%s.%s", keyspace, columnFamily));
            if (def == null)
                // no point in proceeding. these values are bad.
                return null;
            try 
            {
                switch (specifier)
                {
                    case Key:
                        comparator = FBUtilities.getComparator(def.getKey_validation_class());
                        break;
                    case Validator:
                        comparator = FBUtilities.getComparator(def.getDefault_validation_class());
                        break;
                    case Comparator:
                    default:
                        comparator = FBUtilities.getComparator(def.getComparator_type());
                        break;
                }
                comparators.put(key, comparator);
            }
            catch (ConfigurationException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        return comparator;
    }

    /**
     * uses the AbstractType to map a column name to a string.  Relies on AT.fromString() and AT.getString()
     * @param keyspace
     * @param columnFamily
     * @param name
     * @return
     */
    public String colNameAsString(String keyspace, String columnFamily, String name) 
    {
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, null);
        ByteBuffer bb = comparator.fromString(name);
        return comparator.getString(bb);
    }

    /**
     * uses the AbstractType to map a column name to a string.
     * @param keyspace
     * @param columnFamily
     * @param name
     * @return
     */
    public String colNameAsString(String keyspace, String columnFamily, byte[] name) 
    {
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, null);
        return comparator.getString(ByteBuffer.wrap(name));
    }

    /**
     * converts a column value to a string.
     * @param value
     * @return
     */
    public static String colValueAsString(Object value) {
        if (value instanceof String)
            return (String)value;
        else if (value instanceof byte[])
            return ByteBufferUtil.bytesToHex(ByteBuffer.wrap((byte[])value));
        else
            return value.toString();
    }
    
    /** constructs a typed column */
    public TypedColumn makeCol(String keyspace, String columnFamily, byte[] name, byte[] value)
    {
        CfDef cfDef = cfDefs.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, cfDef);
        AbstractType validator = getComparator(keyspace, columnFamily, Specifier.Validator, null);
        return new TypedColumn(comparator, name, validator, value);
    }
}
