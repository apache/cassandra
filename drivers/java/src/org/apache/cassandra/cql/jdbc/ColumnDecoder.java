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


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes columns from bytes into instances of their respective expected types.
 */
class ColumnDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnDecoder.class);

    // basically denotes column or value.
    enum Specifier
    {
        Comparator,
        Validator,
        ColumnSpecific
    }

    private Map<String, CFMetaData> metadata = new HashMap<String, CFMetaData>();

    /**
     * is specific per set of keyspace definitions.
     */
    public ColumnDecoder(List<KsDef> defs)
    {
        for (KsDef ks : defs)
        {
            for (CfDef cf : ks.getCf_defs())
            {
                try
                {
                    metadata.put(String.format("%s.%s", ks.getName(), cf.getName()), CFMetaData.convertToCFMetaData(cf));
                }
                catch (InvalidRequestException e)
                {
                    throw new RuntimeException(e);
                }
                catch (ConfigurationException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * @param keyspace     ALWAYS specify
     * @param columnFamily ALWAYS specify
     * @param specifier    ALWAYS specify
     * @param def          avoids additional map lookup if specified. null is ok though.
     * @return
     */
    AbstractType getComparator(String keyspace, String columnFamily, Specifier specifier, CFMetaData def)
    {
        return getComparator(keyspace, columnFamily, null, specifier, def);
    }

    // same as above, but can get column-specific validators.
    AbstractType getComparator(String keyspace, String columnFamily, byte[] column, Specifier specifier, CFMetaData def)
    {
        if (def == null)
            def = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        if (def == null)
            // no point in proceeding. these values are bad.
            throw new AssertionError();
        switch (specifier)
        {
            case ColumnSpecific:
                return def.getValueValidator(ByteBuffer.wrap(column));
            case Validator:
                return def.getDefaultValidator();
            case Comparator:
                return def.comparator;
            default:
                throw new AssertionError();
        }
    }

    /**
     * uses the AbstractType to map a column name to a string.
     *
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
     * constructs a typed column
     */
    public TypedColumn makeCol(String keyspace, String columnFamily, Column column)
    {
        CFMetaData cfDef = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractType comparator = getComparator(keyspace, columnFamily, Specifier.Comparator, cfDef);
        AbstractType validator = getComparator(keyspace, columnFamily, column.getName(), Specifier.ColumnSpecific, null);
        return new TypedColumn(column, comparator, validator);
    }

    public AbstractType getKeyValidator(String keyspace, String columnFamily)
    {
        CFMetaData def = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        return def.getKeyValidator();
    }
}
