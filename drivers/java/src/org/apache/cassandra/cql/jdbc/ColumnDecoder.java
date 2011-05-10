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
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes columns from bytes into instances of their respective expected types.
 */
class ColumnDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnDecoder.class);

    private final Map<String, CFMetaData> metadata = new HashMap<String, CFMetaData>();

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
                    metadata.put(String.format("%s.%s", ks.getName(), cf.getName()), CFMetaData.fromThrift(cf));
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

    AbstractType getComparator(String keyspace, String columnFamily)
    {
        return metadata.get(String.format("%s.%s", keyspace, columnFamily)).comparator;
    }

    AbstractType getNameType(String keyspace, String columnFamily, ByteBuffer name)
    {

        CFMetaData md = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        try
        {
            if (ByteBufferUtil.string(name).equalsIgnoreCase(ByteBufferUtil.string(md.getKeyName())))
                return AsciiType.instance;
        }
        catch (CharacterCodingException e)
        {
            // not be the key name
        }
        return md.comparator;
    }

    AbstractType getValueType(String keyspace, String columnFamily, ByteBuffer name)
    {
        CFMetaData md = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        try
        {
            if (ByteBufferUtil.string(name).equalsIgnoreCase(ByteBufferUtil.string(md.getKeyName())))
                return md.getKeyValidator();
        }
        catch (CharacterCodingException e)
        {
            // not be the key name
        }
        ColumnDefinition cd = md.getColumnDefinition(name);
        return cd == null ? md.getDefaultValidator() : cd.getValidator();
    }

    public AbstractType getKeyValidator(String keyspace, String columnFamily)
    {
        return metadata.get(String.format("%s.%s", keyspace, columnFamily)).getKeyValidator();
    }

    /** uses the AbstractType to map a column name to a string. */
    public String colNameAsString(String keyspace, String columnFamily, ByteBuffer name)
    {
        AbstractType comparator = getNameType(keyspace, columnFamily, name);
        return comparator.getString(name);
    }

    /** constructs a typed column */
    public TypedColumn makeCol(String keyspace, String columnFamily, Column column)
    {
        return new TypedColumn(column,
                               getNameType(keyspace, columnFamily, column.name),
                               getValueType(keyspace, columnFamily, column.name));
    }
}
