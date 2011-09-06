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

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.sql.SQLNonTransientException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes columns from bytes into instances of their respective expected types.
 */
class ColumnDecoder
{
    public final static ByteBuffer DEFAULT_KEY_NAME = ByteBufferUtil.bytes("KEY");
//    private static final Logger logger = LoggerFactory.getLogger(ColumnDecoder.class);

    private class CFamMeta
    {
        String comparator;
        String defaultValidator;
        ByteBuffer keyAlias;
        String keyValidator;
        Map<ByteBuffer, String> columnMeta = new HashMap<ByteBuffer, String>();
        
        private CFamMeta(CfDef cf)
        {
            comparator = cf.getComparator_type();
            defaultValidator = cf.getDefault_validation_class();
            keyAlias = cf.key_alias;
            keyValidator = cf.getKey_validation_class();
            
            for (ColumnDef colDef : cf.getColumn_metadata())
                columnMeta.put(colDef.name, colDef.getValidation_class());
        }
    }

    private final Map<String, CFamMeta> metadata = new HashMap<String, CFamMeta>();

    /**
     * is specific per set of keyspace definitions.
     */
    public ColumnDecoder(List<KsDef> defs)
    {
        for (KsDef ks : defs)
            for (CfDef cf : ks.getCf_defs())
                metadata.put(String.format("%s.%s", ks.getName(), cf.getName()), new CFamMeta(cf));
    }

    protected AbstractJdbcType<?> getComparator(String keyspace, String columnFamily)
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractJdbcType<?> type = (cf != null) ? TypesMap.getTermForComparator(cf.comparator) : null;
        return (type == null) ? null : type;
    }

    private AbstractJdbcType<?> getNameType(String keyspace, String columnFamily, ByteBuffer name)
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        try
        {
            if (ByteBufferUtil.string(name).equalsIgnoreCase(ByteBufferUtil.string(cf.keyAlias)))
                return AsciiTerm.instance;
        }
        catch (CharacterCodingException e)
        {
            // not be the key name
        }
        return TypesMap.getTermForComparator(cf.comparator);
    }

    private AbstractJdbcType<?> getValueType(String keyspace, String columnFamily, ByteBuffer name)
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        if (cf == null)
            return null;
        
        try
        {
            if (ByteBufferUtil.string(name).equalsIgnoreCase(ByteBufferUtil.string(cf.keyAlias)))
                return TypesMap.getTermForComparator(cf.keyValidator);
        }
        catch (CharacterCodingException e)
        {
            // not be the key name
        }
        
        AbstractJdbcType<?> type = TypesMap.getTermForComparator(cf.columnMeta.get(name));
        return (type != null) ? type : TypesMap.getTermForComparator(cf.defaultValidator);
    }

    public AbstractJdbcType<?> getKeyValidator(String keyspace, String columnFamily)
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        AbstractJdbcType<?> type = (cf != null) ? TypesMap.getTermForComparator(cf.keyValidator) : null;
        return (type == null) ? null : type;
    }

    /** uses the AbstractType to map a column name to a string. */
    public String colNameAsString(String keyspace, String columnFamily, ByteBuffer name)
    {
        AbstractJdbcType<?> comparator = getNameType(keyspace, columnFamily, name);
        return comparator.getString(name);
    }

    /** constructs a typed column */
    public TypedColumn makeCol(String keyspace, String columnFamily, Column column)
    {
        return new TypedColumn(column,
                               getNameType(keyspace, columnFamily, column.name),
                               getValueType(keyspace, columnFamily, column.name));
    }

    /** constructs a typed column to hold the key
     * @throws SQLNonTransientException */
    public TypedColumn makeKeyColumn(String keyspace, String columnFamily, byte[] key) throws SQLNonTransientException
    {
        CFamMeta cf = metadata.get(String.format("%s.%s", keyspace, columnFamily));
        if (cf == null)
            throw new SQLNonTransientException(String.format("could not find decoder metadata for: %s.%s",
                                                                       keyspace,
                                                                       columnFamily));

        Column column = new Column(cf.keyAlias).setValue(key).setTimestamp(-1);
        return new TypedColumn(column,
                               getNameType(keyspace, columnFamily, (cf.keyAlias != null) ? cf.keyAlias : DEFAULT_KEY_NAME),
                               getValueType(keyspace, columnFamily, (cf.keyAlias != null) ? cf.keyAlias : DEFAULT_KEY_NAME));
    }
}
