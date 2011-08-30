package org.apache.cassandra.config;
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


import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.util.Utf8;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnDefinition
{
    
    public final ByteBuffer name;
    private AbstractType validator;
    private IndexType index_type;
    private Map<String,String> index_options;
    private String index_name;
    
    public ColumnDefinition(ByteBuffer name, AbstractType validator, IndexType index_type, Map<String, String> index_options, String index_name) throws ConfigurationException
    {
        this.name = name;
        this.index_name = index_name;
        this.validator = validator;
    
        this.setIndexType(index_type, index_options);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ColumnDefinition that = (ColumnDefinition) o;
        if (index_name != null ? !index_name.equals(that.index_name) : that.index_name != null)
            return false;
        if (index_type != that.index_type)
            return false;
        if (index_options != null ? !index_options.equals(that.index_options) : that.index_options != null)
            return false;
        if (!name.equals(that.name))
            return false;
        return !(validator != null ? !validator.equals(that.validator) : that.validator != null);
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (validator != null ? validator.hashCode() : 0);
        result = 31 * result + (index_type != null ? index_type.hashCode() : 0);
        result = 31 * result + (index_options != null ? index_options.hashCode() : 0);
        result = 31 * result + (index_name != null ? index_name.hashCode() : 0);
        return result;
    }

    public org.apache.cassandra.db.migration.avro.ColumnDef toAvro()
    {
        org.apache.cassandra.db.migration.avro.ColumnDef cd = new org.apache.cassandra.db.migration.avro.ColumnDef();
        cd.name = ByteBufferUtil.clone(name);
        cd.validation_class = new Utf8(validator.toString());
        cd.index_type = index_type == null
                      ? null
                      : org.apache.cassandra.db.migration.avro.IndexType.valueOf(index_type.name());
        cd.index_name = index_name == null ? null : new Utf8(index_name);
        cd.index_options = getCharSequenceMap(index_options);
        return cd;
    }

    public static ColumnDefinition fromAvro(org.apache.cassandra.db.migration.avro.ColumnDef cd)
    {
        IndexType index_type = cd.index_type == null ? null : Enum.valueOf(IndexType.class, cd.index_type.name());
        String index_name = cd.index_name == null ? null : cd.index_name.toString();
        try
        {
            AbstractType validatorType = TypeParser.parse(cd.validation_class);
            return new ColumnDefinition(ByteBufferUtil.clone(cd.name), validatorType, index_type, getStringMap(cd.index_options), index_name);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ColumnDefinition fromThrift(ColumnDef thriftColumnDef) throws ConfigurationException
    {
        return new ColumnDefinition(ByteBufferUtil.clone(thriftColumnDef.name),
                                    TypeParser.parse(thriftColumnDef.validation_class),
                                    thriftColumnDef.index_type,
                                    thriftColumnDef.index_options,
                                    thriftColumnDef.index_name);
    }

    public static Map<ByteBuffer, ColumnDefinition> fromThrift(List<ColumnDef> thriftDefs) throws ConfigurationException
    {
        if (thriftDefs == null)
            return new HashMap<ByteBuffer,ColumnDefinition>();

        Map<ByteBuffer, ColumnDefinition> cds = new TreeMap<ByteBuffer, ColumnDefinition>();
        for (ColumnDef thriftColumnDef : thriftDefs)
            cds.put(ByteBufferUtil.clone(thriftColumnDef.name), fromThrift(thriftColumnDef));

        return cds;
    }

    @Override
    public String toString()
    {
        return "ColumnDefinition{" +
               "name=" + ByteBufferUtil.bytesToHex(name) +
               ", validator=" + validator +
               ", index_type=" + index_type +
               ", index_name='" + index_name + '\'' +
               '}';
    }

    public String getIndexName()
    {
        return index_name;
    }
    
    public void setIndexName(String s)
    {
        index_name = s;
    }

    public void setIndexType(IndexType index_type, Map<String,String> index_options) throws ConfigurationException
    {
        this.index_type = index_type;
        this.index_options = index_options;         
    }

    public IndexType getIndexType()
    {
        return index_type;
    }
    
    public Map<String,String> getIndexOptions()
    {
        return index_options;
    }
    
    public AbstractType getValidator()
    {
        return validator;
    }

    public void setValidator(AbstractType validator)
    {
        this.validator = validator;
    }
    
    public static Map<String,String> getStringMap(Map<CharSequence, CharSequence> charMap)
    {
        if (charMap == null)
            return null;
        
        Map<String,String> stringMap = new HashMap<String, String>();
            
        for (Map.Entry<CharSequence, CharSequence> entry : charMap.entrySet())        
            stringMap.put(entry.getKey().toString(), entry.getValue().toString());
            
            
        return stringMap;
    }
    
    private static Map<CharSequence, CharSequence> getCharSequenceMap(Map<String,String> stringMap)
    {
        if (stringMap == null)
            return null;
        
        Map<CharSequence, CharSequence> charMap = new HashMap<CharSequence, CharSequence>();
        
        for (Map.Entry<String, String> entry : stringMap.entrySet())
            charMap.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()));
        
        return charMap;
    }
}
