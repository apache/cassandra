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
    public final static String D_COLDEF_INDEXTYPE = "KEYS";
    public final static String D_COLDEF_INDEXNAME = null;
    public final ByteBuffer name;
    private AbstractType validator;
    private IndexType index_type;
    private String index_name;

    public ColumnDefinition(ByteBuffer name, AbstractType validator, IndexType index_type, String index_name)
    {
        this.name = name;
        this.index_type = index_type;
        this.index_name = index_name;
        this.validator = validator;
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
        if (index_type != null ? !index_type.equals(that.index_type) : that.index_type != null)
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
        result = 31 * result + (index_name != null ? index_name.hashCode() : 0);
        return result;
    }

    public org.apache.cassandra.db.migration.avro.ColumnDef deflate()
    {
        org.apache.cassandra.db.migration.avro.ColumnDef cd = new org.apache.cassandra.db.migration.avro.ColumnDef();
        cd.name = name;
        cd.validation_class = new Utf8(validator.toString());
        cd.index_type = index_type == null ? null :
            Enum.valueOf(org.apache.cassandra.db.migration.avro.IndexType.class, index_type.name());
        cd.index_name = index_name == null ? null : new Utf8(index_name);
        return cd;
    }

    public static ColumnDefinition inflate(org.apache.cassandra.db.migration.avro.ColumnDef cd)
    {
        IndexType index_type = cd.index_type == null ? null :
            Enum.valueOf(IndexType.class, cd.index_type.name());
        String index_name = cd.index_name == null ? null : cd.index_name.toString();
        try
        {
            AbstractType validatorType = TypeParser.parse(cd.validation_class);
            return new ColumnDefinition(cd.name, validatorType, index_type, index_name);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ColumnDefinition fromColumnDef(ColumnDef thriftColumnDef) throws ConfigurationException
    {
        AbstractType validatorType = TypeParser.parse(thriftColumnDef.validation_class);
        return new ColumnDefinition(ByteBufferUtil.clone(thriftColumnDef.name), validatorType, thriftColumnDef.index_type, thriftColumnDef.index_name);
    }
    
    public static ColumnDefinition fromColumnDef(org.apache.cassandra.db.migration.avro.ColumnDef avroColumnDef) throws ConfigurationException
    {
        validateIndexType(avroColumnDef);
        AbstractType validatorType = TypeParser.parse(avroColumnDef.validation_class);
        return new ColumnDefinition(avroColumnDef.name,
                validatorType,
                IndexType.valueOf(avroColumnDef.index_type == null ? D_COLDEF_INDEXTYPE : avroColumnDef.index_type.name()),
                avroColumnDef.index_name == null ? D_COLDEF_INDEXNAME : avroColumnDef.index_name.toString());
    }

    public static Map<ByteBuffer, ColumnDefinition> fromColumnDef(List<ColumnDef> thriftDefs) throws ConfigurationException
    {
        if (thriftDefs == null)
            return new HashMap<ByteBuffer,ColumnDefinition>();

        Map<ByteBuffer, ColumnDefinition> cds = new TreeMap<ByteBuffer, ColumnDefinition>();
        for (ColumnDef thriftColumnDef : thriftDefs)
            cds.put(ByteBufferUtil.clone(thriftColumnDef.name), fromColumnDef(thriftColumnDef));

        return cds;
    }
    
    public static void validateIndexType(org.apache.cassandra.db.migration.avro.ColumnDef avroColumnDef) throws ConfigurationException
    {
        if ((avroColumnDef.index_name != null) && (avroColumnDef.index_type == null))
            throw new ConfigurationException("index_name cannot be set if index_type is not also set");
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


    public IndexType getIndexType()
    {
        return index_type;
    }

    public void setIndexType(IndexType index_type)
    {
        this.index_type = index_type;
    }

    public AbstractType getValidator()
    {
        return validator;
    }

    public void setValidator(AbstractType validator)
    {
        this.validator = validator;
    }
}
