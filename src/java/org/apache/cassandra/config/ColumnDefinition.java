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
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.collect.Maps;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.json;

public class ColumnDefinition
{
    public final ByteBuffer name;
    private AbstractType<?> validator;
    private IndexType index_type;
    private Map<String,String> index_options;
    private String index_name;

    public ColumnDefinition(ByteBuffer name, AbstractType<?> validator, IndexType index_type, Map<String, String> index_options, String index_name)
    {
        assert name != null && validator != null;
        this.name = name;
        this.index_name = index_name;
        this.validator = validator;

        this.setIndexType(index_type, index_options);
    }

    public static ColumnDefinition ascii(String name)
    {
        return new ColumnDefinition(ByteBufferUtil.bytes(name), AsciiType.instance, null, null, null);
    }

    public static ColumnDefinition bool(String name)
    {
        return new ColumnDefinition(ByteBufferUtil.bytes(name), BooleanType.instance, null, null, null);
    }

    public static ColumnDefinition utf8(String name)
    {
        return new ColumnDefinition(ByteBufferUtil.bytes(name), UTF8Type.instance, null, null, null);
    }

    public static ColumnDefinition int32(String name)
    {
        return new ColumnDefinition(ByteBufferUtil.bytes(name), Int32Type.instance, null, null, null);
    }

    public static ColumnDefinition double_(String name)
    {
        return new ColumnDefinition(ByteBufferUtil.bytes(name), DoubleType.instance, null, null, null);
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

    public ColumnDef toThrift()
    {
        ColumnDef cd = new ColumnDef();

        cd.setName(ByteBufferUtil.clone(name));
        cd.setValidation_class(validator.toString());

        cd.setIndex_type(index_type == null
                            ? null
                            : IndexType.valueOf(index_type.name()));
        cd.setIndex_name(index_name == null ? null : index_name);
        cd.setIndex_options(index_options == null ? null : Maps.newHashMap(index_options));

        return cd;
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

    /**
     * Drop specified column from the schema using given row.
     *
     * @param rm         The schema row mutation
     * @param cfName     The name of the parent ColumnFamily
     * @param timestamp  The timestamp to use for column modification
     */
    public void deleteFromSchema(RowMutation rm, String cfName, AbstractType<?> comparator, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_COLUMNS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "validator"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_type"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_options"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_name"));
    }

    public void toSchema(RowMutation rm, String cfName, AbstractType<?> comparator, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_COLUMNS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(Column.create(validator.toString(), timestamp, cfName, comparator.getString(name), "validator"));
        cf.addColumn(index_type == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_type")
                                        : Column.create(index_type.toString(), timestamp, cfName, comparator.getString(name), "index_type"));
        cf.addColumn(index_options == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_options")
                                           : Column.create(json(index_options), timestamp, cfName, comparator.getString(name), "index_options"));
        cf.addColumn(index_name == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), "index_name")
                                        : Column.create(index_name, timestamp, cfName, comparator.getString(name), "index_name"));
    }

    public void apply(ColumnDefinition def, AbstractType<?> comparator)  throws ConfigurationException
    {
        if (getIndexType() != null && def.getIndexType() != null)
        {
            // If an index is set (and not drop by this update), the validator shouldn't be change to a non-compatible one
            if (!def.getValidator().isCompatibleWith(getValidator()))
                throw new ConfigurationException(String.format("Cannot modify validator to a non-compatible one for column %s since an index is set", comparator.getString(name)));

            assert getIndexName() != null;
            if (!getIndexName().equals(def.getIndexName()))
                throw new ConfigurationException("Cannot modify index name");
        }

        setValidator(def.getValidator());
        setIndexType(def.getIndexType(), def.getIndexOptions());
        setIndexName(def.getIndexName());
    }

    /**
     * Deserialize columns from low-level representation
     *
     * @return Thrift-based deserialized representation of the column
     * @param row
     */
    public static List<ColumnDefinition> fromSchema(Row row, AbstractType<?> comparator)
    {
        if (row.cf == null)
            return Collections.emptyList();

        List<ColumnDefinition> cds = new ArrayList<ColumnDefinition>();
        for (UntypedResultSet.Row result : QueryProcessor.resultify("SELECT * FROM system.schema_columns", row))
        {
            try
            {
                IndexType index_type = null;
                Map<String,String> index_options = null;
                String index_name = null;

                if (result.has("index_type"))
                    index_type = IndexType.valueOf(result.getString("index_type"));
                if (result.has("index_options"))
                    index_options = FBUtilities.fromJsonMap(result.getString("index_options"));
                if (result.has("index_name"))
                    index_name = result.getString("index_name");

                cds.add(new ColumnDefinition(comparator.fromString(result.getString("column")),
                                             TypeParser.parse(result.getString("validator")),
                                             index_type,
                                             index_options,
                                             index_name));
            }
            catch (ConfigurationException e)
            {
                throw new RuntimeException(e);
            }
        }

        return cds;
    }

    public static Row readSchema(String ksName, String cfName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(SystemTable.getSchemaKSKey(ksName));
        ColumnFamilyStore columnsStore = SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNS_CF);
        ColumnFamily cf = columnsStore.getColumnFamily(key,
                                                       new QueryPath(SystemTable.SCHEMA_COLUMNS_CF),
                                                       DefsTable.searchComposite(cfName, true),
                                                       DefsTable.searchComposite(cfName, false),
                                                       false,
                                                       Integer.MAX_VALUE);
        return new Row(key, cf);
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

    public void setIndexType(IndexType index_type, Map<String,String> index_options)
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

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    public void setValidator(AbstractType<?> validator)
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
}
