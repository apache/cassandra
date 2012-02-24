/*
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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.migration.MigrationHelper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.migration.MigrationHelper.*;

public class ColumnDefinition
{
    public final ByteBuffer name;
    private AbstractType<?> validator;
    private IndexType index_type;
    private Map<String,String> index_options;
    private String index_name;

    public ColumnDefinition(ByteBuffer name, AbstractType<?> validator, IndexType index_type, Map<String, String> index_options, String index_name)
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

    @Deprecated
    public static ColumnDefinition fromAvro(org.apache.cassandra.db.migration.avro.ColumnDef cd)
    {
        IndexType index_type = cd.index_type == null ? null : Enum.valueOf(IndexType.class, cd.index_type.name());
        String index_name = cd.index_name == null ? null : cd.index_name.toString();
        try
        {
            AbstractType<?> validatorType = TypeParser.parse(cd.validation_class);
            return new ColumnDefinition(ByteBufferUtil.clone(cd.name), validatorType, index_type, getStringMap(cd.index_options), index_name);
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
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

    public static Map<ByteBuffer, ColumnDef> toMap(List<ColumnDef> columnDefs)
    {
        Map<ByteBuffer, ColumnDef> map = new HashMap<ByteBuffer, ColumnDef>();

        if (columnDefs == null)
            return map;

        for (ColumnDef columnDef : columnDefs)
            map.put(columnDef.name, columnDef);

        return map;
    }

    /**
     * Drop specified column from the schema using given row mutation.
     *
     * @param mutation   The schema row mutation
     * @param cfName     The name of the parent ColumnFamily
     * @param comparator The comparator to serialize column name in human-readable format
     * @param columnName The column name as String
     * @param timestamp  The timestamp to use for column modification
     */
    public static void deleteFromSchema(RowMutation mutation, String cfName, AbstractType comparator, ByteBuffer columnName, long timestamp)
    {
        toSchema(mutation, comparator, cfName, columnName, null, timestamp, true);
    }

    /**
     * Add new/update column to/in the schema.
     *
     * @param mutation   The schema row mutation
     * @param cfName     The name of the parent ColumnFamily
     * @param comparator The comparator to serialize column name in human-readable format
     * @param columnDef  The Thrift-based column definition that contains all attributes
     * @param timestamp  The timestamp to use for column modification
     */
    public static void addToSchema(RowMutation mutation, String cfName, AbstractType comparator, ColumnDef columnDef, long timestamp)
    {
        toSchema(mutation, comparator, cfName, columnDef.name, columnDef, timestamp, false);
    }

    /**
     * Serialize given ColumnDef into given schema row mutation to add or drop it.
     *
     * @param mutation   The mutation to use for serialization
     * @param comparator The comparator to serialize column name in human-readable format
     * @param cfName     The name of the parent ColumnFamily
     * @param columnName The column name as String
     * @param columnDef  The Thrift-based column definition that contains all attributes
     * @param timestamp  The timestamp to use for column modification
     * @param delete     The flag which indicates if column should be deleted or added to the schema
     */
    private static void toSchema(RowMutation mutation, AbstractType comparator, String cfName, ByteBuffer columnName, ColumnDef columnDef, long timestamp, boolean delete)
    {
        for (ColumnDef._Fields field : ColumnDef._Fields.values())
        {
            QueryPath path = new QueryPath(SystemTable.SCHEMA_COLUMNS_CF,
                                           null,
                                           compositeNameFor(cfName,
                                                            readableColumnName(columnName, comparator),
                                                            field.getFieldName()));

            if (delete)
                mutation.delete(path, timestamp);
            else
                mutation.add(path, valueAsBytes(columnDef.getFieldValue(field)), timestamp);
        }
    }

    public static ColumnFamily readSchema(String ksName, String cfName)
    {
        DecoratedKey key = StorageService.getPartitioner().decorateKey(SystemTable.getSchemaKSKey(ksName));
        ColumnFamilyStore columnsStore = SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNS_CF);
        return columnsStore.getColumnFamily(key,
                                            new QueryPath(SystemTable.SCHEMA_COLUMNS_CF),
                                            MigrationHelper.searchComposite(cfName, true),
                                            MigrationHelper.searchComposite(cfName, false),
                                            false,
                                            Integer.MAX_VALUE);
    }

    /**
     * Deserialize columns from low-level representation
     *
     * @return Thrift-based deserialized representation of the column
     */
    public static List<ColumnDef> fromSchema(ColumnFamily columns)
    {

        if (columns == null || columns.isEmpty())
            return Collections.emptyList();

        // contenders to be a valid columns, re-check is done after all attributes
        // were read from serialized state, if ColumnDef has all required fields it gets promoted to be returned
        Map<String, ColumnDef> contenders = new HashMap<String, ColumnDef>();

        for (IColumn column : columns.getSortedColumns())
        {
            if (column.isMarkedForDelete())
                continue;

            // column name format <cf>:<column name>:<attribute name>
            String[] components = columns.getComparator().getString(column.name()).split(":");
            assert components.length == 3;

            ColumnDef columnDef = contenders.get(components[1]);

            if (columnDef == null)
            {
                columnDef = new ColumnDef();
                contenders.put(components[1], columnDef);
            }

            ColumnDef._Fields field = ColumnDef._Fields.findByName(components[2]);
            columnDef.setFieldValue(field, deserializeValue(column.value(), getValueClass(ColumnDef.class, field.getFieldName())));
        }

        List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();

        for (ColumnDef columnDef : contenders.values())
        {
            if (columnDef.isSetName() && columnDef.isSetValidation_class())
                columnDefs.add(columnDef);
        }

        return columnDefs;
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
