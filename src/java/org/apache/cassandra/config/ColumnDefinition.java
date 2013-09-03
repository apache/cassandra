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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.json;

public class ColumnDefinition
{
    // system.schema_columns column names
    private static final String COLUMN_NAME = "column_name";
    private static final String VALIDATOR = "validator";
    private static final String INDEX_TYPE = "index_type";
    private static final String INDEX_OPTIONS = "index_options";
    private static final String INDEX_NAME = "index_name";
    private static final String COMPONENT_INDEX = "component_index";
    private static final String TYPE = "type";

    /*
     * The type of CQL3 column this definition represents.
     * There is 3 main type of CQL3 columns: those parts of the partition key,
     * those parts of the clustering key and the other, regular ones.
     * But when COMPACT STORAGE is used, there is by design only one regular
     * column, whose name is not stored in the data contrarily to the column of
     * type REGULAR. Hence the COMPACT_VALUE type to distinguish it below.
     *
     * Note that thrift/CQL2 only know about definitions of type REGULAR (and
     * the ones whose componentIndex == null).
     */
    public enum Type
    {
        PARTITION_KEY,
        CLUSTERING_KEY,
        REGULAR,
        COMPACT_VALUE
    }

    public final ByteBuffer name;
    private AbstractType<?> validator;
    private IndexType indexType;
    private Map<String,String> indexOptions;
    private String indexName;
    public final Type type;

    /*
     * If the column comparator is a composite type, indicates to which
     * component this definition refers to. If null, the definition refers to
     * the full column name.
     */
    public final Integer componentIndex;

    public static ColumnDefinition partitionKeyDef(ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(name, validator, componentIndex, Type.PARTITION_KEY);
    }

    public static ColumnDefinition clusteringKeyDef(ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(name, validator, componentIndex, Type.CLUSTERING_KEY);
    }

    public static ColumnDefinition regularDef(ByteBuffer name, AbstractType<?> validator, Integer componentIndex)
    {
        return new ColumnDefinition(name, validator, componentIndex, Type.REGULAR);
    }

    public static ColumnDefinition compactValueDef(ByteBuffer name, AbstractType<?> validator)
    {
        return new ColumnDefinition(name, validator, null, Type.COMPACT_VALUE);
    }

    public ColumnDefinition(ByteBuffer name, AbstractType<?> validator, Integer componentIndex, Type type)
    {
        this(name, validator, null, null, null, componentIndex, type);
    }

    @VisibleForTesting
    public ColumnDefinition(ByteBuffer name,
                            AbstractType<?> validator,
                            IndexType indexType,
                            Map<String, String> indexOptions,
                            String indexName,
                            Integer componentIndex,
                            Type type)
    {
        assert name != null && validator != null;
        this.name = name;
        this.indexName = indexName;
        this.validator = validator;
        this.componentIndex = componentIndex;
        this.setIndexType(indexType, indexOptions);
        this.type = type;
    }

    public ColumnDefinition copy()
    {
        return new ColumnDefinition(name, validator, indexType, indexOptions, indexName, componentIndex, type);
    }

    public ColumnDefinition cloneWithNewName(ByteBuffer newName)
    {
        return new ColumnDefinition(newName, validator, indexType, indexOptions, indexName, componentIndex, type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ColumnDefinition))
            return false;

        ColumnDefinition cd = (ColumnDefinition) o;

        return Objects.equal(name, cd.name)
            && Objects.equal(validator, cd.validator)
            && Objects.equal(componentIndex, cd.componentIndex)
            && Objects.equal(indexName, cd.indexName)
            && Objects.equal(indexType, cd.indexType)
            && Objects.equal(indexOptions, cd.indexOptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, validator, componentIndex, indexName, indexType, indexOptions);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add("name", ByteBufferUtil.bytesToHex(name))
                      .add("validator", validator)
                      .add("type", type)
                      .add("componentIndex", componentIndex)
                      .add("indexName", indexName)
                      .add("indexType", indexType)
                      .toString();
    }

    public boolean isThriftCompatible()
    {
        return type == ColumnDefinition.Type.REGULAR && componentIndex == null;
    }

    public static List<ColumnDef> toThrift(Map<ByteBuffer, ColumnDefinition> columns)
    {
        List<ColumnDef> thriftDefs = new ArrayList<>(columns.size());
        for (ColumnDefinition def : columns.values())
            if (def.type == ColumnDefinition.Type.REGULAR)
                thriftDefs.add(def.toThrift());
        return thriftDefs;
    }

    public ColumnDef toThrift()
    {
        ColumnDef cd = new ColumnDef();

        cd.setName(ByteBufferUtil.clone(name));
        cd.setValidation_class(validator.toString());
        cd.setIndex_type(indexType == null ? null : org.apache.cassandra.thrift.IndexType.valueOf(indexType.name()));
        cd.setIndex_name(indexName == null ? null : indexName);
        cd.setIndex_options(indexOptions == null ? null : Maps.newHashMap(indexOptions));

        return cd;
    }

    public static ColumnDefinition fromThrift(ColumnDef thriftColumnDef, boolean isSuper) throws SyntaxException, ConfigurationException
    {
        // For super columns, the componentIndex is 1 because the ColumnDefinition applies to the column component.
        return new ColumnDefinition(ByteBufferUtil.clone(thriftColumnDef.name),
                                    TypeParser.parse(thriftColumnDef.validation_class),
                                    thriftColumnDef.index_type == null ? null : IndexType.valueOf(thriftColumnDef.index_type.name()),
                                    thriftColumnDef.index_options,
                                    thriftColumnDef.index_name,
                                    isSuper ? 1 : null,
                                    Type.REGULAR);
    }

    public static Map<ByteBuffer, ColumnDefinition> fromThrift(List<ColumnDef> thriftDefs, boolean isSuper) throws SyntaxException, ConfigurationException
    {
        if (thriftDefs == null)
            return new HashMap<>();

        Map<ByteBuffer, ColumnDefinition> cds = new TreeMap<>();
        for (ColumnDef thriftColumnDef : thriftDefs)
            cds.put(ByteBufferUtil.clone(thriftColumnDef.name), fromThrift(thriftColumnDef, isSuper));

        return cds;
    }

    /**
     * Drop specified column from the schema using given row.
     *
     * @param rm         The schema row mutation
     * @param cfName     The name of the parent ColumnFamily
     * @param timestamp  The timestamp to use for column modification
     */
    public void deleteFromSchema(RowMutation rm, String cfName, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(CFMetaData.SchemaColumnsCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        ColumnNameBuilder builder = CFMetaData.SchemaColumnsCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName)).add(name);
        cf.addAtom(new RangeTombstone(builder.build(), builder.buildAsEndOfRange(), timestamp, ldt));
    }

    public void toSchema(RowMutation rm, String cfName, AbstractType<?> comparator, long timestamp)
    {
        ColumnFamily cf = rm.addOrGet(CFMetaData.SchemaColumnsCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(Column.create("", timestamp, cfName, comparator.getString(name), ""));
        cf.addColumn(Column.create(validator.toString(), timestamp, cfName, comparator.getString(name), VALIDATOR));
        cf.addColumn(indexType == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), INDEX_TYPE)
                                       : Column.create(indexType.toString(), timestamp, cfName, comparator.getString(name), INDEX_TYPE));
        cf.addColumn(indexOptions == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), INDEX_OPTIONS)
                                          : Column.create(json(indexOptions), timestamp, cfName, comparator.getString(name), INDEX_OPTIONS));
        cf.addColumn(indexName == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), INDEX_NAME)
                                       : Column.create(indexName, timestamp, cfName, comparator.getString(name), INDEX_NAME));
        cf.addColumn(componentIndex == null ? DeletedColumn.create(ldt, timestamp, cfName, comparator.getString(name), COMPONENT_INDEX)
                                            : Column.create(componentIndex, timestamp, cfName, comparator.getString(name), COMPONENT_INDEX));
        cf.addColumn(Column.create(type.toString().toLowerCase(), timestamp, cfName, comparator.getString(name), TYPE));
    }

    public void apply(ColumnDefinition def, AbstractType<?> comparator)  throws ConfigurationException
    {
        assert type == def.type && Objects.equal(componentIndex, def.componentIndex);

        if (getIndexType() != null && def.getIndexType() != null)
        {
            // If an index is set (and not drop by this update), the validator shouldn't be change to a non-compatible one
            if (!def.getValidator().isCompatibleWith(getValidator()))
                throw new ConfigurationException(String.format("Cannot modify validator to a non-compatible one for column %s since an index is set",
                                                                comparator.getString(name)));

            assert getIndexName() != null;
            if (!getIndexName().equals(def.getIndexName()))
                throw new ConfigurationException("Cannot modify index name");
        }

        setValidator(def.getValidator());
        setIndexType(def.getIndexType(), def.getIndexOptions());
        setIndexName(def.getIndexName());
    }

    /**
     * Deserialize columns from storage-level representation
     *
     * @param serializedColumns storage-level partition containing the column definitions
     * @return the list of processed ColumnDefinitions
     */
    public static List<ColumnDefinition> fromSchema(Row serializedColumns, CFMetaData cfm)
    {
        List<ColumnDefinition> cds = new ArrayList<>();

        String query = String.format("SELECT * FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.SCHEMA_COLUMNS_CF);
        for (UntypedResultSet.Row row : QueryProcessor.resultify(query, serializedColumns))
        {
            Type type = row.has(TYPE)
                      ? Enum.valueOf(Type.class, row.getString(TYPE).toUpperCase())
                      : Type.REGULAR;

            Integer componentIndex = null;
            if (row.has(COMPONENT_INDEX))
                componentIndex = row.getInt(COMPONENT_INDEX);
            else if (type == Type.CLUSTERING_KEY && cfm.isSuper())
                componentIndex = 1; // A ColumnDefinition for super columns applies to the column component

            ByteBuffer name = cfm.getComponentComparator(componentIndex, type).fromString(row.getString(COLUMN_NAME));

            AbstractType<?> validator;
            try
            {
                validator = TypeParser.parse(row.getString(VALIDATOR));
            }
            catch (RequestValidationException e)
            {
                throw new RuntimeException(e);
            }

            IndexType indexType = null;
            if (row.has(INDEX_TYPE))
                indexType = IndexType.valueOf(row.getString(INDEX_TYPE));

            Map<String, String> indexOptions = null;
            if (row.has(INDEX_OPTIONS))
                indexOptions = FBUtilities.fromJsonMap(row.getString(INDEX_OPTIONS));

            String indexName = null;
            if (row.has(INDEX_NAME))
                indexName = row.getString(INDEX_NAME);

            cds.add(new ColumnDefinition(name, validator, indexType, indexOptions, indexName, componentIndex, type));
        }

        return cds;
    }

    public String getIndexName()
    {
        return indexName;
    }

    public ColumnDefinition setIndexName(String indexName)
    {
        this.indexName = indexName;
        return this;
    }

    public ColumnDefinition setIndexType(IndexType indexType, Map<String,String> indexOptions)
    {
        this.indexType = indexType;
        this.indexOptions = indexOptions;
        return this;
    }

    public ColumnDefinition setIndex(String indexName, IndexType indexType, Map<String,String> indexOptions)
    {
        return setIndexName(indexName).setIndexType(indexType, indexOptions);
    }

    public boolean isIndexed()
    {
        return indexType != null;
    }

    public IndexType getIndexType()
    {
        return indexType;
    }

    public Map<String,String> getIndexOptions()
    {
        return indexOptions;
    }

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    public void setValidator(AbstractType<?> validator)
    {
        this.validator = validator;
    }
}
