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
package org.apache.cassandra.db.virtual;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.marshal.UserType;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.base.Strings;

/**
 * Abstract base class for virtual tables that expose metadata on schema elements.
 * <p>
 * This class provides a unified implementation for tables that expose metadata across:
 * <ul>
 *   <li>Keyspaces - metadata on keyspaces</li>
 *   <li>Tables - metadata on tables</li>
 *   <li>Columns - metadata on columns</li>
 *   <li>User-Defined Types (UDTs) - metadata on UDTs</li>
 * </ul>
 * </p>
 * <p>
 * Subclasses must implement methods to extract the specific metadata field
 * (e.g., comment, security label) from each schema element type.
 * </p>
 */
abstract class AbstractSchemaMetadataTable extends AbstractVirtualTable
{
    /*
        As clustering keys cannot be null, using an EMPTY_VALUE in the results for non-existent
        columns in query results.
    */
    private static final String EMPTY_VALUE = "";

    private static final String OBJECT_TYPE = "object_type";
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String COLUMN_NAME = "column_name";
    private static final String UDT_NAME = "udt_name";
    private static final String FIELD_NAME = "field_name";

    private final SchemaTableType schemaTableType;

    /**
     * Schema object types that can have metadata.
     */
    protected enum ObjectType
    {
        KEYSPACE,
        TABLE,
        COLUMN,
        UDT,
        FIELD;

        @Override
        public String toString()
        {
            return name();
        }

        /**
         * Parse object type string to enum.
         * @param objectType the string representation of the object type
         * @return ObjectType enum value or null if invalid
         */
        static ObjectType parse(String objectType)
        {
            try
            {
                return ObjectType.valueOf(objectType);
            }
            catch (IllegalArgumentException e)
            {
                return null;
            }
        }
    }

    protected enum SchemaTableType
    {
        COMMENT("schema_comments", "comment"),
        SECURITY_LABEL("schema_security_labels", "security_label");

        private final String tableName;
        private final String columnName;

        SchemaTableType(String tableName, String columnName)
        {
            this.tableName = tableName;
            this.columnName = columnName;
        }
    }

    protected AbstractSchemaMetadataTable(String keyspace, SchemaTableType schemaTableType)
    {
        super(TableMetadata.builder(keyspace, schemaTableType.tableName)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(OBJECT_TYPE, UTF8Type.instance)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(COLUMN_NAME, UTF8Type.instance)
                           .addClusteringColumn(UDT_NAME, UTF8Type.instance)
                           .addClusteringColumn(FIELD_NAME, UTF8Type.instance)
                           .addRegularColumn(schemaTableType.columnName, UTF8Type.instance)
                           .build());
        this.schemaTableType = schemaTableType;
    }

    /**
     * Extract metadata from a keyspace.
     * @param keyspace the keyspace metadata
     * @return the metadata value, or null if not present
     */
    protected abstract String extractKeyspaceMetadata(KeyspaceMetadata keyspace);

    /**
     * Extract metadata from a table.
     * @param table the table metadata
     * @return the metadata value, or null if not present
     */
    protected abstract String extractTableMetadata(TableMetadata table);

    /**
     * Extract metadata from a column.
     * @param column the column metadata
     * @return the metadata value, or null if not present
     */
    protected abstract String extractColumnMetadata(ColumnMetadata column);

    /**
     * Extract metadata from a UDT.
     * @param udt the user-defined type
     * @return the metadata value, or null if not present
     */
    protected abstract String extractUdtMetadata(UserType udt);

    /**
     * Extract metadata from a UDT field.
     * @param udt the user-defined type
     * @param fieldName the field name
     * @return the metadata value, or null if not present
     */
    protected abstract String extractFieldMetadata(UserType udt, String fieldName);

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        SchemaProvider schemaProvider = Schema.instance;

        for (String keyspaceName : schemaProvider.getKeyspaces())
        {
            KeyspaceMetadata keyspace = schemaProvider.getKeyspaceMetadata(keyspaceName);
            if (keyspace == null)
            {
                continue;
            }
            addKeyspaceRow(result, keyspace);

            for (TableMetadata table : keyspace.tables)
            {
                addTableRow(result, keyspace, table);

                for (ColumnMetadata column : table.columns())
                    addColumnRow(result, keyspace, table, column);
            }

            for (UserType udt : keyspace.types)
            {
                addUdtRow(result, keyspace, udt);

                for (org.apache.cassandra.cql3.FieldIdentifier field : udt.fieldNames())
                    addFieldRow(result, keyspace, udt, field.toString());
            }
        }
        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        ByteBuffer key = partitionKey.getKey();
        ByteBuffer[] components = ((CompositeType) metadata().partitionKeyType).split(key);
        String objectType = UTF8Type.instance.compose(components[0]);
        String keyspaceName = UTF8Type.instance.compose(components[1]);

        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new InvalidRequestException("Unknown keyspace: '" + keyspaceName + '\'');

        ObjectType type = ObjectType.parse(objectType);
        if (type == null)
            throw new InvalidRequestException("Unknown object type: '" + objectType +
                                              "'. Valid types are: " + Arrays.toString(ObjectType.values()));

        switch (type)
        {
            case KEYSPACE:
                addKeyspaceRow(result, keyspace);
                break;
            case TABLE:
                for (TableMetadata table : keyspace.tables)
                    addTableRow(result, keyspace, table);
                break;
            case COLUMN:
                for (TableMetadata table : keyspace.tables)
                    for (ColumnMetadata column : table.columns())
                        addColumnRow(result, keyspace, table, column);
                break;
            case UDT:
                for (UserType udt : keyspace.types)
                    addUdtRow(result, keyspace, udt);
                break;
            case FIELD:
                for (UserType udt : keyspace.types)
                    for (org.apache.cassandra.cql3.FieldIdentifier field : udt.fieldNames())
                        addFieldRow(result, keyspace, udt, field.toString());
                break;
        }
        return result;
    }

    /**
     * Add a row for keyspace metadata if present.
     */
    private void addKeyspaceRow(SimpleDataSet result, KeyspaceMetadata keyspace)
    {
        String metadata = Strings.emptyToNull(extractKeyspaceMetadata(keyspace));

        if (metadata != null)
            result.row(ObjectType.KEYSPACE.toString(), keyspace.name, EMPTY_VALUE, EMPTY_VALUE, EMPTY_VALUE, EMPTY_VALUE)
                  .column(schemaTableType.columnName, metadata);
    }

    /**
     * Add a row for table metadata if present.
     */
    private void addTableRow(SimpleDataSet result, KeyspaceMetadata keyspace, TableMetadata table)
    {
        String metadata = Strings.emptyToNull(extractTableMetadata(table));

        if (metadata != null)
            result.row(ObjectType.TABLE.toString(), keyspace.name, table.name, EMPTY_VALUE, EMPTY_VALUE, EMPTY_VALUE)
                  .column(schemaTableType.columnName, metadata);
    }

    /**
     * Add a row for column metadata if present.
     */
    private void addColumnRow(SimpleDataSet result, KeyspaceMetadata keyspace, TableMetadata table, ColumnMetadata column)
    {
        String metadata = Strings.emptyToNull(extractColumnMetadata(column));

        if (metadata != null)
            result.row(ObjectType.COLUMN.toString(), keyspace.name, table.name, column.name.toString(), EMPTY_VALUE, EMPTY_VALUE)
                  .column(schemaTableType.columnName, metadata);
    }

    /**
     * Add a row for UDT metadata if present.
     */
    private void addUdtRow(SimpleDataSet result, KeyspaceMetadata keyspace, UserType udt)
    {
        String metadata = Strings.emptyToNull(extractUdtMetadata(udt));

        if (metadata != null)
            result.row(ObjectType.UDT.toString(), keyspace.name, EMPTY_VALUE, EMPTY_VALUE, udt.getNameAsString(), EMPTY_VALUE)
                  .column(schemaTableType.columnName, metadata);
    }

    /**
     * Add a row for UDT field metadata if present.
     */
    private void addFieldRow(SimpleDataSet result, KeyspaceMetadata keyspace, UserType udt, String fieldName)
    {
        String metadata = Strings.emptyToNull(extractFieldMetadata(udt, fieldName));

        if (metadata != null)
            result.row(ObjectType.FIELD.toString(), keyspace.name, EMPTY_VALUE, EMPTY_VALUE, udt.getNameAsString(), fieldName)
                  .column(schemaTableType.columnName, metadata);
    }
}
