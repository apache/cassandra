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
package org.apache.cassandra.cql;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;

import java.nio.ByteBuffer;

public class AlterTableStatement
{
    public static enum OperationType
    {
        ADD, ALTER, DROP
    }

    public final OperationType oType;
    public final String columnFamily, columnName, validator;

    public AlterTableStatement(String columnFamily, OperationType type, String columnName)
    {
        this(columnFamily, type, columnName, null);
    }

    public AlterTableStatement(String columnFamily, OperationType type, String columnName, String validator)
    {
        this.columnFamily = columnFamily;
        this.oType = type;
        this.columnName = columnName;
        this.validator = CreateColumnFamilyStatement.comparators.get(validator); // used only for ADD/ALTER commands
    }

    public CfDef getCfDef(String keyspace) throws ConfigurationException, InvalidRequestException
    {
        CFMetaData meta = Schema.instance.getCFMetaData(keyspace, columnFamily);

        CfDef cfDef = meta.toAvro();

        ByteBuffer columnName = meta.comparator.fromString(this.columnName);

        switch (oType)
        {
            case ADD:
                if (cfDef.key_alias != null && cfDef.key_alias.equals(columnName))
                    throw new InvalidRequestException("Invalid column name: "
                                                      + this.columnName
                                                      + ", because it equals to key_alias.");

                cfDef.column_metadata.add(new ColumnDefinition(columnName,
                                                               TypeParser.parse(validator),
                                                               null,
                                                               null,
                                                               null).toAvro());
                break;

            case ALTER:
                ColumnDefinition column = meta.getColumnDefinition(columnName);

                if (column == null)
                    throw new InvalidRequestException(String.format("Column '%s' was not found in CF '%s'",
                                                                    this.columnName,
                                                                    columnFamily));

                column.setValidator(TypeParser.parse(validator));

                cfDef.column_metadata.add(column.toAvro());
                break;

            case DROP:
                ColumnDef toDelete = null;

                for (ColumnDef columnDef : cfDef.column_metadata)
                {
                    if (columnDef.name.equals(columnName))
                    {
                        toDelete = columnDef;
                    }
                }

                if (toDelete == null)
                    throw new InvalidRequestException(String.format("Column '%s' was not found in CF '%s'",
                                                                    this.columnName,
                                                                    columnFamily));

                // it is impossible to use ColumnDefinition.deflate() in remove() method
                // it will throw java.lang.ClassCastException: java.lang.String cannot be cast to org.apache.avro.util.Utf8
                // some where deep inside of Avro
                cfDef.column_metadata.remove(toDelete);
                break;
        }

        return cfDef;
    }

    public String toString()
    {
        return String.format("AlterTableStatement(cf=%s, type=%s, column=%s, validator=%s)",
                             columnFamily,
                             oType,
                             columnName,
                             validator);
    }

}
