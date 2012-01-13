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

import org.apache.avro.util.Utf8;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.migration.avro.CfDef;
import org.apache.cassandra.db.migration.avro.ColumnDef;
import org.apache.cassandra.thrift.InvalidRequestException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AlterTableStatement
{
    public static enum OperationType
    {
        ADD, ALTER, DROP, OPTS
    }

    public final OperationType oType;
    public final String columnFamily, columnName, validator;
    private final CFPropDefs cfProps = new CFPropDefs();

    public AlterTableStatement(String columnFamily, OperationType type, String columnName)
    {
        this(columnFamily, type, columnName, null);
    }

    public AlterTableStatement(String columnFamily, OperationType type, String columnName, String validator)
    {
        this(columnFamily, type, columnName, validator, null);
    }

    public AlterTableStatement(String columnFamily, OperationType type, String columnName, String validator, Map<String, String> propertyMap)
    {
        this.columnFamily = columnFamily;
        this.oType = type;
        this.columnName = columnName;
        this.validator = CFPropDefs.comparators.get(validator); // used only for ADD/ALTER commands

        if (propertyMap != null)
        {
            for (Map.Entry<String, String> prop : propertyMap.entrySet())
            {
                cfProps.addProperty(prop.getKey(), prop.getValue());
            }
        }
    }

    public CfDef getCfDef(String keyspace) throws ConfigurationException, InvalidRequestException
    {
        CFMetaData meta = Schema.instance.getCFMetaData(keyspace, columnFamily);

        CfDef cfDef = meta.toAvro();

        ByteBuffer columnName = this.oType == OperationType.OPTS ? null
                                                                 : meta.comparator.fromString(this.columnName);

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

            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

                cfProps.validate();
                applyPropertiesToCfDef(cfDef, cfProps);
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

    public static void applyPropertiesToCfDef(CfDef cfDef, CFPropDefs cfProps) throws InvalidRequestException
    {
        if (cfProps.hasProperty(CFPropDefs.KW_COMPARATOR))
        {
            throw new InvalidRequestException("Can't change CF comparator after creation");
        }
        if (cfProps.hasProperty(CFPropDefs.KW_COMMENT))
        {
            cfDef.comment = new Utf8(cfProps.getProperty(CFPropDefs.KW_COMMENT));
        }
        if (cfProps.hasProperty(CFPropDefs.KW_DEFAULTVALIDATION))
        {
            try
            {
                cfDef.default_validation_class = new Utf8(cfProps.getValidator().toString());
            }
            catch (ConfigurationException e)
            {
                throw new InvalidRequestException(String.format("Invalid validation type %s",
                                                                cfProps.getProperty(CFPropDefs.KW_DEFAULTVALIDATION)));
            }
        }

        cfDef.read_repair_chance = cfProps.getPropertyDouble(CFPropDefs.KW_READREPAIRCHANCE, cfDef.read_repair_chance);
        cfDef.gc_grace_seconds = cfProps.getPropertyInt(CFPropDefs.KW_GCGRACESECONDS, cfDef.gc_grace_seconds);
        cfDef.replicate_on_write = cfProps.getPropertyBoolean(CFPropDefs.KW_REPLICATEONWRITE, cfDef.replicate_on_write);
        cfDef.min_compaction_threshold = cfProps.getPropertyInt(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, cfDef.min_compaction_threshold);
        cfDef.max_compaction_threshold = cfProps.getPropertyInt(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, cfDef.max_compaction_threshold);

        if (!cfProps.compactionStrategyOptions.isEmpty())
        {
            cfDef.compaction_strategy_options = new HashMap<CharSequence, CharSequence>();
            for (Map.Entry<String, String> entry : cfProps.compactionStrategyOptions.entrySet())
            {
                cfDef.compaction_strategy_options.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()));
            }
        }

        if (!cfProps.compressionParameters.isEmpty())
        {
            cfDef.compression_options = new HashMap<CharSequence, CharSequence>();
            for (Map.Entry<String, String> entry : cfProps.compressionParameters.entrySet())
            {
                cfDef.compression_options.put(new Utf8(entry.getKey()), new Utf8(entry.getValue()));
            }
        }
    }
}
