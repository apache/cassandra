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
package org.apache.cassandra.cql;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.compress.CompressionParameters;
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

    public CFMetaData getCFMetaData(String keyspace) throws ConfigurationException, InvalidRequestException
    {
        CFMetaData meta = Schema.instance.getCFMetaData(keyspace, columnFamily);
        CFMetaData cfm = meta.clone();

        ByteBuffer columnName = this.oType == OperationType.OPTS ? null
                                                                 : meta.comparator.fromString(this.columnName);

        switch (oType)
        {
            case ADD:
                if (!cfm.getKeyAliases().isEmpty() && cfm.getKeyAliases().contains(columnName))
                    throw new InvalidRequestException("Invalid column name: "
                                                      + this.columnName
                                                      + ", because it equals to a key alias.");

                cfm.addColumnDefinition(new ColumnDefinition(columnName,
                                                             TypeParser.parse(validator),
                                                             null,
                                                             null,
                                                             null,
                                                             null));
                break;

            case ALTER:
                // We only look for the first key alias which is ok for CQL2
                if (!cfm.getKeyAliases().isEmpty() && cfm.getKeyAliases().get(0).equals(columnName))
                {
                    cfm.keyValidator(TypeParser.parse(validator));
                }
                else
                {
                    ColumnDefinition toUpdate = null;

                    for (ColumnDefinition columnDef : cfm.getColumn_metadata().values())
                    {
                        if (columnDef.name.equals(columnName))
                        {
                            toUpdate = columnDef;
                            break;
                        }
                    }

                    if (toUpdate == null)
                        throw new InvalidRequestException(String.format("Column '%s' was not found in CF '%s'",
                                    this.columnName,
                                    columnFamily));

                    toUpdate.setValidator(TypeParser.parse(validator));
                }
                break;

            case DROP:
                ColumnDefinition toDelete = null;

                for (ColumnDefinition columnDef : cfm.getColumn_metadata().values())
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

                cfm.removeColumnDefinition(toDelete);
                break;

            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

                cfProps.validate();
                applyPropertiesToCFMetadata(cfm, cfProps);
                break;
        }

        return cfm;
    }

    public String toString()
    {
        return String.format("AlterTableStatement(cf=%s, type=%s, column=%s, validator=%s)",
                             columnFamily,
                             oType,
                             columnName,
                             validator);
    }

    public static void applyPropertiesToCFMetadata(CFMetaData cfm, CFPropDefs cfProps) throws InvalidRequestException, ConfigurationException
    {
        if (cfProps.hasProperty(CFPropDefs.KW_COMPARATOR))
        {
            throw new InvalidRequestException("Can't change CF comparator after creation");
        }
        if (cfProps.hasProperty(CFPropDefs.KW_COMMENT))
        {
            cfm.comment(cfProps.getProperty(CFPropDefs.KW_COMMENT));
        }
        if (cfProps.hasProperty(CFPropDefs.KW_DEFAULTVALIDATION))
        {
            try
            {
                cfm.defaultValidator(cfProps.getValidator());
            }
            catch (ConfigurationException e)
            {
                throw new InvalidRequestException(String.format("Invalid validation type %s",
                                                                cfProps.getProperty(CFPropDefs.KW_DEFAULTVALIDATION)));
            }
        }

        cfm.readRepairChance(cfProps.getPropertyDouble(CFPropDefs.KW_READREPAIRCHANCE, cfm.getReadRepairChance()));
        cfm.dcLocalReadRepairChance(cfProps.getPropertyDouble(CFPropDefs.KW_DCLOCALREADREPAIRCHANCE, cfm.getDcLocalReadRepair()));
        cfm.gcGraceSeconds(cfProps.getPropertyInt(CFPropDefs.KW_GCGRACESECONDS, cfm.getGcGraceSeconds()));
        cfm.replicateOnWrite(cfProps.getPropertyBoolean(CFPropDefs.KW_REPLICATEONWRITE, cfm.getReplicateOnWrite()));
        cfm.minCompactionThreshold(cfProps.getPropertyInt(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, cfm.getMinCompactionThreshold()));
        cfm.maxCompactionThreshold(cfProps.getPropertyInt(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, cfm.getMaxCompactionThreshold()));
        cfm.caching(CFMetaData.Caching.fromString(cfProps.getPropertyString(CFPropDefs.KW_CACHING, cfm.getCaching().toString())));
        cfm.bloomFilterFpChance(cfProps.getPropertyDouble(CFPropDefs.KW_BF_FP_CHANCE, cfm.getBloomFilterFpChance()));

        if (!cfProps.compactionStrategyOptions.isEmpty())
        {
            cfm.compactionStrategyOptions(new HashMap<String, String>());
            for (Map.Entry<String, String> entry : cfProps.compactionStrategyOptions.entrySet())
                cfm.compactionStrategyOptions.put(entry.getKey(), entry.getValue());
        }

        if (!cfProps.compressionParameters.isEmpty())
        {
            cfm.compressionParameters(CompressionParameters.create(cfProps.compressionParameters));
        }
    }
}
