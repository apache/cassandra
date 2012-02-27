/*
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
 */
package org.apache.cassandra.cql3.statements;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.migration.AddColumnFamily;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.io.compress.CompressionParameters;

/** A <code>CREATE COLUMNFAMILY</code> parsed from a CQL query statement. */
public class CreateColumnFamilyStatement extends SchemaAlteringStatement
{
    private AbstractType<?> comparator;
    private AbstractType<?> defaultValidator;
    private AbstractType<?> keyValidator;

    private ByteBuffer keyAlias;
    private List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;

    private final Map<ColumnIdentifier, String> columns = new HashMap<ColumnIdentifier, String>();
    private final CFPropDefs properties;

    public CreateColumnFamilyStatement(CFName name, CFPropDefs properties)
    {
        super(name);
        this.properties = properties;
    }

    // Column definitions
    private Map<ByteBuffer, ColumnDefinition> getColumns() throws InvalidRequestException
    {
        Map<ByteBuffer, ColumnDefinition> columnDefs = new HashMap<ByteBuffer, ColumnDefinition>();

        for (Map.Entry<ColumnIdentifier, String> col : columns.entrySet())
        {
            AbstractType<?> validator = CFPropDefs.parseType(col.getValue());
            
            columnDefs.put(col.getKey().key, new ColumnDefinition(col.getKey().key, validator, null, null, null));
        }

        return columnDefs;
    }

    public Migration getMigration() throws InvalidRequestException, ConfigurationException, IOException
    {
        CFMetaData cfmd = getCFMetaData();
        ThriftValidation.validateCfDef(cfmd.toThrift(), null);
        return new AddColumnFamily(cfmd);
    }

    /**
     * Returns a CFMetaData instance based on the parameters parsed from this
     * <code>CREATE</code> statement, or defaults where applicable.
     *
     * @return a CFMetaData instance corresponding to the values parsed from this statement
     * @throws InvalidRequestException on failure to validate parsed parameters
     */
    public CFMetaData getCFMetaData() throws InvalidRequestException
    {
        CFMetaData newCFMD;
        try
        {
            newCFMD = new CFMetaData(keyspace(),
                                     columnFamily(),
                                     ColumnFamilyType.Standard,
                                     comparator,
                                     null);

            newCFMD.comment(properties.get(CFPropDefs.KW_COMMENT))
                   .readRepairChance(properties.getDouble(CFPropDefs.KW_READREPAIRCHANCE, CFMetaData.DEFAULT_READ_REPAIR_CHANCE))
                   .dclocalReadRepairChance(properties.getDouble(CFPropDefs.KW_DCLOCALREADREPAIRCHANCE, CFMetaData.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE))
                   .replicateOnWrite(properties.getBoolean(CFPropDefs.KW_REPLICATEONWRITE, CFMetaData.DEFAULT_REPLICATE_ON_WRITE))
                   .gcGraceSeconds(properties.getInt(CFPropDefs.KW_GCGRACESECONDS, CFMetaData.DEFAULT_GC_GRACE_SECONDS))
                   .defaultValidator(defaultValidator)
                   .minCompactionThreshold(properties.getInt(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD))
                   .maxCompactionThreshold(properties.getInt(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD))
                   .columnMetadata(getColumns())
                   .keyValidator(keyValidator)
                   .keyAlias(keyAlias)
                   .columnAliases(columnAliases)
                   .valueAlias(valueAlias)
                   .compactionStrategyOptions(properties.compactionStrategyOptions)
                   .compressionParameters(CompressionParameters.create(properties.compressionParameters))
                   .validate();
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.toString());
        }
        return newCFMD;
    }

    public static class RawStatement extends CFStatement
    {
        private final Map<ColumnIdentifier, String> definitions = new HashMap<ColumnIdentifier, String>();
        private final CFPropDefs properties = new CFPropDefs();

        private final List<ColumnIdentifier> keyAliases = new ArrayList<ColumnIdentifier>();
        private List<ColumnIdentifier> columnAliases = new ArrayList<ColumnIdentifier>();

        private boolean useCompactStorage;
        private Multiset<ColumnIdentifier> definedNames = HashMultiset.create(1);

        public RawStatement(CFName name)
        {
            super(name);
        }

        /**
         * Transform this raw statement into a CreateColumnFamilyStatement.
         */
        public ParsedStatement.Prepared prepare() throws InvalidRequestException
        {
            try
            {
                // Column family name
                if (!columnFamily().matches("\\w+"))
                    throw new InvalidRequestException(String.format("\"%s\" is not a valid column family name (must be alphanumeric character only: [0-9A-Za-z]+)", columnFamily()));
                if (columnFamily().length() > 32)
                    throw new InvalidRequestException(String.format("Column family names shouldn't be more than 32 character long (got \"%s\")", columnFamily()));

                for (Multiset.Entry<ColumnIdentifier> entry : definedNames.entrySet())
                    if (entry.getCount() > 1)
                        throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));

                properties.validate();

                CreateColumnFamilyStatement stmt = new CreateColumnFamilyStatement(cfName, properties);
                stmt.setBoundTerms(getBoundsTerms());
                stmt.columns.putAll(definitions); // we'll remove what is not a column below

                // Ensure that exactly one key has been specified.
                if (keyAliases.size() == 0)
                    throw new InvalidRequestException("You must specify a PRIMARY KEY");
                else if (keyAliases.size() > 1)
                    throw new InvalidRequestException("You may only specify one PRIMARY KEY");

                stmt.keyAlias = keyAliases.get(0).key;
                stmt.keyValidator = getTypeAndRemove(stmt.columns, keyAliases.get(0));

                // Handle column aliases
                if (columnAliases != null && !columnAliases.isEmpty())
                {
                    // If we use compact storage and have only one alias, it is a
                    // standard "dynamic" CF, otherwise it's a composite
                    if (useCompactStorage && columnAliases.size() == 1)
                    {
                        stmt.columnAliases.add(columnAliases.get(0).key);
                        stmt.comparator = getTypeAndRemove(stmt.columns, columnAliases.get(0));
                    }
                    else
                    {
                        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
                        for (ColumnIdentifier t : columnAliases)
                        {
                            stmt.columnAliases.add(t.key);
                            types.add(getTypeAndRemove(stmt.columns, t));
                        }
                        // For sparse, we must add the last UTF8 component
                        if (!useCompactStorage)
                            types.add(CFDefinition.definitionType);

                        if (types.isEmpty())
                            throw new IllegalStateException("Nonsensical empty parameter list for CompositeType");
                        stmt.comparator = CompositeType.getInstance(types);
                    }
                }
                else
                {
                    stmt.comparator = CFDefinition.definitionType;
                }

                if (useCompactStorage)
                {
                    // There should be only one column definition remaining, which gives us the default validator.
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("COMPACT STORAGE requires one definition not part of the PRIMARY KEY, none found");
                    if (stmt.columns.size() > 1)
                        throw new InvalidRequestException(String.format("COMPACT STORAGE allows only one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), ", ")));

                    Map.Entry<ColumnIdentifier, String> lastEntry = stmt.columns.entrySet().iterator().next();
                    stmt.defaultValidator = CFPropDefs.parseType(lastEntry.getValue());
                    stmt.valueAlias = lastEntry.getKey().key;
                    stmt.columns.remove(lastEntry.getKey());
                }
                else
                {
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");

                    // There is no way to insert/access a column that is not defined for non-compact
                    // storage, so the actual validator don't matter much.
                    stmt.defaultValidator = CFDefinition.definitionType;
                }

                return new ParsedStatement.Prepared(stmt);
            }
            catch (ConfigurationException e)
            {
                throw new InvalidRequestException(e.getMessage());
            }
        }

        private static AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, String> columns, ColumnIdentifier t) throws InvalidRequestException, ConfigurationException
        {
            String typeStr = columns.get(t);
            if (typeStr == null)
                throw new InvalidRequestException(String.format("Unkown definition %s referenced in PRIMARY KEY", t));
            columns.remove(t);
            return CFPropDefs.parseType(typeStr);
        }

        public void addDefinition(ColumnIdentifier def, String type)
        {
            definedNames.add(def);
            definitions.put(def, type);
        }

        public void setKeyAlias(ColumnIdentifier alias)
        {
            keyAliases.add(alias);
        }

        public void addColumnAlias(ColumnIdentifier alias)
        {
            columnAliases.add(alias);
        }

        public void addProperty(String name, String value)
        {
            properties.addProperty(name, value);
        }

        public void setCompactStorage()
        {
            useCompactStorage = true;
        }

        public void checkAccess(ClientState state)
        {
            throw new UnsupportedOperationException();
        }

        public CqlResult execute(ClientState state, List<ByteBuffer> variables)
        {
            throw new UnsupportedOperationException();
        }
    }
}
