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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.exceptions.*;
import org.apache.commons.lang.StringUtils;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/** A <code>CREATE COLUMNFAMILY</code> parsed from a CQL query statement. */
public class CreateColumnFamilyStatement extends SchemaAlteringStatement
{
    public AbstractType<?> comparator;
    private AbstractType<?> defaultValidator;
    private AbstractType<?> keyValidator;

    private final List<ByteBuffer> keyAliases = new ArrayList<ByteBuffer>();
    private final List<ByteBuffer> columnAliases = new ArrayList<ByteBuffer>();
    private ByteBuffer valueAlias;

    private final Map<ColumnIdentifier, AbstractType> columns = new HashMap<ColumnIdentifier, AbstractType>();
    private final CFPropDefs properties;

    public CreateColumnFamilyStatement(CFName name, CFPropDefs properties)
    {
        super(name);
        this.properties = properties;

        try
        {
            if (!this.properties.hasProperty(CFPropDefs.KW_COMPRESSION) && CFMetaData.DEFAULT_COMPRESSOR != null)
                this.properties.addProperty(CFPropDefs.KW_COMPRESSION,
                                            new HashMap<String, String>()
                                            {{
                                                put(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR);
                                            }});
        }
        catch (SyntaxException e)
        {
            throw new AssertionError(e);
        }
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.CREATE);
    }

    // Column definitions
    private Map<ByteBuffer, ColumnDefinition> getColumns() throws InvalidRequestException
    {
        Map<ByteBuffer, ColumnDefinition> columnDefs = new HashMap<ByteBuffer, ColumnDefinition>();
        Integer componentIndex = null;
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType) comparator;
            componentIndex = ct.types.get(ct.types.size() - 1) instanceof ColumnToCollectionType
                           ? ct.types.size() - 2
                           : ct.types.size() - 1;
        }

        for (Map.Entry<ColumnIdentifier, AbstractType> col : columns.entrySet())
        {
            columnDefs.put(col.getKey().key, new ColumnDefinition(col.getKey().key, col.getValue(), null, null, null, componentIndex));
        }

        return columnDefs;
    }

    public void announceMigration() throws RequestValidationException
    {
        MigrationManager.announceNewColumnFamily(getCFMetaData());
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.CREATED;
    }

    /**
     * Returns a CFMetaData instance based on the parameters parsed from this
     * <code>CREATE</code> statement, or defaults where applicable.
     *
     * @return a CFMetaData instance corresponding to the values parsed from this statement
     * @throws InvalidRequestException on failure to validate parsed parameters
     */
    public CFMetaData getCFMetaData() throws RequestValidationException
    {
        CFMetaData newCFMD;
        newCFMD = new CFMetaData(keyspace(),
                                 columnFamily(),
                                 ColumnFamilyType.Standard,
                                 comparator,
                                 null);
        applyPropertiesTo(newCFMD);
        return newCFMD;
    }

    public void applyPropertiesTo(CFMetaData cfmd) throws RequestValidationException
    {
        cfmd.defaultValidator(defaultValidator)
            .columnMetadata(getColumns())
            .keyValidator(keyValidator)
            .keyAliases(keyAliases)
            .columnAliases(columnAliases)
            .valueAlias(valueAlias);

        properties.applyToCFMetadata(cfmd);
    }

    public static class RawStatement extends CFStatement
    {
        private final Map<ColumnIdentifier, CQL3Type> definitions = new HashMap<ColumnIdentifier, CQL3Type>();
        public final CFPropDefs properties = new CFPropDefs();

        private final List<List<ColumnIdentifier>> keyAliases = new ArrayList<List<ColumnIdentifier>>();
        private final List<ColumnIdentifier> columnAliases = new ArrayList<ColumnIdentifier>();
        private final Map<ColumnIdentifier, Boolean> definedOrdering = new LinkedHashMap<ColumnIdentifier, Boolean>(); // Insertion ordering is important

        private boolean useCompactStorage;
        private final Multiset<ColumnIdentifier> definedNames = HashMultiset.create(1);

        public RawStatement(CFName name)
        {
            super(name);
        }

        /**
         * Transform this raw statement into a CreateColumnFamilyStatement.
         */
        public ParsedStatement.Prepared prepare() throws RequestValidationException
        {
            // Column family name
            if (!columnFamily().matches("\\w+"))
                throw new InvalidRequestException(String.format("\"%s\" is not a valid column family name (must be alphanumeric character only: [0-9A-Za-z]+)", columnFamily()));
            if (columnFamily().length() > Schema.NAME_LENGTH)
                throw new InvalidRequestException(String.format("Column family names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, columnFamily()));

            for (Multiset.Entry<ColumnIdentifier> entry : definedNames.entrySet())
                if (entry.getCount() > 1)
                    throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));

            properties.validate();

            CreateColumnFamilyStatement stmt = new CreateColumnFamilyStatement(cfName, properties);
            stmt.setBoundTerms(getBoundsTerms());

            Map<ByteBuffer, CollectionType> definedCollections = null;
            for (Map.Entry<ColumnIdentifier, CQL3Type> entry : definitions.entrySet())
            {
                ColumnIdentifier id = entry.getKey();
                CQL3Type pt = entry.getValue();
                if (pt.isCollection())
                {
                    if (definedCollections == null)
                        definedCollections = new HashMap<ByteBuffer, CollectionType>();
                    definedCollections.put(id.key, (CollectionType)pt.getType());
                }
                stmt.columns.put(id, pt.getType()); // we'll remove what is not a column below
            }

            if (keyAliases.size() != 1)
                throw new InvalidRequestException("You must specify one and only one PRIMARY KEY");

            List<ColumnIdentifier> kAliases = keyAliases.get(0);

            List<AbstractType<?>> keyTypes = new ArrayList<AbstractType<?>>(kAliases.size());
            for (ColumnIdentifier alias : kAliases)
            {
                stmt.keyAliases.add(alias.key);
                AbstractType<?> t = getTypeAndRemove(stmt.columns, alias);
                if (t instanceof CounterColumnType)
                    throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", alias));
                keyTypes.add(t);
            }
            stmt.keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);

            // Handle column aliases
            if (columnAliases.isEmpty())
            {
                if (useCompactStorage)
                {
                    // There should remain some column definition since it is a non-composite "static" CF
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");

                    if (definedCollections != null)
                        throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");

                    stmt.comparator = CFDefinition.definitionType;
                }
                else
                {
                    List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(definedCollections == null ? 1 : 2);
                    types.add(CFDefinition.definitionType);
                    if (definedCollections != null)
                        types.add(ColumnToCollectionType.getInstance(definedCollections));
                    stmt.comparator = CompositeType.getInstance(types);
                }
            }
            else
            {
                // If we use compact storage and have only one alias, it is a
                // standard "dynamic" CF, otherwise it's a composite
                if (useCompactStorage && columnAliases.size() == 1)
                {
                    if (definedCollections != null)
                        throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");
                    stmt.columnAliases.add(columnAliases.get(0).key);
                    stmt.comparator = getTypeAndRemove(stmt.columns, columnAliases.get(0));
                    if (stmt.comparator instanceof CounterColumnType)
                        throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", stmt.columnAliases.get(0)));
                }
                else
                {
                    List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(columnAliases.size() + 1);
                    for (ColumnIdentifier t : columnAliases)
                    {
                        stmt.columnAliases.add(t.key);

                        AbstractType<?> type = getTypeAndRemove(stmt.columns, t);
                        if (type instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", t.key));
                        types.add(type);
                    }

                    if (useCompactStorage)
                    {
                        if (definedCollections != null)
                            throw new InvalidRequestException("Collection types are not supported with COMPACT STORAGE");
                    }
                    else
                    {
                        // For sparse, we must add the last UTF8 component
                        // and the collection type if there is one
                        types.add(CFDefinition.definitionType);
                        if (definedCollections != null)
                            types.add(ColumnToCollectionType.getInstance(definedCollections));
                    }

                    if (types.isEmpty())
                        throw new IllegalStateException("Nonsensical empty parameter list for CompositeType");
                    stmt.comparator = CompositeType.getInstance(types);
                }
            }

            if (useCompactStorage && !stmt.columnAliases.isEmpty())
            {
                if (stmt.columns.isEmpty())
                {
                    // The only value we'll insert will be the empty one, so the default validator don't matter
                    stmt.defaultValidator = BytesType.instance;
                    // We need to distinguish between
                    //   * I'm upgrading from thrift so the valueAlias is null
                    //   * I've define my table with only a PK (and the column value will be empty)
                    // So, we use an empty valueAlias (rather than null) for the second case
                    stmt.valueAlias = ByteBufferUtil.EMPTY_BYTE_BUFFER;
                }
                else
                {
                    if (stmt.columns.size() > 1)
                        throw new InvalidRequestException(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), ", ")));

                    Map.Entry<ColumnIdentifier, AbstractType> lastEntry = stmt.columns.entrySet().iterator().next();
                    stmt.defaultValidator = lastEntry.getValue();
                    stmt.valueAlias = lastEntry.getKey().key;
                    stmt.columns.remove(lastEntry.getKey());
                }
            }
            else
            {
                // For compact, we are in the "static" case, so we need at least one column defined. For non-compact however, having
                // just the PK is fine since we have CQL3 row marker.
                if (useCompactStorage && stmt.columns.isEmpty())
                    throw new InvalidRequestException("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");

                // There is no way to insert/access a column that is not defined for non-compact storage, so
                // the actual validator don't matter much (except that we want to recognize counter CF as limitation apply to them).
                stmt.defaultValidator = !stmt.columns.isEmpty() && (stmt.columns.values().iterator().next() instanceof CounterColumnType)
                    ? CounterColumnType.instance
                    : BytesType.instance;
            }


            // If we give a clustering order, we must explicitely do so for all aliases and in the order of the PK
            if (!definedOrdering.isEmpty())
            {
                if (definedOrdering.size() > columnAliases.size())
                    throw new InvalidRequestException("Too much columns provided for CLUSTERING ORDER");

                int i = 0;
                for (ColumnIdentifier id : definedOrdering.keySet())
                {
                    ColumnIdentifier c = columnAliases.get(i);
                    if (!id.equals(c))
                    {
                        if (definedOrdering.containsKey(c))
                            throw new InvalidRequestException(String.format("The order of columns in the CLUSTERING ORDER directive must be the one of the clustering key (%s must appear before %s)", c, id));
                        else
                            throw new InvalidRequestException(String.format("Missing CLUSTERING ORDER for column %s", c));
                    }
                    ++i;
                }
            }

            return new ParsedStatement.Prepared(stmt);
        }

        private AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, AbstractType> columns, ColumnIdentifier t) throws InvalidRequestException, ConfigurationException
        {
            AbstractType type = columns.get(t);
            if (type == null)
                throw new InvalidRequestException(String.format("Unkown definition %s referenced in PRIMARY KEY", t));
            if (type instanceof CollectionType)
                throw new InvalidRequestException(String.format("Invalid collection type for PRIMARY KEY component %s", t));
            columns.remove(t);
            Boolean isReversed = definedOrdering.get(t);
            return isReversed != null && isReversed ? ReversedType.getInstance(type) : type;
        }

        public void addDefinition(ColumnIdentifier def, CQL3Type type)
        {
            definedNames.add(def);
            definitions.put(def, type);
        }

        public void addKeyAliases(List<ColumnIdentifier> aliases)
        {
            keyAliases.add(aliases);
        }

        public void addColumnAlias(ColumnIdentifier alias)
        {
            columnAliases.add(alias);
        }

        public void setOrdering(ColumnIdentifier alias, boolean reversed)
        {
            definedOrdering.put(alias, reversed);
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
