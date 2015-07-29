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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.utils.ByteBufferUtil;

/** A <code>CREATE TABLE</code> parsed from a CQL query statement. */
public class CreateTableStatement extends SchemaAlteringStatement
{
    private List<AbstractType<?>> keyTypes;
    private List<AbstractType<?>> clusteringTypes;

    private Map<ByteBuffer, CollectionType> collections = new HashMap<>();

    private final List<ColumnIdentifier> keyAliases = new ArrayList<>();
    private final List<ColumnIdentifier> columnAliases = new ArrayList<>();
    private ByteBuffer valueAlias;

    private boolean isDense;
    private boolean isCompound;
    private boolean hasCounters;

    // use a TreeMap to preserve ordering across JDK versions (see CASSANDRA-9492)
    private final Map<ColumnIdentifier, AbstractType> columns = new TreeMap<>(new Comparator<ColumnIdentifier>()
    {
        public int compare(ColumnIdentifier o1, ColumnIdentifier o2)
        {
            return o1.bytes.compareTo(o2.bytes);
        }
    });
    private final Set<ColumnIdentifier> staticColumns;
    private final CFPropDefs properties;
    private final boolean ifNotExists;

    public CreateTableStatement(CFName name, CFPropDefs properties, boolean ifNotExists, Set<ColumnIdentifier> staticColumns)
    {
        super(name);
        this.properties = properties;
        this.ifNotExists = ifNotExists;
        this.staticColumns = staticColumns;

        if (!this.properties.hasProperty(CFPropDefs.KW_COMPRESSION) && CFMetaData.DEFAULT_COMPRESSOR != null)
            this.properties.addProperty(CFPropDefs.KW_COMPRESSION,
                                        new HashMap<String, String>()
                                        {{
                                            put(CompressionParameters.CLASS, CFMetaData.DEFAULT_COMPRESSOR);
                                        }});
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(keyspace(), Permission.CREATE);
    }

    public void validate(ClientState state)
    {
        // validated in announceMigration()
    }

    public boolean announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        try
        {
            MigrationManager.announceNewColumnFamily(getCFMetaData(), isLocalOnly);
            return true;
        }
        catch (AlreadyExistsException e)
        {
            if (ifNotExists)
                return false;
            throw e;
        }
    }

    public Event.SchemaChange changeEvent()
    {
        return new Event.SchemaChange(Event.SchemaChange.Change.CREATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }

    protected void grantPermissionsToCreator(QueryState state)
    {
        try
        {
            IResource resource = DataResource.table(keyspace(), columnFamily());
            DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                     resource.applicablePermissions(),
                                                     resource,
                                                     RoleResource.role(state.getClientState().getUser().getName()));
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public CFMetaData.Builder metadataBuilder()
    {
        CFMetaData.Builder builder = CFMetaData.Builder.create(keyspace(), columnFamily(), isDense, isCompound, hasCounters);
        for (int i = 0; i < keyAliases.size(); i++)
            builder.addPartitionKey(keyAliases.get(i), keyTypes.get(i));
        for (int i = 0; i < columnAliases.size(); i++)
            builder.addClusteringColumn(columnAliases.get(i), clusteringTypes.get(i));

        boolean isStaticCompact = !isDense && !isCompound;
        for (Map.Entry<ColumnIdentifier, AbstractType> entry : columns.entrySet())
        {
            ColumnIdentifier name = entry.getKey();
            // Note that for "static" no-clustering compact storage we use static for the defined columns
            if (staticColumns.contains(name) || isStaticCompact)
                builder.addStaticColumn(name, entry.getValue());
            else
                builder.addRegularColumn(name, entry.getValue());
        }

        boolean isCompactTable = isDense || !isCompound;
        if (isCompactTable)
        {
            CompactTables.DefaultNames names = CompactTables.defaultNameGenerator(builder.usedColumnNames());
            // Compact tables always have a clustering and a single regular value.
            if (isStaticCompact)
            {
                builder.addClusteringColumn(names.defaultClusteringName(), UTF8Type.instance);
                builder.addRegularColumn(names.defaultCompactValueName(), hasCounters ? CounterColumnType.instance : BytesType.instance);
            }
            else if (isDense && !builder.hasRegulars())
            {
                // Even for dense, we might not have our regular column if it wasn't part of the declaration. If
                // that's the case, add it but with a specific EmptyType so we can recognize that case later
                builder.addRegularColumn(names.defaultCompactValueName(), EmptyType.instance);
            }
        }

        return builder;
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
        CFMetaData newCFMD = metadataBuilder().build();
        applyPropertiesTo(newCFMD);
        return newCFMD;
    }

    public void applyPropertiesTo(CFMetaData cfmd) throws RequestValidationException
    {
        properties.applyToCFMetadata(cfmd);
    }

    public static class RawStatement extends CFStatement
    {
        private final Map<ColumnIdentifier, CQL3Type.Raw> definitions = new HashMap<>();
        public final CFPropDefs properties = new CFPropDefs();

        private final List<List<ColumnIdentifier>> keyAliases = new ArrayList<List<ColumnIdentifier>>();
        private final List<ColumnIdentifier> columnAliases = new ArrayList<ColumnIdentifier>();
        private final Map<ColumnIdentifier, Boolean> definedOrdering = new LinkedHashMap<ColumnIdentifier, Boolean>(); // Insertion ordering is important
        private final Set<ColumnIdentifier> staticColumns = new HashSet<ColumnIdentifier>();

        private boolean useCompactStorage;
        private final Multiset<ColumnIdentifier> definedNames = HashMultiset.create(1);

        private final boolean ifNotExists;

        public RawStatement(CFName name, boolean ifNotExists)
        {
            super(name);
            this.ifNotExists = ifNotExists;
        }

        /**
         * Transform this raw statement into a CreateTableStatement.
         */
        public ParsedStatement.Prepared prepare() throws RequestValidationException
        {
            // Column family name
            if (!columnFamily().matches("\\w+"))
                throw new InvalidRequestException(String.format("\"%s\" is not a valid table name (must be alphanumeric character only: [0-9A-Za-z]+)", columnFamily()));
            if (columnFamily().length() > Schema.NAME_LENGTH)
                throw new InvalidRequestException(String.format("Table names shouldn't be more than %s characters long (got \"%s\")", Schema.NAME_LENGTH, columnFamily()));

            for (Multiset.Entry<ColumnIdentifier> entry : definedNames.entrySet())
                if (entry.getCount() > 1)
                    throw new InvalidRequestException(String.format("Multiple definition of identifier %s", entry.getElement()));

            properties.validate();

            CreateTableStatement stmt = new CreateTableStatement(cfName, properties, ifNotExists, staticColumns);

            for (Map.Entry<ColumnIdentifier, CQL3Type.Raw> entry : definitions.entrySet())
            {
                ColumnIdentifier id = entry.getKey();
                CQL3Type pt = entry.getValue().prepare(keyspace());
                if (pt.isCollection() && ((CollectionType)pt.getType()).isMultiCell())
                    stmt.collections.put(id.bytes, (CollectionType)pt.getType());
                if (entry.getValue().isCounter())
                    stmt.hasCounters = true;
                stmt.columns.put(id, pt.getType()); // we'll remove what is not a column below
            }

            if (keyAliases.isEmpty())
                throw new InvalidRequestException("No PRIMARY KEY specifed (exactly one required)");
            if (keyAliases.size() > 1)
                throw new InvalidRequestException("Multiple PRIMARY KEYs specifed (exactly one required)");
            if (stmt.hasCounters && properties.getDefaultTimeToLive() > 0)
                throw new InvalidRequestException("Cannot set default_time_to_live on a table with counters");

            List<ColumnIdentifier> kAliases = keyAliases.get(0);
            stmt.keyTypes = new ArrayList<AbstractType<?>>(kAliases.size());
            for (ColumnIdentifier alias : kAliases)
            {
                stmt.keyAliases.add(alias);
                AbstractType<?> t = getTypeAndRemove(stmt.columns, alias);
                if (t instanceof CounterColumnType)
                    throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", alias));
                if (staticColumns.contains(alias))
                    throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", alias));
                stmt.keyTypes.add(t);
            }

            stmt.clusteringTypes = new ArrayList<>(columnAliases.size());
            // Handle column aliases
            for (ColumnIdentifier t : columnAliases)
            {
                stmt.columnAliases.add(t);

                AbstractType<?> type = getTypeAndRemove(stmt.columns, t);
                if (type instanceof CounterColumnType)
                    throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", t));
                if (staticColumns.contains(t))
                    throw new InvalidRequestException(String.format("Static column %s cannot be part of the PRIMARY KEY", t));
                stmt.clusteringTypes.add(type);
            }

            // We've handled anything that is not a rpimary key so stmt.columns only contains NON-PK columns. So
            // if it's a counter table, make sure we don't have non-counter types
            if (stmt.hasCounters)
            {
                for (AbstractType<?> type : stmt.columns.values())
                    if (!type.isCounter())
                        throw new InvalidRequestException("Cannot mix counter and non counter columns in the same table");
            }

            // Dense means that on the thrift side, no part of the "thrift column name" stores a "CQL/metadata column name".
            // This means COMPACT STORAGE with at least one clustering type (otherwise it's a thrift "static" CF).
            stmt.isDense = useCompactStorage && !stmt.clusteringTypes.isEmpty();
            // Compound means that on the thrift side, the "thrift column name" is a composite one. It's the case unless
            // we use compact storage COMPACT STORAGE and we have either no clustering columns (thrift "static" CF) or
            // only one of them (if more than one, it's a "dense composite").
            stmt.isCompound = !(useCompactStorage && stmt.clusteringTypes.size() <= 1);

            // For COMPACT STORAGE, we reject any "feature" that we wouldn't be able to translate back to thrift.
            if (useCompactStorage)
            {
                if (!stmt.collections.isEmpty())
                    throw new InvalidRequestException("Non-frozen collection types are not supported with COMPACT STORAGE");
                if (!staticColumns.isEmpty())
                    throw new InvalidRequestException("Static columns are not supported in COMPACT STORAGE tables");

                if (stmt.clusteringTypes.isEmpty())
                {
                    // It's a thrift "static CF" so there should be some columns definition
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("No definition found that is not part of the PRIMARY KEY");
                }

                if (stmt.isDense)
                {
                    // We can have no columns (only the PK), but we can't have more than one.
                    if (stmt.columns.size() > 1)
                        throw new InvalidRequestException(String.format("COMPACT STORAGE with composite PRIMARY KEY allows no more than one column not part of the PRIMARY KEY (got: %s)", StringUtils.join(stmt.columns.keySet(), ", ")));
                }
                else
                {
                    // we are in the "static" case, so we need at least one column defined. For non-compact however, having
                    // just the PK is fine.
                    if (stmt.columns.isEmpty())
                        throw new InvalidRequestException("COMPACT STORAGE with non-composite PRIMARY KEY require one column not part of the PRIMARY KEY, none given");
                }
            }
            else
            {
                if (stmt.clusteringTypes.isEmpty() && !staticColumns.isEmpty())
                {
                    // Static columns only make sense if we have at least one clustering column. Otherwise everything is static anyway
                    if (columnAliases.isEmpty())
                        throw new InvalidRequestException("Static columns are only useful (and thus allowed) if the table has at least one clustering column");
                }
            }

            // If we give a clustering order, we must explicitly do so for all aliases and in the order of the PK
            if (!definedOrdering.isEmpty())
            {
                if (definedOrdering.size() > columnAliases.size())
                    throw new InvalidRequestException("Only clustering key columns can be defined in CLUSTERING ORDER directive");

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

        private AbstractType<?> getTypeAndRemove(Map<ColumnIdentifier, AbstractType> columns, ColumnIdentifier t) throws InvalidRequestException
        {
            AbstractType type = columns.get(t);
            if (type == null)
                throw new InvalidRequestException(String.format("Unknown definition %s referenced in PRIMARY KEY", t));
            if (type.isCollection() && type.isMultiCell())
                throw new InvalidRequestException(String.format("Invalid collection type for PRIMARY KEY component %s", t));

            columns.remove(t);
            Boolean isReversed = definedOrdering.get(t);
            return isReversed != null && isReversed ? ReversedType.getInstance(type) : type;
        }

        public void addDefinition(ColumnIdentifier def, CQL3Type.Raw type, boolean isStatic)
        {
            definedNames.add(def);
            definitions.put(def, type);
            if (isStatic)
                staticColumns.add(def);
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
    }
}
