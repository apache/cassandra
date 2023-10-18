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
package org.apache.cassandra.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.SchemaElement;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.utils.AbstractIterator;
import org.github.jamm.Unmetered;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.cassandra.schema.IndexMetadata.isNameValid;

@Unmetered
public class TableMetadata implements SchemaElement
{
    private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);

    // Please note that currently the only one truly useful flag is COUNTER, as the rest of the flags were about
    // differencing between CQL tables and the various types of COMPACT STORAGE tables (pre-4.0). As those "compact"
    // tables are not supported anymore, no tables should be either SUPER or DENSE, and they should all be COMPOUND.
    public enum Flag
    {
        // As mentioned above, all tables on 4.0+ will have the COMPOUND flag, making the flag of little value. However,
        // on upgrade from pre-4.0, we want to detect if a tables does _not_ have this flag, in which case this would
        // be a compact table on which DROP COMPACT STORAGE has _not_ be used and fail startup. This is also why we
        // still write this flag for all tables. Once we drop support for upgrading from pre-4.0 versions (and so are
        // sure all tables do have the flag), we can stop writing this flag and ignore it when present (deprecate it).
        // Later, we'll be able to drop the flag from this enum completely.
        COMPOUND,
        DENSE,
        COUNTER,
        // The only reason we still have those is that on the first startup after an upgrade from pre-4.0, we cannot
        // guarantee some tables won't have those flags (users having forgotten to use DROP COMPACT STORAGE before
        // upgrading). So we still "deserialize" those flags correctly, but otherwise prevent startup if any table
        // have them. Once we drop support for upgrading from pre-4.0, we can remove those values.
        /** @deprecated See CASSANDRA-16217 */
        @Deprecated(since = "4.0") SUPER;

        /*
         *  We call dense a CF for which each component of the comparator is a clustering column, i.e. no
         * component is used to store a regular column names. In other words, non-composite static "thrift"
         * and CQL3 CF are *not* dense.
         */
        public static boolean isDense(Set<TableMetadata.Flag> flags)
        {
            return flags.contains(TableMetadata.Flag.DENSE);
        }

        public static boolean isCompound(Set<TableMetadata.Flag> flags)
        {
            return flags.contains(TableMetadata.Flag.COMPOUND);
        }


        public static boolean isSuper(Set<TableMetadata.Flag> flags)
        {
            return flags.contains(TableMetadata.Flag.SUPER);
        }

        public static boolean isCQLTable(Set<TableMetadata.Flag> flags)
        {
            return !isSuper(flags) && !isDense(flags) && isCompound(flags);
        }

        public static boolean isStaticCompactTable(Set<TableMetadata.Flag> flags)
        {
            return !Flag.isSuper(flags) && !Flag.isDense(flags) && !Flag.isCompound(flags);
        }

        public static Set<Flag> fromStringSet(Set<String> strings)
        {
            return strings.stream().map(String::toUpperCase).map(Flag::valueOf).collect(toSet());
        }

        public static Set<String> toStringSet(Set<Flag> flags)
        {
            return flags.stream().map(Flag::toString).map(String::toLowerCase).collect(toSet());
        }
    }

    public enum Kind
    {
        REGULAR, INDEX, VIEW, VIRTUAL
    }

    public final String keyspace;
    public final String name;
    public final TableId id;

    public final IPartitioner partitioner;
    public final Kind kind;
    public final TableParams params;
    public final ImmutableSet<Flag> flags;

    @Nullable
    private final String indexName; // derived from table name

    /*
     * All CQL3 columns definition are stored in the columns map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and
     * 2) for the partition key and clustering key ones, those list are ordered by the "component index" of the elements.
     */
    public final ImmutableMap<ByteBuffer, DroppedColumn> droppedColumns;
    final ImmutableMap<ByteBuffer, ColumnMetadata> columns;

    protected final ImmutableList<ColumnMetadata> partitionKeyColumns;
    protected final ImmutableList<ColumnMetadata> clusteringColumns;
    protected final RegularAndStaticColumns regularAndStaticColumns;

    public final Indexes indexes;
    public final Triggers triggers;

    // derived automatically from flags and columns
    public final AbstractType<?> partitionKeyType;
    public final ClusteringComparator comparator;

    // performance hacks; TODO see if all are really necessary
    public final DataResource resource;

    protected TableMetadata(Builder builder)
    {
        flags = Sets.immutableEnumSet(builder.flags);
        keyspace = builder.keyspace;
        name = builder.name;
        id = builder.id;

        partitioner = builder.partitioner;
        kind = builder.kind;
        params = builder.params.build();

        indexName = kind == Kind.INDEX ? name.substring(name.indexOf('.') + 1) : null;

        droppedColumns = ImmutableMap.copyOf(builder.droppedColumns);
        Collections.sort(builder.partitionKeyColumns);
        partitionKeyColumns = ImmutableList.copyOf(builder.partitionKeyColumns);
        Collections.sort(builder.clusteringColumns);
        clusteringColumns = ImmutableList.copyOf(builder.clusteringColumns);
        regularAndStaticColumns = RegularAndStaticColumns.builder().addAll(builder.regularAndStaticColumns).build();
        columns = ImmutableMap.copyOf(builder.columns);

        indexes = builder.indexes;
        triggers = builder.triggers;

        partitionKeyType = partitionKeyColumns.size() == 1
                         ? partitionKeyColumns.get(0).type
                         : CompositeType.getInstance(transform(partitionKeyColumns, t -> t.type));

        comparator = new ClusteringComparator(transform(clusteringColumns, c -> c.type));

        resource = DataResource.table(keyspace, name);
    }

    public static Builder builder(String keyspace, String table)
    {
        return new Builder(keyspace, table);
    }

    public static Builder builder(String keyspace, String table, TableId id)
    {
        return new Builder(keyspace, table, id);
    }

    public Builder unbuild()
    {
        return builder(keyspace, name, id)
               .partitioner(partitioner)
               .kind(kind)
               .params(params)
               .flags(flags)
               .addColumns(columns())
               .droppedColumns(droppedColumns)
               .indexes(indexes)
               .triggers(triggers);
    }

    public boolean isIndex()
    {
        return kind == Kind.INDEX;
    }

    public TableMetadata withSwapped(TableParams params)
    {
        return unbuild().params(params).build();
    }

    public TableMetadata withSwapped(Set<Flag> flags)
    {
        return unbuild().flags(flags).build();
    }

    public TableMetadata withSwapped(Triggers triggers)
    {
        return unbuild().triggers(triggers).build();
    }

    public TableMetadata withSwapped(Indexes indexes)
    {
        return unbuild().indexes(indexes).build();
    }

    public boolean isView()
    {
        return kind == Kind.VIEW;
    }

    public boolean isVirtual()
    {
        return kind == Kind.VIRTUAL;
    }

    public Optional<String> indexName()
    {
        return Optional.ofNullable(indexName);
    }

    public boolean isCounter()
    {
        return flags.contains(Flag.COUNTER);
    }

    public boolean isCompactTable()
    {
        return false;
    }
    
    public boolean isIncrementalBackupsEnabled()
    {
        return params.incrementalBackups;
    }

    public boolean isStaticCompactTable()
    {
        return false;
    }

    public ImmutableCollection<ColumnMetadata> columns()
    {
        return columns.values();
    }

    public Iterable<ColumnMetadata> primaryKeyColumns()
    {
        return Iterables.concat(partitionKeyColumns, clusteringColumns);
    }

    public ImmutableList<ColumnMetadata> partitionKeyColumns()
    {
        return partitionKeyColumns;
    }

    public ImmutableList<ColumnMetadata> clusteringColumns()
    {
        return clusteringColumns;
    }

    public RegularAndStaticColumns regularAndStaticColumns()
    {
        return regularAndStaticColumns;
    }

    public Columns regularColumns()
    {
        return regularAndStaticColumns.regulars;
    }

    public Columns staticColumns()
    {
        return regularAndStaticColumns.statics;
    }

    /*
     * An iterator over all column definitions but that respect the order of a SELECT *.
     */
    public Iterator<ColumnMetadata> allColumnsInSelectOrder()
    {
        Iterator<ColumnMetadata> partitionKeyIter = partitionKeyColumns.iterator();
        Iterator<ColumnMetadata> clusteringIter = clusteringColumns.iterator();
        Iterator<ColumnMetadata> otherColumns = regularAndStaticColumns.selectOrderIterator();

        return columnsIterator(partitionKeyIter, clusteringIter, otherColumns);
    }

    /**
     * Returns an iterator over all column definitions that respect the order of the CREATE statement.
     */
    public Iterator<ColumnMetadata> allColumnsInCreateOrder()
    {
        Iterator<ColumnMetadata> partitionKeyIter = partitionKeyColumns.iterator();
        Iterator<ColumnMetadata> clusteringIter = clusteringColumns.iterator();
        Iterator<ColumnMetadata> otherColumns = regularAndStaticColumns.iterator();

        return columnsIterator(partitionKeyIter, clusteringIter, otherColumns);
    }

    private static Iterator<ColumnMetadata> columnsIterator(Iterator<ColumnMetadata> partitionKeys,
                                                            Iterator<ColumnMetadata> clusteringColumns,
                                                            Iterator<ColumnMetadata> otherColumns)
    {
        return new AbstractIterator<ColumnMetadata>()
        {
            protected ColumnMetadata computeNext()
            {
                if (partitionKeys.hasNext())
                    return partitionKeys.next();

                if (clusteringColumns.hasNext())
                    return clusteringColumns.next();

                return otherColumns.hasNext() ? otherColumns.next() : endOfData();
            }
        };
    }

    /**
     * Returns the ColumnMetadata for {@code name}.
     */
    public ColumnMetadata getColumn(ColumnIdentifier name)
    {
        return columns.get(name.bytes);
    }
    /**
     * Returns the column of the provided name if it exists, but throws a user-visible exception if that column doesn't
     * exist.
     *
     * <p>This method is for finding columns from a name provided by the user, and as such it does _not_ returne hidden
     * columns (throwing that the column is unknown instead).
     *
     * @param name the name of an existing non-hidden column of this table.
     * @return the column metadata corresponding to {@code name}.
     *
     * @throws InvalidRequestException if there is no non-hidden column named {@code name} in this table.
     */
    public ColumnMetadata getExistingColumn(ColumnIdentifier name)
    {
        ColumnMetadata def = getColumn(name);
        if (def == null)
            throw new InvalidRequestException(format("Undefined column name %s in table %s", name.toCQLString(), this));
        return def;
    }
    /*
     * In general it is preferable to work with ColumnIdentifier to make it
     * clear that we are talking about a CQL column, not a cell name, but there
     * is a few cases where all we have is a ByteBuffer (when dealing with IndexExpression
     * for instance) so...
     */
    public ColumnMetadata getColumn(ByteBuffer name)
    {
        return columns.get(name);
    }

    public ColumnMetadata getDroppedColumn(ByteBuffer name)
    {
        DroppedColumn dropped = droppedColumns.get(name);
        return dropped == null ? null : dropped.column;
    }

    /**
     * Returns a "fake" ColumnMetadata corresponding to the dropped column {@code name}
     * of {@code null} if there is no such dropped column.
     *
     * @param name - the column name
     * @param isStatic - whether the column was a static column, if known
     */
    public ColumnMetadata getDroppedColumn(ByteBuffer name, boolean isStatic)
    {
        DroppedColumn dropped = droppedColumns.get(name);
        if (dropped == null)
            return null;

        if (isStatic && !dropped.column.isStatic())
            return ColumnMetadata.staticColumn(this, name, dropped.column.type);

        return dropped.column;
    }

    public boolean hasStaticColumns()
    {
        return !staticColumns().isEmpty();
    }

    /**
     * @return {@code true} if the table has any masked column, {@code false} otherwise.
     */
    public boolean hasMaskedColumns()
    {
        for (ColumnMetadata column : columns.values())
        {
            if (column.isMasked())
                return true;
        }
        return false;
    }

    /**
     * @param function a user function
     * @return {@code true} if the table has any masked column depending on the specified user function,
     * {@code false} otherwise.
     */
    public boolean dependsOn(Function function)
    {
        for (ColumnMetadata column : columns.values())
        {
            ColumnMask mask = column.getMask();
            if (mask != null && mask.function.name().equals(function.name()))
                return true;
        }
        return false;
    }

    public void validate()
    {
        if (!isNameValid(keyspace))
            except("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", SchemaConstants.NAME_LENGTH, keyspace);

        if (!isNameValid(name))
            except("Table name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", SchemaConstants.NAME_LENGTH, name);

        params.validate();

        if (partitionKeyColumns.stream().anyMatch(c -> c.type.isCounter()))
            except("PRIMARY KEY columns cannot contain counters");

        // Mixing counter with non counter columns is not supported (#2614)
        if (isCounter())
        {
            for (ColumnMetadata column : regularAndStaticColumns)
                if (!(column.type.isCounter()) && !isSuperColumnMapColumnName(column.name))
                    except("Cannot have a non counter column (\"%s\") in a counter table", column.name);
        }
        else
        {
            for (ColumnMetadata column : regularAndStaticColumns)
                if (column.type.isCounter())
                    except("Cannot have a counter column (\"%s\") in a non counter table", column.name);
        }

        // All tables should have a partition key
        if (partitionKeyColumns.isEmpty())
            except("Missing partition keys for table %s", toString());

        indexes.validate(this);
    }

    /**
     * To support backward compatibility with thrift super columns in the C* 3.0+ storage engine, we encode said super
     * columns as a CQL {@code map<blob, blob>}. To ensure the name of this map did not conflict with any other user
     * defined columns, we used the empty name (which is otherwise not allowed for user created columns).
     * <p>
     * While all thrift-based tables must have been converted to "CQL" ones with "DROP COMPACT STORAGE" (before
     * upgrading to C* 4.0, which stop supporting non-CQL tables completely), a converted super-column table will still
     * have this map with an empty name. And the reason we need to recognize it still, is that for backward
     * compatibility we need to support counters in values of this map while it's not supported in any other map.
     *
     * TODO: it's probably worth lifting the limitation of not allowing counters as map values. It works fully
     *   internally (since we had to support it for this special map) and doesn't feel particularly dangerous to
     *   support. Doing so would remove this special case, but would also let user that do have an upgraded super-column
     *   table with counters to rename that weirdly name map to something more meaningful (it's not possible today
     *   as after renaming the validation in {@link #validate} would trigger).
     */
    private static boolean isSuperColumnMapColumnName(ColumnIdentifier columnName)
    {
        return !columnName.bytes.hasRemaining();
    }

    public void validateCompatibility(TableMetadata previous)
    {
        if (isIndex())
            return;

        if (!previous.keyspace.equals(keyspace))
            except("Keyspace mismatch (found %s; expected %s)", keyspace, previous.keyspace);

        if (!previous.name.equals(name))
            except("Table mismatch (found %s; expected %s)", name, previous.name);

        if (!previous.id.equals(id))
            except("Table ID mismatch (found %s; expected %s)", id, previous.id);

        if (!previous.flags.equals(flags) && (!Flag.isCQLTable(flags) || Flag.isCQLTable(previous.flags)))
            except("Table type mismatch (found %s; expected %s)", flags, previous.flags);

        if (previous.partitionKeyColumns.size() != partitionKeyColumns.size())
        {
            except("Partition keys of different length (found %s; expected %s)",
                   partitionKeyColumns.size(),
                   previous.partitionKeyColumns.size());
        }

        for (int i = 0; i < partitionKeyColumns.size(); i++)
        {
            if (!partitionKeyColumns.get(i).type.isCompatibleWith(previous.partitionKeyColumns.get(i).type))
            {
                except("Partition key column mismatch (found %s; expected %s)",
                       partitionKeyColumns.get(i).type,
                       previous.partitionKeyColumns.get(i).type);
            }
        }

        if (previous.clusteringColumns.size() != clusteringColumns.size())
        {
            except("Clustering columns of different length (found %s; expected %s)",
                   clusteringColumns.size(),
                   previous.clusteringColumns.size());
        }

        for (int i = 0; i < clusteringColumns.size(); i++)
        {
            if (!clusteringColumns.get(i).type.isCompatibleWith(previous.clusteringColumns.get(i).type))
            {
                except("Clustering column mismatch (found %s; expected %s)",
                       clusteringColumns.get(i).type,
                       previous.clusteringColumns.get(i).type);
            }
        }

        for (ColumnMetadata previousColumn : previous.regularAndStaticColumns)
        {
            ColumnMetadata column = getColumn(previousColumn.name);
            if (column != null && !column.type.isCompatibleWith(previousColumn.type))
                except("Column mismatch (found %s; expected %s)", column, previousColumn);
        }
    }

    public ClusteringComparator partitionKeyAsClusteringComparator()
    {
        return new ClusteringComparator(partitionKeyColumns.stream().map(c -> c.type).collect(toList()));
    }

    /**
     * Generate a table name for an index corresponding to the given column.
     * This is NOT the same as the index's name! This is only used in sstable filenames and is not exposed to users.
     *
     * @param info A definition of the column with index
     *
     * @return name of the index table
     */
    public String indexTableName(IndexMetadata info)
    {
        // TODO simplify this when info.index_name is guaranteed to be set
        return name + Directories.SECONDARY_INDEX_NAME_SEPARATOR + info.name;
    }

    /**
     * @return true if the change as made impacts queries/updates on the table,
     *         e.g. any columns or indexes were added, removed, or altered; otherwise, false is returned.
     *         Used to determine whether prepared statements against this table need to be re-prepared.
     */
    boolean changeAffectsPreparedStatements(TableMetadata updated)
    {
        return !partitionKeyColumns.equals(updated.partitionKeyColumns)
            || !clusteringColumns.equals(updated.clusteringColumns)
            || !regularAndStaticColumns.equals(updated.regularAndStaticColumns)
            || !indexes.equals(updated.indexes)
            || params.defaultTimeToLive != updated.params.defaultTimeToLive
            || params.gcGraceSeconds != updated.params.gcGraceSeconds
            || ( !Flag.isCQLTable(flags) && Flag.isCQLTable(updated.flags) );
    }

    /**
     * There is a couple of places in the code where we need a TableMetadata object and don't have one readily available
     * and know that only the keyspace and name matter. This creates such "fake" metadata. Use only if you know what
     * you're doing.
     */
    public static TableMetadata minimal(String keyspace, String name)
    {
        return TableMetadata.builder(keyspace, name)
                            .addPartitionKeyColumn("key", BytesType.instance)
                            .build();
    }

    public TableMetadata updateIndexTableMetadata(TableParams baseTableParams)
    {
        TableParams.Builder builder = baseTableParams.unbuild().gcGraceSeconds(0);

        // Depends on parent's cache setting, turn on its index table's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        builder.caching(baseTableParams.caching.cacheKeys() ? CachingParams.CACHE_KEYS : CachingParams.CACHE_NOTHING);

        return unbuild().params(builder.build()).build();
    }

    boolean referencesUserType(ByteBuffer name)
    {
        return any(columns(), c -> c.type.referencesUserType(name));
    }

    public TableMetadata withUpdatedUserType(UserType udt)
    {
        if (!referencesUserType(udt.name))
            return this;

        Builder builder = unbuild();
        columns().forEach(c -> builder.alterColumnType(c.name, c.type.withUpdatedUserType(udt)));

        return builder.build();
    }

    protected void except(String format, Object... args)
    {
        throw new ConfigurationException(keyspace + "." + name + ": " + format(format, args));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof TableMetadata))
            return false;

        TableMetadata tm = (TableMetadata) o;

        return equalsWithoutColumns(tm) && columns.equals(tm.columns);
    }

    private boolean equalsWithoutColumns(TableMetadata tm)
    {
        return keyspace.equals(tm.keyspace)
            && name.equals(tm.name)
            && id.equals(tm.id)
            && partitioner.equals(tm.partitioner)
            && kind == tm.kind
            && params.equals(tm.params)
            && flags.equals(tm.flags)
            && droppedColumns.equals(tm.droppedColumns)
            && indexes.equals(tm.indexes)
            && triggers.equals(tm.triggers);
    }

    Optional<Difference> compare(TableMetadata other)
    {
        return equalsWithoutColumns(other)
             ? compareColumns(other.columns)
             : Optional.of(Difference.SHALLOW);
    }

    private Optional<Difference> compareColumns(Map<ByteBuffer, ColumnMetadata> other)
    {
        if (!columns.keySet().equals(other.keySet()))
            return Optional.of(Difference.SHALLOW);

        boolean differsDeeply = false;

        for (Map.Entry<ByteBuffer, ColumnMetadata> entry : columns.entrySet())
        {
            ColumnMetadata thisColumn = entry.getValue();
            ColumnMetadata thatColumn = other.get(entry.getKey());

            Optional<Difference> difference = thisColumn.compare(thatColumn);
            if (difference.isPresent())
            {
                switch (difference.get())
                {
                    case SHALLOW:
                        return difference;
                    case DEEP:
                        differsDeeply = true;
                }
            }
        }

        return differsDeeply ? Optional.of(Difference.DEEP) : Optional.empty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, name, id, partitioner, kind, params, flags, columns, droppedColumns, indexes, triggers);
    }

    @Override
    public String toString()
    {
        return format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(name));
    }

    public String toDebugString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("keyspace", keyspace)
                          .add("table", name)
                          .add("id", id)
                          .add("partitioner", partitioner)
                          .add("kind", kind)
                          .add("params", params)
                          .add("flags", flags)
                          .add("columns", columns())
                          .add("droppedColumns", droppedColumns.values())
                          .add("indexes", indexes)
                          .add("triggers", triggers)
                          .toString();
    }

    public static final class Builder
    {
        final String keyspace;
        final String name;

        private TableId id;

        private IPartitioner partitioner;
        private Kind kind = Kind.REGULAR;
        private TableParams.Builder params = TableParams.builder();

        // See the comment on Flag.COMPOUND definition for why we (still) inconditionally add this flag.
        private Set<Flag> flags = EnumSet.of(Flag.COMPOUND);
        private Triggers triggers = Triggers.none();
        private Indexes indexes = Indexes.none();

        private final Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        private final Map<ByteBuffer, ColumnMetadata> columns = new HashMap<>();
        private final List<ColumnMetadata> partitionKeyColumns = new ArrayList<>();
        private final List<ColumnMetadata> clusteringColumns = new ArrayList<>();
        private final List<ColumnMetadata> regularAndStaticColumns = new ArrayList<>();

        private Builder(String keyspace, String name, TableId id)
        {
            this.keyspace = keyspace;
            this.name = name;
            this.id = id;
        }

        private Builder(String keyspace, String name)
        {
            this.keyspace = keyspace;
            this.name = name;
        }

        public TableMetadata build()
        {
            if (partitioner == null)
                partitioner = DatabaseDescriptor.getPartitioner();

            if (id == null)
            {
                // make sure vtables use determiniestic ids so they can be referenced in calls cross-nodes
                // see CASSANDRA-17295
                if (DatabaseDescriptor.useDeterministicTableID() || kind == Kind.VIRTUAL) id = TableId.unsafeDeterministic(keyspace, name);
                else id = TableId.generate();
            }

            if (Flag.isCQLTable(flags))
                return new TableMetadata(this);
            else
                return new CompactTableMetadata(this);
        }

        public Builder id(TableId val)
        {
            id = val;
            return this;
        }

        public Builder partitioner(IPartitioner val)
        {
            partitioner = val;
            return this;
        }

        public Builder kind(Kind val)
        {
            kind = val;
            return this;
        }

        public Builder params(TableParams val)
        {
            params = val.unbuild();
            return this;
        }

        public Builder allowAutoSnapshot(boolean val)
        {
            params.allowAutoSnapshot(val);
            return this;
        }

        public Builder bloomFilterFpChance(double val)
        {
            params.bloomFilterFpChance(val);
            return this;
        }

        public Builder caching(CachingParams val)
        {
            params.caching(val);
            return this;
        }

        public Builder comment(String val)
        {
            params.comment(val);
            return this;
        }

        public Builder compaction(CompactionParams val)
        {
            params.compaction(val);
            return this;
        }

        public Builder compression(CompressionParams val)
        {
            params.compression(val);
            return this;
        }

        public Builder defaultTimeToLive(int val)
        {
            params.defaultTimeToLive(val);
            return this;
        }

        public Builder gcGraceSeconds(int val)
        {
            params.gcGraceSeconds(val);
            return this;
        }

        public Builder maxIndexInterval(int val)
        {
            params.maxIndexInterval(val);
            return this;
        }

        public Builder memtableFlushPeriod(int val)
        {
            params.memtableFlushPeriodInMs(val);
            return this;
        }

        public Builder minIndexInterval(int val)
        {
            params.minIndexInterval(val);
            return this;
        }

        public Builder crcCheckChance(double val)
        {
            params.crcCheckChance(val);
            return this;
        }

        public Builder speculativeRetry(SpeculativeRetryPolicy val)
        {
            params.speculativeRetry(val);
            return this;
        }

        public Builder additionalWritePolicy(SpeculativeRetryPolicy val)
        {
            params.additionalWritePolicy(val);
            return this;
        }

        public Builder extensions(Map<String, ByteBuffer> val)
        {
            params.extensions(val);
            return this;
        }

        public Builder flags(Set<Flag> val)
        {
            flags = val;
            return this;
        }

        public Builder memtable(MemtableParams val)
        {
            params.memtable(val);
            return this;
        }


        public Builder isCounter(boolean val)
        {
            return flag(Flag.COUNTER, val);
        }

        private Builder flag(Flag flag, boolean set)
        {
            if (set) flags.add(flag); else flags.remove(flag);
            return this;
        }

        public Builder triggers(Triggers val)
        {
            triggers = val;
            return this;
        }

        public Builder indexes(Indexes val)
        {
            indexes = val;
            return this;
        }

        public Builder addPartitionKeyColumn(String name, AbstractType<?> type)
        {
            return addPartitionKeyColumn(name, type, null);
        }

        public Builder addPartitionKeyColumn(String name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addPartitionKeyColumn(ColumnIdentifier.getInterned(name, false), type, mask);
        }

        public Builder addPartitionKeyColumn(ColumnIdentifier name, AbstractType<?> type)
        {
            return addPartitionKeyColumn(name, type, null);
        }

        public Builder addPartitionKeyColumn(ColumnIdentifier name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, partitionKeyColumns.size(), ColumnMetadata.Kind.PARTITION_KEY, mask));
        }

        public Builder addClusteringColumn(String name, AbstractType<?> type)
        {
            return addClusteringColumn(name, type, null);
        }

        public Builder addClusteringColumn(String name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addClusteringColumn(ColumnIdentifier.getInterned(name, false), type, mask);
        }

        public Builder addClusteringColumn(ColumnIdentifier name, AbstractType<?> type)
        {
            return addClusteringColumn(name, type, null);
        }

        public Builder addClusteringColumn(ColumnIdentifier name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, clusteringColumns.size(), ColumnMetadata.Kind.CLUSTERING, mask));
        }

        public Builder addRegularColumn(String name, AbstractType<?> type)
        {
            return addRegularColumn(name, type, null);
        }

        public Builder addRegularColumn(String name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addRegularColumn(ColumnIdentifier.getInterned(name, false), type, mask);
        }

        public Builder addRegularColumn(ColumnIdentifier name, AbstractType<?> type)
        {
            return addRegularColumn(name, type, null);
        }

        public Builder addRegularColumn(ColumnIdentifier name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR, mask));
        }

        public Builder addStaticColumn(String name, AbstractType<?> type)
        {
            return addStaticColumn(name, type, null);
        }

        public Builder addStaticColumn(String name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addStaticColumn(ColumnIdentifier.getInterned(name, false), type, mask);
        }

        public Builder addStaticColumn(ColumnIdentifier name, AbstractType<?> type)
        {
            return addStaticColumn(name, type, null);
        }

        public Builder addStaticColumn(ColumnIdentifier name, AbstractType<?> type, @Nullable ColumnMask mask)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.STATIC, mask));
        }

        public Builder addColumn(ColumnMetadata column)
        {
            if (columns.containsKey(column.name.bytes))
                throw new IllegalArgumentException();

            switch (column.kind)
            {
                case PARTITION_KEY:
                    partitionKeyColumns.add(column);
                    Collections.sort(partitionKeyColumns);
                    break;
                case CLUSTERING:
                    column.type.checkComparable();
                    clusteringColumns.add(column);
                    Collections.sort(clusteringColumns);
                    break;
                default:
                    regularAndStaticColumns.add(column);
            }

            columns.put(column.name.bytes, column);

            return this;
        }

        public Builder addColumns(Iterable<ColumnMetadata> columns)
        {
            columns.forEach(this::addColumn);
            return this;
        }

        public Builder droppedColumns(Map<ByteBuffer, DroppedColumn> droppedColumns)
        {
            this.droppedColumns.clear();
            this.droppedColumns.putAll(droppedColumns);
            return this;
        }

        /**
         * Records a deprecated column for a system table.
         */
        public Builder recordDeprecatedSystemColumn(String name, AbstractType<?> type)
        {
            // As we play fast and loose with the removal timestamp, make sure this is misued for a non system table.
            assert SchemaConstants.isLocalSystemKeyspace(keyspace);
            recordColumnDrop(ColumnMetadata.regularColumn(keyspace, this.name, name, type), Long.MAX_VALUE);
            return this;
        }

        public Builder recordColumnDrop(ColumnMetadata column, long timeMicros)
        {
            droppedColumns.put(column.name.bytes, new DroppedColumn(column.withNewType(column.type.expandUserTypes()), timeMicros));
            return this;
        }

        public Iterable<ColumnMetadata> columns()
        {
            return columns.values();
        }

        public int numColumns()
        {
            return columns.size();
        }

        public Set<String> columnNames()
        {
            return columns.values().stream().map(c -> c.name.toString()).collect(toSet());
        }

        public ColumnMetadata getColumn(ColumnIdentifier identifier)
        {
            return columns.get(identifier.bytes);
        }

        public ColumnMetadata getColumn(ByteBuffer name)
        {
            return columns.get(name);
        }

        public boolean hasRegularColumns()
        {
            return regularAndStaticColumns.stream().anyMatch(ColumnMetadata::isRegular);
        }

        /*
         * The following methods all assume a Builder with valid set of partition key, clustering, regular and static columns.
         */

        public Builder removeRegularOrStaticColumn(ColumnIdentifier identifier)
        {
            ColumnMetadata column = columns.get(identifier.bytes);
            if (column == null || column.isPrimaryKeyColumn())
                throw new IllegalArgumentException();

            columns.remove(identifier.bytes);
            regularAndStaticColumns.remove(column);

            return this;
        }

        public Builder renamePrimaryKeyColumn(ColumnIdentifier from, ColumnIdentifier to)
        {
            if (columns.containsKey(to.bytes))
                throw new IllegalArgumentException();

            ColumnMetadata column = columns.get(from.bytes);
            if (column == null || !column.isPrimaryKeyColumn())
                throw new IllegalArgumentException();

            ColumnMetadata newColumn = column.withNewName(to);
            if (column.isPartitionKey())
                partitionKeyColumns.set(column.position(), newColumn);
            else
                clusteringColumns.set(column.position(), newColumn);

            columns.remove(from.bytes);
            columns.put(to.bytes, newColumn);

            return this;
        }

        public Builder alterColumnMask(ColumnIdentifier name, @Nullable ColumnMask mask)
        {
            ColumnMetadata column = columns.get(name.bytes);
            if (column == null)
                throw new IllegalArgumentException();

            ColumnMetadata newColumn = column.withNewMask(mask);

            updateColumn(column, newColumn);

            return this;
        }

        Builder alterColumnType(ColumnIdentifier name, AbstractType<?> type)
        {
            ColumnMetadata column = columns.get(name.bytes);
            if (column == null)
                throw new IllegalArgumentException();

            ColumnMetadata newColumn = column.withNewType(type);

            updateColumn(column, newColumn);

            return this;
        }

        private void updateColumn(ColumnMetadata column, ColumnMetadata newColumn)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    partitionKeyColumns.set(column.position(), newColumn);
                    break;
                case CLUSTERING:
                    clusteringColumns.set(column.position(), newColumn);
                    break;
                case REGULAR:
                case STATIC:
                    regularAndStaticColumns.remove(column);
                    regularAndStaticColumns.add(newColumn);
                    break;
            }

            columns.put(column.name.bytes, newColumn);
        }
    }
    
    /**
     * A table with strict liveness filters/ignores rows without PK liveness info,
     * effectively tying the row liveness to its primary key liveness.
     *
     * Currently this is only used by views with normal base column as PK column
     * so updates to other columns do not make the row live when the base column
     * is not live. See CASSANDRA-11500.
     *
     * TODO: does not belong here, should be gone
     */
    public boolean enforceStrictLiveness()
    {
        return isView() && Keyspace.open(keyspace).viewManager.getByName(name).enforceStrictLiveness();
    }

    /**
     * Returns the names of all the user types referenced by this table.
     *
     * @return the names of all the user types referenced by this table.
     */
    public Set<ByteBuffer> getReferencedUserTypes()
    {
        Set<ByteBuffer> types = new LinkedHashSet<>();
        columns().forEach(c -> addUserTypes(c.type, types));
        return types;
    }

    /**
     * Find all user types used by the specified type and add them to the set.
     *
     * @param type the type to check for user types.
     * @param types the set of UDT names to which to add new user types found in {@code type}. Note that the
     * insertion ordering is important and ensures that if a user type A uses another user type B, then B will appear
     * before A in iteration order.
     */
    private static void addUserTypes(AbstractType<?> type, Set<ByteBuffer> types)
    {
        // Reach into subtypes first, so that if the type is a UDT, it's dependencies are recreated first.
        type.subTypes().forEach(t -> addUserTypes(t, types));

        if (type.isUDT())
            types.add(((UserType)type).name);
    }

    @Override
    public SchemaElementType elementType()
    {
        return SchemaElementType.TABLE;
    }

    @Override
    public String elementKeyspace()
    {
        return keyspace;
    }

    @Override
    public String elementName()
    {
        return name;
    }

    @Override
    public String toCqlString(boolean withInternals, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder(2048);
        appendCqlTo(builder, withInternals, withInternals, ifNotExists);
        return builder.toString();
    }

    public String toCqlString(boolean includeDroppedColumns,
                              boolean withInternals,
                              boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder(2048);
        appendCqlTo(builder, includeDroppedColumns, withInternals, ifNotExists);
        return builder.toString();
    }

    public void appendCqlTo(CqlBuilder builder,
                            boolean includeDroppedColumns,
                            boolean withInternals,
                            boolean ifNotExists)
    {
        assert !isView();

        String createKeyword = "CREATE";
        if (isVirtual())
        {
            builder.append(String.format("/*\n" +
                    "Warning: Table %s is a virtual table and cannot be recreated with CQL.\n" +
                    "Structure, for reference:\n",
                                         toString()));
            createKeyword = "VIRTUAL";
        }

        builder.append(createKeyword)
               .append(" TABLE ");

        if (ifNotExists)
            builder.append("IF NOT EXISTS ");

        builder.append(toString())
               .append(" (")
               .newLine()
               .increaseIndent();

        boolean hasSingleColumnPrimaryKey = partitionKeyColumns.size() == 1 && clusteringColumns.isEmpty();

        appendColumnDefinitions(builder, includeDroppedColumns, hasSingleColumnPrimaryKey);

        if (!hasSingleColumnPrimaryKey)
            appendPrimaryKey(builder);

        builder.decreaseIndent()
               .append(')');

        builder.append(" WITH ")
               .increaseIndent();

        appendTableOptions(builder, withInternals);

        builder.decreaseIndent();

        if (isVirtual())
        {
            builder.newLine()
                   .append("*/");
        }

        if (includeDroppedColumns)
            appendDropColumns(builder);
    }

    private void appendColumnDefinitions(CqlBuilder builder,
                                         boolean includeDroppedColumns,
                                         boolean hasSingleColumnPrimaryKey)
    {
        Iterator<ColumnMetadata> iter = allColumnsInCreateOrder();
        while (iter.hasNext())
        {
            ColumnMetadata column = iter.next();
            // If the column has been re-added after a drop, we don't include it right away. Instead, we'll add the
            // dropped one first below, then we'll issue the DROP and then the actual ADD for this column, thus
            // simulating the proper sequence of events.
            if (includeDroppedColumns && droppedColumns.containsKey(column.name.bytes))
                continue;

            column.appendCqlTo(builder);

            if (hasSingleColumnPrimaryKey && column.isPartitionKey())
                builder.append(" PRIMARY KEY");

            if (!hasSingleColumnPrimaryKey || (includeDroppedColumns && !droppedColumns.isEmpty()) || iter.hasNext())
                builder.append(',');

            builder.newLine();
        }

        if (includeDroppedColumns)
        {
            Iterator<DroppedColumn> iterDropped = droppedColumns.values().iterator();
            while (iterDropped.hasNext())
            {
                DroppedColumn dropped = iterDropped.next();
                dropped.column.appendCqlTo(builder);

                if (!hasSingleColumnPrimaryKey || iter.hasNext())
                    builder.append(',');

                builder.newLine();
            }
        }
    }

    void appendPrimaryKey(CqlBuilder builder)
    {
        List<ColumnMetadata> partitionKeyColumns = partitionKeyColumns();
        List<ColumnMetadata> clusteringColumns = clusteringColumns();

        if (isStaticCompactTable())
            clusteringColumns = Collections.emptyList();

        builder.append("PRIMARY KEY (");
        if (partitionKeyColumns.size() > 1)
        {
            builder.append('(')
                   .appendWithSeparators(partitionKeyColumns, (b, c) -> b.append(c.name), ", ")
                   .append(')');
        }
        else
        {
            builder.append(partitionKeyColumns.get(0).name);
        }

        if (!clusteringColumns.isEmpty())
            builder.append(", ")
                   .appendWithSeparators(clusteringColumns, (b, c) -> b.append(c.name), ", ");

        builder.append(')')
               .newLine();
    }

    void appendTableOptions(CqlBuilder builder, boolean withInternals)
    {
        if (withInternals)
            builder.append("ID = ")
                   .append(id.toString())
                   .newLine()
                   .append("AND ");

        List<ColumnMetadata> clusteringColumns = clusteringColumns();
        if (!clusteringColumns.isEmpty())
        {
            builder.append("CLUSTERING ORDER BY (")
                   .appendWithSeparators(clusteringColumns, (b, c) -> c.appendNameAndOrderTo(b), ", ")
                   .append(')')
                   .newLine()
                   .append("AND ");
        }

        if (isVirtual())
        {
            builder.append("comment = ").appendWithSingleQuotes(params.comment);
        }
        else
        {
            params.appendCqlTo(builder, isView());
        }
        builder.append(";");
    }

    private void appendDropColumns(CqlBuilder builder)
    {
        for (Entry<ByteBuffer, DroppedColumn> entry : droppedColumns.entrySet())
        {
            DroppedColumn dropped = entry.getValue();

            builder.newLine()
                   .append("ALTER TABLE ")
                   .append(toString())
                   .append(" DROP ")
                   .append(dropped.column.name)
                   .append(" USING TIMESTAMP ")
                   .append(dropped.droppedTime)
                   .append(';');

            ColumnMetadata column = getColumn(entry.getKey());
            if (column != null)
            {
                builder.newLine()
                       .append("ALTER TABLE ")
                       .append(toString())
                       .append(" ADD ");

                column.appendCqlTo(builder);

                builder.append(';');
            }
        }
    }

    /**
     * Returns a string representation of a partition in a CQL-friendly format.
     *
     * For non-composite types it returns the result of {@link org.apache.cassandra.cql3.CQL3Type#toCQLLiteral}
     * applied to the partition key.
     * For composite types it applies {@link org.apache.cassandra.cql3.CQL3Type#toCQLLiteral} to each subkey and
     * combines the results into a tuple.
     *
     * @param partitionKey a partition key
     * @return CQL-like string representation of a partition key
     */
    public String partitionKeyAsCQLLiteral(ByteBuffer partitionKey)
    {
        return primaryKeyAsCQLLiteral(partitionKey, Clustering.EMPTY);
    }

    /**
     * Returns a string representation of a primary key in a CQL-friendly format.
     *
     * @param partitionKey the partition key part of the primary key
     * @param clustering the clustering key part of the primary key
     * @return a CQL-like string representation of the specified primary key
     */
    public String primaryKeyAsCQLLiteral(ByteBuffer partitionKey, Clustering<?> clustering)
    {
        int clusteringSize = clustering.size();

        String[] literals;
        int i = 0;

        if (partitionKeyType instanceof CompositeType)
        {
            List<AbstractType<?>> components = partitionKeyType.getComponents();
            int size = components.size();
            literals = new String[size + clusteringSize];
            ByteBuffer[] values = ((CompositeType) partitionKeyType).split(partitionKey);
            for (i = 0; i < size; i++)
            {
                literals[i] = asCQLLiteral(components.get(i), values[i]);
            }
        }
        else
        {
            literals = new String[1 + clusteringSize];
            literals[i++] = asCQLLiteral(partitionKeyType, partitionKey);
        }

        for (int j = 0; j < clusteringSize; j++)
        {
            literals[i++] = asCQLLiteral(clusteringColumns().get(j).type, clustering.bufferAt(j));
        }

        return i == 1 ? literals[0] : "(" + String.join(", ", literals) + ")";
    }

    private static String asCQLLiteral(AbstractType<?> type, ByteBuffer value)
    {
        return type.asCQL3Type().toCQLLiteral(value);
    }

    public static class CompactTableMetadata extends TableMetadata
    {

        /*
         * For dense tables, this alias the single non-PK column the table contains (since it can only have one). We keep
         * that as convenience to access that column more easily (but we could replace calls by regularAndStaticColumns().iterator().next()
         * for those tables in practice).
         */
        public final ColumnMetadata compactValueColumn;

        private final Set<ColumnMetadata> hiddenColumns;
        protected CompactTableMetadata(Builder builder)
        {
            super(builder);

            compactValueColumn = getCompactValueColumn(regularAndStaticColumns);

            if (isCompactTable() && Flag.isDense(this.flags) && hasEmptyCompactValue())
            {
                hiddenColumns = Collections.singleton(compactValueColumn);
            }
            else if (isCompactTable() && !Flag.isDense(this.flags))
            {
                hiddenColumns = Sets.newHashSetWithExpectedSize(clusteringColumns.size() + 1);
                hiddenColumns.add(compactValueColumn);
                hiddenColumns.addAll(clusteringColumns);

            }
            else
            {
                hiddenColumns = Collections.emptySet();
            }
        }

        @Override
        public boolean isCompactTable()
        {
            return true;
        }

        public ColumnMetadata getExistingColumn(ColumnIdentifier name)
        {
            ColumnMetadata def = getColumn(name);
            if (def == null || isHiddenColumn(def))
                throw new InvalidRequestException(format("Undefined column name %s in table %s", name.toCQLString(), this));
            return def;
        }

        public boolean isHiddenColumn(ColumnMetadata def)
        {
            return hiddenColumns.contains(def);
        }

        @Override
        public Iterator<ColumnMetadata> allColumnsInSelectOrder()
        {
            boolean isStaticCompactTable = isStaticCompactTable();
            boolean noNonPkColumns = hasEmptyCompactValue();

            Iterator<ColumnMetadata> partitionKeyIter = partitionKeyColumns.iterator();
            Iterator<ColumnMetadata> clusteringIter =
            isStaticCompactTable ? Collections.emptyIterator() : clusteringColumns.iterator();
            Iterator<ColumnMetadata> otherColumns = noNonPkColumns ? Collections.emptyIterator()
                                                                   : (isStaticCompactTable ? staticColumns().selectOrderIterator()
                                                                                           : regularAndStaticColumns.selectOrderIterator());

            return columnsIterator(partitionKeyIter, clusteringIter, otherColumns);
        }

        public ImmutableList<ColumnMetadata> createStatementClusteringColumns()
        {
            return isStaticCompactTable() ? ImmutableList.of() : clusteringColumns;
        }

        public Iterator<ColumnMetadata> allColumnsInCreateOrder()
        {
            boolean isStaticCompactTable = isStaticCompactTable();
            boolean noNonPkColumns = !Flag.isCQLTable(flags) && hasEmptyCompactValue();

            Iterator<ColumnMetadata> partitionKeyIter = partitionKeyColumns.iterator();
            Iterator<ColumnMetadata> clusteringIter;

            if (isStaticCompactTable())
                clusteringIter = Collections.EMPTY_LIST.iterator();
            else
                clusteringIter = createStatementClusteringColumns().iterator();

            Iterator<ColumnMetadata> otherColumns;

            if (noNonPkColumns)
            {
                otherColumns = Collections.emptyIterator();
            }
            else if (isStaticCompactTable)
            {
                List<ColumnMetadata> columns = new ArrayList<>();
                for (ColumnMetadata c : regularAndStaticColumns)
                {
                    if (c.isStatic())
                        columns.add(new ColumnMetadata(c.ksName, c.cfName, c.name, c.type, -1, ColumnMetadata.Kind.REGULAR, c.getMask()));
                }
                otherColumns = columns.iterator();
            }
            else
            {
                otherColumns = regularAndStaticColumns.iterator();
            }

            return columnsIterator(partitionKeyIter, clusteringIter, otherColumns);
        }

        public boolean hasEmptyCompactValue()
        {
            return compactValueColumn.type instanceof EmptyType;
        }

        public void validate()
        {
            super.validate();

            // A compact table should always have a clustering
            if (!Flag.isCQLTable(flags) && clusteringColumns.isEmpty())
                except("For table %s, isDense=%b, isCompound=%b, clustering=%s", toString(),
                       Flag.isDense(flags), Flag.isCompound(flags), clusteringColumns);
        }

        AbstractType<?> staticCompactOrSuperTableColumnNameType()
        {
            assert isStaticCompactTable();
            return clusteringColumns.get(0).type;
        }

        public AbstractType<?> columnDefinitionNameComparator(ColumnMetadata.Kind kind)
        {
            return (Flag.isSuper(this.flags) && kind == ColumnMetadata.Kind.REGULAR) ||
                   (isStaticCompactTable() && kind == ColumnMetadata.Kind.STATIC)
                   ? staticCompactOrSuperTableColumnNameType()
                   : UTF8Type.instance;
        }

        @Override
        public boolean isStaticCompactTable()
        {
            return !Flag.isSuper(flags) && !Flag.isDense(flags) && !Flag.isCompound(flags);
        }

        public void appendCqlTo(CqlBuilder builder,
                                boolean includeDroppedColumns,
                                boolean internals,
                                boolean ifNotExists)
        {
            builder.append("/*")
                   .newLine()
                   .append("Warning: Table ")
                   .append(toString())
                   .append(" omitted because it has constructs not compatible with CQL (was created via legacy API).")
                   .newLine()
                   .append("Approximate structure, for reference:")
                   .newLine()
                   .append("(this should not be used to reproduce this schema)")
                   .newLine()
                   .newLine();

            super.appendCqlTo(builder, includeDroppedColumns, internals, ifNotExists);

            builder.newLine()
                   .append("*/");
        }

        void appendTableOptions(CqlBuilder builder, boolean internals)
        {
            builder.append("COMPACT STORAGE")
                   .newLine()
                   .append("AND ");

            super.appendTableOptions(builder, internals);
        }

        public static ColumnMetadata getCompactValueColumn(RegularAndStaticColumns columns)
        {
            assert columns.regulars.simpleColumnCount() == 1 && columns.regulars.complexColumnCount() == 0;
            return columns.regulars.getSimple(0);
        }

    }

}