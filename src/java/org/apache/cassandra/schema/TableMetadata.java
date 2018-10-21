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
import java.util.*;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.utils.AbstractIterator;
import org.github.jamm.Unmetered;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;
import static org.apache.cassandra.schema.IndexMetadata.isNameValid;

@Unmetered
public final class TableMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);
    private static final ImmutableSet<Flag> DEFAULT_CQL_FLAGS = ImmutableSet.of(Flag.COMPOUND);
    private static final ImmutableSet<Flag> DEPRECATED_CS_FLAGS = ImmutableSet.of(Flag.DENSE, Flag.SUPER);

    public static final String COMPACT_STORAGE_HALT_MESSAGE =
             "Compact Tables are not allowed in Cassandra starting with 4.0 version. " +
             "Use `ALTER ... DROP COMPACT STORAGE` command supplied in 3.x/3.11 Cassandra " +
             "in order to migrate off Compact Storage.";

    private static final String COMPACT_STORAGE_DEPRECATION_MESSAGE =
             "Incorrect set of flags is was detected in table {}.{}: '{}'. \n" +
             "Starting with version 4.0, '{}' flags are deprecated and every table has to have COMPOUND flag. \n" +
             "Forcing the following set of flags: '{}'";

    public enum Flag
    {
        SUPER, COUNTER, DENSE, COMPOUND;

        public static boolean isCQLCompatible(Set<Flag> flags)
        {
            return !flags.contains(Flag.DENSE) && !flags.contains(Flag.SUPER) && flags.contains(Flag.COMPOUND);
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

    private final ImmutableList<ColumnMetadata> partitionKeyColumns;
    private final ImmutableList<ColumnMetadata> clusteringColumns;
    private final RegularAndStaticColumns regularAndStaticColumns;

    public final Indexes indexes;
    public final Triggers triggers;

    // derived automatically from flags and columns
    public final AbstractType<?> partitionKeyType;
    public final ClusteringComparator comparator;

    /*
     * For dense tables, this alias the single non-PK column the table contains (since it can only have one). We keep
     * that as convenience to access that column more easily (but we could replace calls by regularAndStaticColumns().iterator().next()
     * for those tables in practice).
     */
    public final ColumnMetadata compactValueColumn;

    // performance hacks; TODO see if all are really necessary
    public final DataResource resource;

    private TableMetadata(Builder builder)
    {
        if (!Flag.isCQLCompatible(builder.flags))
        {
            flags = ImmutableSet.copyOf(Sets.union(Sets.difference(builder.flags, DEPRECATED_CS_FLAGS), DEFAULT_CQL_FLAGS));
            logger.warn(COMPACT_STORAGE_DEPRECATION_MESSAGE, builder.keyspace, builder.name,  builder.flags, DEPRECATED_CS_FLAGS, flags);
        }
        else
        {
            flags = Sets.immutableEnumSet(builder.flags);
        }
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

        compactValueColumn = isCompactTable()
                           ? CompactTables.getCompactValueColumn(regularAndStaticColumns, isSuper())
                           : null;

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

    /*
     *  We call dense a CF for which each component of the comparator is a clustering column, i.e. no
     * component is used to store a regular column names. In other words, non-composite static "thrift"
     * and CQL3 CF are *not* dense.
     */
    public boolean isDense()
    {
        return flags.contains(Flag.DENSE);
    }

    public boolean isCompound()
    {
        return flags.contains(Flag.COMPOUND);
    }

    public boolean isSuper()
    {
        return flags.contains(Flag.SUPER);
    }

    public boolean isCounter()
    {
        return flags.contains(Flag.COUNTER);
    }

    public boolean isCQLTable()
    {
        return !isSuper() && !isDense() && isCompound();
    }

    public boolean isCompactTable()
    {
        return !isCQLTable();
    }

    public boolean isStaticCompactTable()
    {
        return !isSuper() && !isDense() && !isCompound();
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
     * This also "hide" the clustering/regular columns for a non-CQL3 non-dense table for backward compatibility
     * sake.
     */
    public Iterator<ColumnMetadata> allColumnsInSelectOrder()
    {
        final boolean isStaticCompactTable = isStaticCompactTable();
        final boolean noNonPkColumns = isCompactTable() && CompactTables.hasEmptyCompactValue(this);

        return new AbstractIterator<ColumnMetadata>()
        {
            private final Iterator<ColumnMetadata> partitionKeyIter = partitionKeyColumns.iterator();
            private final Iterator<ColumnMetadata> clusteringIter =
                isStaticCompactTable ? Collections.emptyIterator() : clusteringColumns.iterator();
            private final Iterator<ColumnMetadata> otherColumns =
                noNonPkColumns
              ? Collections.emptyIterator()
              : (isStaticCompactTable ? staticColumns().selectOrderIterator()
                                      : regularAndStaticColumns.selectOrderIterator());

            protected ColumnMetadata computeNext()
            {
                if (partitionKeyIter.hasNext())
                    return partitionKeyIter.next();

                if (clusteringIter.hasNext())
                    return clusteringIter.next();

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
                if (!(column.type.isCounter()) && !CompactTables.isSuperColumnMapColumn(column))
                    except("Cannot have a non counter column (\"%s\") in a counter table", column.name);
        }
        else
        {
            for (ColumnMetadata column : regularAndStaticColumns)
                if (column.type.isCounter())
                    except("Cannot have a counter column (\"%s\") in a non counter column table", column.name);
        }

        // All tables should have a partition key
        if (partitionKeyColumns.isEmpty())
            except("Missing partition keys for table %s", toString());

        // A compact table should always have a clustering
        if (isCompactTable() && clusteringColumns.isEmpty())
            except("For table %s, isDense=%b, isCompound=%b, clustering=%s", toString(), isDense(), isCompound(), clusteringColumns);

        if (!indexes.isEmpty() && isSuper())
            except("Secondary indexes are not supported on super column families");

        indexes.validate(this);
    }

    void validateCompatibility(TableMetadata previous)
    {
        if (isIndex())
            return;

        if (!previous.keyspace.equals(keyspace))
            except("Keyspace mismatch (found %s; expected %s)", keyspace, previous.keyspace);

        if (!previous.name.equals(name))
            except("Table mismatch (found %s; expected %s)", name, previous.name);

        if (!previous.id.equals(id))
            except("Table ID mismatch (found %s; expected %s)", id, previous.id);

        if (!previous.flags.equals(flags))
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
     * The type to use to compare column names in "static compact"
     * tables or superColum ones.
     * <p>
     * This exists because for historical reasons, "static compact" tables as
     * well as super column ones can have non-UTF8 column names.
     * <p>
     * This method should only be called for superColumn tables and "static
     * compact" ones. For any other table, all column names are UTF8.
     */
    AbstractType<?> staticCompactOrSuperTableColumnNameType()
    {
        if (isSuper())
        {
            assert compactValueColumn != null && compactValueColumn.type instanceof MapType;
            return ((MapType) compactValueColumn.type).nameComparator();
        }

        assert isStaticCompactTable();
        return clusteringColumns.get(0).type;
    }

    public AbstractType<?> columnDefinitionNameComparator(ColumnMetadata.Kind kind)
    {
        return (isSuper() && kind == ColumnMetadata.Kind.REGULAR) || (isStaticCompactTable() && kind == ColumnMetadata.Kind.STATIC)
             ? staticCompactOrSuperTableColumnNameType()
             : UTF8Type.instance;
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
            || params.gcGraceSeconds != updated.params.gcGraceSeconds;
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

    private void except(String format, Object... args)
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
        return String.format("%s.%s", ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(name));
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

        // Setting compound as default as "normal" CQL tables are compound and that's what we want by default
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
                id = TableId.generate();

            return new TableMetadata(this);
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

        public Builder isSuper(boolean val)
        {
            return flag(Flag.SUPER, val);
        }

        public Builder isCounter(boolean val)
        {
            return flag(Flag.COUNTER, val);
        }

        public Builder isDense(boolean val)
        {
            return flag(Flag.DENSE, val);
        }

        public Builder isCompound(boolean val)
        {
            return flag(Flag.COMPOUND, val);
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

        public Builder addPartitionKeyColumn(String name, AbstractType type)
        {
            return addPartitionKeyColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addPartitionKeyColumn(ColumnIdentifier name, AbstractType type)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, partitionKeyColumns.size(), ColumnMetadata.Kind.PARTITION_KEY));
        }

        public Builder addClusteringColumn(String name, AbstractType type)
        {
            return addClusteringColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addClusteringColumn(ColumnIdentifier name, AbstractType type)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, clusteringColumns.size(), ColumnMetadata.Kind.CLUSTERING));
        }

        public Builder addRegularColumn(String name, AbstractType type)
        {
            return addRegularColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addRegularColumn(ColumnIdentifier name, AbstractType type)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.REGULAR));
        }

        public Builder addStaticColumn(String name, AbstractType type)
        {
            return addStaticColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addStaticColumn(ColumnIdentifier name, AbstractType type)
        {
            return addColumn(new ColumnMetadata(keyspace, this.name, name, type, ColumnMetadata.NO_POSITION, ColumnMetadata.Kind.STATIC));
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

        Builder addColumns(Iterable<ColumnMetadata> columns)
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

        Builder alterColumnType(ColumnIdentifier name, AbstractType<?> type)
        {
            ColumnMetadata column = columns.get(name.bytes);
            if (column == null)
                throw new IllegalArgumentException();

            ColumnMetadata newColumn = column.withNewType(type);

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

            return this;
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
}
