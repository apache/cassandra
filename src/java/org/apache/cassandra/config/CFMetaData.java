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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.SuperColumnCompatibility;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.*;
import org.github.jamm.Unmetered;

/**
 * This class can be tricky to modify. Please read http://wiki.apache.org/cassandra/ConfigurationNotes for how to do so safely.
 */
@Unmetered
public final class CFMetaData
{
    public enum Flag
    {
        SUPER, COUNTER, DENSE, COMPOUND
    }

    private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");

    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public static final Serializer serializer = new Serializer();

    //REQUIRED
    public final UUID cfId;                           // internal id, never exposed to user
    public final String ksName;                       // name of keyspace
    public final String cfName;                       // name of this column family
    public final Pair<String, String> ksAndCFName;
    public final byte[] ksAndCFBytes;

    private final boolean isCounter;
    private final boolean isView;
    private final boolean isIndex;

    public volatile ClusteringComparator comparator;  // bytes, long, timeuuid, utf8, etc. This is built directly from clusteringColumns
    public final IPartitioner partitioner;            // partitioner the table uses
    private volatile AbstractType<?> keyValidator;

    private final Serializers serializers;

    // non-final, for now
    private volatile ImmutableSet<Flag> flags;
    private volatile boolean isDense;
    private volatile boolean isCompound;
    private volatile boolean isSuper;

    public volatile TableParams params = TableParams.DEFAULT;

    private volatile Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
    private volatile Triggers triggers = Triggers.none();
    private volatile Indexes indexes = Indexes.none();

    /*
     * All CQL3 columns definition are stored in the columnMetadata map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and 2) for the partition key and
     * clustering key ones, those list are ordered by the "component index" of the
     * elements.
     */
    private volatile Map<ByteBuffer, ColumnDefinition> columnMetadata = new HashMap<>();
    private volatile List<ColumnDefinition> partitionKeyColumns;  // Always of size keyValidator.componentsCount, null padded if necessary
    private volatile List<ColumnDefinition> clusteringColumns;    // Of size comparator.componentsCount or comparator.componentsCount -1, null padded if necessary
    private volatile PartitionColumns partitionColumns;           // Always non-PK, non-clustering columns

    // For dense tables, this alias the single non-PK column the table contains (since it can only have one). We keep
    // that as convenience to access that column more easily (but we could replace calls by partitionColumns().iterator().next()
    // for those tables in practice).
    private volatile ColumnDefinition compactValueColumn;

    public final DataResource resource;

    //For hot path serialization it's often easier to store this info here
    private volatile ColumnFilter allColumnFilter;

    /**
     * These two columns are "virtual" (e.g. not persisted together with schema).
     *
     * They are stored here to avoid re-creating during SELECT and UPDATE queries, where
     * they are used to allow presenting supercolumn families in the CQL-compatible
     * format. See {@link SuperColumnCompatibility} for more details.
     **/
    private volatile ColumnDefinition superCfKeyColumn;
    private volatile ColumnDefinition superCfValueColumn;

    /** Caches a non-compact version of the metadata for compact tables to be used with the NO_COMPACT protocol option. */
    private volatile CFMetaData nonCompactCopy = null;

    public boolean isSuperColumnKeyColumn(ColumnDefinition cd)
    {
        return cd.name.equals(superCfKeyColumn.name);
    }

    public boolean isSuperColumnValueColumn(ColumnDefinition cd)
    {
        return cd.name.equals(superCfValueColumn.name);
    }

    public ColumnDefinition superColumnValueColumn()
    {
        return superCfValueColumn;
    }

    public ColumnDefinition superColumnKeyColumn() { return superCfKeyColumn; }

    /*
     * All of these methods will go away once CFMetaData becomes completely immutable.
     */
    public CFMetaData params(TableParams params)
    {
        this.params = params;
        return this;
    }

    public CFMetaData bloomFilterFpChance(double prop)
    {
        params = TableParams.builder(params).bloomFilterFpChance(prop).build();
        return this;
    }

    public CFMetaData caching(CachingParams prop)
    {
        params = TableParams.builder(params).caching(prop).build();
        return this;
    }

    public CFMetaData comment(String prop)
    {
        params = TableParams.builder(params).comment(prop).build();
        return this;
    }

    public CFMetaData compaction(CompactionParams prop)
    {
        params = TableParams.builder(params).compaction(prop).build();
        return this;
    }

    public CFMetaData compression(CompressionParams prop)
    {
        params = TableParams.builder(params).compression(prop).build();
        return this;
    }

    public CFMetaData dcLocalReadRepairChance(double prop)
    {
        params = TableParams.builder(params).dcLocalReadRepairChance(prop).build();
        return this;
    }

    public CFMetaData defaultTimeToLive(int prop)
    {
        params = TableParams.builder(params).defaultTimeToLive(prop).build();
        return this;
    }

    public CFMetaData gcGraceSeconds(int prop)
    {
        params = TableParams.builder(params).gcGraceSeconds(prop).build();
        return this;
    }

    public CFMetaData maxIndexInterval(int prop)
    {
        params = TableParams.builder(params).maxIndexInterval(prop).build();
        return this;
    }

    public CFMetaData memtableFlushPeriod(int prop)
    {
        params = TableParams.builder(params).memtableFlushPeriodInMs(prop).build();
        return this;
    }

    public CFMetaData minIndexInterval(int prop)
    {
        params = TableParams.builder(params).minIndexInterval(prop).build();
        return this;
    }

    public CFMetaData readRepairChance(double prop)
    {
        params = TableParams.builder(params).readRepairChance(prop).build();
        return this;
    }

    public CFMetaData crcCheckChance(double prop)
    {
        params = TableParams.builder(params).crcCheckChance(prop).build();
        return this;
    }

    public CFMetaData speculativeRetry(SpeculativeRetryParam prop)
    {
        params = TableParams.builder(params).speculativeRetry(prop).build();
        return this;
    }

    public CFMetaData extensions(Map<String, ByteBuffer> extensions)
    {
        params = TableParams.builder(params).extensions(extensions).build();
        return this;
    }

    public CFMetaData droppedColumns(Map<ByteBuffer, DroppedColumn> cols)
    {
        droppedColumns = cols;
        return this;
    }

    public CFMetaData triggers(Triggers prop)
    {
        triggers = prop;
        return this;
    }

    public CFMetaData indexes(Indexes indexes)
    {
        this.indexes = indexes;
        return this;
    }

    private CFMetaData(String keyspace,
                       String name,
                       UUID cfId,
                       boolean isSuper,
                       boolean isCounter,
                       boolean isDense,
                       boolean isCompound,
                       boolean isView,
                       List<ColumnDefinition> partitionKeyColumns,
                       List<ColumnDefinition> clusteringColumns,
                       PartitionColumns partitionColumns,
                       IPartitioner partitioner,
                       ColumnDefinition superCfKeyColumn,
                       ColumnDefinition superCfValueColumn)
    {
        this.cfId = cfId;
        this.ksName = keyspace;
        this.cfName = name;
        ksAndCFName = Pair.create(keyspace, name);
        byte[] ksBytes = FBUtilities.toWriteUTFBytes(ksName);
        byte[] cfBytes = FBUtilities.toWriteUTFBytes(cfName);
        ksAndCFBytes = Arrays.copyOf(ksBytes, ksBytes.length + cfBytes.length);
        System.arraycopy(cfBytes, 0, ksAndCFBytes, ksBytes.length, cfBytes.length);

        this.isDense = isSuper ? (isDense || SuperColumnCompatibility.recalculateIsDense(partitionColumns.regulars)) : isDense;

        this.isCompound = isCompound;
        this.isSuper = isSuper;
        this.isCounter = isCounter;
        this.isView = isView;

        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        if (isSuper)
            flags.add(Flag.SUPER);
        if (isCounter)
            flags.add(Flag.COUNTER);
        if (isDense)
            flags.add(Flag.DENSE);
        if (isCompound)
            flags.add(Flag.COMPOUND);
        this.flags = Sets.immutableEnumSet(flags);

        isIndex = cfName.contains(".");

        assert partitioner != null : "This assertion failure is probably due to accessing Schema.instance " +
                                     "from client-mode tools - See CASSANDRA-8143.";
        this.partitioner = partitioner;

        // A compact table should always have a clustering
        assert isCQLTable() || !clusteringColumns.isEmpty() : String.format("For table %s.%s, isDense=%b, isCompound=%b, clustering=%s", ksName, cfName, isDense, isCompound, clusteringColumns);

        // All tables should have a partition key
        assert !partitionKeyColumns.isEmpty() : String.format("Have no partition keys for table %s.%s", ksName, cfName);

        this.partitionKeyColumns = partitionKeyColumns;
        this.clusteringColumns = clusteringColumns;
        this.partitionColumns = partitionColumns;

        this.superCfKeyColumn = superCfKeyColumn;
        this.superCfValueColumn = superCfValueColumn;

        //This needs to happen before serializers are set
        //because they use comparator.subtypes()
        rebuild();

        this.serializers = new Serializers(this);
        this.resource = DataResource.table(ksName, cfName);
    }

    // This rebuild informations that are intrinsically duplicate of the table definition but
    // are kept because they are often useful in a different format.
    private void rebuild()
    {
        // A non-compact copy will be created lazily
        this.nonCompactCopy = null;

        if (isCompactTable())
        {
            this.compactValueColumn = isSuper() ?
                                      SuperColumnCompatibility.getCompactValueColumn(partitionColumns) :
                                      CompactTables.getCompactValueColumn(partitionColumns);
        }

        Map<ByteBuffer, ColumnDefinition> newColumnMetadata = Maps.newHashMapWithExpectedSize(partitionKeyColumns.size() + clusteringColumns.size() + partitionColumns.size());

        if (isSuper() && isDense())
        {
            CompactTables.DefaultNames defaultNames = SuperColumnCompatibility.columnNameGenerator(partitionKeyColumns, clusteringColumns, partitionColumns);
            if (superCfKeyColumn == null)
                superCfKeyColumn = SuperColumnCompatibility.getSuperCfKeyColumn(this, clusteringColumns, defaultNames);
            if (superCfValueColumn == null)
                superCfValueColumn = SuperColumnCompatibility.getSuperCfValueColumn(this, partitionColumns, superCfKeyColumn, defaultNames);

            for (ColumnDefinition def : partitionKeyColumns)
                newColumnMetadata.put(def.name.bytes, def);
            newColumnMetadata.put(clusteringColumns.get(0).name.bytes, clusteringColumns.get(0));
            newColumnMetadata.put(superCfKeyColumn.name.bytes, SuperColumnCompatibility.getSuperCfSschemaRepresentation(superCfKeyColumn));
            newColumnMetadata.put(superCfValueColumn.name.bytes, superCfValueColumn);
            newColumnMetadata.put(compactValueColumn.name.bytes, compactValueColumn);
            clusteringColumns = Arrays.asList(clusteringColumns().get(0));
            partitionColumns = PartitionColumns.of(compactValueColumn);
        }
        else
        {
            for (ColumnDefinition def : partitionKeyColumns)
                newColumnMetadata.put(def.name.bytes, def);
            for (ColumnDefinition def : clusteringColumns)
                newColumnMetadata.put(def.name.bytes, def);
            for (ColumnDefinition def : partitionColumns)
                newColumnMetadata.put(def.name.bytes, def);
        }
        this.columnMetadata = newColumnMetadata;

        List<AbstractType<?>> keyTypes = extractTypes(partitionKeyColumns);
        this.keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);

        if (isSuper())
            this.comparator = new ClusteringComparator(clusteringColumns.get(0).type);
        else
            this.comparator = new ClusteringComparator(extractTypes(clusteringColumns));

        this.allColumnFilter = ColumnFilter.all(this);
    }

    public Indexes getIndexes()
    {
        return indexes;
    }

    public ColumnFilter getAllColumnFilter()
    {
        return allColumnFilter;
    }

    public static CFMetaData create(String ksName,
                                    String name,
                                    UUID cfId,
                                    boolean isDense,
                                    boolean isCompound,
                                    boolean isSuper,
                                    boolean isCounter,
                                    boolean isView,
                                    List<ColumnDefinition> columns,
                                    IPartitioner partitioner)
    {
        List<ColumnDefinition> partitions = new ArrayList<>();
        List<ColumnDefinition> clusterings = new ArrayList<>();
        PartitionColumns.Builder builder = PartitionColumns.builder();

        for (ColumnDefinition column : columns)
        {
            switch (column.kind)
            {
                case PARTITION_KEY:
                    partitions.add(column);
                    break;
                case CLUSTERING:
                    clusterings.add(column);
                    break;
                default:
                    builder.add(column);
                    break;
            }
        }

        Collections.sort(partitions);
        Collections.sort(clusterings);

        return new CFMetaData(ksName,
                              name,
                              cfId,
                              isSuper,
                              isCounter,
                              isDense,
                              isCompound,
                              isView,
                              partitions,
                              clusterings,
                              builder.build(),
                              partitioner,
                              null,
                              null);
    }

    public static List<AbstractType<?>> extractTypes(Iterable<ColumnDefinition> clusteringColumns)
    {
        List<AbstractType<?>> types = new ArrayList<>();
        for (ColumnDefinition def : clusteringColumns)
            types.add(def.type);
        return types;
    }

    public Set<Flag> flags()
    {
        return flags;
    }

    /**
     * There is a couple of places in the code where we need a CFMetaData object and don't have one readily available
     * and know that only the keyspace and name matter. This creates such "fake" metadata. Use only if you know what
     * you're doing.
     */
    public static CFMetaData createFake(String keyspace, String name)
    {
        return CFMetaData.Builder.create(keyspace, name).addPartitionKey("key", BytesType.instance).build();
    }

    public Triggers getTriggers()
    {
        return triggers;
    }

    // Compiles a system metadata
    public static CFMetaData compile(String cql, String keyspace)
    {
        CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
        parsed.prepareKeyspace(keyspace);
        CreateTableStatement statement = (CreateTableStatement) ((CreateTableStatement.RawStatement) parsed).prepare(Types.none()).statement;

        return statement.metadataBuilder()
                        .withId(generateLegacyCfId(keyspace, statement.columnFamily()))
                        .build()
                        .params(statement.params())
                        .readRepairChance(0.0)
                        .dcLocalReadRepairChance(0.0)
                        .gcGraceSeconds(0)
                        .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1));
    }

    /**
     * Generates deterministic UUID from keyspace/columnfamily name pair.
     * This is used to generate the same UUID for {@code C* version < 2.1}
     *
     * Since 2.1, this is only used for system columnfamilies and tests.
     */
    public static UUID generateLegacyCfId(String ksName, String cfName)
    {
        return UUID.nameUUIDFromBytes(ArrayUtils.addAll(ksName.getBytes(), cfName.getBytes()));
    }

    public CFMetaData reloadIndexMetadataProperties(CFMetaData parent)
    {
        TableParams.Builder indexParams = TableParams.builder(parent.params);

        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        if (parent.params.caching.cacheKeys())
            indexParams.caching(CachingParams.CACHE_KEYS);
        else
            indexParams.caching(CachingParams.CACHE_NOTHING);

        indexParams.readRepairChance(0.0)
                   .dcLocalReadRepairChance(0.0)
                   .gcGraceSeconds(0);

        return params(indexParams.build());
    }

    /**
     * Returns a cached non-compact version of this table. Cached version has to be invalidated
     * every time the table is rebuilt.
     */
    public CFMetaData asNonCompact()
    {
        assert isCompactTable() : "Can't get non-compact version of a CQL table";

        // Note that this is racy, but re-computing the non-compact copy a few times on first uses isn't a big deal so
        // we don't bother.
        if (nonCompactCopy == null)
        {
            nonCompactCopy = copyOpts(new CFMetaData(ksName,
                                                     cfName,
                                                     cfId,
                                                     false,
                                                     isCounter,
                                                     false,
                                                     true,
                                                     isView,
                                                     copy(partitionKeyColumns),
                                                     copy(clusteringColumns),
                                                     copy(partitionColumns),
                                                     partitioner,
                                                     superCfKeyColumn,
                                                     superCfValueColumn),
                                      this);
        }

        return nonCompactCopy;
    }

    public CFMetaData copy()
    {
        return copy(cfId);
    }

    /**
     * Clones the CFMetaData, but sets a different cfId
     *
     * @param newCfId the cfId for the cloned CFMetaData
     * @return the cloned CFMetaData instance with the new cfId
     */
    public CFMetaData copy(UUID newCfId)
    {
        return copyOpts(new CFMetaData(ksName,
                                       cfName,
                                       newCfId,
                                       isSuper(),
                                       isCounter(),
                                       isDense(),
                                       isCompound(),
                                       isView(),
                                       copy(partitionKeyColumns),
                                       copy(clusteringColumns),
                                       copy(partitionColumns),
                                       partitioner,
                                       superCfKeyColumn,
                                       superCfValueColumn),
                        this);
    }

    public CFMetaData copy(IPartitioner partitioner)
    {
        return copyOpts(new CFMetaData(ksName,
                                       cfName,
                                       cfId,
                                       isSuper,
                                       isCounter,
                                       isDense,
                                       isCompound,
                                       isView,
                                       copy(partitionKeyColumns),
                                       copy(clusteringColumns),
                                       copy(partitionColumns),
                                       partitioner,
                                       superCfKeyColumn,
                                       superCfValueColumn),
                        this);
    }

    private static List<ColumnDefinition> copy(List<ColumnDefinition> l)
    {
        List<ColumnDefinition> copied = new ArrayList<>(l.size());
        for (ColumnDefinition cd : l)
            copied.add(cd.copy());
        return copied;
    }

    private static PartitionColumns copy(PartitionColumns columns)
    {
        PartitionColumns.Builder newColumns = PartitionColumns.builder();
        for (ColumnDefinition cd : columns)
            newColumns.add(cd.copy());
        return newColumns.build();
    }

    @VisibleForTesting
    public static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        return newCFMD.params(oldCFMD.params)
                      .droppedColumns(new HashMap<>(oldCFMD.droppedColumns))
                      .triggers(oldCFMD.triggers)
                      .indexes(oldCFMD.indexes);
    }

    /**
     * generate a column family name for an index corresponding to the given column.
     * This is NOT the same as the index's name! This is only used in sstable filenames and is not exposed to users.
     *
     * @param info A definition of the column with index
     *
     * @return name of the index ColumnFamily
     */
    public String indexColumnFamilyName(IndexMetadata info)
    {
        // TODO simplify this when info.index_name is guaranteed to be set
        return cfName + Directories.SECONDARY_INDEX_NAME_SEPARATOR + info.name;
    }

    /**
     * true if this CFS contains secondary index data.
     */
    public boolean isIndex()
    {
        return isIndex;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return partitioner.decorateKey(key);
    }

    public Map<ByteBuffer, ColumnDefinition> getColumnMetadata()
    {
        return columnMetadata;
    }

    /**
     *
     * @return The name of the parent cf if this is a seconday index
     */
    public String getParentColumnFamilyName()
    {
        return isIndex ? cfName.substring(0, cfName.indexOf('.')) : null;
    }

    public ReadRepairDecision newReadRepairDecision()
    {
        double chance = ThreadLocalRandom.current().nextDouble();
        if (params.readRepairChance > chance)
            return ReadRepairDecision.GLOBAL;

        if (params.dcLocalReadRepairChance > chance)
            return ReadRepairDecision.DC_LOCAL;

        return ReadRepairDecision.NONE;
    }

    public AbstractType<?> getColumnDefinitionNameComparator(ColumnDefinition.Kind kind)
    {
        return (isSuper() && kind == ColumnDefinition.Kind.REGULAR) || (isStaticCompactTable() && kind == ColumnDefinition.Kind.STATIC)
             ? thriftColumnNameType()
             : UTF8Type.instance;
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public Collection<ColumnDefinition> allColumns()
    {
        return columnMetadata.values();
    }

    private Iterator<ColumnDefinition> nonPkColumnIterator()
    {
        final boolean noNonPkColumns = isCompactTable() && CompactTables.hasEmptyCompactValue(this) && !isSuper();
        if (noNonPkColumns)
        {
            return Collections.<ColumnDefinition>emptyIterator();
        }
        else if (isStaticCompactTable())
        {
            return partitionColumns.statics.selectOrderIterator();
        }
        else if (isSuper())
        {
            if (isDense)
                return Iterators.forArray(superCfKeyColumn, superCfValueColumn);
            else
                return Iterators.filter(partitionColumns.iterator(), (c) -> !c.type.isCollection());
        }
        else
            return partitionColumns().selectOrderIterator();
    }

    // An iterator over all column definitions but that respect the order of a SELECT *.
    // This also hides the clustering/regular columns for a non-CQL3 non-dense table for backward compatibility
    // sake (those are accessible through thrift but not through CQL currently) and exposes the key and value
    // columns for supercolumn family.
    public Iterator<ColumnDefinition> allColumnsInSelectOrder()
    {
        return new AbstractIterator<ColumnDefinition>()
        {
            private final Iterator<ColumnDefinition> partitionKeyIter = partitionKeyColumns.iterator();
            private final Iterator<ColumnDefinition> clusteringIter = isStaticCompactTable() ? Collections.<ColumnDefinition>emptyIterator() : clusteringColumns.iterator();
            private final Iterator<ColumnDefinition> otherColumns = nonPkColumnIterator();

            protected ColumnDefinition computeNext()
            {
                if (partitionKeyIter.hasNext())
                    return partitionKeyIter.next();

                if (clusteringIter.hasNext())
                    return clusteringIter.next();

                return otherColumns.hasNext() ? otherColumns.next() : endOfData();
            }
        };
    }

    public Iterable<ColumnDefinition> primaryKeyColumns()
    {
        return Iterables.concat(partitionKeyColumns, clusteringColumns);
    }

    public List<ColumnDefinition> partitionKeyColumns()
    {
        return partitionKeyColumns;
    }

    public List<ColumnDefinition> clusteringColumns()
    {
        return clusteringColumns;
    }

    public PartitionColumns partitionColumns()
    {
        return partitionColumns;
    }

    public ColumnDefinition compactValueColumn()
    {
        return compactValueColumn;
    }

    public ClusteringComparator getKeyValidatorAsClusteringComparator()
    {
        boolean isCompound = keyValidator instanceof CompositeType;
        List<AbstractType<?>> types = isCompound
                                    ? ((CompositeType) keyValidator).types
                                    : Collections.<AbstractType<?>>singletonList(keyValidator);
        return new ClusteringComparator(types);
    }

    public static ByteBuffer serializePartitionKey(ClusteringPrefix keyAsClustering)
    {
        // TODO: we should stop using Clustering for partition keys. Maybe we can add
        // a few methods to DecoratedKey so we don't have to (note that while using a Clustering
        // allows to use buildBound(), it's actually used for partition keys only when every restriction
        // is an equal, so we could easily create a specific method for keys for that.
        if (keyAsClustering.size() == 1)
            return keyAsClustering.get(0);

        ByteBuffer[] values = new ByteBuffer[keyAsClustering.size()];
        for (int i = 0; i < keyAsClustering.size(); i++)
            values[i] = keyAsClustering.get(i);
        return CompositeType.build(values);
    }

    public Map<ByteBuffer, DroppedColumn> getDroppedColumns()
    {
        return droppedColumns;
    }

    public ColumnDefinition getDroppedColumnDefinition(ByteBuffer name)
    {
        return getDroppedColumnDefinition(name, false);
    }

    /**
     * Returns a "fake" ColumnDefinition corresponding to the dropped column {@code name}
     * of {@code null} if there is no such dropped column.
     *
     * @param name - the column name
     * @param isStatic - whether the column was a static column, if known
     */
    public ColumnDefinition getDroppedColumnDefinition(ByteBuffer name, boolean isStatic)
    {
        DroppedColumn dropped = droppedColumns.get(name);
        if (dropped == null)
            return null;

        // We need the type for deserialization purpose. If we don't have the type however,
        // it means that it's a dropped column from before 3.0, and in that case using
        // BytesType is fine for what we'll be using it for, even if that's a hack.
        AbstractType<?> type = dropped.type == null ? BytesType.instance : dropped.type;
        return isStatic
               ? ColumnDefinition.staticDef(this, name, type)
               : ColumnDefinition.regularDef(this, name, type);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CFMetaData))
            return false;

        CFMetaData other = (CFMetaData) o;

        return Objects.equal(cfId, other.cfId)
            && Objects.equal(flags, other.flags)
            && Objects.equal(ksName, other.ksName)
            && Objects.equal(cfName, other.cfName)
            && Objects.equal(params, other.params)
            && Objects.equal(comparator, other.comparator)
            && Objects.equal(keyValidator, other.keyValidator)
            && Objects.equal(columnMetadata, other.columnMetadata)
            && Objects.equal(droppedColumns, other.droppedColumns)
            && Objects.equal(triggers, other.triggers)
            && Objects.equal(indexes, other.indexes);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(cfId)
            .append(ksName)
            .append(cfName)
            .append(flags)
            .append(comparator)
            .append(params)
            .append(keyValidator)
            .append(columnMetadata)
            .append(droppedColumns)
            .append(triggers)
            .append(indexes)
            .toHashCode();
    }

    /**
     * Updates CFMetaData in-place to match cfm
     *
     * @return true if any change was made which impacts queries/updates on the table,
     *         e.g. any columns or indexes were added, removed, or altered; otherwise, false is returned.
     *         Used to determine whether prepared statements against this table need to be re-prepared.
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    @VisibleForTesting
    public boolean apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);

        validateCompatibility(cfm);

        partitionKeyColumns = cfm.partitionKeyColumns;
        clusteringColumns = cfm.clusteringColumns;

        boolean changeAffectsStatements = !partitionColumns.equals(cfm.partitionColumns);
        partitionColumns = cfm.partitionColumns;
        superCfKeyColumn = cfm.superCfKeyColumn;
        superCfValueColumn = cfm.superCfValueColumn;

        isDense = cfm.isDense;
        isCompound = cfm.isCompound;
        isSuper = cfm.isSuper;

        flags = cfm.flags;

        rebuild();

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        params = cfm.params;

        if (!cfm.droppedColumns.isEmpty())
            droppedColumns = cfm.droppedColumns;

        triggers = cfm.triggers;

        changeAffectsStatements |= !indexes.equals(cfm.indexes);
        indexes = cfm.indexes;

        logger.debug("application result is {}", this);

        return changeAffectsStatements;
    }

    public void validateCompatibility(CFMetaData cfm) throws ConfigurationException
    {
        // validate
        if (!cfm.ksName.equals(ksName))
            throw new ConfigurationException(String.format("Keyspace mismatch (found %s; expected %s)",
                                                           cfm.ksName, ksName));
        if (!cfm.cfName.equals(cfName))
            throw new ConfigurationException(String.format("Column family mismatch (found %s; expected %s)",
                                                           cfm.cfName, cfName));
        if (!cfm.cfId.equals(cfId))
            throw new ConfigurationException(String.format("Column family ID mismatch (found %s; expected %s)",
                                                           cfm.cfId, cfId));
    }


    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
        if (!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass))
            throw new ConfigurationException(String.format("Specified compaction strategy class (%s) is not derived from AbstractCompactionStrategy", className));

        return strategyClass;
    }

    public static AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs,
                                                                              CompactionParams compactionParams)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor =
                compactionParams.klass().getConstructor(ColumnFamilyStore.class, Map.class);
            return constructor.newInstance(cfs, compactionParams.options());
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the ColumnDefinition for {@code name}.
     */
    public ColumnDefinition getColumnDefinition(ColumnIdentifier name)
    {
       return getColumnDefinition(name.bytes);
    }

    // In general it is preferable to work with ColumnIdentifier to make it
    // clear that we are talking about a CQL column, not a cell name, but there
    // is a few cases where all we have is a ByteBuffer (when dealing with IndexExpression
    // for instance) so...
    public ColumnDefinition getColumnDefinition(ByteBuffer name)
    {
        return columnMetadata.get(name);
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty()
               && name.length() <= SchemaConstants.NAME_LENGTH && PATTERN_WORD_CHARS.matcher(name).matches();
    }

    public CFMetaData validate() throws ConfigurationException
    {
        rebuild();

        if (!isNameValid(ksName))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", SchemaConstants.NAME_LENGTH, ksName));
        if (!isNameValid(cfName))
            throw new ConfigurationException(String.format("ColumnFamily name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", SchemaConstants.NAME_LENGTH, cfName));

        params.validate();

        for (int i = 0; i < comparator.size(); i++)
        {
            if (comparator.subtype(i) instanceof CounterColumnType)
                throw new ConfigurationException("CounterColumnType is not a valid comparator");
        }
        if (keyValidator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid key validator");

        // Mixing counter with non counter columns is not supported (#2614)
        if (isCounter())
        {
            for (ColumnDefinition def : partitionColumns())
                if (!(def.type instanceof CounterColumnType) && (!isSuper() || isSuperColumnValueColumn(def)))
                    throw new ConfigurationException("Cannot add a non counter column (" + def + ") in a counter column family");
        }
        else
        {
            for (ColumnDefinition def : allColumns())
                if (def.type instanceof CounterColumnType)
                    throw new ConfigurationException("Cannot add a counter column (" + def.name + ") in a non counter column family");
        }

        if (!indexes.isEmpty() && isSuper())
            throw new ConfigurationException("Secondary indexes are not supported on super column families");

        // initialize a set of names NOT in the CF under consideration
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(ksName);

        Set<String> indexNames = ksm == null ? new HashSet<>() : ksm.existingIndexNames(cfName);
        for (IndexMetadata index : indexes)
        {
            // check index names against this CF _and_ globally
            if (indexNames.contains(index.name))
                throw new ConfigurationException("Duplicate index name " + index.name);
            indexNames.add(index.name);

            index.validate(this);
        }

        return this;
    }

    // The comparator to validate the definition name with thrift.
    public AbstractType<?> thriftColumnNameType()
    {
        if (isSuper())
        {
            ColumnDefinition def = compactValueColumn();
            assert def != null && def.type instanceof MapType;
            return ((MapType)def.type).nameComparator();
        }

        assert isStaticCompactTable();
        return clusteringColumns.get(0).type;
    }

    public CFMetaData addColumnDefinition(ColumnDefinition def) throws ConfigurationException
    {
        if (columnMetadata.containsKey(def.name.bytes))
            throw new ConfigurationException(String.format("Cannot add column %s, a column with the same name already exists", def.name));

        return addOrReplaceColumnDefinition(def);
    }

    // This method doesn't check if a def of the same name already exist and should only be used when we
    // know this cannot happen.
    public CFMetaData addOrReplaceColumnDefinition(ColumnDefinition def)
    {
        // Adds the definition and rebuild what is necessary. We could call rebuild() but it's not too hard to
        // only rebuild the necessary bits.
        switch (def.kind)
        {
            case PARTITION_KEY:
                partitionKeyColumns.set(def.position(), def);
                break;
            case CLUSTERING:
                clusteringColumns.set(def.position(), def);
                break;
            case REGULAR:
            case STATIC:
                PartitionColumns.Builder builder = PartitionColumns.builder();
                for (ColumnDefinition column : partitionColumns)
                    if (!column.name.equals(def.name))
                        builder.add(column);
                builder.add(def);
                partitionColumns = builder.build();
                // If dense, we must have modified the compact value since that's the only one we can have.
                if (isDense())
                    this.compactValueColumn = def;
                break;
        }
        this.columnMetadata.put(def.name.bytes, def);
        return this;
    }

    public boolean removeColumnDefinition(ColumnDefinition def)
    {
        assert !def.isPartitionKey();
        boolean removed = columnMetadata.remove(def.name.bytes) != null;
        if (removed)
            partitionColumns = partitionColumns.without(def);
        return removed;
    }

    /**
     * Adds the column definition as a dropped column, recording the drop with the provided timestamp.
     */
    public void recordColumnDrop(ColumnDefinition def, long timeMicros)
    {
        droppedColumns.put(def.name.bytes, new DroppedColumn(def.name.toString(), def.type, timeMicros));
    }

    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to) throws InvalidRequestException
    {
        ColumnDefinition def = getColumnDefinition(from);

        if (def == null)
            throw new InvalidRequestException(String.format("Cannot rename unknown column %s in keyspace %s", from, cfName));

        if (getColumnDefinition(to) != null)
            throw new InvalidRequestException(String.format("Cannot rename column %s to %s in keyspace %s; another column of that name already exist", from, to, cfName));

        if (def.isPartOfCellName(isCQLTable(), isSuper()) && !isDense())
        {
            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
        }

        if (!getIndexes().isEmpty())
        {
            ColumnFamilyStore store = Keyspace.openAndGetStore(this);
            Set<IndexMetadata> dependentIndexes = store.indexManager.getDependentIndexes(def);
            if (!dependentIndexes.isEmpty())
                throw new InvalidRequestException(String.format("Cannot rename column %s because it has " +
                                                                "dependent secondary indexes (%s)",
                                                                from,
                                                                dependentIndexes.stream()
                                                                                .map(i -> i.name)
                                                                                .collect(Collectors.joining(","))));
        }

        if (isSuper() && isDense())
        {
            if (isSuperColumnKeyColumn(def))
            {
                columnMetadata.remove(superCfKeyColumn.name.bytes);
                superCfKeyColumn = superCfKeyColumn.withNewName(to);
                columnMetadata.put(superCfKeyColumn.name.bytes, SuperColumnCompatibility.getSuperCfSschemaRepresentation(superCfKeyColumn));
            }
            else if (isSuperColumnValueColumn(def))
            {
                columnMetadata.remove(superCfValueColumn.name.bytes);
                superCfValueColumn = superCfValueColumn.withNewName(to);
                columnMetadata.put(superCfValueColumn.name.bytes, superCfValueColumn);
            }
            else
                addOrReplaceColumnDefinition(def.withNewName(to));
        }
        else
        {
            addOrReplaceColumnDefinition(def.withNewName(to));
        }


        // removeColumnDefinition doesn't work for partition key (expectedly) but renaming one is fine so we still
        // want to update columnMetadata.
        if (def.isPartitionKey())
            columnMetadata.remove(def.name.bytes);
        else
            removeColumnDefinition(def);
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

    /**
     * Returns whether this CFMetaData can be returned to thrift.
     */
    public boolean isThriftCompatible()
    {
        return isCompactTable();
    }

    public boolean hasStaticColumns()
    {
        return !partitionColumns.statics.isEmpty();
    }

    public boolean hasCollectionColumns()
    {
        for (ColumnDefinition def : partitionColumns())
            if (def.type instanceof CollectionType && def.type.isMultiCell())
                return true;
        return false;
    }

    public boolean hasComplexColumns()
    {
        for (ColumnDefinition def : partitionColumns())
            if (def.isComplex())
                return true;
        return false;
    }

    public boolean hasDroppedCollectionColumns()
    {
        for (DroppedColumn def : getDroppedColumns().values())
            if (def.type instanceof CollectionType && def.type.isMultiCell())
                return true;
        return false;
    }

    public boolean isSuper()
    {
        return isSuper;
    }

    public boolean isCounter()
    {
        return isCounter;
    }

    // We call dense a CF for which each component of the comparator is a clustering column, i.e. no
    // component is used to store a regular column names. In other words, non-composite static "thrift"
    // and CQL3 CF are *not* dense.
    public boolean isDense()
    {
        return isDense;
    }

    public boolean isCompound()
    {
        return isCompound;
    }

    public boolean isView()
    {
        return isView;
    }

    /**
     * A table with strict liveness filters/ignores rows without PK liveness info,
     * effectively tying the row liveness to its primary key liveness.
     *
     * Currently this is only used by views with normal base column as PK column
     * so updates to other columns do not make the row live when the base column
     * is not live. See CASSANDRA-11500.
     */
    public boolean enforceStrictLiveness()
    {
        return isView && Keyspace.open(ksName).viewManager.getByName(cfName).enforceStrictLiveness();
    }

    public Serializers serializers()
    {
        return serializers;
    }

    public AbstractType<?> makeLegacyDefaultValidator()
    {
        if (isCounter())
            return CounterColumnType.instance;
        else if (isCompactTable())
            return isSuper() ? ((MapType)compactValueColumn().type).valueComparator() : compactValueColumn().type;
        else
            return BytesType.instance;
    }

    public static Set<Flag> flagsFromStrings(Set<String> strings)
    {
        return strings.stream()
                      .map(String::toUpperCase)
                      .map(Flag::valueOf)
                      .collect(Collectors.toSet());
    }

    public static Set<String> flagsToStrings(Set<Flag> flags)
    {
        return flags.stream()
                    .map(Flag::toString)
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
    }


    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("ksName", ksName)
            .append("cfName", cfName)
            .append("flags", flags)
            .append("params", params)
            .append("comparator", comparator)
            .append("partitionColumns", partitionColumns)
            .append("partitionKeyColumns", partitionKeyColumns)
            .append("clusteringColumns", clusteringColumns)
            .append("keyValidator", keyValidator)
            .append("columnMetadata", columnMetadata.values())
            .append("droppedColumns", droppedColumns)
            .append("triggers", triggers)
            .append("indexes", indexes)
            .toString();
    }

    public static class Builder
    {
        private final String keyspace;
        private final String table;
        private final boolean isDense;
        private final boolean isCompound;
        private final boolean isSuper;
        private final boolean isCounter;
        private final boolean isView;
        private Optional<IPartitioner> partitioner;

        private UUID tableId;

        private final List<Pair<ColumnIdentifier, AbstractType>> partitionKeys = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> clusteringColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> staticColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> regularColumns = new ArrayList<>();

        private Builder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter, boolean isView)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.isDense = isDense;
            this.isCompound = isCompound;
            this.isSuper = isSuper;
            this.isCounter = isCounter;
            this.isView = isView;
            this.partitioner = Optional.empty();
        }

        public static Builder create(String keyspace, String table)
        {
            return create(keyspace, table, false, true, false);
        }

        public static Builder create(String keyspace, String table, boolean isDense, boolean isCompound, boolean isCounter)
        {
            return create(keyspace, table, isDense, isCompound, false, isCounter);
        }

        public static Builder create(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter)
        {
            return new Builder(keyspace, table, isDense, isCompound, isSuper, isCounter, false);
        }

        public static Builder createView(String keyspace, String table)
        {
            return new Builder(keyspace, table, false, true, false, false, true);
        }

        public static Builder createDense(String keyspace, String table, boolean isCompound, boolean isCounter)
        {
            return create(keyspace, table, true, isCompound, isCounter);
        }

        public static Builder createSuper(String keyspace, String table, boolean isCounter)
        {
            return create(keyspace, table, true, true, true, isCounter);
        }

        public Builder withPartitioner(IPartitioner partitioner)
        {
            this.partitioner = Optional.ofNullable(partitioner);
            return this;
        }

        public Builder withId(UUID tableId)
        {
            this.tableId = tableId;
            return this;
        }

        public Builder addPartitionKey(String name, AbstractType type)
        {
            return addPartitionKey(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addPartitionKey(ColumnIdentifier name, AbstractType type)
        {
            this.partitionKeys.add(Pair.create(name, type));
            return this;
        }

        public Builder addClusteringColumn(String name, AbstractType type)
        {
            return addClusteringColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addClusteringColumn(ColumnIdentifier name, AbstractType type)
        {
            this.clusteringColumns.add(Pair.create(name, type));
            return this;
        }

        public Builder addRegularColumn(String name, AbstractType type)
        {
            return addRegularColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addRegularColumn(ColumnIdentifier name, AbstractType type)
        {
            this.regularColumns.add(Pair.create(name, type));
            return this;
        }

        public boolean hasRegulars()
        {
            return !this.regularColumns.isEmpty();
        }

        public Builder addStaticColumn(String name, AbstractType type)
        {
            return addStaticColumn(ColumnIdentifier.getInterned(name, false), type);
        }

        public Builder addStaticColumn(ColumnIdentifier name, AbstractType type)
        {
            this.staticColumns.add(Pair.create(name, type));
            return this;
        }

        public Set<String> usedColumnNames()
        {
            Set<String> usedNames = Sets.newHashSetWithExpectedSize(partitionKeys.size() + clusteringColumns.size() + staticColumns.size() + regularColumns.size());
            for (Pair<ColumnIdentifier, AbstractType> p : partitionKeys)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : clusteringColumns)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : staticColumns)
                usedNames.add(p.left.toString());
            for (Pair<ColumnIdentifier, AbstractType> p : regularColumns)
                usedNames.add(p.left.toString());
            return usedNames;
        }

        public CFMetaData build()
        {
            if (tableId == null)
                tableId = UUIDGen.getTimeUUID();

            List<ColumnDefinition> partitions = new ArrayList<>(partitionKeys.size());
            List<ColumnDefinition> clusterings = new ArrayList<>(clusteringColumns.size());
            PartitionColumns.Builder builder = PartitionColumns.builder();

            for (int i = 0; i < partitionKeys.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = partitionKeys.get(i);
                partitions.add(new ColumnDefinition(keyspace, table, p.left, p.right, i, ColumnDefinition.Kind.PARTITION_KEY));
            }

            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = clusteringColumns.get(i);
                clusterings.add(new ColumnDefinition(keyspace, table, p.left, p.right, i, ColumnDefinition.Kind.CLUSTERING));
            }

            for (Pair<ColumnIdentifier, AbstractType> p : regularColumns)
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, ColumnDefinition.NO_POSITION, ColumnDefinition.Kind.REGULAR));

            for (Pair<ColumnIdentifier, AbstractType> p : staticColumns)
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, ColumnDefinition.NO_POSITION, ColumnDefinition.Kind.STATIC));

            return new CFMetaData(keyspace,
                                  table,
                                  tableId,
                                  isSuper,
                                  isCounter,
                                  isDense,
                                  isCompound,
                                  isView,
                                  partitions,
                                  clusterings,
                                  builder.build(),
                                  partitioner.orElseGet(DatabaseDescriptor::getPartitioner),
                                  null,
                                  null);
        }
    }

    public static class Serializer
    {
        public void serialize(CFMetaData metadata, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(metadata.cfId, out, version);
        }

        public CFMetaData deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
            CFMetaData metadata = Schema.instance.getCFMetaData(cfId);
            if (metadata == null)
            {
                String message = String.format("Couldn't find table for cfId %s. If a table was just " +
                        "created, this is likely due to the schema not being fully propagated.  Please wait for schema " +
                        "agreement on table creation.", cfId);
                throw new UnknownColumnFamilyException(message, cfId);
            }

            return metadata;
        }

        public long serializedSize(CFMetaData metadata, int version)
        {
            return UUIDSerializer.serializer.serializedSize(metadata.cfId, version);
        }
    }

    public static class DroppedColumn
    {
        // we only allow dropping REGULAR columns, from CQL-native tables, so the names are always of UTF8Type
        public final String name;
        public final AbstractType<?> type;

        // drop timestamp, in microseconds, yet with millisecond granularity
        public final long droppedTime;

        public DroppedColumn(String name, AbstractType<?> type, long droppedTime)
        {
            this.name = name;
            this.type = type;
            this.droppedTime = droppedTime;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof DroppedColumn))
                return false;

            DroppedColumn dc = (DroppedColumn) o;

            return name.equals(dc.name) && type.equals(dc.type) && droppedTime == dc.droppedTime;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(name, type, droppedTime);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("name", name)
                              .add("type", type)
                              .add("droppedTime", droppedTime)
                              .toString();
        }
    }
}
