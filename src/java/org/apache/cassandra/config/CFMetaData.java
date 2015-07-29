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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.MaterializedViews;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.Triggers;
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
        SUPER, COUNTER, DENSE, COMPOUND, MATERIALIZEDVIEW
    }

    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public static final Serializer serializer = new Serializer();

    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.0;
    public final static double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.1;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static Class<? extends AbstractCompactionStrategy> DEFAULT_COMPACTION_STRATEGY_CLASS = SizeTieredCompactionStrategy.class;
    public final static CachingOptions DEFAULT_CACHING_STRATEGY = CachingOptions.KEYS_ONLY;
    public final static int DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    public final static SpeculativeRetry DEFAULT_SPECULATIVE_RETRY = new SpeculativeRetry(SpeculativeRetry.RetryType.PERCENTILE, 0.99);
    public final static int DEFAULT_MIN_INDEX_INTERVAL = 128;
    public final static int DEFAULT_MAX_INDEX_INTERVAL = 2048;

    // Note that this is the default only for user created tables
    public final static String DEFAULT_COMPRESSOR = LZ4Compressor.class.getCanonicalName();

    // Note that this need to come *before* any CFMetaData is defined so before the compile below.
    private static final Comparator<ColumnDefinition> regularColumnComparator = new Comparator<ColumnDefinition>()
    {
        public int compare(ColumnDefinition def1, ColumnDefinition def2)
        {
            return ByteBufferUtil.compareUnsigned(def1.name.bytes, def2.name.bytes);
        }
    };

    public static class SpeculativeRetry
    {
        public enum RetryType
        {
            NONE, CUSTOM, PERCENTILE, ALWAYS
        }

        public final RetryType type;
        public final double value;

        private SpeculativeRetry(RetryType type, double value)
        {
            this.type = type;
            this.value = value;
        }

        public static SpeculativeRetry fromString(String retry) throws ConfigurationException
        {
            String name = retry.toUpperCase();
            try
            {
                if (name.endsWith(RetryType.PERCENTILE.toString()))
                {
                    double value = Double.parseDouble(name.substring(0, name.length() - 10));
                    if (value > 100 || value < 0)
                        throw new ConfigurationException("PERCENTILE should be between 0 and 100, but was " + value);
                    return new SpeculativeRetry(RetryType.PERCENTILE, (value / 100));
                }
                else if (name.endsWith("MS"))
                {
                    double value = Double.parseDouble(name.substring(0, name.length() - 2));
                    return new SpeculativeRetry(RetryType.CUSTOM, value);
                }
                else
                {
                    return new SpeculativeRetry(RetryType.valueOf(name), 0);
                }
            }
            catch (IllegalArgumentException e)
            {
                // ignore to throw the below exception.
            }
            throw new ConfigurationException("invalid speculative_retry type: " + retry);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof SpeculativeRetry))
                return false;
            SpeculativeRetry rhs = (SpeculativeRetry) obj;
            return Objects.equal(type, rhs.type) && Objects.equal(value, rhs.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(type, value);
        }

        @Override
        public String toString()
        {
            switch (type)
            {
            case PERCENTILE:
                // TODO switch to BigDecimal so round-tripping isn't lossy
                return (value * 100) + "PERCENTILE";
            case CUSTOM:
                return value + "ms";
            default:
                return type.toString();
            }
        }
    }

    //REQUIRED
    public final UUID cfId;                           // internal id, never exposed to user
    public final String ksName;                       // name of keyspace
    public final String cfName;                       // name of this column family

    private final ImmutableSet<Flag> flags;
    private final boolean isDense;
    private final boolean isCompound;
    private final boolean isSuper;
    private final boolean isCounter;
    private final boolean isMaterializedView;

    public volatile ClusteringComparator comparator;  // bytes, long, timeuuid, utf8, etc. This is built directly from clusteringColumns

    private final Serializers serializers;

    //OPTIONAL
    private volatile String comment = "";
    private volatile double readRepairChance = DEFAULT_READ_REPAIR_CHANCE;
    private volatile double dcLocalReadRepairChance = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
    private volatile int gcGraceSeconds = DEFAULT_GC_GRACE_SECONDS;
    private volatile AbstractType<?> keyValidator = BytesType.instance;
    private volatile int minCompactionThreshold = DEFAULT_MIN_COMPACTION_THRESHOLD;
    private volatile int maxCompactionThreshold = DEFAULT_MAX_COMPACTION_THRESHOLD;
    private volatile Double bloomFilterFpChance = null;
    private volatile CachingOptions caching = DEFAULT_CACHING_STRATEGY;
    private volatile int minIndexInterval = DEFAULT_MIN_INDEX_INTERVAL;
    private volatile int maxIndexInterval = DEFAULT_MAX_INDEX_INTERVAL;
    private volatile int memtableFlushPeriod = 0;
    private volatile int defaultTimeToLive = DEFAULT_DEFAULT_TIME_TO_LIVE;
    private volatile SpeculativeRetry speculativeRetry = DEFAULT_SPECULATIVE_RETRY;
    private volatile Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
    private volatile Triggers triggers = Triggers.none();
    private volatile MaterializedViews materializedViews = MaterializedViews.none();

    /*
     * All CQL3 columns definition are stored in the columnMetadata map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and 2) for the partition key and
     * clustering key ones, those list are ordered by the "component index" of the
     * elements.
     */
    private final Map<ByteBuffer, ColumnDefinition> columnMetadata = new ConcurrentHashMap<>(); // not on any hot path
    private volatile List<ColumnDefinition> partitionKeyColumns;  // Always of size keyValidator.componentsCount, null padded if necessary
    private volatile List<ColumnDefinition> clusteringColumns;    // Of size comparator.componentsCount or comparator.componentsCount -1, null padded if necessary
    private volatile PartitionColumns partitionColumns;

    // For dense tables, this alias the single non-PK column the table contains (since it can only have one). We keep
    // that as convenience to access that column more easily (but we could replace calls by partitionColumns().iterator().next()
    // for those tables in practice).
    private volatile ColumnDefinition compactValueColumn;

    public volatile Class<? extends AbstractCompactionStrategy> compactionStrategyClass = DEFAULT_COMPACTION_STRATEGY_CLASS;
    public volatile Map<String, String> compactionStrategyOptions = new HashMap<>();

    public volatile CompressionParameters compressionParameters = CompressionParameters.noCompression();

    // attribute setters that return the modified CFMetaData instance
    public CFMetaData comment(String prop) {comment = Strings.nullToEmpty(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dcLocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(CachingOptions prop) {caching = prop; return this;}
    public CFMetaData minIndexInterval(int prop) {minIndexInterval = prop; return this;}
    public CFMetaData maxIndexInterval(int prop) {maxIndexInterval = prop; return this;}
    public CFMetaData memtableFlushPeriod(int prop) {memtableFlushPeriod = prop; return this;}
    public CFMetaData defaultTimeToLive(int prop) {defaultTimeToLive = prop; return this;}
    public CFMetaData speculativeRetry(SpeculativeRetry prop) {speculativeRetry = prop; return this;}
    public CFMetaData droppedColumns(Map<ByteBuffer, DroppedColumn> cols) {droppedColumns = cols; return this;}
    public CFMetaData triggers(Triggers prop) {triggers = prop; return this;}
    public CFMetaData materializedViews(MaterializedViews prop) {materializedViews = prop; return this;}

    private CFMetaData(String keyspace,
                       String name,
                       UUID cfId,
                       boolean isSuper,
                       boolean isCounter,
                       boolean isDense,
                       boolean isCompound,
                       boolean isMaterializedView,
                       List<ColumnDefinition> partitionKeyColumns,
                       List<ColumnDefinition> clusteringColumns,
                       PartitionColumns partitionColumns)
    {
        this.cfId = cfId;
        this.ksName = keyspace;
        this.cfName = name;

        this.isDense = isDense;
        this.isCompound = isCompound;
        this.isSuper = isSuper;
        this.isCounter = isCounter;
        this.isMaterializedView = isMaterializedView;

        EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        if (isSuper)
            flags.add(Flag.SUPER);
        if (isCounter)
            flags.add(Flag.COUNTER);
        if (isDense)
            flags.add(Flag.DENSE);
        if (isCompound)
            flags.add(Flag.COMPOUND);
        if (isMaterializedView)
            flags.add(Flag.MATERIALIZEDVIEW);
        this.flags = Sets.immutableEnumSet(flags);

        // A compact table should always have a clustering
        assert isCQLTable() || !clusteringColumns.isEmpty() : String.format("For table %s.%s, isDense=%b, isCompound=%b, clustering=%s", ksName, cfName, isDense, isCompound, clusteringColumns);

        this.partitionKeyColumns = partitionKeyColumns;
        this.clusteringColumns = clusteringColumns;
        this.partitionColumns = partitionColumns;

        this.serializers = new Serializers(this);
        rebuild();
    }

    // This rebuild informations that are intrinsically duplicate of the table definition but
    // are kept because they are often useful in a different format.
    private void rebuild()
    {
        this.comparator = new ClusteringComparator(extractTypes(clusteringColumns));

        this.columnMetadata.clear();
        for (ColumnDefinition def : partitionKeyColumns)
            this.columnMetadata.put(def.name.bytes, def);
        for (ColumnDefinition def : clusteringColumns)
            this.columnMetadata.put(def.name.bytes, def);
        for (ColumnDefinition def : partitionColumns)
            this.columnMetadata.put(def.name.bytes, def);

        List<AbstractType<?>> keyTypes = extractTypes(partitionKeyColumns);
        this.keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);

        if (isCompactTable())
            this.compactValueColumn = CompactTables.getCompactValueColumn(partitionColumns, isSuper());
    }

    public MaterializedViews getMaterializedViews()
    {
        return materializedViews;
    }

    public static CFMetaData create(String ksName,
                                    String name,
                                    UUID cfId,
                                    boolean isDense,
                                    boolean isCompound,
                                    boolean isSuper,
                                    boolean isCounter,
                                    boolean isMaterializedView,
                                    List<ColumnDefinition> columns)
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
                              isMaterializedView,
                              partitions,
                              clusterings,
                              builder.build());
    }

    private static List<AbstractType<?>> extractTypes(List<ColumnDefinition> clusteringColumns)
    {
        List<AbstractType<?>> types = new ArrayList<>(clusteringColumns.size());
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
        CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
        CFMetaData.Builder builder = statement.metadataBuilder();
        builder.withId(generateLegacyCfId(keyspace, statement.columnFamily()));
        CFMetaData cfm = builder.build();
        statement.applyPropertiesTo(cfm);

        return cfm.readRepairChance(0)
                  .dcLocalReadRepairChance(0)
                  .gcGraceSeconds(0)
                  .memtableFlushPeriod(3600 * 1000);
    }

    /**
     * Generates deterministic UUID from keyspace/columnfamily name pair.
     * This is used to generate the same UUID for C* version < 2.1
     *
     * Since 2.1, this is only used for system columnfamilies and tests.
     */
    public static UUID generateLegacyCfId(String ksName, String cfName)
    {
        return UUID.nameUUIDFromBytes(ArrayUtils.addAll(ksName.getBytes(), cfName.getBytes()));
    }

    public CFMetaData reloadIndexMetadataProperties(CFMetaData parent)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        CachingOptions indexCaching = parent.getCaching().keyCache.isEnabled()
                                    ? CachingOptions.KEYS_ONLY
                                    : CachingOptions.NONE;

        return this.readRepairChance(0.0)
                   .dcLocalReadRepairChance(0.0)
                   .gcGraceSeconds(0)
                   .caching(indexCaching)
                   .speculativeRetry(parent.speculativeRetry)
                   .minCompactionThreshold(parent.minCompactionThreshold)
                   .maxCompactionThreshold(parent.maxCompactionThreshold)
                   .compactionStrategyClass(parent.compactionStrategyClass)
                   .compactionStrategyOptions(parent.compactionStrategyOptions)
                   .compressionParameters(parent.compressionParameters);
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
                                       isMaterializedView(),
                                       copy(partitionKeyColumns),
                                       copy(clusteringColumns),
                                       copy(partitionColumns)),
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
        return newCFMD.comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(new HashMap<>(oldCFMD.compactionStrategyOptions))
                      .compressionParameters(oldCFMD.compressionParameters.copy())
                      .bloomFilterFpChance(oldCFMD.getBloomFilterFpChance())
                      .caching(oldCFMD.caching)
                      .defaultTimeToLive(oldCFMD.defaultTimeToLive)
                      .minIndexInterval(oldCFMD.minIndexInterval)
                      .maxIndexInterval(oldCFMD.maxIndexInterval)
                      .speculativeRetry(oldCFMD.speculativeRetry)
                      .memtableFlushPeriod(oldCFMD.memtableFlushPeriod)
                      .droppedColumns(new HashMap<>(oldCFMD.droppedColumns))
                      .triggers(oldCFMD.triggers)
                      .materializedViews(oldCFMD.materializedViews);
    }

    /**
     * generate a column family name for an index corresponding to the given column.
     * This is NOT the same as the index's name! This is only used in sstable filenames and is not exposed to users.
     *
     * @param info A definition of the column with index
     *
     * @return name of the index ColumnFamily
     */
    public String indexColumnFamilyName(ColumnDefinition info)
    {
        // TODO simplify this when info.index_name is guaranteed to be set
        return cfName + Directories.SECONDARY_INDEX_NAME_SEPARATOR + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name.bytes) : info.getIndexName());
    }

    public String getComment()
    {
        return comment;
    }

    /**
     * The '.' char is the only way to identify if the CFMetadata is for a secondary index
     */
    public boolean isSecondaryIndex()
    {
        return cfName.contains(".");
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
        return isSecondaryIndex() ? cfName.substring(0, cfName.indexOf('.')) : null;
    }

    public double getReadRepairChance()
    {
        return readRepairChance;
    }

    public double getDcLocalReadRepairChance()
    {
        return dcLocalReadRepairChance;
    }

    public ReadRepairDecision newReadRepairDecision()
    {
        double chance = ThreadLocalRandom.current().nextDouble();
        if (getReadRepairChance() > chance)
            return ReadRepairDecision.GLOBAL;

        if (getDcLocalReadRepairChance() > chance)
            return ReadRepairDecision.DC_LOCAL;

        return ReadRepairDecision.NONE;
    }

    public AbstractType<?> getColumnDefinitionNameComparator(ColumnDefinition.Kind kind)
    {
        return (isSuper() && kind == ColumnDefinition.Kind.REGULAR) || (isStaticCompactTable() && kind == ColumnDefinition.Kind.STATIC)
             ? thriftColumnNameType()
             : UTF8Type.instance;
    }

    public int getGcGraceSeconds()
    {
        return gcGraceSeconds;
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public int getMinCompactionThreshold()
    {
        return minCompactionThreshold;
    }

    public int getMaxCompactionThreshold()
    {
        return maxCompactionThreshold;
    }

    public CompressionParameters compressionParameters()
    {
        return compressionParameters;
    }

    public Collection<ColumnDefinition> allColumns()
    {
        return columnMetadata.values();
    }

    // An iterator over all column definitions but that respect the order of a SELECT *.
    // This also "hide" the clustering/regular columns for a non-CQL3 non-dense table for backward compatibility
    // sake (those are accessible through thrift but not through CQL currently).
    public Iterator<ColumnDefinition> allColumnsInSelectOrder()
    {
        final boolean isStaticCompactTable = isStaticCompactTable();
        final boolean noNonPkColumns = isCompactTable() && CompactTables.hasEmptyCompactValue(this);
        return new AbstractIterator<ColumnDefinition>()
        {
            private final Iterator<ColumnDefinition> partitionKeyIter = partitionKeyColumns.iterator();
            private final Iterator<ColumnDefinition> clusteringIter = isStaticCompactTable ? Collections.<ColumnDefinition>emptyIterator() : clusteringColumns.iterator();
            private final Iterator<ColumnDefinition> otherColumns = noNonPkColumns
                                                                  ? Collections.<ColumnDefinition>emptyIterator()
                                                                  : (isStaticCompactTable
                                                                     ?  partitionColumns.statics.selectOrderIterator()
                                                                     :  partitionColumns.selectOrderIterator());

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

    public double getBloomFilterFpChance()
    {
        // we disallow bFFPC==null starting in 1.2.1 but tolerated it before that
        return (bloomFilterFpChance == null || bloomFilterFpChance == 0)
               ? compactionStrategyClass == LeveledCompactionStrategy.class ? 0.1 : 0.01
               : bloomFilterFpChance;
    }

    public CachingOptions getCaching()
    {
        return caching;
    }

    public int getMinIndexInterval()
    {
        return minIndexInterval;
    }

    public int getMaxIndexInterval()
    {
        return maxIndexInterval;
    }

    public SpeculativeRetry getSpeculativeRetry()
    {
        return speculativeRetry;
    }

    public int getMemtableFlushPeriod()
    {
        return memtableFlushPeriod;
    }

    public int getDefaultTimeToLive()
    {
        return defaultTimeToLive;
    }

    public Map<ByteBuffer, DroppedColumn> getDroppedColumns()
    {
        return droppedColumns;
    }

    /**
     * Returns a "fake" ColumnDefinition corresponding to the dropped column {@code name}
     * of {@code null} if there is no such dropped column.
     */
    public ColumnDefinition getDroppedColumnDefinition(ByteBuffer name)
    {
        DroppedColumn dropped = droppedColumns.get(name);
        if (dropped == null)
            return null;

        // We need the type for deserialization purpose. If we don't have the type however,
        // it means that it's a dropped column from before 3.0, and in that case using
        // BytesType is fine for what we'll be using it for, even if that's a hack.
        AbstractType<?> type = dropped.type == null ? BytesType.instance : dropped.type;
        return ColumnDefinition.regularDef(this, name, type);
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
            && Objects.equal(comparator, other.comparator)
            && Objects.equal(comment, other.comment)
            && Objects.equal(readRepairChance, other.readRepairChance)
            && Objects.equal(dcLocalReadRepairChance, other.dcLocalReadRepairChance)
            && Objects.equal(gcGraceSeconds, other.gcGraceSeconds)
            && Objects.equal(keyValidator, other.keyValidator)
            && Objects.equal(minCompactionThreshold, other.minCompactionThreshold)
            && Objects.equal(maxCompactionThreshold, other.maxCompactionThreshold)
            && Objects.equal(columnMetadata, other.columnMetadata)
            && Objects.equal(compactionStrategyClass, other.compactionStrategyClass)
            && Objects.equal(compactionStrategyOptions, other.compactionStrategyOptions)
            && Objects.equal(compressionParameters, other.compressionParameters)
            && Objects.equal(getBloomFilterFpChance(), other.getBloomFilterFpChance())
            && Objects.equal(memtableFlushPeriod, other.memtableFlushPeriod)
            && Objects.equal(caching, other.caching)
            && Objects.equal(defaultTimeToLive, other.defaultTimeToLive)
            && Objects.equal(minIndexInterval, other.minIndexInterval)
            && Objects.equal(maxIndexInterval, other.maxIndexInterval)
            && Objects.equal(speculativeRetry, other.speculativeRetry)
            && Objects.equal(droppedColumns, other.droppedColumns)
            && Objects.equal(triggers, other.triggers)
            && Objects.equal(materializedViews, other.materializedViews);
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
            .append(comment)
            .append(readRepairChance)
            .append(dcLocalReadRepairChance)
            .append(gcGraceSeconds)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(columnMetadata)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .append(getBloomFilterFpChance())
            .append(memtableFlushPeriod)
            .append(caching)
            .append(defaultTimeToLive)
            .append(minIndexInterval)
            .append(maxIndexInterval)
            .append(speculativeRetry)
            .append(droppedColumns)
            .append(triggers)
            .append(materializedViews)
            .toHashCode();
    }

    /**
     * Updates this object in place to match the definition in the system schema tables.
     * @return true if any columns were added, removed, or altered; otherwise, false is returned
     */
    public boolean reload()
    {
        return apply(SchemaKeyspace.createTableFromName(ksName, cfName));
    }

    /**
     * Updates CFMetaData in-place to match cfm
     *
     * @return true if any columns were added, removed, or altered; otherwise, false is returned
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    @VisibleForTesting
    public boolean apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);

        validateCompatility(cfm);

        partitionKeyColumns = cfm.partitionKeyColumns;
        clusteringColumns = cfm.clusteringColumns;

        boolean hasColumnChange = !partitionColumns.equals(cfm.partitionColumns);
        partitionColumns = cfm.partitionColumns;

        rebuild();

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        comment = Strings.nullToEmpty(cfm.comment);
        readRepairChance = cfm.readRepairChance;
        dcLocalReadRepairChance = cfm.dcLocalReadRepairChance;
        gcGraceSeconds = cfm.gcGraceSeconds;
        keyValidator = cfm.keyValidator;
        minCompactionThreshold = cfm.minCompactionThreshold;
        maxCompactionThreshold = cfm.maxCompactionThreshold;

        bloomFilterFpChance = cfm.getBloomFilterFpChance();
        caching = cfm.caching;
        minIndexInterval = cfm.minIndexInterval;
        maxIndexInterval = cfm.maxIndexInterval;
        memtableFlushPeriod = cfm.memtableFlushPeriod;
        defaultTimeToLive = cfm.defaultTimeToLive;
        speculativeRetry = cfm.speculativeRetry;

        if (!cfm.droppedColumns.isEmpty())
            droppedColumns = cfm.droppedColumns;

        compactionStrategyClass = cfm.compactionStrategyClass;
        compactionStrategyOptions = cfm.compactionStrategyOptions;

        compressionParameters = cfm.compressionParameters;

        triggers = cfm.triggers;
        materializedViews = cfm.materializedViews;

        logger.debug("application result is {}", this);

        return hasColumnChange;
    }

    public void validateCompatility(CFMetaData cfm) throws ConfigurationException
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

        if (!cfm.flags.equals(flags))
            throw new ConfigurationException("types do not match.");

        if (!cfm.comparator.isCompatibleWith(comparator))
            throw new ConfigurationException(String.format("Column family comparators do not match or are not compatible (found %s; expected %s).", cfm.comparator.getClass().getSimpleName(), comparator.getClass().getSimpleName()));
    }

    public static void validateCompactionOptions(Class<? extends AbstractCompactionStrategy> strategyClass, Map<String, String> options) throws ConfigurationException
    {
        try
        {
            if (options == null)
                return;

            Map<?,?> unknownOptions = (Map) strategyClass.getMethod("validateOptions", Map.class).invoke(null, options);
            if (!unknownOptions.isEmpty())
                throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", unknownOptions.keySet(), strategyClass.getSimpleName()));
        }
        catch (NoSuchMethodException e)
        {
            logger.warn("Compaction Strategy {} does not have a static validateOptions method. Validation ignored", strategyClass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();
            throw new ConfigurationException("Failed to validate compaction options: " + options);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate compaction options: " + options);
        }
    }

    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
        if (!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass))
            throw new ConfigurationException(String.format("Specified compaction strategy class (%s) is not derived from AbstractReplicationStrategy", className));

        return strategyClass;
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor =
                compactionStrategyClass.getConstructor(ColumnFamilyStore.class, Map.class);
            return constructor.newInstance(cfs, compactionStrategyOptions);
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
        return columnMetadata.get(name.bytes);
    }

    // In general it is preferable to work with ColumnIdentifier to make it
    // clear that we are talking about a CQL column, not a cell name, but there
    // is a few cases where all we have is a ByteBuffer (when dealing with IndexExpression
    // for instance) so...
    public ColumnDefinition getColumnDefinition(ByteBuffer name)
    {
        return columnMetadata.get(name);
    }

    public ColumnDefinition getColumnDefinitionForIndex(String indexName)
    {
        for (ColumnDefinition def : allColumns())
        {
            if (indexName.equals(def.getIndexName()))
                return def;
        }
        return null;
    }

    /**
     * Convert a null index_name to appropriate default name according to column status
     */
    public void addDefaultIndexNames() throws ConfigurationException
    {
        // if this is ColumnFamily update we need to add previously defined index names to the existing columns first
        UUID cfId = Schema.instance.getId(ksName, cfName);
        if (cfId != null)
        {
            CFMetaData cfm = Schema.instance.getCFMetaData(cfId);

            for (ColumnDefinition newDef : allColumns())
            {
                if (!cfm.columnMetadata.containsKey(newDef.name.bytes) || newDef.getIndexType() == null)
                    continue;

                String oldIndexName = cfm.getColumnDefinition(newDef.name).getIndexName();

                if (oldIndexName == null)
                    continue;

                if (newDef.getIndexName() != null && !oldIndexName.equals(newDef.getIndexName()))
                    throw new ConfigurationException("Can't modify index name: was '" + oldIndexName + "' changed to '" + newDef.getIndexName() + "'.");

                newDef.setIndexName(oldIndexName);
            }
        }

        Set<String> existingNames = existingIndexNames(null);
        for (ColumnDefinition column : allColumns())
        {
            if (column.getIndexType() != null && column.getIndexName() == null)
            {
                String baseName = getDefaultIndexName(cfName, column.name);
                String indexName = baseName;
                int i = 0;
                while (existingNames.contains(indexName))
                    indexName = baseName + '_' + (++i);
                column.setIndexName(indexName);
            }
        }
    }

    public static String getDefaultIndexName(String cfName, ColumnIdentifier columnName)
    {
        return (cfName + "_" + columnName + "_idx").replaceAll("\\W", "");
    }

    public static boolean isNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.length() <= Schema.NAME_LENGTH && name.matches("\\w+");
    }

    public static boolean isIndexNameValid(String name)
    {
        return name != null && !name.isEmpty() && name.matches("\\w+");
    }

    public CFMetaData validate() throws ConfigurationException
    {
        rebuild();

        if (!isNameValid(ksName))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, ksName));
        if (!isNameValid(cfName))
            throw new ConfigurationException(String.format("ColumnFamily name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", Schema.NAME_LENGTH, cfName));

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
                if (!(def.type instanceof CounterColumnType) && !CompactTables.isSuperColumnMapColumn(def))
                    throw new ConfigurationException("Cannot add a non counter column (" + def.name + ") in a counter column family");
        }
        else
        {
            for (ColumnDefinition def : allColumns())
                if (def.type instanceof CounterColumnType)
                    throw new ConfigurationException("Cannot add a counter column (" + def.name + ") in a non counter column family");
        }

        // initialize a set of names NOT in the CF under consideration
        Set<String> indexNames = existingIndexNames(cfName);
        for (ColumnDefinition c : allColumns())
        {
            if (c.getIndexType() == null)
            {
                if (c.getIndexName() != null)
                    throw new ConfigurationException("Index name cannot be set without index type");
            }
            else
            {
                if (isSuper())
                    throw new ConfigurationException("Secondary indexes are not supported on super column families");
                if (!isIndexNameValid(c.getIndexName()))
                    throw new ConfigurationException("Illegal index name " + c.getIndexName());
                // check index names against this CF _and_ globally
                if (indexNames.contains(c.getIndexName()))
                    throw new ConfigurationException("Duplicate index name " + c.getIndexName());
                indexNames.add(c.getIndexName());

                if (c.getIndexType() == IndexType.CUSTOM)
                {
                    if (c.getIndexOptions() == null || !c.hasIndexOption(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
                        throw new ConfigurationException("Required index option missing: " + SecondaryIndex.CUSTOM_INDEX_OPTION_NAME);
                }

                // This method validates the column metadata but does not intialize the index
                SecondaryIndex.createInstance(null, c);
            }
        }

        validateCompactionThresholds();

        if (bloomFilterFpChance != null && bloomFilterFpChance == 0)
            throw new ConfigurationException("Zero false positives is impossible; bloom filter false positive chance bffpc must be 0 < bffpc <= 1");

        validateIndexIntervalThresholds();

        return this;
    }

    private static Set<String> existingIndexNames(String cfToExclude)
    {
        Set<String> indexNames = new HashSet<>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            if (cfToExclude == null || !cfs.name.equals(cfToExclude))
                for (ColumnDefinition cd : cfs.metadata.allColumns())
                    indexNames.add(cd.getIndexName());
        return indexNames;
    }

    private void validateCompactionThresholds() throws ConfigurationException
    {
        if (maxCompactionThreshold == 0)
        {
            logger.warn("Disabling compaction by setting max or min compaction has been deprecated, " +
                    "set the compaction strategy option 'enabled' to 'false' instead");
            return;
        }

        if (minCompactionThreshold <= 1)
            throw new ConfigurationException(String.format("Min compaction threshold cannot be less than 2 (got %d).", minCompactionThreshold));

        if (minCompactionThreshold > maxCompactionThreshold)
            throw new ConfigurationException(String.format("Min compaction threshold (got %d) cannot be greater than max compaction threshold (got %d)",
                                                            minCompactionThreshold, maxCompactionThreshold));
    }

    private void validateIndexIntervalThresholds() throws ConfigurationException
    {
        if (minIndexInterval <= 0)
            throw new ConfigurationException(String.format("Min index interval must be greater than 0 (got %d).", minIndexInterval));
        if (maxIndexInterval < minIndexInterval)
            throw new ConfigurationException(String.format("Max index interval (%d) must be greater than the min index " +
                                                           "interval (%d).", maxIndexInterval, minIndexInterval));
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

    public CFMetaData addAllColumnDefinitions(Collection<ColumnDefinition> defs)
    {
        for (ColumnDefinition def : defs)
            addOrReplaceColumnDefinition(def);
        return this;
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
                List<AbstractType<?>> keyTypes = extractTypes(partitionKeyColumns);
                keyValidator = keyTypes.size() == 1 ? keyTypes.get(0) : CompositeType.getInstance(keyTypes);
                break;
            case CLUSTERING:
                clusteringColumns.set(def.position(), def);
                comparator = new ClusteringComparator(extractTypes(clusteringColumns));
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

    public void recordColumnDrop(ColumnDefinition def)
    {
        droppedColumns.put(def.name.bytes, new DroppedColumn(def.name.toString(), def.type, FBUtilities.timestampMicros()));
    }

    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to) throws InvalidRequestException
    {
        ColumnDefinition def = getColumnDefinition(from);
        if (def == null)
            throw new InvalidRequestException(String.format("Cannot rename unknown column %s in keyspace %s", from, cfName));

        if (getColumnDefinition(to) != null)
            throw new InvalidRequestException(String.format("Cannot rename column %s to %s in keyspace %s; another column of that name already exist", from, to, cfName));

        if (def.isPartOfCellName(isCQLTable(), isSuper()))
        {
            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
        }
        else if (def.isIndexed())
        {
            throw new InvalidRequestException(String.format("Cannot rename column %s because it is secondary indexed", from));
        }

        ColumnDefinition newDef = def.withNewName(to);
        addOrReplaceColumnDefinition(newDef);

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

    public boolean isMaterializedView()
    {
        return isMaterializedView;
    }

    public Serializers serializers()
    {
        return serializers;
    }

    public AbstractType<?> makeLegacyDefaultValidator()
    {
        return isCounter()
             ? CounterColumnType.instance
             : (isCompactTable() ? compactValueColumn().type : BytesType.instance);
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("ksName", ksName)
            .append("cfName", cfName)
            .append("flags", flags)
            .append("comparator", comparator)
            .append("partitionColumns", partitionColumns)
            .append("partitionKeyColumns", partitionKeyColumns)
            .append("clusteringColumns", clusteringColumns)
            .append("comment", comment)
            .append("readRepairChance", readRepairChance)
            .append("dcLocalReadRepairChance", dcLocalReadRepairChance)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("columnMetadata", columnMetadata.values())
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionParameters", compressionParameters.asMap())
            .append("bloomFilterFpChance", getBloomFilterFpChance())
            .append("memtableFlushPeriod", memtableFlushPeriod)
            .append("caching", caching)
            .append("defaultTimeToLive", defaultTimeToLive)
            .append("minIndexInterval", minIndexInterval)
            .append("maxIndexInterval", maxIndexInterval)
            .append("speculativeRetry", speculativeRetry)
            .append("droppedColumns", droppedColumns)
            .append("triggers", triggers)
            .append("materializedViews", materializedViews)
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
        private final boolean isMaterializedView;

        private UUID tableId;

        private final List<Pair<ColumnIdentifier, AbstractType>> partitionKeys = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> clusteringColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> staticColumns = new ArrayList<>();
        private final List<Pair<ColumnIdentifier, AbstractType>> regularColumns = new ArrayList<>();

        private Builder(String keyspace, String table, boolean isDense, boolean isCompound, boolean isSuper, boolean isCounter, boolean isMaterializedView)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.isDense = isDense;
            this.isCompound = isCompound;
            this.isSuper = isSuper;
            this.isCounter = isCounter;
            this.isMaterializedView = isMaterializedView;
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
            return create(keyspace, table, false, false, true, isCounter);
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
            Set<String> usedNames = new HashSet<>();
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
                Integer componentIndex = partitionKeys.size() == 1 ? null : i;
                partitions.add(new ColumnDefinition(keyspace, table, p.left, p.right, componentIndex, ColumnDefinition.Kind.PARTITION_KEY));
            }

            for (int i = 0; i < clusteringColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = clusteringColumns.get(i);
                clusterings.add(new ColumnDefinition(keyspace, table, p.left, p.right, i, ColumnDefinition.Kind.CLUSTERING));
            }

            for (int i = 0; i < regularColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = regularColumns.get(i);
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, null, ColumnDefinition.Kind.REGULAR));
            }

            for (int i = 0; i < staticColumns.size(); i++)
            {
                Pair<ColumnIdentifier, AbstractType> p = staticColumns.get(i);
                builder.add(new ColumnDefinition(keyspace, table, p.left, p.right, null, ColumnDefinition.Kind.STATIC));
            }

            return new CFMetaData(keyspace,
                                  table,
                                  tableId,
                                  isSuper,
                                  isCounter,
                                  isDense,
                                  isCompound,
                                  isMaterializedView,
                                  partitions,
                                  clusterings,
                                  builder.build());
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
