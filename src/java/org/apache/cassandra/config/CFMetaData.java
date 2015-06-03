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

import java.io.DataInput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.db.composites.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.utils.FBUtilities.fromJsonList;
import static org.apache.cassandra.utils.FBUtilities.fromJsonMap;
import static org.apache.cassandra.utils.FBUtilities.json;

/**
 * This class can be tricky to modify. Please read http://wiki.apache.org/cassandra/ConfigurationNotes for how to do so safely.
 */
@Unmetered
public final class CFMetaData
{
    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

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

    public static final CFMetaData IndexCf = compile("CREATE TABLE \"" + SystemKeyspace.INDEX_CF + "\" ("
                                                     + "table_name text,"
                                                     + "index_name text,"
                                                     + "PRIMARY KEY (table_name, index_name)"
                                                     + ") WITH COMPACT STORAGE AND COMMENT='indexes that have been completed'");

    public static final CFMetaData SchemaKeyspacesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_KEYSPACES_CF + " ("
                                                               + "keyspace_name text PRIMARY KEY,"
                                                               + "durable_writes boolean,"
                                                               + "strategy_class text,"
                                                               + "strategy_options text"
                                                               + ") WITH COMPACT STORAGE AND COMMENT='keyspace definitions' AND gc_grace_seconds=604800");

    public static final CFMetaData SchemaColumnFamiliesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF + " ("
                                                                    + "keyspace_name text,"
                                                                    + "columnfamily_name text,"
                                                                    + "cf_id uuid," // post-2.1 UUID cfid
                                                                    + "type text,"
                                                                    + "is_dense boolean,"
                                                                    + "comparator text,"
                                                                    + "subcomparator text,"
                                                                    + "comment text,"
                                                                    + "read_repair_chance double,"
                                                                    + "local_read_repair_chance double,"
                                                                    + "gc_grace_seconds int,"
                                                                    + "default_validator text,"
                                                                    + "key_validator text,"
                                                                    + "min_compaction_threshold int,"
                                                                    + "max_compaction_threshold int,"
                                                                    + "memtable_flush_period_in_ms int,"
                                                                    + "key_aliases text,"
                                                                    + "bloom_filter_fp_chance double,"
                                                                    + "caching text,"
                                                                    + "default_time_to_live int,"
                                                                    + "compaction_strategy_class text,"
                                                                    + "compression_parameters text,"
                                                                    + "value_alias text,"
                                                                    + "column_aliases text,"
                                                                    + "compaction_strategy_options text,"
                                                                    + "speculative_retry text,"
                                                                    + "index_interval int,"
                                                                    + "min_index_interval int,"
                                                                    + "max_index_interval int,"
                                                                    + "dropped_columns map<text, bigint>,"
                                                                    + "PRIMARY KEY (keyspace_name, columnfamily_name)"
                                                                    + ") WITH COMMENT='ColumnFamily definitions' AND gc_grace_seconds=604800");

    public static final CFMetaData SchemaColumnsCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_COLUMNS_CF + " ("
                                                             + "keyspace_name text,"
                                                             + "columnfamily_name text,"
                                                             + "column_name text,"
                                                             + "validator text,"
                                                             + "index_type text,"
                                                             + "index_options text,"
                                                             + "index_name text,"
                                                             + "component_index int,"
                                                             + "type text,"
                                                             + "PRIMARY KEY(keyspace_name, columnfamily_name, column_name)"
                                                             + ") WITH COMMENT='ColumnFamily column attributes' AND gc_grace_seconds=604800");

    public static final CFMetaData SchemaTriggersCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_TRIGGERS_CF + " ("
                                                              + "keyspace_name text,"
                                                              + "columnfamily_name text,"
                                                              + "trigger_name text,"
                                                              + "trigger_options map<text, text>,"
                                                              + "PRIMARY KEY (keyspace_name, columnfamily_name, trigger_name)"
                                                              + ") WITH COMMENT='triggers metadata table' AND gc_grace_seconds=604800");

    public static final CFMetaData SchemaUserTypesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_USER_TYPES_CF + " ("
                                                               + "keyspace_name text,"
                                                               + "type_name text,"
                                                               + "field_names list<text>,"
                                                               + "field_types list<text>,"
                                                               + "PRIMARY KEY (keyspace_name, type_name)"
                                                               + ") WITH COMMENT='Defined user types' AND gc_grace_seconds=604800");

    public static final CFMetaData HintsCf = compile("CREATE TABLE " + SystemKeyspace.HINTS_CF + " ("
                                                     + "target_id uuid,"
                                                     + "hint_id timeuuid,"
                                                     + "message_version int,"
                                                     + "mutation blob,"
                                                     + "PRIMARY KEY (target_id, hint_id, message_version)"
                                                     + ") WITH COMPACT STORAGE "
                                                     + "AND COMPACTION={'class' : 'SizeTieredCompactionStrategy', 'enabled' : false} "
                                                     + "AND COMMENT='hints awaiting delivery'"
                                                     + "AND gc_grace_seconds=0");

    public static final CFMetaData PeersCf = compile("CREATE TABLE " + SystemKeyspace.PEERS_CF + " ("
                                                     + "peer inet PRIMARY KEY,"
                                                     + "host_id uuid,"
                                                     + "tokens set<varchar>,"
                                                     + "schema_version uuid,"
                                                     + "release_version text,"
                                                     + "rpc_address inet,"
                                                     + "preferred_ip inet,"
                                                     + "data_center text,"
                                                     + "rack text"
                                                     + ") WITH COMMENT='known peers in the cluster'");

    public static final CFMetaData PeerEventsCf = compile("CREATE TABLE " + SystemKeyspace.PEER_EVENTS_CF + " ("
                                                          + "peer inet PRIMARY KEY,"
                                                          + "hints_dropped map<uuid, int>"
                                                          + ") WITH COMMENT='cf contains events related to peers'");

    public static final CFMetaData LocalCf = compile("CREATE TABLE " + SystemKeyspace.LOCAL_CF + " ("
                                                     + "key text PRIMARY KEY,"
                                                     + "tokens set<varchar>,"
                                                     + "cluster_name text,"
                                                     + "gossip_generation int,"
                                                     + "bootstrapped text,"
                                                     + "host_id uuid,"
                                                     + "release_version text,"
                                                     + "thrift_version text,"
                                                     + "cql_version text,"
                                                     + "native_protocol_version text,"
                                                     + "data_center text,"
                                                     + "rack text,"
                                                     + "partitioner text,"
                                                     + "schema_version uuid,"
                                                     + "truncated_at map<uuid, blob>,"
                                                     + "rpc_address inet,"
                                                     + "broadcast_address inet"
                                                     + ") WITH COMMENT='information about the local node'");

    public static final CFMetaData TraceSessionsCf = compile("CREATE TABLE " + Tracing.SESSIONS_CF + " ("
                                                             + "session_id uuid PRIMARY KEY,"
                                                             + "coordinator inet,"
                                                             + "request text,"
                                                             + "started_at timestamp,"
                                                             + "parameters map<text, text>,"
                                                             + "duration int"
                                                             + ") WITH COMMENT='traced sessions' AND default_time_to_live=86400",
                                                             Tracing.TRACE_KS);

    public static final CFMetaData TraceEventsCf = compile("CREATE TABLE " + Tracing.EVENTS_CF + " ("
                                                           + "session_id uuid,"
                                                           + "event_id timeuuid,"
                                                           + "source inet,"
                                                           + "thread text,"
                                                           + "activity text,"
                                                           + "source_elapsed int,"
                                                           + "PRIMARY KEY (session_id, event_id)"
                                                           + ") WITH default_time_to_live=86400",
                                                           Tracing.TRACE_KS);

    public static final CFMetaData BatchlogCf = compile("CREATE TABLE " + SystemKeyspace.BATCHLOG_CF + " ("
                                                        + "id uuid PRIMARY KEY,"
                                                        + "written_at timestamp,"
                                                        + "data blob,"
                                                        + "version int,"
                                                        + ") WITH COMMENT='uncommited batches' AND gc_grace_seconds=0 "
                                                        + "AND COMPACTION={'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 2}");

    public static final CFMetaData RangeXfersCf = compile("CREATE TABLE " + SystemKeyspace.RANGE_XFERS_CF + " ("
                                                          + "token_bytes blob PRIMARY KEY,"
                                                          + "requested_at timestamp"
                                                          + ") WITH COMMENT='ranges requested for transfer here'");

    public static final CFMetaData CompactionLogCf = compile("CREATE TABLE " + SystemKeyspace.COMPACTION_LOG + " ("
                                                             + "id uuid PRIMARY KEY,"
                                                             + "keyspace_name text,"
                                                             + "columnfamily_name text,"
                                                             + "inputs set<int>"
                                                             + ") WITH COMMENT='unfinished compactions'");

    public static final CFMetaData PaxosCf = compile("CREATE TABLE " + SystemKeyspace.PAXOS_CF + " ("
                                                     + "row_key blob,"
                                                     + "cf_id UUID,"
                                                     + "in_progress_ballot timeuuid,"
                                                     + "proposal_ballot timeuuid,"
                                                     + "proposal blob,"
                                                     + "most_recent_commit_at timeuuid,"
                                                     + "most_recent_commit blob,"
                                                     + "PRIMARY KEY (row_key, cf_id)"
                                                     + ") WITH COMMENT='in-progress paxos proposals' "
                                                     + "AND COMPACTION={'class' : 'LeveledCompactionStrategy'}");

    public static final CFMetaData SSTableActivityCF = compile("CREATE TABLE " + SystemKeyspace.SSTABLE_ACTIVITY_CF + " ("
                                                               + "keyspace_name text,"
                                                               + "columnfamily_name text,"
                                                               + "generation int,"
                                                               + "rate_15m double,"
                                                               + "rate_120m double,"
                                                               + "PRIMARY KEY ((keyspace_name, columnfamily_name, generation))"
                                                               + ") WITH COMMENT='historic sstable read rates'");

    public static final CFMetaData CompactionHistoryCf = compile("CREATE TABLE " + SystemKeyspace.COMPACTION_HISTORY_CF + " ("
                                                                 + "id uuid,"
                                                                 + "keyspace_name text,"
                                                                 + "columnfamily_name text,"
                                                                 + "compacted_at timestamp,"
                                                                 + "bytes_in bigint,"
                                                                 + "bytes_out bigint,"
                                                                 + "rows_merged map<int, bigint>,"
                                                                 + "PRIMARY KEY (id)"
                                                                 + ") WITH COMMENT='show all compaction history' AND DEFAULT_TIME_TO_LIVE=604800");

    public static final CFMetaData SizeEstimatesCf = compile("CREATE TABLE " + SystemKeyspace.SIZE_ESTIMATES_CF + " ("
                                                             + "keyspace_name text,"
                                                             + "table_name text,"
                                                             + "range_start text,"
                                                             + "range_end text,"
                                                             + "mean_partition_size bigint,"
                                                             + "partitions_count bigint,"
                                                             + "PRIMARY KEY ((keyspace_name), table_name, range_start, range_end)"
                                                             + ") WITH COMMENT='per-table primary range size estimates' "
                                                             + "AND gc_grace_seconds=0");


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
                        throw new ConfigurationException("PERCENTILE should be between 0 and 100");
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
    public final ColumnFamilyType cfType;             // standard, super
    public volatile CellNameType comparator;          // bytes, long, timeuuid, utf8, etc.

    //OPTIONAL
    private volatile String comment = "";
    private volatile double readRepairChance = DEFAULT_READ_REPAIR_CHANCE;
    private volatile double dcLocalReadRepairChance = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
    private volatile int gcGraceSeconds = DEFAULT_GC_GRACE_SECONDS;
    private volatile AbstractType<?> defaultValidator = BytesType.instance;
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
    private volatile Map<ColumnIdentifier, Long> droppedColumns = new HashMap<>();
    private volatile Map<String, TriggerDefinition> triggers = new HashMap<>();
    private volatile boolean isPurged = false;
    /*
     * All CQL3 columns definition are stored in the columnMetadata map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and 2) for the partition key and
     * clustering key ones, those list are ordered by the "component index" of the
     * elements.
     */
    public static final String DEFAULT_KEY_ALIAS = "key";
    public static final String DEFAULT_COLUMN_ALIAS = "column";
    public static final String DEFAULT_VALUE_ALIAS = "value";

    // We call dense a CF for which each component of the comparator is a clustering column, i.e. no
    // component is used to store a regular column names. In other words, non-composite static "thrift"
    // and CQL3 CF are *not* dense.
    private volatile Boolean isDense; // null means "we don't know and need to infer from other data"

    private volatile Map<ByteBuffer, ColumnDefinition> columnMetadata = new HashMap<>();
    private volatile List<ColumnDefinition> partitionKeyColumns;  // Always of size keyValidator.componentsCount, null padded if necessary
    private volatile List<ColumnDefinition> clusteringColumns;    // Of size comparator.componentsCount or comparator.componentsCount -1, null padded if necessary
    private volatile SortedSet<ColumnDefinition> regularColumns;  // We use a sorted set so iteration is of predictable order (for SELECT for instance)
    private volatile SortedSet<ColumnDefinition> staticColumns;   // Same as above
    private volatile ColumnDefinition compactValueColumn;

    public volatile Class<? extends AbstractCompactionStrategy> compactionStrategyClass = DEFAULT_COMPACTION_STRATEGY_CLASS;
    public volatile Map<String, String> compactionStrategyOptions = new HashMap<>();

    public volatile CompressionParameters compressionParameters = new CompressionParameters(null);

    // attribute setters that return the modified CFMetaData instance
    public CFMetaData comment(String prop) { comment = Strings.nullToEmpty(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dcLocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType<?> prop) {defaultValidator = prop; return this;}
    public CFMetaData keyValidator(AbstractType<?> prop) {keyValidator = prop; return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(Double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(CachingOptions prop) {caching = prop; return this;}
    public CFMetaData minIndexInterval(int prop) {minIndexInterval = prop; return this;}
    public CFMetaData maxIndexInterval(int prop) {maxIndexInterval = prop; return this;}
    public CFMetaData memtableFlushPeriod(int prop) {memtableFlushPeriod = prop; return this;}
    public CFMetaData defaultTimeToLive(int prop) {defaultTimeToLive = prop; return this;}
    public CFMetaData speculativeRetry(SpeculativeRetry prop) {speculativeRetry = prop; return this;}
    public CFMetaData droppedColumns(Map<ColumnIdentifier, Long> cols) {droppedColumns = cols; return this;}
    public CFMetaData triggers(Map<String, TriggerDefinition> prop) {triggers = prop; return this;}
    public CFMetaData isDense(Boolean prop) {isDense = prop; return this;}
    /**
     * Create new ColumnFamily metadata with generated random ID.
     * When loading from existing schema, use CFMetaData
     *
     * @param keyspace keyspace name
     * @param name column family name
     * @param comp default comparator
     */
    public CFMetaData(String keyspace, String name, ColumnFamilyType type, CellNameType comp)
    {
        this(keyspace, name, type, comp, UUIDGen.getTimeUUID());
    }

    private CFMetaData(String keyspace, String name, ColumnFamilyType type, CellNameType comp, UUID id)
    {
        cfId = id;
        ksName = keyspace;
        cfName = name;
        cfType = type;
        comparator = comp;
    }

    public static CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp, AbstractType<?> subcc)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(makeRawAbstractType(comp, subcc), true);
        return new CFMetaData(keyspace, name, subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super, cellNameType);
    }

    public static CFMetaData sparseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        CellNameType cellNameType = CellNames.fromAbstractType(comp, false);
        return new CFMetaData(keyspace, name, ColumnFamilyType.Standard, cellNameType);
    }

    public static CFMetaData denseCFMetaData(String keyspace, String name, AbstractType<?> comp)
    {
        return denseCFMetaData(keyspace, name, comp, null);
    }

    private static AbstractType<?> makeRawAbstractType(AbstractType<?> comparator, AbstractType<?> subComparator)
    {
        return subComparator == null ? comparator : CompositeType.getInstance(Arrays.asList(comparator, subComparator));
    }

    public Map<String, TriggerDefinition> getTriggers()
    {
        return triggers;
    }

    private static CFMetaData compile(String cql)
    {
        return compile(cql, Keyspace.SYSTEM_KS);
    }

    @VisibleForTesting
    public static CFMetaData compile(String cql, String keyspace)
    {
        try
        {
            CFStatement parsed = (CFStatement)QueryProcessor.parseStatement(cql);
            parsed.prepareKeyspace(keyspace);
            CreateTableStatement statement = (CreateTableStatement) parsed.prepare().statement;
            CFMetaData cfm = newSystemMetadata(keyspace, statement.columnFamily(), "", statement.comparator);
            statement.applyPropertiesTo(cfm);
            return cfm.rebuild();
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
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

    private static CFMetaData newSystemMetadata(String keyspace, String cfName, String comment, CellNameType comparator)
    {
        return new CFMetaData(keyspace, cfName, ColumnFamilyType.Standard, comparator, generateLegacyCfId(keyspace, cfName))
                             .comment(comment)
                             .readRepairChance(0)
                             .dcLocalReadRepairChance(0)
                             .gcGraceSeconds(0)
                             .memtableFlushPeriod(3600 * 1000);
    }

    /**
     * Creates CFMetaData for secondary index CF.
     * Secondary index CF has the same CF ID as parent's.
     *
     * @param parent Parent CF where secondary index is created
     * @param info Column definition containing secondary index definition
     * @param indexComparator Comparator for secondary index
     * @return CFMetaData for secondary index
     */
    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, CellNameType indexComparator)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        CachingOptions indexCaching = parent.getCaching().keyCache.isEnabled()
                             ? CachingOptions.KEYS_ONLY
                             : CachingOptions.NONE;

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, indexComparator, parent.cfId)
                             .keyValidator(info.type)
                             .readRepairChance(0.0)
                             .dcLocalReadRepairChance(0.0)
                             .gcGraceSeconds(0)
                             .caching(indexCaching)
                             .speculativeRetry(parent.speculativeRetry)
                             .compactionStrategyClass(parent.compactionStrategyClass)
                             .compactionStrategyOptions(parent.compactionStrategyOptions)
                             .reloadSecondaryIndexMetadata(parent)
                             .rebuild();
    }

    public CFMetaData reloadSecondaryIndexMetadata(CFMetaData parent)
    {
        minCompactionThreshold(parent.minCompactionThreshold);
        maxCompactionThreshold(parent.maxCompactionThreshold);
        compactionStrategyClass(parent.compactionStrategyClass);
        compactionStrategyOptions(parent.compactionStrategyOptions);
        compressionParameters(parent.compressionParameters);
        return this;
    }

    public CFMetaData copy()
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, cfId), this);
    }

    /**
     * Clones the CFMetaData, but sets a different cfId
     *
     * @param newCfId the cfId for the cloned CFMetaData
     * @return the cloned CFMetaData instance with the new cfId
     */
    public CFMetaData copy(UUID newCfId)
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, newCfId), this);
    }

    static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        List<ColumnDefinition> clonedColumns = new ArrayList<>(oldCFMD.allColumns().size());
        for (ColumnDefinition cd : oldCFMD.allColumns())
            clonedColumns.add(cd.copy());

        return newCFMD.addAllColumnDefinitions(clonedColumns)
                      .comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .keyValidator(oldCFMD.keyValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(new HashMap<>(oldCFMD.compactionStrategyOptions))
                      .compressionParameters(oldCFMD.compressionParameters.copy())
                      .bloomFilterFpChance(oldCFMD.bloomFilterFpChance)
                      .caching(oldCFMD.caching)
                      .defaultTimeToLive(oldCFMD.defaultTimeToLive)
                      .minIndexInterval(oldCFMD.minIndexInterval)
                      .maxIndexInterval(oldCFMD.maxIndexInterval)
                      .speculativeRetry(oldCFMD.speculativeRetry)
                      .memtableFlushPeriod(oldCFMD.memtableFlushPeriod)
                      .droppedColumns(new HashMap<>(oldCFMD.droppedColumns))
                      .triggers(new HashMap<>(oldCFMD.triggers))
                      .isDense(oldCFMD.isDense)
                      .rebuild();
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

    public boolean isSuper()
    {
        return cfType == ColumnFamilyType.Super;
    }

    /**
     * The '.' char is the only way to identify if the CFMetadata is for a secondary index
     */
    public boolean isSecondaryIndex()
    {
        return cfName.contains(".");
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

    public double getDcLocalReadRepair()
    {
        return dcLocalReadRepairChance;
    }

    public ReadRepairDecision newReadRepairDecision()
    {
        double chance = ThreadLocalRandom.current().nextDouble();
        if (getReadRepairChance() > chance)
            return ReadRepairDecision.GLOBAL;

        if (getDcLocalReadRepair() > chance)
            return ReadRepairDecision.DC_LOCAL;

        return ReadRepairDecision.NONE;
    }

    public int getGcGraceSeconds()
    {
        return gcGraceSeconds;
    }

    public AbstractType<?> getDefaultValidator()
    {
        return defaultValidator;
    }

    public AbstractType<?> getKeyValidator()
    {
        return keyValidator;
    }

    public Integer getMinCompactionThreshold()
    {
        return minCompactionThreshold;
    }

    public Integer getMaxCompactionThreshold()
    {
        return maxCompactionThreshold;
    }

    // Used by CQL2 only.
    public String getCQL2KeyName()
    {
        if (partitionKeyColumns.size() > 1)
            throw new IllegalStateException("Cannot acces column family with composite key from CQL < 3.0.0");

        // For compatibility sake, we uppercase if it's the default alias as we used to return it that way in resultsets.
        String str = partitionKeyColumns.get(0).name.toString();
        return str.equalsIgnoreCase(DEFAULT_KEY_ALIAS) ? str.toUpperCase() : str;
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
    public Iterator<ColumnDefinition> allColumnsInSelectOrder()
    {
        return new AbstractIterator<ColumnDefinition>()
        {
            private final Iterator<ColumnDefinition> partitionKeyIter = partitionKeyColumns.iterator();
            private final Iterator<ColumnDefinition> clusteringIter = clusteringColumns.iterator();
            private boolean valueDone;
            private final Iterator<ColumnDefinition> staticIter = staticColumns.iterator();
            private final Iterator<ColumnDefinition> regularIter = regularColumns.iterator();

            protected ColumnDefinition computeNext()
            {
                if (partitionKeyIter.hasNext())
                    return partitionKeyIter.next();

                if (clusteringIter.hasNext())
                    return clusteringIter.next();

                if (staticIter.hasNext())
                    return staticIter.next();

                if (compactValueColumn != null && !valueDone)
                {
                    valueDone = true;
                    // If the compactValueColumn is empty, this means we have a dense table but
                    // with only a PK. As far as selects are concerned, we should ignore the value.
                    if (compactValueColumn.name.bytes.hasRemaining())
                        return compactValueColumn;
                }

                if (regularIter.hasNext())
                    return regularIter.next();

                return endOfData();
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

    public Set<ColumnDefinition> regularColumns()
    {
        return regularColumns;
    }

    public Set<ColumnDefinition> staticColumns()
    {
        return staticColumns;
    }

    public Iterable<ColumnDefinition> regularAndStaticColumns()
    {
        return Iterables.concat(staticColumns, regularColumns);
    }

    public ColumnDefinition compactValueColumn()
    {
        return compactValueColumn;
    }

    // TODO: we could use CType for key validation too to make this unnecessary but
    // it's unclear it would be a win overall
    public CType getKeyValidatorAsCType()
    {
        return keyValidator instanceof CompositeType
             ? new CompoundCType(((CompositeType) keyValidator).types)
             : new SimpleCType(keyValidator);
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

    public Map<ColumnIdentifier, Long> getDroppedColumns()
    {
        return droppedColumns;
    }

    public Boolean getIsDense()
    {
        return isDense;
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
            && Objects.equal(ksName, other.ksName)
            && Objects.equal(cfName, other.cfName)
            && Objects.equal(cfType, other.cfType)
            && Objects.equal(comparator, other.comparator)
            && Objects.equal(comment, other.comment)
            && Objects.equal(readRepairChance, other.readRepairChance)
            && Objects.equal(dcLocalReadRepairChance, other.dcLocalReadRepairChance)
            && Objects.equal(gcGraceSeconds, other.gcGraceSeconds)
            && Objects.equal(defaultValidator, other.defaultValidator)
            && Objects.equal(keyValidator, other.keyValidator)
            && Objects.equal(minCompactionThreshold, other.minCompactionThreshold)
            && Objects.equal(maxCompactionThreshold, other.maxCompactionThreshold)
            && Objects.equal(columnMetadata, other.columnMetadata)
            && Objects.equal(compactionStrategyClass, other.compactionStrategyClass)
            && Objects.equal(compactionStrategyOptions, other.compactionStrategyOptions)
            && Objects.equal(compressionParameters, other.compressionParameters)
            && Objects.equal(bloomFilterFpChance, other.bloomFilterFpChance)
            && Objects.equal(memtableFlushPeriod, other.memtableFlushPeriod)
            && Objects.equal(caching, other.caching)
            && Objects.equal(defaultTimeToLive, other.defaultTimeToLive)
            && Objects.equal(minIndexInterval, other.minIndexInterval)
            && Objects.equal(maxIndexInterval, other.maxIndexInterval)
            && Objects.equal(speculativeRetry, other.speculativeRetry)
            && Objects.equal(droppedColumns, other.droppedColumns)
            && Objects.equal(triggers, other.triggers)
            && Objects.equal(isDense, other.isDense);
    }

    @Override
    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(cfId)
            .append(ksName)
            .append(cfName)
            .append(cfType)
            .append(comparator)
            .append(comment)
            .append(readRepairChance)
            .append(dcLocalReadRepairChance)
            .append(gcGraceSeconds)
            .append(defaultValidator)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(columnMetadata)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .append(bloomFilterFpChance)
            .append(memtableFlushPeriod)
            .append(caching)
            .append(defaultTimeToLive)
            .append(minIndexInterval)
            .append(maxIndexInterval)
            .append(speculativeRetry)
            .append(droppedColumns)
            .append(triggers)
            .append(isDense)
            .toHashCode();
    }

    public AbstractType<?> getValueValidator(CellName cellName)
    {
        ColumnDefinition def = getColumnDefinition(cellName);
        return def == null ? defaultValidator : def.type;
    }

    /** applies implicit defaults to cf definition. useful in updates */
    private static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def)
    {
        if (!cf_def.isSetComment())
            cf_def.setComment("");
        if (!cf_def.isSetMin_compaction_threshold())
            cf_def.setMin_compaction_threshold(CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD);
        if (!cf_def.isSetMax_compaction_threshold())
            cf_def.setMax_compaction_threshold(CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD);
        if (cf_def.compaction_strategy == null)
            cf_def.compaction_strategy = DEFAULT_COMPACTION_STRATEGY_CLASS.getSimpleName();
        if (cf_def.compaction_strategy_options == null)
            cf_def.compaction_strategy_options = Collections.emptyMap();
        if (!cf_def.isSetCompression_options())
        {
            cf_def.setCompression_options(new HashMap<String, String>()
            {{
                if (DEFAULT_COMPRESSOR != null)
                    put(CompressionParameters.SSTABLE_COMPRESSION, DEFAULT_COMPRESSOR);
            }});
        }
        if (!cf_def.isSetDefault_time_to_live())
            cf_def.setDefault_time_to_live(CFMetaData.DEFAULT_DEFAULT_TIME_TO_LIVE);
        if (!cf_def.isSetDclocal_read_repair_chance())
            cf_def.setDclocal_read_repair_chance(CFMetaData.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE);

        // if index_interval was set, use that for the min_index_interval default
        if (!cf_def.isSetMin_index_interval())
        {
            if (cf_def.isSetIndex_interval())
                cf_def.setMin_index_interval(cf_def.getIndex_interval());
            else
                cf_def.setMin_index_interval(CFMetaData.DEFAULT_MIN_INDEX_INTERVAL);
        }
        if (!cf_def.isSetMax_index_interval())
        {
            // ensure the max is at least as large as the min
            cf_def.setMax_index_interval(Math.max(cf_def.min_index_interval, CFMetaData.DEFAULT_MAX_INDEX_INTERVAL));
        }
    }

    public static CFMetaData fromThrift(CfDef cf_def) throws InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, Collections.<ColumnDefinition>emptyList());
    }

    public static CFMetaData fromThriftForUpdate(CfDef cf_def, CFMetaData toUpdate) throws InvalidRequestException, ConfigurationException
    {
        return internalFromThrift(cf_def, toUpdate.allColumns());
    }

    // Convert a thrift CfDef, given a list of ColumnDefinitions to copy over to the created CFMetadata before the CQL metadata are rebuild
    private static CFMetaData internalFromThrift(CfDef cf_def, Collection<ColumnDefinition> previousCQLMetadata) throws InvalidRequestException, ConfigurationException
    {
        ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
        if (cfType == null)
            throw new InvalidRequestException("Invalid column type " + cf_def.column_type);

        applyImplicitDefaults(cf_def);

        try
        {
            AbstractType<?> rawComparator = TypeParser.parse(cf_def.comparator_type);
            AbstractType<?> subComparator = cfType == ColumnFamilyType.Standard
                                          ? null
                                          : cf_def.subcomparator_type == null ? BytesType.instance : TypeParser.parse(cf_def.subcomparator_type);

            AbstractType<?> fullRawComparator = makeRawAbstractType(rawComparator, subComparator);

            AbstractType<?> keyValidator = cf_def.isSetKey_validation_class() ? TypeParser.parse(cf_def.key_validation_class) : null;

            // Convert the REGULAR definitions from the input CfDef
            List<ColumnDefinition> defs = ColumnDefinition.fromThrift(cf_def.keyspace, cf_def.name, rawComparator, subComparator, cf_def.column_metadata);

            // Add the keyAlias if there is one, since that's on CQL metadata that thrift can actually change (for
            // historical reasons)
            boolean hasKeyAlias = cf_def.isSetKey_alias() && keyValidator != null && !(keyValidator instanceof CompositeType);
            if (hasKeyAlias)
                defs.add(ColumnDefinition.partitionKeyDef(cf_def.keyspace, cf_def.name, cf_def.key_alias, keyValidator, null));

            // Now add any CQL metadata that we want to copy, skipping the keyAlias if there was one
            for (ColumnDefinition def : previousCQLMetadata)
            {
                // isPartOfCellName basically means 'is not just a CQL metadata'
                if (def.isPartOfCellName())
                    continue;

                if (def.kind == ColumnDefinition.Kind.PARTITION_KEY && hasKeyAlias)
                    continue;

                defs.add(def);
            }

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, calculateIsDense(fullRawComparator, defs));

            UUID cfId = Schema.instance.getId(cf_def.keyspace, cf_def.name);
            if (cfId == null)
                cfId = UUIDGen.getTimeUUID();

            CFMetaData newCFMD = new CFMetaData(cf_def.keyspace, cf_def.name, cfType, comparator, cfId);

            newCFMD.addAllColumnDefinitions(defs);

            if (keyValidator != null)
                newCFMD.keyValidator(keyValidator);
            if (cf_def.isSetGc_grace_seconds())
                newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds);
            if (cf_def.isSetMin_compaction_threshold())
                newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold);
            if (cf_def.isSetMax_compaction_threshold())
                newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold);
            if (cf_def.isSetCompaction_strategy())
                newCFMD.compactionStrategyClass(createCompactionStrategy(cf_def.compaction_strategy));
            if (cf_def.isSetCompaction_strategy_options())
                newCFMD.compactionStrategyOptions(new HashMap<>(cf_def.compaction_strategy_options));
            if (cf_def.isSetBloom_filter_fp_chance())
                newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
            if (cf_def.isSetMemtable_flush_period_in_ms())
                newCFMD.memtableFlushPeriod(cf_def.memtable_flush_period_in_ms);
            if (cf_def.isSetCaching() || cf_def.isSetCells_per_row_to_cache())
                newCFMD.caching(CachingOptions.fromThrift(cf_def.caching, cf_def.cells_per_row_to_cache));
            if (cf_def.isSetRead_repair_chance())
                newCFMD.readRepairChance(cf_def.read_repair_chance);
            if (cf_def.isSetDefault_time_to_live())
                newCFMD.defaultTimeToLive(cf_def.default_time_to_live);
            if (cf_def.isSetDclocal_read_repair_chance())
                newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);
            if (cf_def.isSetMin_index_interval())
                newCFMD.minIndexInterval(cf_def.min_index_interval);
            if (cf_def.isSetMax_index_interval())
                newCFMD.maxIndexInterval(cf_def.max_index_interval);
            if (cf_def.isSetSpeculative_retry())
                newCFMD.speculativeRetry(SpeculativeRetry.fromString(cf_def.speculative_retry));
            if (cf_def.isSetTriggers())
                newCFMD.triggers(TriggerDefinition.fromThrift(cf_def.triggers));

            return newCFMD.comment(cf_def.comment)
                          .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                          .compressionParameters(CompressionParameters.create(cf_def.compression_options))
                          .rebuild();
        }
        catch (SyntaxException | MarshalException e)
        {
            throw new ConfigurationException(e.getMessage());
        }
    }

    /**
     * Create CFMetaData from thrift {@link CqlRow} that contains columns from schema_columnfamilies.
     *
     * @param columnsRes CqlRow containing columns from schema_columnfamilies.
     * @return CFMetaData derived from CqlRow
     */
    public static CFMetaData fromThriftCqlRow(CqlRow cf, CqlResult columnsRes)
    {
        UntypedResultSet.Row cfRow = new UntypedResultSet.Row(convertThriftCqlRow(cf));

        List<Map<String, ByteBuffer>> cols = new ArrayList<>(columnsRes.rows.size());
        for (CqlRow row : columnsRes.rows)
            cols.add(convertThriftCqlRow(row));
        UntypedResultSet colsRow = UntypedResultSet.create(cols);

        return fromSchemaNoTriggers(cfRow, colsRow);
    }

    private static Map<String, ByteBuffer> convertThriftCqlRow(CqlRow row)
    {
        Map<String, ByteBuffer> m = new HashMap<>();
        for (org.apache.cassandra.thrift.Column column : row.getColumns())
            m.put(UTF8Type.instance.getString(column.bufferForName()), column.value);
        return m;
    }

    /**
     * Updates this object in place to match the definition in the system schema tables.
     * @return true if any columns were added, removed, or altered; otherwise, false is returned
     */
    public boolean reload()
    {
        Row cfDefRow = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, ksName, cfName);

        if (cfDefRow.cf == null || !cfDefRow.cf.hasColumns())
            throw new RuntimeException(String.format("%s not found in the schema definitions keyspace.", ksName + ":" + cfName));

        try
        {
            return apply(fromSchema(cfDefRow));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates CFMetaData in-place to match cf_def
     *
     * *Note*: This method left package-private only for DefsTest, don't use directly!
     *
     * @return true if any columns were added, removed, or altered; otherwise, false is returned
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    boolean apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);

        validateCompatility(cfm);

        // TODO: this method should probably return a new CFMetaData so that
        // 1) we can keep comparator final
        // 2) updates are applied atomically
        comparator = cfm.comparator;

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        comment = Strings.nullToEmpty(cfm.comment);
        readRepairChance = cfm.readRepairChance;
        dcLocalReadRepairChance = cfm.dcLocalReadRepairChance;
        gcGraceSeconds = cfm.gcGraceSeconds;
        defaultValidator = cfm.defaultValidator;
        keyValidator = cfm.keyValidator;
        minCompactionThreshold = cfm.minCompactionThreshold;
        maxCompactionThreshold = cfm.maxCompactionThreshold;

        bloomFilterFpChance = cfm.bloomFilterFpChance;
        caching = cfm.caching;
        minIndexInterval = cfm.minIndexInterval;
        maxIndexInterval = cfm.maxIndexInterval;
        memtableFlushPeriod = cfm.memtableFlushPeriod;
        defaultTimeToLive = cfm.defaultTimeToLive;
        speculativeRetry = cfm.speculativeRetry;

        if (!cfm.droppedColumns.isEmpty())
            droppedColumns = cfm.droppedColumns;

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(columnMetadata, cfm.columnMetadata);
        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            removeColumnDefinition(cd);
        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            addColumnDefinition(cd);
        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition oldDef = columnMetadata.get(name);
            ColumnDefinition def = cfm.columnMetadata.get(name);
            addOrReplaceColumnDefinition(oldDef.apply(def));
        }

        compactionStrategyClass = cfm.compactionStrategyClass;
        compactionStrategyOptions = cfm.compactionStrategyOptions;

        compressionParameters = cfm.compressionParameters;

        triggers = cfm.triggers;

        isDense(cfm.isDense);

        rebuild();
        logger.debug("application result is {}", this);

        return !columnDiff.entriesOnlyOnLeft().isEmpty() ||
               !columnDiff.entriesOnlyOnRight().isEmpty() ||
               !columnDiff.entriesDiffering().isEmpty();
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

        if (!cfm.cfType.equals(cfType))
            throw new ConfigurationException("types do not match.");

        if (!cfm.comparator.isCompatibleWith(comparator))
            throw new ConfigurationException("comparators do not match or are not compatible.");
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
            throw new ConfigurationException("Failed to validate compaction options");
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate compaction options");
        }
    }

    public static Class<? extends AbstractCompactionStrategy> createCompactionStrategy(String className) throws ConfigurationException
    {
        className = className.contains(".") ? className : "org.apache.cassandra.db.compaction." + className;
        Class<AbstractCompactionStrategy> strategyClass = FBUtilities.classForName(className, "compaction strategy");
        if (className.equals(WrappingCompactionStrategy.class.getName()))
            throw new ConfigurationException("You can't set WrappingCompactionStrategy as the compaction strategy!");
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

    // converts CFM to thrift CfDef
    public org.apache.cassandra.thrift.CfDef toThrift()
    {
        org.apache.cassandra.thrift.CfDef def = new org.apache.cassandra.thrift.CfDef(ksName, cfName);
        def.setColumn_type(cfType.name());

        if (isSuper())
        {
            def.setComparator_type(comparator.subtype(0).toString());
            def.setSubcomparator_type(comparator.subtype(1).toString());
        }
        else
        {
            def.setComparator_type(comparator.toString());
        }

        def.setComment(Strings.nullToEmpty(comment));
        def.setRead_repair_chance(readRepairChance);
        def.setDclocal_read_repair_chance(dcLocalReadRepairChance);
        def.setGc_grace_seconds(gcGraceSeconds);
        def.setDefault_validation_class(defaultValidator == null ? null : defaultValidator.toString());
        def.setKey_validation_class(keyValidator.toString());
        def.setMin_compaction_threshold(minCompactionThreshold);
        def.setMax_compaction_threshold(maxCompactionThreshold);
        // We only return the alias if only one is set since thrift don't know about multiple key aliases
        if (partitionKeyColumns.size() == 1)
            def.setKey_alias(partitionKeyColumns.get(0).name.bytes);
        def.setColumn_metadata(ColumnDefinition.toThrift(columnMetadata));
        def.setCompaction_strategy(compactionStrategyClass.getName());
        def.setCompaction_strategy_options(new HashMap<>(compactionStrategyOptions));
        def.setCompression_options(compressionParameters.asThriftOptions());
        if (bloomFilterFpChance != null)
            def.setBloom_filter_fp_chance(bloomFilterFpChance);
        def.setMin_index_interval(minIndexInterval);
        def.setMax_index_interval(maxIndexInterval);
        def.setMemtable_flush_period_in_ms(memtableFlushPeriod);
        def.setCaching(caching.toThriftCaching());
        def.setCells_per_row_to_cache(caching.toThriftCellsPerRow());
        def.setDefault_time_to_live(defaultTimeToLive);
        def.setSpeculative_retry(speculativeRetry.toString());
        def.setTriggers(TriggerDefinition.toThrift(triggers));

        return def;
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

    /**
     * Returns a ColumnDefinition given a cell name.
     */
    public ColumnDefinition getColumnDefinition(CellName cellName)
    {
        ColumnIdentifier id = cellName.cql3ColumnName(this);
        ColumnDefinition def = id == null
                             ? getColumnDefinition(cellName.toByteBuffer())  // Means a dense layout, try the full column name
                             : getColumnDefinition(id);

        // It's possible that the def is a PRIMARY KEY or COMPACT_VALUE one in case a concrete cell
        // name conflicts with a CQL column name, which can happen in 2 cases:
        // 1) because the user inserted a cell through Thrift that conflicts with a default "alias" used
        //    by CQL for thrift tables (see #6892).
        // 2) for COMPACT STORAGE tables with a single utf8 clustering column, the cell name can be anything,
        //    including a CQL column name (without this being a problem).
        // In any case, this is fine, this just mean that columnDefinition is not the ColumnDefinition we are
        // looking for.
        return def != null && def.isPartOfCellName() ? def : null;
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

    public Iterator<OnDiskAtom> getOnDiskIterator(DataInput in, Descriptor.Version version)
    {
        return getOnDiskIterator(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    }

    public Iterator<OnDiskAtom> getOnDiskIterator(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version)
    {
        return AbstractCell.onDiskIterator(in, flag, expireBefore, version, comparator);
    }

    public AtomDeserializer getOnDiskDeserializer(DataInput in, Descriptor.Version version)
    {
        return new AtomDeserializer(comparator, in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
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

        if (cfType == null)
            throw new ConfigurationException(String.format("Invalid column family type for %s", cfName));

        for (int i = 0; i < comparator.size(); i++)
        {
            if (comparator.subtype(i) instanceof CounterColumnType)
                throw new ConfigurationException("CounterColumnType is not a valid comparator");
        }
        if (keyValidator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid key validator");

        // Mixing counter with non counter columns is not supported (#2614)
        if (defaultValidator instanceof CounterColumnType)
        {
            for (ColumnDefinition def : regularAndStaticColumns())
                if (!(def.type instanceof CounterColumnType))
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
                if (cfType == ColumnFamilyType.Super)
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

    /**
     * Create schema mutations to update this metadata to provided new state.
     *
     * @param newState The new metadata (for the same CF)
     * @param modificationTimestamp Timestamp to use for mutation
     * @param fromThrift whether the newState comes from thrift
     *
     * @return Difference between attributes in form of schema mutation
     */
    public Mutation toSchemaUpdate(CFMetaData newState, long modificationTimestamp, boolean fromThrift)
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));

        newState.toSchemaNoColumnsNoTriggers(mutation, modificationTimestamp);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(columnMetadata, newState.columnMetadata);

        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (fromThrift && cd.kind != ColumnDefinition.Kind.REGULAR)
                continue;

            cd.deleteFromSchema(mutation, modificationTimestamp);
        }

        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            cd.toSchema(mutation, modificationTimestamp);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition cd = newState.columnMetadata.get(name);
            cd.toSchema(mutation, modificationTimestamp);
        }

        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(triggers, newState.triggers);

        // dropped triggers
        for (TriggerDefinition td : triggerDiff.entriesOnlyOnLeft().values())
            td.deleteFromSchema(mutation, cfName, modificationTimestamp);

        // newly created triggers
        for (TriggerDefinition td : triggerDiff.entriesOnlyOnRight().values())
            td.toSchema(mutation, cfName, modificationTimestamp);

        return mutation;
    }

    /**
     * Remove all CF attributes from schema
     *
     * @param timestamp Timestamp to use
     *
     * @return Mutation to use to completely remove cf from schema
     */
    public Mutation dropFromSchema(long timestamp)
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));
        ColumnFamily cf = mutation.addOrGet(SchemaColumnFamiliesCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        Composite prefix = SchemaColumnFamiliesCf.comparator.make(cfName);
        cf.addAtom(new RangeTombstone(prefix, prefix.end(), timestamp, ldt));

        for (ColumnDefinition cd : allColumns())
            cd.deleteFromSchema(mutation, timestamp);

        for (TriggerDefinition td : triggers.values())
            td.deleteFromSchema(mutation, cfName, timestamp);

        for (String indexName : Keyspace.open(this.ksName).getColumnFamilyStore(this.cfName).getBuiltIndexes())
        {
            ColumnFamily indexCf = mutation.addOrGet(IndexCf);
            indexCf.addTombstone(indexCf.getComparator().makeCellName(indexName), ldt, timestamp);
        }

        return mutation;
    }

    public boolean isPurged()
    {
        return isPurged;
    }

    void markPurged()
    {
        isPurged = true;
    }

    public void toSchema(Mutation mutation, long timestamp)
    {
        toSchemaNoColumnsNoTriggers(mutation, timestamp);

        for (TriggerDefinition td : triggers.values())
            td.toSchema(mutation, cfName, timestamp);

        for (ColumnDefinition cd : allColumns())
            cd.toSchema(mutation, timestamp);
    }

    private void toSchemaNoColumnsNoTriggers(Mutation mutation, long timestamp)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        ColumnFamily cf = mutation.addOrGet(SchemaColumnFamiliesCf);
        Composite prefix = SchemaColumnFamiliesCf.comparator.make(cfName);
        CFRowAdder adder = new CFRowAdder(cf, prefix, timestamp);

        adder.add("cf_id", cfId);
        adder.add("type", cfType.toString());

        if (isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            adder.add("comparator", comparator.subtype(0).toString());
            adder.add("subcomparator", comparator.subtype(1).toString());
        }
        else
        {
            adder.add("comparator", comparator.toString());
        }

        adder.add("comment", comment);
        adder.add("read_repair_chance", readRepairChance);
        adder.add("local_read_repair_chance", dcLocalReadRepairChance);
        adder.add("gc_grace_seconds", gcGraceSeconds);
        adder.add("default_validator", defaultValidator.toString());
        adder.add("key_validator", keyValidator.toString());
        adder.add("min_compaction_threshold", minCompactionThreshold);
        adder.add("max_compaction_threshold", maxCompactionThreshold);
        adder.add("bloom_filter_fp_chance", bloomFilterFpChance);

        adder.add("memtable_flush_period_in_ms", memtableFlushPeriod);
        adder.add("caching", caching.toString());
        adder.add("default_time_to_live", defaultTimeToLive);
        adder.add("compaction_strategy_class", compactionStrategyClass.getName());
        adder.add("compression_parameters", json(compressionParameters.asThriftOptions()));
        adder.add("compaction_strategy_options", json(compactionStrategyOptions));
        adder.add("min_index_interval", minIndexInterval);
        adder.add("max_index_interval", maxIndexInterval);
        adder.add("index_interval", null);
        adder.add("speculative_retry", speculativeRetry.toString());

        for (Map.Entry<ColumnIdentifier, Long> entry : droppedColumns.entrySet())
            adder.addMapEntry("dropped_columns", entry.getKey().toString(), entry.getValue());

        adder.add("is_dense", isDense);

        // Save the CQL3 metadata "the old way" for compatibility sake
        adder.add("key_aliases", aliasesToJson(partitionKeyColumns));
        adder.add("column_aliases", aliasesToJson(clusteringColumns));
        adder.add("value_alias", compactValueColumn == null ? null : compactValueColumn.name.toString());
    }

    // Package protected for use by tests
    static CFMetaData fromSchemaNoTriggers(UntypedResultSet.Row result, UntypedResultSet serializedColumnDefinitions)
    {
        try
        {
            String ksName = result.getString("keyspace_name");
            String cfName = result.getString("columnfamily_name");

            AbstractType<?> rawComparator = TypeParser.parse(result.getString("comparator"));
            AbstractType<?> subComparator = result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null;
            ColumnFamilyType cfType = ColumnFamilyType.valueOf(result.getString("type"));

            AbstractType<?> fullRawComparator = makeRawAbstractType(rawComparator, subComparator);

            List<ColumnDefinition> columnDefs = ColumnDefinition.fromSchema(serializedColumnDefinitions,
                                                                            ksName,
                                                                            cfName,
                                                                            fullRawComparator,
                                                                            cfType == ColumnFamilyType.Super);

            boolean isDense = result.has("is_dense")
                            ? result.getBoolean("is_dense")
                            : calculateIsDense(fullRawComparator, columnDefs);

            CellNameType comparator = CellNames.fromAbstractType(fullRawComparator, isDense);

            // if we are upgrading, we use id generated from names initially
            UUID cfId = result.has("cf_id")
                      ? result.getUUID("cf_id")
                      : generateLegacyCfId(ksName, cfName);

            CFMetaData cfm = new CFMetaData(ksName, cfName, cfType, comparator, cfId);
            cfm.isDense(isDense);

            cfm.readRepairChance(result.getDouble("read_repair_chance"));
            cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
            cfm.gcGraceSeconds(result.getInt("gc_grace_seconds"));
            cfm.defaultValidator(TypeParser.parse(result.getString("default_validator")));
            cfm.keyValidator(TypeParser.parse(result.getString("key_validator")));
            cfm.minCompactionThreshold(result.getInt("min_compaction_threshold"));
            cfm.maxCompactionThreshold(result.getInt("max_compaction_threshold"));
            if (result.has("comment"))
                cfm.comment(result.getString("comment"));
            if (result.has("bloom_filter_fp_chance"))
                cfm.bloomFilterFpChance(result.getDouble("bloom_filter_fp_chance"));
            if (result.has("memtable_flush_period_in_ms"))
                cfm.memtableFlushPeriod(result.getInt("memtable_flush_period_in_ms"));
            cfm.caching(CachingOptions.fromString(result.getString("caching")));
            if (result.has("default_time_to_live"))
                cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
            if (result.has("speculative_retry"))
                cfm.speculativeRetry(SpeculativeRetry.fromString(result.getString("speculative_retry")));
            cfm.compactionStrategyClass(createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
            cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));

            // migrate old index_interval values to min_index_interval, if present
            if (result.has("min_index_interval"))
                cfm.minIndexInterval(result.getInt("min_index_interval"));
            else if (result.has("index_interval"))
                cfm.minIndexInterval(result.getInt("index_interval"));
            if (result.has("max_index_interval"))
                cfm.maxIndexInterval(result.getInt("max_index_interval"));

            /*
             * The info previously hold by key_aliases, column_aliases and value_alias is now stored in columnMetadata (because 1) this
             * make more sense and 2) this allow to store indexing information).
             * However, for upgrade sake we need to still be able to read those old values. Moreover, we cannot easily
             * remove those old columns once "converted" to columnMetadata because that would screw up nodes that may
             * not have upgraded. So for now we keep the both info and in sync, even though its redundant.
             */
            if (result.has("key_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(fromJsonList(result.getString("key_aliases"))), cfm.keyValidator, ColumnDefinition.Kind.PARTITION_KEY);
            if (result.has("column_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(fromJsonList(result.getString("column_aliases"))), cfm.comparator.asAbstractType(), ColumnDefinition.Kind.CLUSTERING_COLUMN);
            if (result.has("value_alias"))
                cfm.addColumnMetadataFromAliases(Collections.singletonList(result.getBytes("value_alias")), cfm.defaultValidator, ColumnDefinition.Kind.COMPACT_VALUE);

            if (result.has("dropped_columns"))
                cfm.droppedColumns(convertDroppedColumns(result.getMap("dropped_columns", UTF8Type.instance, LongType.instance)));

            for (ColumnDefinition cd : columnDefs)
                cfm.addOrReplaceColumnDefinition(cd);

            return cfm.rebuild();
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void addColumnMetadataFromAliases(List<ByteBuffer> aliases, AbstractType<?> comparator, ColumnDefinition.Kind kind)
    {
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)comparator;
            for (int i = 0; i < aliases.size(); ++i)
            {
                if (aliases.get(i) != null)
                {
                    addOrReplaceColumnDefinition(new ColumnDefinition(this, aliases.get(i), ct.types.get(i), i, kind));
                }
            }
        }
        else
        {
            assert aliases.size() <= 1;
            if (!aliases.isEmpty() && aliases.get(0) != null)
                addOrReplaceColumnDefinition(new ColumnDefinition(this, aliases.get(0), comparator, null, kind));
        }
    }

    /**
     * Deserialize CF metadata from low-level representation
     *
     * @return Thrift-based metadata deserialized from schema
     */
    public static CFMetaData fromSchema(UntypedResultSet.Row result)
    {
        String ksName = result.getString("keyspace_name");
        String cfName = result.getString("columnfamily_name");

        Row serializedColumns = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_COLUMNS_CF, ksName, cfName);
        CFMetaData cfm = fromSchemaNoTriggers(result, ColumnDefinition.resultify(serializedColumns));

        Row serializedTriggers = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_TRIGGERS_CF, ksName, cfName);
        addTriggerDefinitionsFromSchema(cfm, serializedTriggers);

        return cfm;
    }

    private static CFMetaData fromSchema(Row row)
    {
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", row).one();
        return fromSchema(result);
    }

    private String aliasesToJson(List<ColumnDefinition> rawAliases)
    {
        if (rawAliases == null)
            return null;

        List<String> aliases = new ArrayList<>(rawAliases.size());
        for (ColumnDefinition rawAlias : rawAliases)
            aliases.add(rawAlias.name.toString());
        return json(aliases);
    }

    private static List<ByteBuffer> aliasesFromStrings(List<String> aliases)
    {
        List<ByteBuffer> rawAliases = new ArrayList<>(aliases.size());
        for (String alias : aliases)
            rawAliases.add(UTF8Type.instance.decompose(alias));
        return rawAliases;
    }

    private static Map<ColumnIdentifier, Long> convertDroppedColumns(Map<String, Long> raw)
    {
        Map<ColumnIdentifier, Long> converted = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : raw.entrySet())
            converted.put(new ColumnIdentifier(entry.getKey(), true), entry.getValue());
        return converted;
    }

    /**
     * Convert current metadata into schema mutation
     *
     * @param timestamp Timestamp to use
     *
     * @return Low-level representation of the CF
     *
     * @throws ConfigurationException if any of the attributes didn't pass validation
     */
    public Mutation toSchema(long timestamp) throws ConfigurationException
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));
        toSchema(mutation, timestamp);
        return mutation;
    }

    // The comparator to validate the definition name.

    public AbstractType<?> getColumnDefinitionComparator(ColumnDefinition def)
    {
        return getComponentComparator(def.isOnAllComponents() ? null : def.position(), def.kind);
    }

    public AbstractType<?> getComponentComparator(Integer componentIndex, ColumnDefinition.Kind kind)
    {
        switch (kind)
        {
            case REGULAR:
                if (componentIndex == null)
                    return comparator.asAbstractType();

                AbstractType<?> t = comparator.subtype(componentIndex);
                assert t != null : "Non-sensical component index";
                return t;
            default:
                // CQL3 column names are UTF8
                return UTF8Type.instance;
        }
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
        if (def.kind == ColumnDefinition.Kind.REGULAR)
            comparator.addCQL3Column(def.name);
        columnMetadata.put(def.name.bytes, def);
        return this;
    }

    public boolean removeColumnDefinition(ColumnDefinition def)
    {
        if (def.kind == ColumnDefinition.Kind.REGULAR)
            comparator.removeCQL3Column(def.name);
        return columnMetadata.remove(def.name.bytes) != null;
    }

    private static void addTriggerDefinitionsFromSchema(CFMetaData cfDef, Row serializedTriggerDefinitions)
    {
        for (TriggerDefinition td : TriggerDefinition.fromSchema(serializedTriggerDefinitions))
            cfDef.triggers.put(td.name, td);
    }

    public void addTriggerDefinition(TriggerDefinition def) throws InvalidRequestException
    {
        if (containsTriggerDefinition(def))
            throw new InvalidRequestException(
                String.format("Cannot create trigger %s, a trigger with the same name already exists", def.name));
        triggers.put(def.name, def);
    }

    public boolean containsTriggerDefinition(TriggerDefinition def)
    {
        return triggers.containsKey(def.name);
    }

    public boolean removeTrigger(String name)
    {
        return triggers.remove(name) != null;
    }

    public void recordColumnDrop(ColumnDefinition def)
    {
        assert !def.isOnAllComponents();
        droppedColumns.put(def.name, FBUtilities.timestampMicros());
    }

    public void renameColumn(ColumnIdentifier from, ColumnIdentifier to) throws InvalidRequestException
    {
        ColumnDefinition def = getColumnDefinition(from);
        if (def == null)
            throw new InvalidRequestException(String.format("Cannot rename unknown column %s in keyspace %s", from, cfName));

        if (getColumnDefinition(to) != null)
            throw new InvalidRequestException(String.format("Cannot rename column %s to %s in keyspace %s; another column of that name already exist", from, to, cfName));

        if (def.isPartOfCellName())
        {
            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
        }
        else if (def.isIndexed())
        {
            throw new InvalidRequestException(String.format("Cannot rename column %s because it is secondary indexed", from));
        }

        ColumnDefinition newDef = def.withNewName(to);
        // don't call addColumnDefinition/removeColumnDefition because we want to avoid recomputing
        // the CQL3 cfDef between those two operation
        addOrReplaceColumnDefinition(newDef);
        removeColumnDefinition(def);
    }

    public CFMetaData rebuild()
    {
        if (isDense == null)
            isDense(calculateIsDense(comparator.asAbstractType(), allColumns()));

        List<ColumnDefinition> pkCols = nullInitializedList(keyValidator.componentsCount());
        List<ColumnDefinition> ckCols = nullInitializedList(comparator.clusteringPrefixSize());
        // We keep things sorted to get consistent/predictable order in select queries
        SortedSet<ColumnDefinition> regCols = new TreeSet<>(regularColumnComparator);
        SortedSet<ColumnDefinition> statCols = new TreeSet<>(regularColumnComparator);
        ColumnDefinition compactCol = null;

        for (ColumnDefinition def : allColumns())
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    assert !(def.isOnAllComponents() && keyValidator instanceof CompositeType);
                    pkCols.set(def.position(), def);
                    break;
                case CLUSTERING_COLUMN:
                    assert !(def.isOnAllComponents() && comparator.isCompound());
                    ckCols.set(def.position(), def);
                    break;
                case REGULAR:
                    regCols.add(def);
                    break;
                case STATIC:
                    statCols.add(def);
                    break;
                case COMPACT_VALUE:
                    assert compactCol == null : "There shouldn't be more than one compact value defined: got " + compactCol + " and " + def;
                    compactCol = def;
                    break;
            }
        }

        // Now actually assign the correct value. This is not atomic, but then again, updating CFMetaData is never atomic anyway.
        partitionKeyColumns = addDefaultKeyAliases(pkCols);
        clusteringColumns = addDefaultColumnAliases(ckCols);
        regularColumns = regCols;
        staticColumns = statCols;
        compactValueColumn = addDefaultValueAlias(compactCol);
        return this;
    }

    private List<ColumnDefinition> addDefaultKeyAliases(List<ColumnDefinition> pkCols)
    {
        for (int i = 0; i < pkCols.size(); i++)
        {
            if (pkCols.get(i) == null)
            {
                Integer idx = null;
                AbstractType<?> type = keyValidator;
                if (keyValidator instanceof CompositeType)
                {
                    idx = i;
                    type = ((CompositeType)keyValidator).types.get(i);
                }
                // For compatibility sake, we call the first alias 'key' rather than 'key1'. This
                // is inconsistent with column alias, but it's probably not worth risking breaking compatibility now.
                ByteBuffer name = ByteBufferUtil.bytes(i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1));
                ColumnDefinition newDef = ColumnDefinition.partitionKeyDef(this, name, type, idx);
                addOrReplaceColumnDefinition(newDef);
                pkCols.set(i, newDef);
            }
        }
        return pkCols;
    }

    private List<ColumnDefinition> addDefaultColumnAliases(List<ColumnDefinition> ckCols)
    {
        for (int i = 0; i < ckCols.size(); i++)
        {
            if (ckCols.get(i) == null)
            {
                Integer idx;
                AbstractType<?> type;
                if (comparator.isCompound())
                {
                    idx = i;
                    type = comparator.subtype(i);
                }
                else
                {
                    idx = null;
                    type = comparator.asAbstractType();
                }
                ByteBuffer name = ByteBufferUtil.bytes(DEFAULT_COLUMN_ALIAS + (i + 1));
                ColumnDefinition newDef = ColumnDefinition.clusteringKeyDef(this, name, type, idx);
                addOrReplaceColumnDefinition(newDef);
                ckCols.set(i, newDef);
            }
        }
        return ckCols;
    }

    private ColumnDefinition addDefaultValueAlias(ColumnDefinition compactValueDef)
    {
        if (comparator.isDense())
        {
            if (compactValueDef != null)
                return compactValueDef;

            ColumnDefinition newDef = ColumnDefinition.compactValueDef(this, ByteBufferUtil.bytes(DEFAULT_VALUE_ALIAS), defaultValidator);
            addOrReplaceColumnDefinition(newDef);
            return newDef;
        }
        else
        {
            assert compactValueDef == null;
            return null;
        }
    }

    /*
     * We call dense a CF for which each component of the comparator is a clustering column, i.e. no
     * component is used to store a regular column names. In other words, non-composite static "thrift"
     * and CQL3 CF are *not* dense.
     * We save whether the table is dense or not during table creation through CQL, but we don't have this
     * information for table just created through thrift, nor for table prior to CASSANDRA-7744, so this
     * method does its best to infer whether the table is dense or not based on other elements.
     */
    private static boolean calculateIsDense(AbstractType<?> comparator, Collection<ColumnDefinition> defs)
    {
        /*
         * As said above, this method is only here because we need to deal with thrift upgrades.
         * Once a CF has been "upgraded", i.e. we've rebuilt and save its CQL3 metadata at least once,
         * then we'll have saved the "is_dense" value and will be good to go.
         *
         * But non-upgraded thrift CF (and pre-7744 CF) will have no value for "is_dense", so we need
         * to infer that information without relying on it in that case. And for the most part this is
         * easy, a CF that has at least one REGULAR definition is not dense. But the subtlety is that not
         * having a REGULAR definition may not mean dense because of CQL3 definitions that have only the
         * PRIMARY KEY defined.
         *
         * So we need to recognize those special case CQL3 table with only a primary key. If we have some
         * clustering columns, we're fine as said above. So the only problem is that we cannot decide for
         * sure if a CF without REGULAR columns nor CLUSTERING_COLUMN definition is meant to be dense, or if it
         * has been created in CQL3 by say:
         *    CREATE TABLE test (k int PRIMARY KEY)
         * in which case it should not be dense. However, we can limit our margin of error by assuming we are
         * in the latter case only if the comparator is exactly CompositeType(UTF8Type).
         */
        boolean hasRegular = false;
        int maxClusteringIdx = -1;
        for (ColumnDefinition def : defs)
        {
            switch (def.kind)
            {
                case CLUSTERING_COLUMN:
                    maxClusteringIdx = Math.max(maxClusteringIdx, def.position());
                    break;
                case REGULAR:
                    hasRegular = true;
                    break;
            }
        }

        return maxClusteringIdx >= 0
             ? maxClusteringIdx == comparator.componentsCount() - 1
             : !hasRegular && !isCQL3OnlyPKComparator(comparator);
    }

    private static boolean isCQL3OnlyPKComparator(AbstractType<?> comparator)
    {
        if (!(comparator instanceof CompositeType))
            return false;

        CompositeType ct = (CompositeType)comparator;
        return ct.types.size() == 1 && ct.types.get(0) instanceof UTF8Type;
    }

    public boolean isCQL3Table()
    {
        return !isSuper() && !comparator.isDense() && comparator.isCompound();
    }

    private static <T> List<T> nullInitializedList(int size)
    {
        List<T> l = new ArrayList<>(size);
        for (int i = 0; i < size; ++i)
            l.add(null);
        return l;
    }

    /**
     * Returns whether this CFMetaData can be returned to thrift.
     */
    public boolean isThriftCompatible()
    {
        // Super CF are always "thrift compatible". But since they may have defs with a componentIndex != null,
        // we have to special case here.
        if (isSuper())
            return true;

        for (ColumnDefinition def : allColumns())
        {
            // Non-REGULAR ColumnDefinition are not "thrift compatible" per-se, but it's ok because they hold metadata
            // this is only of use to CQL3, so we will just skip them in toThrift.
            if (def.kind == ColumnDefinition.Kind.REGULAR && !def.isThriftCompatible())
                return false;
        }

        // The table might also have no REGULAR columns (be PK-only), but still be "thrift incompatible". See #7832.
        if (isCQL3OnlyPKComparator(comparator.asAbstractType()) && !isDense)
            return false;

        return true;
    }

    public boolean isCounter()
    {
        return defaultValidator.isCounter();
    }

    public boolean hasStaticColumns()
    {
        return !staticColumns.isEmpty();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
            .append("cfId", cfId)
            .append("ksName", ksName)
            .append("cfName", cfName)
            .append("cfType", cfType)
            .append("comparator", comparator)
            .append("comment", comment)
            .append("readRepairChance", readRepairChance)
            .append("dcLocalReadRepairChance", dcLocalReadRepairChance)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("columnMetadata", columnMetadata.values())
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionParameters", compressionParameters.asThriftOptions())
            .append("bloomFilterFpChance", bloomFilterFpChance)
            .append("memtableFlushPeriod", memtableFlushPeriod)
            .append("caching", caching)
            .append("defaultTimeToLive", defaultTimeToLive)
            .append("minIndexInterval", minIndexInterval)
            .append("maxIndexInterval", maxIndexInterval)
            .append("speculativeRetry", speculativeRetry)
            .append("droppedColumns", droppedColumns)
            .append("triggers", triggers.values())
            .append("isDense", isDense)
            .toString();
    }
}
