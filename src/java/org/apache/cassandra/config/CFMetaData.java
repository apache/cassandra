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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.FBUtilities.*;

public final class CFMetaData
{
    //
    // !! Important !!
    // This class can be tricky to modify.  Please read http://wiki.apache.org/cassandra/ConfigurationNotes
    // for how to do so safely.
    //

    private static final Logger logger = LoggerFactory.getLogger(CFMetaData.class);

    public final static double DEFAULT_READ_REPAIR_CHANCE = 0.1;
    public final static double DEFAULT_DCLOCAL_READ_REPAIR_CHANCE = 0.0;
    public final static boolean DEFAULT_REPLICATE_ON_WRITE = true;
    public final static int DEFAULT_GC_GRACE_SECONDS = 864000;
    public final static int DEFAULT_MIN_COMPACTION_THRESHOLD = 4;
    public final static int DEFAULT_MAX_COMPACTION_THRESHOLD = 32;
    public final static Class<? extends AbstractCompactionStrategy> DEFAULT_COMPACTION_STRATEGY_CLASS = SizeTieredCompactionStrategy.class;
    public final static Caching DEFAULT_CACHING_STRATEGY = Caching.KEYS_ONLY;
    public final static int DEFAULT_DEFAULT_TIME_TO_LIVE = 0;
    public final static SpeculativeRetry DEFAULT_SPECULATIVE_RETRY = new SpeculativeRetry(SpeculativeRetry.RetryType.PERCENTILE, 0.99);
    public final static int DEFAULT_INDEX_INTERVAL = 128;
    public final static boolean DEFAULT_POPULATE_IO_CACHE_ON_FLUSH = false;

    // Note that this is the default only for user created tables
    public final static String DEFAULT_COMPRESSOR = LZ4Compressor.class.getCanonicalName();

    public static final CFMetaData IndexCf = compile("CREATE TABLE \"" + SystemKeyspace.INDEX_CF + "\" ("
                                                     + "table_name text,"
                                                     + "index_name text,"
                                                     + "PRIMARY KEY (table_name, index_name)"
                                                     + ") WITH COMPACT STORAGE AND COMMENT='indexes that have been completed'");

    public static final CFMetaData CounterIdCf = compile("CREATE TABLE \"" + SystemKeyspace.COUNTER_ID_CF + "\" ("
                                                         + "key text,"
                                                         + "id timeuuid,"
                                                         + "PRIMARY KEY (key, id)"
                                                         + ") WITH COMPACT STORAGE AND COMMENT='counter node IDs'");

    public static final CFMetaData SchemaKeyspacesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_KEYSPACES_CF + " ("
                                                               + "keyspace_name text PRIMARY KEY,"
                                                               + "durable_writes boolean,"
                                                               + "strategy_class text,"
                                                               + "strategy_options text"
                                                               + ") WITH COMPACT STORAGE AND COMMENT='keyspace definitions' AND gc_grace_seconds=8640");

    public static final CFMetaData SchemaColumnFamiliesCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF + " ("
                                                                    + "keyspace_name text,"
                                                                    + "columnfamily_name text,"
                                                                    + "type text,"
                                                                    + "comparator text,"
                                                                    + "subcomparator text,"
                                                                    + "comment text,"
                                                                    + "read_repair_chance double,"
                                                                    + "local_read_repair_chance double,"
                                                                    + "replicate_on_write boolean,"
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
                                                                    + "populate_io_cache_on_flush boolean,"
                                                                    + "index_interval int,"
                                                                    + "dropped_columns map<text, bigint>,"
                                                                    + "PRIMARY KEY (keyspace_name, columnfamily_name)"
                                                                    + ") WITH COMMENT='ColumnFamily definitions' AND gc_grace_seconds=8640");

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
                                                             + ") WITH COMMENT='ColumnFamily column attributes' AND gc_grace_seconds=8640");

    public static final CFMetaData SchemaTriggersCf = compile("CREATE TABLE " + SystemKeyspace.SCHEMA_TRIGGERS_CF + " ("
                                                              + "keyspace_name text,"
                                                              + "columnfamily_name text,"
                                                              + "trigger_name text,"
                                                              + "trigger_options map<text, text>,"
                                                              + "PRIMARY KEY (keyspace_name, columnfamily_name, trigger_name)"
                                                              + ") WITH COMMENT='triggers metadata table'");

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
                                                     + "truncated_at map<uuid, blob>"
                                                     + ") WITH COMMENT='information about the local node'");

    public static final CFMetaData TraceSessionsCf = compile("CREATE TABLE " + Tracing.SESSIONS_CF + " ("
                                                             + "session_id uuid PRIMARY KEY,"
                                                             + "coordinator inet,"
                                                             + "request text,"
                                                             + "started_at timestamp,"
                                                             + "parameters map<text, text>,"
                                                             + "duration int"
                                                             + ") WITH COMMENT='traced sessions'",
                                                             Tracing.TRACE_KS);

    public static final CFMetaData TraceEventsCf = compile("CREATE TABLE " + Tracing.EVENTS_CF + " ("
                                                           + "session_id uuid,"
                                                           + "event_id timeuuid,"
                                                           + "source inet,"
                                                           + "thread text,"
                                                           + "activity text,"
                                                           + "source_elapsed int,"
                                                           + "PRIMARY KEY (session_id, event_id)"
                                                           + ")",
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
                                                     + ") WITH COMMENT='in-progress paxos proposals'");

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

    public enum Caching
    {
        ALL, KEYS_ONLY, ROWS_ONLY, NONE;

        public static Caching fromString(String cache) throws ConfigurationException
        {
            try
            {
                return valueOf(cache.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(String.format("%s not found, available types: %s.", cache, StringUtils.join(values(), ", ")));
            }
        }
    }

    public static class SpeculativeRetry
    {
        public enum RetryType
        {
            NONE, CUSTOM, PERCENTILE, ALWAYS;
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
            if (! (obj instanceof SpeculativeRetry))
                return false;
            SpeculativeRetry rhs = (SpeculativeRetry) obj;
            return Objects.equal(type, rhs.type) && Objects.equal(value, rhs.value);
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
    public volatile AbstractType<?> comparator;       // bytes, long, timeuuid, utf8, etc.

    //OPTIONAL
    private volatile String comment = "";
    private volatile double readRepairChance = DEFAULT_READ_REPAIR_CHANCE;
    private volatile double dcLocalReadRepairChance = DEFAULT_DCLOCAL_READ_REPAIR_CHANCE;
    private volatile boolean replicateOnWrite = DEFAULT_REPLICATE_ON_WRITE;
    private volatile int gcGraceSeconds = DEFAULT_GC_GRACE_SECONDS;
    private volatile AbstractType<?> defaultValidator = BytesType.instance;
    private volatile AbstractType<?> keyValidator = BytesType.instance;
    private volatile int minCompactionThreshold = DEFAULT_MIN_COMPACTION_THRESHOLD;
    private volatile int maxCompactionThreshold = DEFAULT_MAX_COMPACTION_THRESHOLD;
    private volatile Double bloomFilterFpChance = null;
    private volatile Caching caching = DEFAULT_CACHING_STRATEGY;
    private volatile int indexInterval = DEFAULT_INDEX_INTERVAL;
    private int memtableFlushPeriod = 0;
    private volatile int defaultTimeToLive = DEFAULT_DEFAULT_TIME_TO_LIVE;
    private volatile SpeculativeRetry speculativeRetry = DEFAULT_SPECULATIVE_RETRY;
    private volatile boolean populateIoCacheOnFlush = DEFAULT_POPULATE_IO_CACHE_ON_FLUSH;
    private volatile Map<ByteBuffer, Long> droppedColumns = new HashMap<>();
    private volatile Map<String, TriggerDefinition> triggers = new HashMap<>();

    /*
     * All CQL3 columns definition are stored in the column_metadata map.
     * On top of that, we keep separated collection of each kind of definition, to
     * 1) allow easy access to each kind and 2) for the partition key and
     * clustering key ones, those list are ordered by the "component index" of the
     * elements.
     */
    public static final String DEFAULT_KEY_ALIAS = "key";
    public static final String DEFAULT_COLUMN_ALIAS = "column";
    public static final String DEFAULT_VALUE_ALIAS = "value";

    private volatile Map<ByteBuffer, ColumnDefinition> column_metadata = new HashMap<>();
    private volatile List<ColumnDefinition> partitionKeyColumns;  // Always of size keyValidator.componentsCount, null padded if necessary
    private volatile List<ColumnDefinition> clusteringKeyColumns; // Of size comparator.componentsCount or comparator.componentsCount -1, null padded if necessary
    private volatile Set<ColumnDefinition> regularColumns;
    private volatile Set<ColumnDefinition> staticColumns;
    private volatile ColumnDefinition compactValueColumn;

    public volatile Class<? extends AbstractCompactionStrategy> compactionStrategyClass = DEFAULT_COMPACTION_STRATEGY_CLASS;
    public volatile Map<String, String> compactionStrategyOptions = new HashMap<>();

    public volatile CompressionParameters compressionParameters = new CompressionParameters(null);

    // Processed infos used by CQL. This can be fully reconstructed from the CFMedata,
    // so it's not saved on disk. It is however costlyish to recreate for each query
    // so we cache it here (and update on each relevant CFMetadata change)
    private volatile CFDefinition cqlCfDef;

    public CFMetaData comment(String prop) { comment = enforceCommentNotNull(prop); return this;}
    public CFMetaData readRepairChance(double prop) {readRepairChance = prop; return this;}
    public CFMetaData dcLocalReadRepairChance(double prop) {dcLocalReadRepairChance = prop; return this;}
    public CFMetaData replicateOnWrite(boolean prop) {replicateOnWrite = prop; return this;}
    public CFMetaData gcGraceSeconds(int prop) {gcGraceSeconds = prop; return this;}
    public CFMetaData defaultValidator(AbstractType<?> prop) {defaultValidator = prop; return this;}
    public CFMetaData keyValidator(AbstractType<?> prop) {keyValidator = prop; return this;}
    public CFMetaData minCompactionThreshold(int prop) {minCompactionThreshold = prop; return this;}
    public CFMetaData maxCompactionThreshold(int prop) {maxCompactionThreshold = prop; return this;}
    public CFMetaData columnMetadata(Map<ByteBuffer,ColumnDefinition> prop) {column_metadata = prop; return this;}
    public CFMetaData compactionStrategyClass(Class<? extends AbstractCompactionStrategy> prop) {compactionStrategyClass = prop; return this;}
    public CFMetaData compactionStrategyOptions(Map<String, String> prop) {compactionStrategyOptions = prop; return this;}
    public CFMetaData compressionParameters(CompressionParameters prop) {compressionParameters = prop; return this;}
    public CFMetaData bloomFilterFpChance(Double prop) {bloomFilterFpChance = prop; return this;}
    public CFMetaData caching(Caching prop) {caching = prop; return this;}
    public CFMetaData indexInterval(int prop) {indexInterval = prop; return this;}
    public CFMetaData memtableFlushPeriod(int prop) {memtableFlushPeriod = prop; return this;}
    public CFMetaData defaultTimeToLive(int prop) {defaultTimeToLive = prop; return this;}
    public CFMetaData speculativeRetry(SpeculativeRetry prop) {speculativeRetry = prop; return this;}
    public CFMetaData populateIoCacheOnFlush(boolean prop) {populateIoCacheOnFlush = prop; return this;}
    public CFMetaData droppedColumns(Map<ByteBuffer, Long> cols) {droppedColumns = cols; return this;}
    public CFMetaData triggers(Map<String, TriggerDefinition> prop) {triggers = prop; return this;}

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp, AbstractType<?> subcc)
    {
        this(keyspace, name, type, makeComparator(type, comp, subcc));
    }

    public CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp)
    {
        this(keyspace, name, type, comp, getId(keyspace, name));
    }

    @VisibleForTesting
    CFMetaData(String keyspace, String name, ColumnFamilyType type, AbstractType<?> comp,  UUID id)
    {
        // (subcc may be null for non-supercolumns)
        // (comp may also be null for custom indexes, which is kind of broken if you ask me)

        ksName = keyspace;
        cfName = name;
        cfType = type;
        comparator = comp;
        cfId = id;
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
            CreateTableStatement statement = (CreateTableStatement) QueryProcessor.parseStatement(cql).prepare().statement;
            CFMetaData cfm = newSystemMetadata(keyspace, statement.columnFamily(), "", statement.comparator, null);
            statement.applyPropertiesTo(cfm);
            return cfm.rebuild();
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static AbstractType<?> makeComparator(ColumnFamilyType cftype, AbstractType<?> comp, AbstractType<?> subcc)
    {
        return cftype == ColumnFamilyType.Super
             ? CompositeType.getInstance(comp, subcc == null ? BytesType.instance : subcc)
             : comp;
    }

    private static String enforceCommentNotNull (CharSequence comment)
    {
        return (comment == null) ? "" : comment.toString();
    }

    static UUID getId(String ksName, String cfName)
    {
        return UUID.nameUUIDFromBytes(ArrayUtils.addAll(ksName.getBytes(), cfName.getBytes()));
    }

    private static CFMetaData newSystemMetadata(String keyspace, String cfName, String comment, AbstractType<?> comparator, AbstractType<?> subcc)
    {
        ColumnFamilyType type = subcc == null ? ColumnFamilyType.Standard : ColumnFamilyType.Super;
        CFMetaData newCFMD = new CFMetaData(keyspace, cfName, type, comparator,  subcc);

        return newCFMD.comment(comment)
                .readRepairChance(0)
                .dcLocalReadRepairChance(0)
                .gcGraceSeconds(0)
                .memtableFlushPeriod(3600 * 1000);
    }

    public static CFMetaData newIndexMetadata(CFMetaData parent, ColumnDefinition info, AbstractType<?> columnComparator)
    {
        // Depends on parent's cache setting, turn on its index CF's cache.
        // Row caching is never enabled; see CASSANDRA-5732
        Caching indexCaching = parent.getCaching() == Caching.ALL || parent.getCaching() == Caching.KEYS_ONLY
                             ? Caching.KEYS_ONLY
                             : Caching.NONE;

        return new CFMetaData(parent.ksName, parent.indexColumnFamilyName(info), ColumnFamilyType.Standard, columnComparator, (AbstractType)null)
                             .keyValidator(info.getValidator())
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

    public CFMetaData clone()
    {
        return copyOpts(new CFMetaData(ksName, cfName, cfType, comparator, cfId), this);
    }

    // Create a new CFMD by changing just the cfName
    public static CFMetaData rename(CFMetaData cfm, String newName)
    {
        return copyOpts(new CFMetaData(cfm.ksName, newName, cfm.cfType, cfm.comparator, cfm.cfId), cfm);
    }

    static CFMetaData copyOpts(CFMetaData newCFMD, CFMetaData oldCFMD)
    {
        Map<ByteBuffer, ColumnDefinition> clonedColumns = new HashMap<>();
        for (ColumnDefinition cd : oldCFMD.column_metadata.values())
        {
            ColumnDefinition cloned = cd.clone();
            clonedColumns.put(cloned.name, cloned);
        }

        return newCFMD.comment(oldCFMD.comment)
                      .readRepairChance(oldCFMD.readRepairChance)
                      .dcLocalReadRepairChance(oldCFMD.dcLocalReadRepairChance)
                      .replicateOnWrite(oldCFMD.replicateOnWrite)
                      .gcGraceSeconds(oldCFMD.gcGraceSeconds)
                      .defaultValidator(oldCFMD.defaultValidator)
                      .keyValidator(oldCFMD.keyValidator)
                      .minCompactionThreshold(oldCFMD.minCompactionThreshold)
                      .maxCompactionThreshold(oldCFMD.maxCompactionThreshold)
                      .columnMetadata(clonedColumns)
                      .compactionStrategyClass(oldCFMD.compactionStrategyClass)
                      .compactionStrategyOptions(new HashMap<>(oldCFMD.compactionStrategyOptions))
                      .compressionParameters(oldCFMD.compressionParameters.copy())
                      .bloomFilterFpChance(oldCFMD.bloomFilterFpChance)
                      .caching(oldCFMD.caching)
                      .defaultTimeToLive(oldCFMD.defaultTimeToLive)
                      .indexInterval(oldCFMD.indexInterval)
                      .speculativeRetry(oldCFMD.speculativeRetry)
                      .memtableFlushPeriod(oldCFMD.memtableFlushPeriod)
                      .populateIoCacheOnFlush(oldCFMD.populateIoCacheOnFlush)
                      .droppedColumns(new HashMap<>(oldCFMD.droppedColumns))
                      .triggers(new HashMap<>(oldCFMD.triggers))
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
        return cfName + Directories.SECONDARY_INDEX_NAME_SEPARATOR + (info.getIndexName() == null ? ByteBufferUtil.bytesToHex(info.name) : info.getIndexName());
    }

    public String getComment()
    {
        return comment;
    }

    public boolean isSuper()
    {
        return cfType == ColumnFamilyType.Super;
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
        double chance = FBUtilities.threadLocalRandom().nextDouble();
        if (getReadRepairChance() > chance)
            return ReadRepairDecision.GLOBAL;

        if (getDcLocalReadRepair() > chance)
            return ReadRepairDecision.DC_LOCAL;

        return ReadRepairDecision.NONE;
    }

    public boolean getReplicateOnWrite()
    {
        return replicateOnWrite;
    }

    public boolean populateIoCacheOnFlush()
    {
        return populateIoCacheOnFlush;
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

        try
        {
            // For compatibility sake, we uppercase if it's the default alias as we used to return it that way in resultsets.
            String str = ByteBufferUtil.string(partitionKeyColumns.get(0).name);
            return str.equalsIgnoreCase(DEFAULT_KEY_ALIAS) ? str.toUpperCase() : str;
        }
        catch (CharacterCodingException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public CompressionParameters compressionParameters()
    {
        return compressionParameters;
    }

    public Collection<ColumnDefinition> allColumns()
    {
        return column_metadata.values();
    }

    public List<ColumnDefinition> partitionKeyColumns()
    {
        return partitionKeyColumns;
    }

    public List<ColumnDefinition> clusteringKeyColumns()
    {
        return clusteringKeyColumns;
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

    public double getBloomFilterFpChance()
    {
        // we disallow bFFPC==null starting in 1.2.1 but tolerated it before that
        return (bloomFilterFpChance == null || bloomFilterFpChance == 0)
               ? compactionStrategyClass == LeveledCompactionStrategy.class ? 0.1 : 0.01
               : bloomFilterFpChance;
    }

    public Caching getCaching()
    {
        return caching;
    }

    public int getIndexInterval()
    {
        return indexInterval;
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

    public Map<ByteBuffer, Long> getDroppedColumns()
    {
        return droppedColumns;
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null || obj.getClass() != getClass())
        {
            return false;
        }

        CFMetaData rhs = (CFMetaData) obj;
        return new EqualsBuilder()
            .append(ksName, rhs.ksName)
            .append(cfName, rhs.cfName)
            .append(cfType, rhs.cfType)
            .append(comparator, rhs.comparator)
            .append(comment, rhs.comment)
            .append(readRepairChance, rhs.readRepairChance)
            .append(dcLocalReadRepairChance, rhs.dcLocalReadRepairChance)
            .append(replicateOnWrite, rhs.replicateOnWrite)
            .append(gcGraceSeconds, rhs.gcGraceSeconds)
            .append(defaultValidator, rhs.defaultValidator)
            .append(keyValidator, rhs.keyValidator)
            .append(minCompactionThreshold, rhs.minCompactionThreshold)
            .append(maxCompactionThreshold, rhs.maxCompactionThreshold)
            .append(cfId, rhs.cfId)
            .append(column_metadata, rhs.column_metadata)
            .append(compactionStrategyClass, rhs.compactionStrategyClass)
            .append(compactionStrategyOptions, rhs.compactionStrategyOptions)
            .append(compressionParameters, rhs.compressionParameters)
            .append(bloomFilterFpChance, rhs.bloomFilterFpChance)
            .append(memtableFlushPeriod, rhs.memtableFlushPeriod)
            .append(caching, rhs.caching)
            .append(defaultTimeToLive, rhs.defaultTimeToLive)
            .append(indexInterval, rhs.indexInterval)
            .append(speculativeRetry, rhs.speculativeRetry)
            .append(populateIoCacheOnFlush, rhs.populateIoCacheOnFlush)
            .append(droppedColumns, rhs.droppedColumns)
            .append(triggers, rhs.triggers)
            .isEquals();
    }

    public int hashCode()
    {
        return new HashCodeBuilder(29, 1597)
            .append(ksName)
            .append(cfName)
            .append(cfType)
            .append(comparator)
            .append(comment)
            .append(readRepairChance)
            .append(dcLocalReadRepairChance)
            .append(replicateOnWrite)
            .append(gcGraceSeconds)
            .append(defaultValidator)
            .append(keyValidator)
            .append(minCompactionThreshold)
            .append(maxCompactionThreshold)
            .append(cfId)
            .append(column_metadata)
            .append(compactionStrategyClass)
            .append(compactionStrategyOptions)
            .append(compressionParameters)
            .append(bloomFilterFpChance)
            .append(memtableFlushPeriod)
            .append(caching)
            .append(defaultTimeToLive)
            .append(indexInterval)
            .append(speculativeRetry)
            .append(populateIoCacheOnFlush)
            .append(droppedColumns)
            .append(triggers)
            .toHashCode();
    }

    /**
     * Like getColumnDefinitionFromColumnName, the argument must be an internal column/cell name.
     */
    public AbstractType<?> getValueValidatorFromColumnName(ByteBuffer columnName)
    {
        ColumnDefinition def = getColumnDefinitionFromColumnName(columnName);
        return def == null ? defaultValidator : def.getValidator();
    }

    /** applies implicit defaults to cf definition. useful in updates */
    public static void applyImplicitDefaults(org.apache.cassandra.thrift.CfDef cf_def)
    {
        if (!cf_def.isSetComment())
            cf_def.setComment("");
        if (!cf_def.isSetReplicate_on_write())
            cf_def.setReplicate_on_write(CFMetaData.DEFAULT_REPLICATE_ON_WRITE);
        if (!cf_def.isSetPopulate_io_cache_on_flush())
            cf_def.setPopulate_io_cache_on_flush(CFMetaData.DEFAULT_POPULATE_IO_CACHE_ON_FLUSH);
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
    }

    public static CFMetaData fromThrift(org.apache.cassandra.thrift.CfDef cf_def) throws InvalidRequestException, ConfigurationException
    {
        ColumnFamilyType cfType = ColumnFamilyType.create(cf_def.column_type);
        if (cfType == null)
            throw new InvalidRequestException("Invalid column type " + cf_def.column_type);

        applyImplicitDefaults(cf_def);

        try
        {
            CFMetaData newCFMD = new CFMetaData(cf_def.keyspace,
                                                cf_def.name,
                                                cfType,
                                                TypeParser.parse(cf_def.comparator_type),
                                                cf_def.subcomparator_type == null ? null : TypeParser.parse(cf_def.subcomparator_type));

            if (cf_def.isSetGc_grace_seconds()) { newCFMD.gcGraceSeconds(cf_def.gc_grace_seconds); }
            if (cf_def.isSetMin_compaction_threshold()) { newCFMD.minCompactionThreshold(cf_def.min_compaction_threshold); }
            if (cf_def.isSetMax_compaction_threshold()) { newCFMD.maxCompactionThreshold(cf_def.max_compaction_threshold); }
            if (cf_def.isSetCompaction_strategy())
                newCFMD.compactionStrategyClass = createCompactionStrategy(cf_def.compaction_strategy);
            if (cf_def.isSetCompaction_strategy_options())
                newCFMD.compactionStrategyOptions(new HashMap<>(cf_def.compaction_strategy_options));
            if (cf_def.isSetBloom_filter_fp_chance())
                newCFMD.bloomFilterFpChance(cf_def.bloom_filter_fp_chance);
            if (cf_def.isSetMemtable_flush_period_in_ms())
                newCFMD.memtableFlushPeriod(cf_def.memtable_flush_period_in_ms);
            if (cf_def.isSetCaching())
                newCFMD.caching(Caching.fromString(cf_def.caching));
            if (cf_def.isSetRead_repair_chance())
                newCFMD.readRepairChance(cf_def.read_repair_chance);
            if (cf_def.isSetDefault_time_to_live())
                newCFMD.defaultTimeToLive(cf_def.default_time_to_live);
            if (cf_def.isSetDclocal_read_repair_chance())
                newCFMD.dcLocalReadRepairChance(cf_def.dclocal_read_repair_chance);
            if (cf_def.isSetIndex_interval())
                newCFMD.indexInterval(cf_def.index_interval);
            if (cf_def.isSetSpeculative_retry())
                newCFMD.speculativeRetry(SpeculativeRetry.fromString(cf_def.speculative_retry));
            if (cf_def.isSetPopulate_io_cache_on_flush())
                newCFMD.populateIoCacheOnFlush(cf_def.populate_io_cache_on_flush);
            if (cf_def.isSetTriggers())
                newCFMD.triggers(TriggerDefinition.fromThrift(cf_def.triggers));

            CompressionParameters cp = CompressionParameters.create(cf_def.compression_options);

            if (cf_def.isSetKey_validation_class()) { newCFMD.keyValidator(TypeParser.parse(cf_def.key_validation_class)); }
            if (cf_def.isSetKey_alias() && !(newCFMD.keyValidator instanceof CompositeType))
            {
                newCFMD.column_metadata.put(cf_def.key_alias, ColumnDefinition.partitionKeyDef(cf_def.key_alias, newCFMD.keyValidator, null));
            }

            return newCFMD.comment(cf_def.comment)
                          .replicateOnWrite(cf_def.replicate_on_write)
                          .defaultValidator(TypeParser.parse(cf_def.default_validation_class))
                          .columnMetadata(ColumnDefinition.fromThrift(cf_def.column_metadata, newCFMD.isSuper()))
                          .compressionParameters(cp)
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
     * @param row CqlRow containing columns from schema_columnfamilies.
     * @return CFMetaData derived from CqlRow
     */
    public static CFMetaData fromThriftCqlRow(CqlRow row)
    {
        Map<String, ByteBuffer> columns = new HashMap<>();
        try
        {
            for (org.apache.cassandra.thrift.Column column : row.getColumns())
                columns.put(ByteBufferUtil.string(column.bufferForName()), column.value);
        }
        catch (CharacterCodingException ignore)
        {
        }
        UntypedResultSet.Row cql3row = new UntypedResultSet.Row(columns);
        return fromSchemaNoColumnsNoTriggers(cql3row);
    }

    public void reload()
    {
        Row cfDefRow = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF, ksName, cfName);

        if (cfDefRow.cf == null || cfDefRow.cf.getColumnCount() == 0)
            throw new RuntimeException(String.format("%s not found in the schema definitions keyspace.", ksName + ":" + cfName));

        try
        {
            apply(fromSchema(cfDefRow));
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Updates CFMetaData in-place to match cf_def
     *
     * *Note*: This method left public only for DefsTest, don't use directly!
     *
     * @throws ConfigurationException if ks/cf names or cf ids didn't match
     */
    public void apply(CFMetaData cfm) throws ConfigurationException
    {
        logger.debug("applying {} to {}", cfm, this);

        validateCompatility(cfm);

        // TODO: this method should probably return a new CFMetaData so that
        // 1) we can keep comparator final
        // 2) updates are applied atomically
        comparator = cfm.comparator;

        // compaction thresholds are checked by ThriftValidation. We shouldn't be doing
        // validation on the apply path; it's too late for that.

        comment = enforceCommentNotNull(cfm.comment);
        readRepairChance = cfm.readRepairChance;
        dcLocalReadRepairChance = cfm.dcLocalReadRepairChance;
        replicateOnWrite = cfm.replicateOnWrite;
        gcGraceSeconds = cfm.gcGraceSeconds;
        defaultValidator = cfm.defaultValidator;
        keyValidator = cfm.keyValidator;
        minCompactionThreshold = cfm.minCompactionThreshold;
        maxCompactionThreshold = cfm.maxCompactionThreshold;

        bloomFilterFpChance = cfm.bloomFilterFpChance;
        memtableFlushPeriod = cfm.memtableFlushPeriod;
        caching = cfm.caching;
        defaultTimeToLive = cfm.defaultTimeToLive;
        speculativeRetry = cfm.speculativeRetry;
        populateIoCacheOnFlush = cfm.populateIoCacheOnFlush;

        if (!cfm.droppedColumns.isEmpty())
            droppedColumns = cfm.droppedColumns;

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, cfm.column_metadata);
        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
            column_metadata.remove(cd.name);
        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            column_metadata.put(cd.name, cd);
        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition oldDef = column_metadata.get(name);
            ColumnDefinition def = cfm.column_metadata.get(name);
            oldDef.apply(def, getColumnDefinitionComparator(oldDef));
        }

        compactionStrategyClass = cfm.compactionStrategyClass;
        compactionStrategyOptions = cfm.compactionStrategyOptions;

        compressionParameters = cfm.compressionParameters;

        triggers = cfm.triggers;

        rebuild();
        logger.debug("application result is {}", this);
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

            Method validateMethod = strategyClass.getMethod("validateOptions", Map.class);
            Map<String, String> unknownOptions = (Map<String, String>) validateMethod.invoke(null, options);
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
        if (!AbstractCompactionStrategy.class.isAssignableFrom(strategyClass))
            throw new ConfigurationException(String.format("Specified compaction strategy class (%s) is not derived from AbstractReplicationStrategy", className));

        return strategyClass;
    }

    public AbstractCompactionStrategy createCompactionStrategyInstance(ColumnFamilyStore cfs)
    {
        try
        {
            Constructor<? extends AbstractCompactionStrategy> constructor = compactionStrategyClass.getConstructor(new Class[] {
                ColumnFamilyStore.class,
                Map.class // options
            });
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
            CompositeType ct = (CompositeType)comparator;
            def.setComparator_type(ct.types.get(0).toString());
            def.setSubcomparator_type(ct.types.get(1).toString());
        }
        else
        {
            def.setComparator_type(comparator.toString());
        }

        def.setComment(enforceCommentNotNull(comment));
        def.setRead_repair_chance(readRepairChance);
        def.setDclocal_read_repair_chance(dcLocalReadRepairChance);
        def.setReplicate_on_write(replicateOnWrite);
        def.setPopulate_io_cache_on_flush(populateIoCacheOnFlush);
        def.setGc_grace_seconds(gcGraceSeconds);
        def.setDefault_validation_class(defaultValidator == null ? null : defaultValidator.toString());
        def.setKey_validation_class(keyValidator.toString());
        def.setMin_compaction_threshold(minCompactionThreshold);
        def.setMax_compaction_threshold(maxCompactionThreshold);
        // We only return the alias if only one is set since thrift don't know about multiple key aliases
        if (partitionKeyColumns.size() == 1)
            def.setKey_alias(partitionKeyColumns.get(0).name);
        def.setColumn_metadata(ColumnDefinition.toThrift(column_metadata));
        def.setCompaction_strategy(compactionStrategyClass.getName());
        def.setCompaction_strategy_options(new HashMap<>(compactionStrategyOptions));
        def.setCompression_options(compressionParameters.asThriftOptions());
        if (bloomFilterFpChance != null)
            def.setBloom_filter_fp_chance(bloomFilterFpChance);
        def.setIndex_interval(indexInterval);
        def.setMemtable_flush_period_in_ms(memtableFlushPeriod);
        def.setCaching(caching.toString());
        def.setDefault_time_to_live(defaultTimeToLive);
        def.setSpeculative_retry(speculativeRetry.toString());
        def.setTriggers(TriggerDefinition.toThrift(triggers));

        return def;
    }

    /**
     * Returns the ColumnDefinition for {@code name}.
     *
     * Note that {@code name} correspond to the returned ColumnDefinition name,
     * and in particular for composite cfs, it should usually be only a
     * component of the full column name. If you have a full column name, use
     * getColumnDefinitionFromColumnName instead.
     */
    public ColumnDefinition getColumnDefinition(ByteBuffer name)
    {
            return column_metadata.get(name);
    }

    /**
     * Returns a ColumnDefinition given a full (internal) column name.
     */
    public ColumnDefinition getColumnDefinitionFromColumnName(ByteBuffer columnName)
    {
        if (!isSuper() && (comparator instanceof CompositeType))
        {
            CompositeType composite = (CompositeType)comparator;
            ByteBuffer[] components = composite.split(columnName);
            for (ColumnDefinition def : regularAndStaticColumns())
            {
                ByteBuffer toCompare;
                if (def.componentIndex == null)
                {
                    toCompare = columnName;
                }
                else
                {
                    if (def.componentIndex >= components.length)
                        break;

                    toCompare = components[def.componentIndex];
                }
                if (def.name.equals(toCompare))
                    return def;
            }
            return null;
        }
        else
        {
            ColumnDefinition def = column_metadata.get(columnName);
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
    }

    public ColumnDefinition getColumnDefinitionForIndex(String indexName)
    {
        for (ColumnDefinition def : column_metadata.values())
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

            for (Map.Entry<ByteBuffer, ColumnDefinition> entry : column_metadata.entrySet())
            {
                ColumnDefinition newDef = entry.getValue();

                if (!cfm.column_metadata.containsKey(entry.getKey()) || newDef.getIndexType() == null)
                    continue;

                String oldIndexName = cfm.column_metadata.get(entry.getKey()).getIndexName();

                if (oldIndexName == null)
                    continue;

                if (newDef.getIndexName() != null && !oldIndexName.equals(newDef.getIndexName()))
                    throw new ConfigurationException("Can't modify index name: was '" + oldIndexName + "' changed to '" + newDef.getIndexName() + "'.");

                newDef.setIndexName(oldIndexName);
            }
        }

        Set<String> existingNames = existingIndexNames(null);
        for (ColumnDefinition column : column_metadata.values())
        {
            if (column.getIndexType() != null && column.getIndexName() == null)
            {
                String baseName = getDefaultIndexName(cfName, getColumnDefinitionComparator(column), column.name);
                String indexName = baseName;
                int i = 0;
                while (existingNames.contains(indexName))
                    indexName = baseName + '_' + (++i);
                column.setIndexName(indexName);
            }
        }
    }

    public static String getDefaultIndexName(String cfName, AbstractType<?> comparator, ByteBuffer columnName)
    {
        return (cfName + "_" + comparator.getString(columnName) + "_idx").replaceAll("\\W", "");
    }

    public Iterator<OnDiskAtom> getOnDiskIterator(DataInput in, int count, Descriptor.Version version)
    {
        return getOnDiskIterator(in, count, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
    }

    public Iterator<OnDiskAtom> getOnDiskIterator(DataInput in, int count, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version)
    {
        if (version.hasSuperColumns && cfType == ColumnFamilyType.Super)
            return SuperColumns.onDiskIterator(in, count, flag, expireBefore);
        return Column.onDiskIterator(in, count, flag, expireBefore, version);
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

        if (comparator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid comparator");
        if (keyValidator instanceof CounterColumnType)
            throw new ConfigurationException("CounterColumnType is not a valid key validator");

        // Mixing counter with non counter columns is not supported (#2614)
        if (defaultValidator instanceof CounterColumnType)
        {
            for (ColumnDefinition def : regularAndStaticColumns())
                if (!(def.getValidator() instanceof CounterColumnType))
                    throw new ConfigurationException("Cannot add a non counter column (" + getColumnDefinitionComparator(def).getString(def.name) + ") in a counter column family");
        }
        else
        {
            for (ColumnDefinition def : column_metadata.values())
                if (def.getValidator() instanceof CounterColumnType)
                    throw new ConfigurationException("Cannot add a counter column (" + getColumnDefinitionComparator(def).getString(def.name) + ") in a non counter column family");
        }

        for (ColumnDefinition def : partitionKeyColumns)
            validateAlias(def, "Key");
        for (ColumnDefinition def : clusteringKeyColumns)
            validateAlias(def, "Column");
        if (compactValueColumn != null)
            validateAlias(compactValueColumn, "Value");

        // initialize a set of names NOT in the CF under consideration
        Set<String> indexNames = existingIndexNames(cfName);
        for (ColumnDefinition c : column_metadata.values())
        {
            AbstractType<?> comparator = getColumnDefinitionComparator(c);

            try
            {
                comparator.validate(c.name);
            }
            catch (MarshalException e)
            {
                throw new ConfigurationException(String.format("Column name %s is not valid for comparator %s",
                                                               ByteBufferUtil.bytesToHex(c.name), comparator));
            }

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
                    if (c.getIndexOptions() == null || !c.getIndexOptions().containsKey(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
                        throw new ConfigurationException("Required index option missing: " + SecondaryIndex.CUSTOM_INDEX_OPTION_NAME);
                }

                // This method validates the column metadata but does not intialize the index
                SecondaryIndex.createInstance(null, c);
            }
        }

        validateCompactionThresholds();

        if (bloomFilterFpChance != null && bloomFilterFpChance == 0)
            throw new ConfigurationException("Zero false positives is impossible; bloom filter false positive chance bffpc must be 0 < bffpc <= 1");

        return this;
    }

    private static Set<String> existingIndexNames(String cfToExclude)
    {
        Set<String> indexNames = new HashSet<String>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            if (cfToExclude == null || !cfs.name.equals(cfToExclude))
                for (ColumnDefinition cd : cfs.metadata.allColumns())
                    indexNames.add(cd.getIndexName());
        }
        return indexNames;
    }

    private static void validateAlias(ColumnDefinition alias, String msg) throws ConfigurationException
    {
        try
        {
            UTF8Type.instance.validate(alias.name);
        }
        catch (MarshalException e)
        {
            throw new ConfigurationException(msg + " alias must be UTF8");
        }
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

    /**
     * Create schema mutations to update this metadata to provided new state.
     *
     * @param newState The new metadata (for the same CF)
     * @param modificationTimestamp Timestamp to use for mutation
     * @param fromThrift whether the newState comes from thrift
     *
     * @return Difference between attributes in form of schema mutation
     */
    public RowMutation toSchemaUpdate(CFMetaData newState, long modificationTimestamp, boolean fromThrift)
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));

        newState.toSchemaNoColumnsNoTriggers(rm, modificationTimestamp);

        MapDifference<ByteBuffer, ColumnDefinition> columnDiff = Maps.difference(column_metadata, newState.column_metadata);

        // columns that are no longer needed
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnLeft().values())
        {
            // Thrift only knows about the REGULAR ColumnDefinition type, so don't consider other type
            // are being deleted just because they are not here.
            if (fromThrift && cd.type != ColumnDefinition.Type.REGULAR)
                continue;

            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);
        }

        // newly added columns
        for (ColumnDefinition cd : columnDiff.entriesOnlyOnRight().values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);

        // old columns with updated attributes
        for (ByteBuffer name : columnDiff.entriesDiffering().keySet())
        {
            ColumnDefinition cd = newState.getColumnDefinition(name);
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), modificationTimestamp);
        }

        MapDifference<String, TriggerDefinition> triggerDiff = Maps.difference(triggers, newState.triggers);

        // dropped triggers
        for (TriggerDefinition td : triggerDiff.entriesOnlyOnLeft().values())
            td.deleteFromSchema(rm, cfName, modificationTimestamp);

        // newly created triggers
        for (TriggerDefinition td : triggerDiff.entriesOnlyOnRight().values())
            td.toSchema(rm, cfName, modificationTimestamp);

        return rm;
    }

    /**
     * Remove all CF attributes from schema
     *
     * @param timestamp Timestamp to use
     *
     * @return RowMutation to use to completely remove cf from schema
     */
    public RowMutation dropFromSchema(long timestamp)
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));
        ColumnFamily cf = rm.addOrGet(SchemaColumnFamiliesCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        ColumnNameBuilder builder = SchemaColumnFamiliesCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName));
        cf.addAtom(new RangeTombstone(builder.build(), builder.buildAsEndOfRange(), timestamp, ldt));

        for (ColumnDefinition cd : column_metadata.values())
            cd.deleteFromSchema(rm, cfName, getColumnDefinitionComparator(cd), timestamp);

        for (TriggerDefinition td : triggers.values())
            td.deleteFromSchema(rm, cfName, timestamp);

        return rm;
    }

    public void toSchema(RowMutation rm, long timestamp)
    {
        toSchemaNoColumnsNoTriggers(rm, timestamp);

        for (TriggerDefinition td : triggers.values())
            td.toSchema(rm, cfName, timestamp);

        for (ColumnDefinition cd : column_metadata.values())
            cd.toSchema(rm, cfName, getColumnDefinitionComparator(cd), timestamp);
    }

    private void toSchemaNoColumnsNoTriggers(RowMutation rm, long timestamp)
    {
        // For property that can be null (and can be changed), we insert tombstones, to make sure
        // we don't keep a property the user has removed
        ColumnFamily cf = rm.addOrGet(SchemaColumnFamiliesCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);

        cf.addColumn(Column.create("", timestamp, cfName, ""));
        cf.addColumn(Column.create(cfType.toString(), timestamp, cfName, "type"));

        if (isSuper())
        {
            // We need to continue saving the comparator and subcomparator separatly, otherwise
            // we won't know at deserialization if the subcomparator should be taken into account
            // TODO: we should implement an on-start migration if we want to get rid of that.
            CompositeType ct = (CompositeType)comparator;
            cf.addColumn(Column.create(ct.types.get(0).toString(), timestamp, cfName, "comparator"));
            cf.addColumn(Column.create(ct.types.get(1).toString(), timestamp, cfName, "subcomparator"));
        }
        else
        {
            cf.addColumn(Column.create(comparator.toString(), timestamp, cfName, "comparator"));
        }

        cf.addColumn(comment == null ? DeletedColumn.create(ldt, timestamp, cfName, "comment")
                                     : Column.create(comment, timestamp, cfName, "comment"));
        cf.addColumn(Column.create(readRepairChance, timestamp, cfName, "read_repair_chance"));
        cf.addColumn(Column.create(dcLocalReadRepairChance, timestamp, cfName, "local_read_repair_chance"));
        cf.addColumn(Column.create(replicateOnWrite, timestamp, cfName, "replicate_on_write"));
        cf.addColumn(Column.create(populateIoCacheOnFlush, timestamp, cfName, "populate_io_cache_on_flush"));
        cf.addColumn(Column.create(gcGraceSeconds, timestamp, cfName, "gc_grace_seconds"));
        cf.addColumn(Column.create(defaultValidator.toString(), timestamp, cfName, "default_validator"));
        cf.addColumn(Column.create(keyValidator.toString(), timestamp, cfName, "key_validator"));
        cf.addColumn(Column.create(minCompactionThreshold, timestamp, cfName, "min_compaction_threshold"));
        cf.addColumn(Column.create(maxCompactionThreshold, timestamp, cfName, "max_compaction_threshold"));
        cf.addColumn(bloomFilterFpChance == null ? DeletedColumn.create(ldt, timestamp, cfName, "bloomFilterFpChance")
                                                 : Column.create(bloomFilterFpChance, timestamp, cfName, "bloom_filter_fp_chance"));
        cf.addColumn(Column.create(memtableFlushPeriod, timestamp, cfName, "memtable_flush_period_in_ms"));
        cf.addColumn(Column.create(caching.toString(), timestamp, cfName, "caching"));
        cf.addColumn(Column.create(defaultTimeToLive, timestamp, cfName, "default_time_to_live"));
        cf.addColumn(Column.create(compactionStrategyClass.getName(), timestamp, cfName, "compaction_strategy_class"));
        cf.addColumn(Column.create(json(compressionParameters.asThriftOptions()), timestamp, cfName, "compression_parameters"));
        cf.addColumn(Column.create(json(compactionStrategyOptions), timestamp, cfName, "compaction_strategy_options"));
        cf.addColumn(Column.create(indexInterval, timestamp, cfName, "index_interval"));
        cf.addColumn(Column.create(speculativeRetry.toString(), timestamp, cfName, "speculative_retry"));

        for (Map.Entry<ByteBuffer, Long> entry : droppedColumns.entrySet())
            cf.addColumn(new Column(makeDroppedColumnName(entry.getKey()), LongType.instance.decompose(entry.getValue()), timestamp));

        // Save the CQL3 metadata "the old way" for compatibility sake
        cf.addColumn(Column.create(aliasesToJson(partitionKeyColumns), timestamp, cfName, "key_aliases"));
        cf.addColumn(Column.create(aliasesToJson(clusteringKeyColumns), timestamp, cfName, "column_aliases"));
        cf.addColumn(compactValueColumn == null ? DeletedColumn.create(ldt, timestamp, cfName, "value_alias")
                                                : Column.create(compactValueColumn.name, timestamp, cfName, "value_alias"));
    }

    // Package protected for use by tests
    static CFMetaData fromSchemaNoColumnsNoTriggers(UntypedResultSet.Row result)
    {
        try
        {
            CFMetaData cfm = new CFMetaData(result.getString("keyspace_name"),
                                            result.getString("columnfamily_name"),
                                            ColumnFamilyType.valueOf(result.getString("type")),
                                            TypeParser.parse(result.getString("comparator")),
                                            result.has("subcomparator") ? TypeParser.parse(result.getString("subcomparator")) : null);

            cfm.readRepairChance(result.getDouble("read_repair_chance"));
            cfm.dcLocalReadRepairChance(result.getDouble("local_read_repair_chance"));
            cfm.replicateOnWrite(result.getBoolean("replicate_on_write"));
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
            cfm.caching(Caching.valueOf(result.getString("caching")));
            if (result.has("default_time_to_live"))
                cfm.defaultTimeToLive(result.getInt("default_time_to_live"));
            if (result.has("speculative_retry"))
                cfm.speculativeRetry(SpeculativeRetry.fromString(result.getString("speculative_retry")));
            cfm.compactionStrategyClass(createCompactionStrategy(result.getString("compaction_strategy_class")));
            cfm.compressionParameters(CompressionParameters.create(fromJsonMap(result.getString("compression_parameters"))));
            cfm.compactionStrategyOptions(fromJsonMap(result.getString("compaction_strategy_options")));
            if (result.has("index_interval"))
            {
                cfm.indexInterval(result.getInt("index_interval"));
            }
            else
            {
                if (DatabaseDescriptor.getIndexInterval() != null)
                {
                    // use index_interval set in cassandra.yaml as default value (in memory only)
                    cfm.indexInterval(DatabaseDescriptor.getIndexInterval());
                }
            }
            if (result.has("populate_io_cache_on_flush"))
                cfm.populateIoCacheOnFlush(result.getBoolean("populate_io_cache_on_flush"));

            /*
             * The info previously hold by key_aliases, column_aliases and value_alias is now stored in column_metadata (because 1) this
             * make more sense and 2) this allow to store indexing information).
             * However, for upgrade sake we need to still be able to read those old values. Moreover, we cannot easily
             * remove those old columns once "converted" to column_metadata because that would screw up nodes that may
             * not have upgraded. So for now we keep the both info and in sync, even though its redundant.
             * In other words, the ColumnDefinition the following lines add may be replaced later when ColumnDefinition.fromSchema
             * is called but that's ok.
             */
            if (result.has("key_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(fromJsonList(result.getString("key_aliases"))), cfm.keyValidator, ColumnDefinition.Type.PARTITION_KEY);
            if (result.has("column_aliases"))
                cfm.addColumnMetadataFromAliases(aliasesFromStrings(fromJsonList(result.getString("column_aliases"))), cfm.comparator, ColumnDefinition.Type.CLUSTERING_KEY);

            if (result.has("value_alias"))
                cfm.addColumnMetadataFromAliases(Collections.<ByteBuffer>singletonList(result.getBytes("value_alias")), cfm.defaultValidator, ColumnDefinition.Type.COMPACT_VALUE);

            if (result.has("dropped_columns"))
                cfm.droppedColumns(convertDroppedColumns(result.getMap("dropped_columns", UTF8Type.instance, LongType.instance)));

            return cfm;
        }
        catch (SyntaxException | ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void addColumnMetadataFromAliases(List<ByteBuffer> aliases, AbstractType<?> comparator, ColumnDefinition.Type type)
    {
        if (comparator instanceof CompositeType)
        {
            CompositeType ct = (CompositeType)comparator;
            for (int i = 0; i < aliases.size(); ++i)
            {
                if (aliases.get(i) != null)
                    column_metadata.put(aliases.get(i), new ColumnDefinition(aliases.get(i), ct.types.get(i), i, type));
            }
        }
        else
        {
            assert aliases.size() <= 1;
            if (!aliases.isEmpty() && aliases.get(0) != null)
                column_metadata.put(aliases.get(0), new ColumnDefinition(aliases.get(0), comparator, null, type));
        }
    }

    /**
     * Deserialize CF metadata from low-level representation
     *
     * @return Thrift-based metadata deserialized from schema
     */
    public static CFMetaData fromSchema(UntypedResultSet.Row result)
    {
        CFMetaData cfDef = fromSchemaNoColumnsNoTriggers(result);

        Row serializedTriggers = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_TRIGGERS_CF, cfDef.ksName, cfDef.cfName);
        addTriggerDefinitionsFromSchema(cfDef, serializedTriggers);

        Row serializedColumns = SystemKeyspace.readSchemaRow(SystemKeyspace.SCHEMA_COLUMNS_CF, cfDef.ksName, cfDef.cfName);
        return addColumnDefinitionsFromSchema(cfDef, serializedColumns);
    }

    private static CFMetaData fromSchema(Row row)
    {
        UntypedResultSet.Row result = QueryProcessor.resultify("SELECT * FROM system.schema_columnfamilies", row).one();
        return fromSchema(result);
    }

    private String aliasesToJson(List<ColumnDefinition> rawAliases)
    {
        List<String> aliases = new ArrayList<String>(rawAliases.size());
        for (ColumnDefinition rawAlias : rawAliases)
            aliases.add(UTF8Type.instance.compose(rawAlias.name));
        return json(aliases);
    }

    private static List<ByteBuffer> aliasesFromStrings(List<String> aliases)
    {
        List<ByteBuffer> rawAliases = new ArrayList<ByteBuffer>(aliases.size());
        for (String alias : aliases)
            rawAliases.add(UTF8Type.instance.decompose(alias));
        return rawAliases;
    }

    private static Map<ByteBuffer, Long> convertDroppedColumns(Map<String, Long> raw)
    {
        Map<ByteBuffer, Long> converted = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : raw.entrySet())
            converted.put(UTF8Type.instance.decompose(entry.getKey()), entry.getValue());
        return converted;
    }

    private ByteBuffer makeDroppedColumnName(ByteBuffer column)
    {
        ColumnNameBuilder builder = SchemaColumnFamiliesCf.cqlCfDef.getColumnNameBuilder();
        builder.add(UTF8Type.instance.decompose(cfName));
        builder.add(UTF8Type.instance.decompose("dropped_columns"));
        return builder.add(column).build();
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
    public RowMutation toSchema(long timestamp) throws ConfigurationException
    {
        RowMutation rm = new RowMutation(Keyspace.SYSTEM_KS, SystemKeyspace.getSchemaKSKey(ksName));
        toSchema(rm, timestamp);
        return rm;
    }

    // The comparator to validate the definition name.

    public AbstractType<?> getColumnDefinitionComparator(ColumnDefinition def)
    {
        return getComponentComparator(def.componentIndex, def.type);
    }

    public AbstractType<?> getComponentComparator(Integer componentIndex, ColumnDefinition.Type type)
    {
        switch (type)
        {
            case REGULAR:
                AbstractType<?> cfComparator = cfType == ColumnFamilyType.Super ? ((CompositeType)comparator).types.get(1) : comparator;
                if (cfComparator instanceof CompositeType)
                {
                    if (componentIndex == null)
                        return cfComparator;

                    List<AbstractType<?>> types = ((CompositeType)cfComparator).types;
                    AbstractType<?> t = types.get(componentIndex);
                    assert t != null : "Non-sensical component index";
                    return t;
                }
                else
                {
                    return cfComparator;
                }
            default:
                // CQL3 column names are UTF8
                return UTF8Type.instance;
        }
    }

    // Package protected for use by tests
    static CFMetaData addColumnDefinitionsFromSchema(CFMetaData cfDef, Row serializedColumnDefinitions)
    {
        for (ColumnDefinition cd : ColumnDefinition.fromSchema(serializedColumnDefinitions, cfDef))
            cfDef.column_metadata.put(cd.name, cd);
        return cfDef.rebuild();
    }

    public void addColumnDefinition(ColumnDefinition def) throws ConfigurationException
    {
        if (column_metadata.containsKey(def.name))
            throw new ConfigurationException(String.format("Cannot add column %s, a column with the same name already exists", getColumnDefinitionComparator(def).getString(def.name)));

        addOrReplaceColumnDefinition(def);
    }

    // This method doesn't check if a def of the same name already exist and should only be used when we
    // know this cannot happen.
    public void addOrReplaceColumnDefinition(ColumnDefinition def)
    {
        column_metadata.put(def.name, def);
    }

    public boolean removeColumnDefinition(ColumnDefinition def)
    {
        return column_metadata.remove(def.name) != null;
    }

    private static void addTriggerDefinitionsFromSchema(CFMetaData cfDef, Row serializedTriggerDefinitions)
    {
        for (TriggerDefinition td : TriggerDefinition.fromSchema(serializedTriggerDefinitions))
            cfDef.triggers.put(td.name, td);
    }

    public void addTriggerDefinition(TriggerDefinition def) throws ConfigurationException
    {
        if (triggers.containsKey(def.name))
            throw new ConfigurationException(String.format("Cannot create trigger %s, a trigger with the same name already exists", def.name));
        triggers.put(def.name, def);
    }

    public boolean removeTrigger(String name)
    {
        return triggers.remove(name) != null;
    }

    public void recordColumnDrop(ColumnDefinition def)
    {
        assert def.componentIndex != null;
        droppedColumns.put(def.name, FBUtilities.timestampMicros());
    }

    public void renameColumn(ByteBuffer from, String strFrom, ByteBuffer to, String strTo) throws InvalidRequestException
    {
        ColumnDefinition def = column_metadata.get(from);
        if (def == null)
            throw new InvalidRequestException(String.format("Cannot rename unknown column %s in keyspace %s", strFrom, cfName));

        if (column_metadata.get(to) != null)
            throw new InvalidRequestException(String.format("Cannot rename column %s to %s in keyspace %s; another column of that name already exist", strFrom, strTo, cfName));

        if (def.isPartOfCellName())
        {
            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", strFrom));
        }
        else if (def.isIndexed())
        {
            throw new InvalidRequestException(String.format("Cannot rename column %s because it is secondary indexed", strFrom));
        }

        ColumnDefinition newDef = def.cloneWithNewName(to);
        // don't call addColumnDefinition/removeColumnDefition because we want to avoid recomputing
        // the CQL3 cfDef between those two operation
        column_metadata.put(newDef.name, newDef);
        column_metadata.remove(def.name);
    }

    public CFMetaData rebuild()
    {
        /*
         * TODO: There is definitively some repetition between the CQL3  metadata stored in this
         * object (partitionKeyColumns, ...) and the one stored in CFDefinition.
         * Ultimately, we should probably merge both. However, there is enough details to fix that
         * it's worth doing that in a separate issue.
         */
        rebuildCQL3Metadata();
        cqlCfDef = new CFDefinition(this);
        return this;
    }

    public CFDefinition getCfDef()
    {
        assert cqlCfDef != null;
        return cqlCfDef;
    }

    private void rebuildCQL3Metadata()
    {
        List<ColumnDefinition> pkCols = nullInitializedList(keyValidator.componentsCount());
        boolean isDense = isDense(comparator, column_metadata.values());
        int nbCkCols = isDense
                     ? comparator.componentsCount()
                     : comparator.componentsCount() - (hasCollection() ? 2 : 1);
        List<ColumnDefinition> ckCols = nullInitializedList(nbCkCols);
        Set<ColumnDefinition> regCols = new HashSet<ColumnDefinition>();
        Set<ColumnDefinition> statCols = new HashSet<ColumnDefinition>();
        ColumnDefinition compactCol = null;

        for (ColumnDefinition def : column_metadata.values())
        {
            switch (def.type)
            {
                case PARTITION_KEY:
                    assert !(def.componentIndex == null && keyValidator instanceof CompositeType);
                    pkCols.set(def.componentIndex == null ? 0 : def.componentIndex, def);
                    break;
                case CLUSTERING_KEY:
                    assert !(def.componentIndex == null && comparator instanceof CompositeType);
                    ckCols.set(def.componentIndex == null ? 0 : def.componentIndex, def);
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
        clusteringKeyColumns = addDefaultColumnAliases(ckCols);
        regularColumns = regCols;
        staticColumns = statCols;
        compactValueColumn = addDefaultValueAlias(compactCol, isDense);
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
                ColumnDefinition newDef = ColumnDefinition.partitionKeyDef(name, type, idx);
                column_metadata.put(newDef.name, newDef);
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
                Integer idx = null;
                AbstractType<?> type = comparator;
                if (comparator instanceof CompositeType)
                {
                    idx = i;
                    type = ((CompositeType)comparator).types.get(i);
                }
                ByteBuffer name = ByteBufferUtil.bytes(DEFAULT_COLUMN_ALIAS + (i + 1));
                ColumnDefinition newDef = ColumnDefinition.clusteringKeyDef(name, type, idx);
                column_metadata.put(newDef.name, newDef);
                ckCols.set(i, newDef);
            }
        }
        return ckCols;
    }

    private ColumnDefinition addDefaultValueAlias(ColumnDefinition compactValueDef, boolean isDense)
    {
        if (isDense)
        {
            if (compactValueDef != null)
                return compactValueDef;

            ColumnDefinition newDef = ColumnDefinition.compactValueDef(ByteBufferUtil.bytes(DEFAULT_VALUE_ALIAS), defaultValidator);
            column_metadata.put(newDef.name, newDef);
            return newDef;
        }
        else
        {
            assert compactValueDef == null;
            return null;
        }
    }

    private boolean hasCollection()
    {
        if (isSuper() || !(comparator instanceof CompositeType))
            return false;

        List<AbstractType<?>> types = ((CompositeType)comparator).types;
        return types.get(types.size() - 1) instanceof ColumnToCollectionType;
    }

    /*
     * We call dense a CF for which each component of the comparator is a clustering column, i.e. no
     * component is used to store a regular column names. In other words, non-composite static "thrift"
     * and CQL3 CF are *not* dense.
     * Note that his method is only used by rebuildCQL3Metadata. Once said metadata are built, finding
     * if a CF is dense amounts more simply to check if clusteringKeyColumns.size() == comparator.componentsCount().
     */
    private static boolean isDense(AbstractType<?> comparator, Collection<ColumnDefinition> defs)
    {
        /*
         * As said above, this method is only here because we need to deal with thrift upgrades.
         * Once a CF has been "upgraded", i.e. we've rebuilt and save its CQL3 metadata at least once,
         * then checking for isDense amounts to looking whether the maximum componentIndex for the
         * CLUSTERING_KEY ColumnDefinitions is equal to comparator.componentsCount() - 1 or not.
         *
         * But non-upgraded thrift CF will have no such CLUSTERING_KEY column definitions, so we need
         * to infer that information without relying on them in that case. And for the most part this is
         * easy, a CF that has at least one REGULAR definition is not dense. But the subtlety is that not
         * having a REGULAR definition may not mean dense because of CQL3 definitions that have only the
         * PRIMARY KEY defined.
         *
         * So we need to recognize those special case CQL3 table with only a primary key. If we have some
         * clustering columns, we're fine as said above. So the only problem is that we cannot decide for
         * sure if a CF without REGULAR columns nor CLUSTERING_KEY definition is meant to be dense, or if it
         * has been created in CQL3 by say:
         *    CREATE TABLE test (k int PRIMARY KEY)
         * in which case it should not be dense. However, we can limit our margin of error by assuming we are
         * in the latter case only if the comparator is exactly CompositeType(UTF8Type).
         */
        boolean hasRegular = false;
        int maxClusteringIdx = -1;
        for (ColumnDefinition def : defs)
        {
            switch (def.type)
            {
                case CLUSTERING_KEY:
                    maxClusteringIdx = Math.max(maxClusteringIdx, def.componentIndex == null ? 0 : def.componentIndex);
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

        for (ColumnDefinition def : column_metadata.values())
        {
            // Non-REGULAR ColumnDefinition are not "thrift compatible" per-se, but it's ok because they hold metadata
            // this is only of use to CQL3, so we will just skip them in toThrift.
            if (def.type == ColumnDefinition.Type.REGULAR && !def.isThriftCompatible())
                return false;
        }
        return true;
    }

    public boolean hasStaticColumns()
    {
        return !staticColumns.isEmpty();
    }

    public ColumnNameBuilder getStaticColumnNameBuilder()
    {
        assert comparator instanceof CompositeType && clusteringKeyColumns().size() > 0;
        CompositeType.Builder builder = CompositeType.Builder.staticBuilder((CompositeType)comparator);
        for (int i = 0; i < clusteringKeyColumns().size(); i++)
            builder.add(ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return builder;
    }

    public void validateColumns(Iterable<Column> columns)
    {
        for (Column column : columns)
            column.validateFields(this);
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
            .append("dclocalReadRepairChance", dcLocalReadRepairChance)
            .append("replicateOnWrite", replicateOnWrite)
            .append("gcGraceSeconds", gcGraceSeconds)
            .append("defaultValidator", defaultValidator)
            .append("keyValidator", keyValidator)
            .append("minCompactionThreshold", minCompactionThreshold)
            .append("maxCompactionThreshold", maxCompactionThreshold)
            .append("column_metadata", column_metadata)
            .append("compactionStrategyClass", compactionStrategyClass)
            .append("compactionStrategyOptions", compactionStrategyOptions)
            .append("compressionOptions", compressionParameters.asThriftOptions())
            .append("bloomFilterFpChance", bloomFilterFpChance)
            .append("memtable_flush_period_in_ms", memtableFlushPeriod)
            .append("caching", caching)
            .append("defaultTimeToLive", defaultTimeToLive)
            .append("speculative_retry", speculativeRetry)
            .append("indexInterval", indexInterval)
            .append("populateIoCacheOnFlush", populateIoCacheOnFlush)
            .append("droppedColumns", droppedColumns)
            .append("triggers", triggers)
            .toString();
    }
}
