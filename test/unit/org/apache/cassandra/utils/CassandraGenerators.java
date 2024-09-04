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
package org.apache.cassandra.utils;

import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.builder.MultilineRecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import accord.local.Node;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SchemaCQLHelper;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalCompositePrefixPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.ReversedLongLocalPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.PingRequest;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.accord.fastpath.FastPathStrategy;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.service.accord.AccordFastPath;
import org.apache.cassandra.service.accord.AccordStaleReplicas;
import org.apache.cassandra.service.accord.fastpath.InheritKeyspaceFastPathStrategy;
import org.apache.cassandra.service.accord.fastpath.ParameterizedFastPathStrategy;
import org.apache.cassandra.service.accord.fastpath.SimpleFastPathStrategy;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationState;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.utils.AbstractTypeGenerators.TypeGenBuilder;
import org.apache.cassandra.utils.AbstractTypeGenerators.ValueDomain;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.utils.AbstractTypeGenerators.allowReversed;
import static org.apache.cassandra.utils.AbstractTypeGenerators.getTypeSupport;
import static org.apache.cassandra.utils.AbstractTypeGenerators.withoutUnsafeEquality;
import static org.apache.cassandra.utils.Generators.IDENTIFIER_GEN;
import static org.apache.cassandra.utils.Generators.SMALL_TIME_SPAN_NANOS;
import static org.apache.cassandra.utils.Generators.TIMESTAMP_NANOS;
import static org.apache.cassandra.utils.Generators.TINY_TIME_SPAN_NANOS;

public final class CassandraGenerators
{
    private static final Pattern NEWLINE_PATTERN = Pattern.compile("\n", Pattern.LITERAL);

    // utility generators for creating more complex types
    private static final Gen<Integer> SMALL_POSITIVE_SIZE_GEN = SourceDSL.integers().between(1, 30);
    private static final Gen<Integer> NETWORK_PORT_GEN = SourceDSL.integers().between(0, 0xFFFF);
    private static final Gen<Boolean> BOOLEAN_GEN = SourceDSL.booleans().all();

    /**
     * Similar to {@link Generators#IDENTIFIER_GEN} but uses a bound of 48 as keyspace has a smaller restriction than other identifiers
     */
    public static final Gen<String> KEYSPACE_NAME_GEN = Generators.regexWord(SourceDSL.integers().between(1, 48));

    public static final Gen<InetAddressAndPort> INET_ADDRESS_AND_PORT_GEN = rnd -> {
        InetAddress address = Generators.INET_ADDRESS_GEN.generate(rnd);
        return InetAddressAndPort.getByAddressOverrideDefaults(address, NETWORK_PORT_GEN.generate(rnd));
    };

    public static final Gen<TableId> TABLE_ID_GEN = Generators.UUID_RANDOM_GEN.map(TableId::fromUUID);
    private static final Gen<TableMetadata.Kind> TABLE_KIND_GEN = SourceDSL.arbitrary().pick(TableMetadata.Kind.REGULAR, TableMetadata.Kind.INDEX, TableMetadata.Kind.VIRTUAL);
    public static final Gen<TableMetadata> TABLE_METADATA_GEN = gen(rnd -> createTableMetadata(IDENTIFIER_GEN.generate(rnd), rnd)).describedAs(CassandraGenerators::toStringRecursive);

    private static final Gen<SinglePartitionReadCommand> SINGLE_PARTITION_READ_COMMAND_GEN = gen(rnd -> {
        TableMetadata metadata = TABLE_METADATA_GEN.generate(rnd);
        long nowInSec = rnd.next(Constraint.between(1, Cell.getVersionedMaxDeletiontionTime()));
        ByteBuffer key = partitionKeyDataGen(metadata).generate(rnd);
        //TODO support all fields of SinglePartitionReadCommand
        return SinglePartitionReadCommand.create(metadata, nowInSec, key, Slices.ALL);
    }).describedAs(CassandraGenerators::toStringRecursive);
    private static final Gen<? extends ReadCommand> READ_COMMAND_GEN = Generate.oneOf(SINGLE_PARTITION_READ_COMMAND_GEN)
                                                                               .describedAs(CassandraGenerators::toStringRecursive);

    // Outbound messages
    private static final Gen<ConnectionType> CONNECTION_TYPE_GEN = SourceDSL.arbitrary().pick(ConnectionType.URGENT_MESSAGES, ConnectionType.SMALL_MESSAGES, ConnectionType.LARGE_MESSAGES);
    public static final Gen<Message<PingRequest>> MESSAGE_PING_GEN = CONNECTION_TYPE_GEN
                                                                     .map(t -> Message.builder(Verb.PING_REQ, PingRequest.get(t)).build())
                                                                     .describedAs(CassandraGenerators::toStringRecursive);
    public static final Gen<Message<? extends ReadCommand>> MESSAGE_READ_COMMAND_GEN = READ_COMMAND_GEN
                                                                                       .<Message<? extends ReadCommand>>map(c -> Message.builder(Verb.READ_REQ, c).build())
                                                                                       .describedAs(CassandraGenerators::toStringRecursive);

    private static Gen<Message<NoPayload>> responseGen(Verb verb)
    {
        return gen(rnd -> {
            long timeSpan = SMALL_TIME_SPAN_NANOS.generate(rnd);
            long delay = TINY_TIME_SPAN_NANOS.generate(rnd); // network & processing delay
            long requestCreatedAt = TIMESTAMP_NANOS.generate(rnd);
            long createdAt = requestCreatedAt + delay;
            long expiresAt = requestCreatedAt + timeSpan;
            return Message.builder(verb, NoPayload.noPayload)
                          .withCreatedAt(createdAt)
                          .withExpiresAt(expiresAt)
                          .from(INET_ADDRESS_AND_PORT_GEN.generate(rnd))
                          .build();
        }).describedAs(CassandraGenerators::toStringRecursive);
    }

    public static final Gen<Message<NoPayload>> MUTATION_RSP_GEN = responseGen(Verb.MUTATION_RSP);
    public static final Gen<Message<NoPayload>> READ_REPAIR_RSP_GEN = responseGen(Verb.READ_REPAIR_RSP);

    public static final Gen<Message<?>> MESSAGE_GEN = Generate.oneOf(cast(MESSAGE_PING_GEN),
                                                                     cast(MESSAGE_READ_COMMAND_GEN),
                                                                     cast(MUTATION_RSP_GEN),
                                                                     cast(READ_REPAIR_RSP_GEN))
                                                              .describedAs(CassandraGenerators::toStringRecursive);

    private CassandraGenerators()
    {

    }

    public static Set<UserType> extractUDTs(TableMetadata metadata)
    {
        Set<UserType> matches = new HashSet<>();
        for (ColumnMetadata col : metadata.columns())
            AbstractTypeGenerators.extractUDTs(col.type, matches);
        return matches;
    }

    public static TableMetadata createTableMetadata(String ks, RandomnessSource rnd)
    {
        return new TableMetadataBuilder().withKeyspaceName(ks).build(rnd);
    }

    public static Gen<String> sstableFormatNames()
    {
        return SourceDSL.arbitrary().pick("big", "bti");
    }

    public static Gen<SSTableFormat<?, ?>> sstableFormat()
    {
        // make sure ordering is determanstic, else repeatability breaks
        NavigableMap<String, SSTableFormat<?, ?>> formats = new TreeMap<>(DatabaseDescriptor.getSSTableFormats());
        return SourceDSL.arbitrary().pick(new ArrayList<>(formats.values()));
    }

    public static class AbstractReplicationStrategyBuilder
    {
        public enum Strategy
        {
            Simple(true),
            NetworkTopology(true),
            Local(false),
            Meta(false);

            public final boolean userAllowed;

            Strategy(boolean userAllowed)
            {
                this.userAllowed = userAllowed;
            }
        }

        private Gen<Strategy> strategyGen = SourceDSL.arbitrary().enumValues(Strategy.class);
        private Gen<String> keyspaceNameGen = KEYSPACE_NAME_GEN;
        private Gen<Integer> rfGen = SourceDSL.integers().between(1, 3);
        private Gen<List<String>> networkTopologyDCGen = rs -> {
            Gen<Integer> numDcsGen = SourceDSL.integers().between(1, 3);
            Gen<String> nameGen = IDENTIFIER_GEN;
            Set<String> dcs = new HashSet<>();
            int targetSize = numDcsGen.generate(rs);
            while (dcs.size() != targetSize)
                dcs.add(nameGen.generate(rs));
            List<String> ordered = new ArrayList<>(dcs);
            ordered.sort(Comparator.naturalOrder());
            return ordered;
        };

        public AbstractReplicationStrategyBuilder withKeyspace(Gen<String> keyspaceNameGen)
        {
            this.keyspaceNameGen = keyspaceNameGen;
            return this;
        }

        public AbstractReplicationStrategyBuilder withKeyspace(String keyspace)
        {
            this.keyspaceNameGen = i -> keyspace;
            return this;
        }

        public AbstractReplicationStrategyBuilder withUserAllowed()
        {
            List<Strategy> allowed = Stream.of(Strategy.values()).filter(s -> s.userAllowed).collect(Collectors.toList());
            strategyGen = SourceDSL.arbitrary().pick(allowed);
            return this;
        }

        public AbstractReplicationStrategyBuilder withRf(Gen<Integer> rfGen)
        {
            this.rfGen = rfGen;
            return this;
        }

        public AbstractReplicationStrategyBuilder withRf(int rf)
        {
            this.rfGen = i -> rf;
            return this;
        }

        public AbstractReplicationStrategyBuilder withDatacenters(Gen<List<String>> networkTopologyDCGen)
        {
            this.networkTopologyDCGen = networkTopologyDCGen;
            return this;
        }

        public AbstractReplicationStrategyBuilder withDatacenters(String first, String... rest)
        {
            if (rest.length == 0)
            {
                this.networkTopologyDCGen = i -> Collections.singletonList(first);
            }
            else
            {
                List<String> all = new ArrayList<>(rest.length + 1);
                all.add(first);
                all.addAll(Arrays.asList(rest));
                this.networkTopologyDCGen = i -> all;
            }
            return this;
        }

        public Gen<AbstractReplicationStrategy> build()
        {
            return rs -> {
                Strategy strategy = strategyGen.generate(rs);
                switch (strategy)
                {
                    case Simple:
                        return new SimpleStrategy(keyspaceNameGen.generate(rs),
                                                  ImmutableMap.of(SimpleStrategy.REPLICATION_FACTOR, rfGen.generate(rs).toString()));
                    case NetworkTopology:
                        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
                        List<String> names = networkTopologyDCGen.generate(rs);
                        for (String name : names)
                            builder.put(name, rfGen.generate(rs).toString());
                        ImmutableMap<String, String> map = builder.build();
                        return new TestableNetworkTopologyStrategy(keyspaceNameGen.generate(rs), map);
                    case Meta:
                        return new MetaStrategy(keyspaceNameGen.generate(rs), ImmutableMap.of());
                    case Local:
                        return new LocalStrategy(keyspaceNameGen.generate(rs), ImmutableMap.of());
                    default:
                        throw new UnsupportedOperationException(strategy.name());
                }
            };
        }
    }

    public static class TestableNetworkTopologyStrategy extends NetworkTopologyStrategy
    {
        public TestableNetworkTopologyStrategy(String keyspaceName, Map<String, String> configOptions) throws ConfigurationException
        {
            super(keyspaceName, configOptions);
        }

        @Override
        public Collection<String> recognizedOptions(ClusterMetadata metadata)
        {
            return configOptions.keySet();
        }
    }

    public static KeyspaceMetadataBuilder regularKeyspace()
    {
        return new KeyspaceMetadataBuilder().withKind(KeyspaceMetadata.Kind.REGULAR);
    }

    public static class KeyspaceMetadataBuilder
    {
        private Gen<String> nameGen = KEYSPACE_NAME_GEN;
        private Gen<KeyspaceMetadata.Kind> kindGen = SourceDSL.arbitrary().enumValues(KeyspaceMetadata.Kind.class);
        private Gen<AbstractReplicationStrategyBuilder> replicationGen = i -> new AbstractReplicationStrategyBuilder();
        private Gen<Boolean> durableWritesGen = SourceDSL.booleans().all();

        public KeyspaceMetadataBuilder withReplication(Gen<AbstractReplicationStrategyBuilder> replicationGen)
        {
            this.replicationGen = replicationGen;
            return this;
        }

        public KeyspaceMetadataBuilder withReplication(AbstractReplicationStrategyBuilder replication)
        {
            this.replicationGen = i -> replication;
            return this;
        }

        public KeyspaceMetadataBuilder withName(Gen<String> nameGen)
        {
            this.nameGen = nameGen;
            return this;
        }

        public KeyspaceMetadataBuilder withName(String name)
        {
            this.nameGen = i -> name;
            return this;
        }

        public KeyspaceMetadataBuilder withKind(Gen<KeyspaceMetadata.Kind> kindGen)
        {
            this.kindGen = kindGen;
            return this;
        }

        public KeyspaceMetadataBuilder withKind(KeyspaceMetadata.Kind kind)
        {
            this.kindGen = i -> kind;
            return this;
        }

        public Gen<KeyspaceMetadata> build()
        {
            return rs -> {
                String name = nameGen.generate(rs);
                KeyspaceMetadata.Kind kind = kindGen.generate(rs);
                AbstractReplicationStrategy replication = replicationGen.generate(rs).withKeyspace(nameGen).build().generate(rs);
                ReplicationParams replicationParams = ReplicationParams.copy(replication);
                boolean durableWrites = durableWritesGen.generate(rs);
                KeyspaceParams params = new KeyspaceParams(durableWrites, replicationParams, FastPathStrategy.simple());
                Tables tables = Tables.none();
                Views views = Views.none();
                Types types = Types.none();
                UserFunctions userFunctions = UserFunctions.none();
                return KeyspaceMetadata.createUnsafe(name, kind, params, tables, views, types, userFunctions);
            };
        }
    }

    public static class TableParamsBuilder
    {
        @Nullable
        private Gen<String> memtableKeyGen = null;
        @Nullable
        private Gen<TransactionalMode> transactionalMode = null;
        @Nullable
        private Gen<FastPathStrategy> fastPathStrategy = null;

        public TableParamsBuilder withKnownMemtables()
        {
            Set<String> known = MemtableParams.knownDefinitions();
            // for testing reason, some invalid types are added; filter out
            List<String> valid = known.stream().filter(name -> !name.startsWith("test_")).collect(Collectors.toList());
            memtableKeyGen = SourceDSL.arbitrary().pick(valid);
            return this;
        }

        public TableParamsBuilder withTransactionalMode(Gen<TransactionalMode> transactionalMode)
        {
            this.transactionalMode = transactionalMode;
            return this;
        }

        public TableParamsBuilder withTransactionalMode()
        {
            return withTransactionalMode(SourceDSL.arbitrary().enumValues(TransactionalMode.class));
        }

        public TableParamsBuilder withTransactionalMode(TransactionalMode transactionalMode)
        {
            return withTransactionalMode(SourceDSL.arbitrary().constant(transactionalMode));
        }

        public TableParamsBuilder withFastPathStrategy()
        {
            fastPathStrategy = rnd -> {
                FastPathStrategy.Kind kind = SourceDSL.arbitrary().enumValues(FastPathStrategy.Kind.class).generate(rnd);
                switch (kind)
                {
                    case SIMPLE:
                        return SimpleFastPathStrategy.instance;
                    case INHERIT_KEYSPACE:
                        return InheritKeyspaceFastPathStrategy.instance;
                    case PARAMETERIZED:
                    {
                        Map<String, String> map = new HashMap<>();
                        int size = SourceDSL.integers().between(1, Integer.MAX_VALUE).generate(rnd);
                        map.put(ParameterizedFastPathStrategy.SIZE, Integer.toString(size));
                        Set<String> names = new HashSet<>();
                        Gen<String> nameGen = SourceDSL.strings().allPossible().ofLengthBetween(1, 10).assuming(s -> !s.trim().isEmpty());
                        int numNames = SourceDSL.integers().between(1, 10).generate(rnd);
                        for (int i = 0; i < numNames; i++)
                        {
                            while (!names.add(nameGen.generate(rnd)))
                            {
                            }
                        }
                        List<String> sortedNames = new ArrayList<>(names);
                        sortedNames.sort(Comparator.naturalOrder());
                        List<String> dcs = new ArrayList<>(names.size());
                        boolean auto = SourceDSL.booleans().all().generate(rnd);
                        if (auto)
                        {
                            dcs.addAll(sortedNames);
                        }
                        else
                        {
                            for (String name : sortedNames)
                            {
                                int weight = SourceDSL.integers().between(0, 10).generate(rnd);
                                dcs.add(name + ":" + weight);
                            }
                        }
                        // str: dcFormat(,dcFormat)*
                        //      dcFormat: name | weight
                        //      weight: int: >= 0
                        //      note: can't mix auto and user defined weight; need one or the other.  Names must be unique
                        map.put(ParameterizedFastPathStrategy.DCS, String.join(",", dcs));
                        return ParameterizedFastPathStrategy.fromMap(map);
                    }
                    default:
                        throw new UnsupportedOperationException(kind.name());
                }
            };
            return this;
        }

        public Gen<TableParams> build()
        {
            return rnd -> {
                TableParams.Builder params = TableParams.builder();
                if (memtableKeyGen != null)
                    params.memtable(MemtableParams.get(memtableKeyGen.generate(rnd)));
                if (transactionalMode != null)
                    params.transactionalMode(transactionalMode.generate(rnd));
                if (fastPathStrategy != null)
                    params.fastPath(fastPathStrategy.generate(rnd));
                return params.build();
            };
        }
    }

    public static TableMetadataBuilder regularTable()
    {
        return new TableMetadataBuilder()
               .withTableKinds(TableMetadata.Kind.REGULAR)
               .withKnownMemtables();
    }

    public static class TableMetadataBuilder
    {
        private Gen<String> ksNameGen = CassandraGenerators.KEYSPACE_NAME_GEN;
        private Gen<String> tableNameGen = IDENTIFIER_GEN;
        private TypeGenBuilder defaultTypeGen = defaultTypeGen();
        private Gen<Boolean> useCounter = ignore -> false;
        private TypeGenBuilder partitionColTypeGen, clusteringColTypeGen, staticColTypeGen, regularColTypeGen;
        private Gen<TableId> tableIdGen = TABLE_ID_GEN;
        private Gen<TableMetadata.Kind> tableKindGen = SourceDSL.arbitrary().constant(TableMetadata.Kind.REGULAR);
        private Gen<Integer> numPartitionColumnsGen = SourceDSL.integers().between(1, 2);
        private Gen<Integer> numClusteringColumnsGen = SourceDSL.integers().between(1, 2);
        private Gen<Integer> numRegularColumnsGen = SourceDSL.integers().between(1, 5);
        private Gen<Integer> numStaticColumnsGen = SourceDSL.integers().between(0, 2);
        @Nullable
        private ColumnNameGen columnNameGen = null;
        private TableParamsBuilder paramsBuilder = new TableParamsBuilder();
        private Gen<IPartitioner> partitionerGen = partitioners();

        public static TypeGenBuilder defaultTypeGen()
        {
            return AbstractTypeGenerators.builder()
                                         .withoutEmpty()
                                         .withDefaultSetKey(withoutUnsafeEquality())
                                         .withMaxDepth(1)
                                         .withoutTypeKinds(AbstractTypeGenerators.TypeKind.COUNTER);
        }

        public TableMetadataBuilder withSimpleColumnNames()
        {
            columnNameGen = (i, kind, offset) -> {
                switch (kind)
                {
                    case PARTITION_KEY: return "pk" + offset;
                    case CLUSTERING: return "ck" + offset;
                    case STATIC: return "s" + offset;
                    case REGULAR: return "v" + offset;
                    default: throw new UnsupportedOperationException("Unknown kind: " + kind);
                }
            };
            return this;
        }

        public TableMetadataBuilder withPartitioner(Gen<IPartitioner> partitionerGen)
        {
            this.partitionerGen = Objects.requireNonNull(partitionerGen);
            return this;
        }

        public TableMetadataBuilder withPartitioner(IPartitioner partitioner)
        {
            return withPartitioner(i -> partitioner);
        }

        public TableMetadataBuilder withUseCounter(boolean useCounter)
        {
            return withUseCounter(ignore -> useCounter);
        }

        public TableMetadataBuilder withUseCounter(Gen<Boolean> useCounter)
        {
            this.useCounter = Objects.requireNonNull(useCounter);
            return this;
        }

        public TableMetadataBuilder withTransactionalMode(Gen<TransactionalMode> transactionalMode)
        {
            paramsBuilder.withTransactionalMode(transactionalMode);
            return this;
        }

        public TableMetadataBuilder withTransactionalMode(TransactionalMode transactionalMode)
        {
            paramsBuilder.withTransactionalMode(transactionalMode);
            return this;
        }

        public TableMetadataBuilder withKnownMemtables()
        {
            paramsBuilder.withKnownMemtables();
            return this;
        }

        public TableMetadataBuilder withKeyspaceName(Gen<String> ksNameGen)
        {
            this.ksNameGen = ksNameGen;
            return this;
        }

        public TableMetadataBuilder withKeyspaceName(String name)
        {
            this.ksNameGen = SourceDSL.arbitrary().constant(name);
            return this;
        }

        public TableMetadataBuilder withTableName(Gen<String> tableNameGen)
        {
            this.tableNameGen = tableNameGen;
            return this;
        }

        public TableMetadataBuilder withTableName(String name)
        {
            this.tableNameGen = SourceDSL.arbitrary().constant(name);
            return this;
        }

        public TableMetadataBuilder withTableId(Gen<TableId> gen)
        {
            this.tableIdGen = gen;
            return this;
        }

        public TableMetadataBuilder withTableId(TableId id)
        {
            this.tableIdGen = SourceDSL.arbitrary().constant(id);
            return this;
        }

        public TableMetadataBuilder withPartitionColumnsCount(int num)
        {
            this.numPartitionColumnsGen = SourceDSL.arbitrary().constant(num);
            return this;
        }

        public TableMetadataBuilder withPartitionColumnsBetween(int min, int max)
        {
            this.numPartitionColumnsGen = SourceDSL.integers().between(min, max);
            return this;
        }

        public TableMetadataBuilder withClusteringColumnsCount(int num)
        {
            this.numClusteringColumnsGen = SourceDSL.arbitrary().constant(num);
            return this;
        }

        public TableMetadataBuilder withClusteringColumnsBetween(int min, int max)
        {
            this.numClusteringColumnsGen = SourceDSL.integers().between(min, max);
            return this;
        }

        public TableMetadataBuilder withRegularColumnsCount(int num)
        {
            this.numRegularColumnsGen = SourceDSL.arbitrary().constant(num);
            return this;
        }

        public TableMetadataBuilder withRegularColumnsBetween(int min, int max)
        {
            this.numRegularColumnsGen = SourceDSL.integers().between(min, max);
            return this;
        }

        public TableMetadataBuilder withStaticColumnsCount(int num)
        {
            this.numStaticColumnsGen = SourceDSL.arbitrary().constant(num);
            return this;
        }

        public TableMetadataBuilder withStaticColumnsBetween(int min, int max)
        {
            this.numStaticColumnsGen = SourceDSL.integers().between(min, max);
            return this;
        }

        public TableMetadataBuilder withDefaultTypeGen(TypeGenBuilder typeGen)
        {
            this.defaultTypeGen = typeGen;
            return this;
        }

        public TableMetadataBuilder withoutEmpty()
        {
            defaultTypeGen.withoutEmpty();
            return this;
        }

        public TableMetadataBuilder withPrimaryColumnTypeGen(TypeGenBuilder typeGen)
        {
            withPartitionColumnTypeGen(typeGen);
            withClusteringColumnTypeGen(typeGen);
            return this;
        }

        public TableMetadataBuilder withPartitionColumnTypeGen(TypeGenBuilder typeGen)
        {
            this.partitionColTypeGen = typeGen;
            return this;
        }

        public TableMetadataBuilder withClusteringColumnTypeGen(TypeGenBuilder typeGen)
        {
            this.clusteringColTypeGen = typeGen;
            return this;
        }

        public TableMetadataBuilder withStaticColumnTypeGen(TypeGenBuilder typeGen)
        {
            this.staticColTypeGen = typeGen;
            return this;
        }

        public TableMetadataBuilder withRegularColumnTypeGen(TypeGenBuilder typeGen)
        {
            this.regularColTypeGen = typeGen;
            return this;
        }

        public TableMetadataBuilder withTableKinds(TableMetadata.Kind... kinds)
        {
            tableKindGen = SourceDSL.arbitrary().pick(kinds);
            return this;
        }

        public Gen<TableMetadata> build()
        {
            return rnd -> build(rnd);
        }

        public TableMetadata build(RandomnessSource rnd)
        {
            Gen<AbstractType<?>> partitionColTypeGen = withoutUnsafeEquality(new TypeGenBuilder(this.partitionColTypeGen != null ? this.partitionColTypeGen : defaultTypeGen)).build();
            Gen<AbstractType<?>> clusteringColTypeGen = withoutUnsafeEquality(new TypeGenBuilder(this.clusteringColTypeGen != null ? this.clusteringColTypeGen : defaultTypeGen)).build();
            Gen<AbstractType<?>> staticColTypeGen = (this.staticColTypeGen != null ? this.staticColTypeGen : defaultTypeGen).build();
            Gen<AbstractType<?>> regularColTypeGen = (this.regularColTypeGen != null ? this.regularColTypeGen : defaultTypeGen).build();

            String ks = ksNameGen.generate(rnd);
            AbstractTypeGenerators.overrideUDTKeyspace(ks);
            try
            {
                String tableName = tableNameGen.generate(rnd);
                TableParams params = paramsBuilder.build().generate(rnd);
                boolean isCounter = useCounter.generate(rnd);
                TableMetadata.Builder builder = TableMetadata.builder(ks, tableName, tableIdGen.generate(rnd))
                                                             .partitioner(partitionerGen.generate(rnd))
                                                             .kind(tableKindGen.generate(rnd))
                                                             .isCounter(isCounter)
                                                             .params(params);

                int numPartitionColumns = numPartitionColumnsGen.generate(rnd);
                int numClusteringColumns = numClusteringColumnsGen.generate(rnd);

                ColumnNameGen nameGen;
                if (columnNameGen != null)
                {
                    nameGen = columnNameGen;
                }
                else
                {
                    Set<String> createdColumnNames = new HashSet<>();
                    // filter for unique names
                    nameGen = (r, i1, i2) -> {
                        String str;
                        while (!createdColumnNames.add(str = IDENTIFIER_GEN.generate(r)))
                        {
                        }
                        return str;
                    };
                }
                for (int i = 0; i < numPartitionColumns; i++)
                    builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.PARTITION_KEY, i, nameGen, partitionColTypeGen, rnd));
                for (int i = 0; i < numClusteringColumns; i++)
                    builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.CLUSTERING, i, nameGen, clusteringColTypeGen, rnd));

                if (isCounter)
                {
                    builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.REGULAR, 0, nameGen, ignore -> CounterColumnType.instance, rnd));
                }
                else
                {
                    int numRegularColumns = numRegularColumnsGen.generate(rnd);
                    int numStaticColumns = numStaticColumnsGen.generate(rnd);
                    for (int i = 0; i < numStaticColumns; i++)
                        builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.STATIC, i, nameGen, staticColTypeGen, rnd));
                    for (int i = 0; i < numRegularColumns; i++)
                        builder.addColumn(createColumnDefinition(ks, tableName, ColumnMetadata.Kind.REGULAR, i, nameGen, regularColTypeGen, rnd));
                }
                return builder.build();
            }
            finally
            {
                AbstractTypeGenerators.clearUDTKeyspace();
            }
        }
    }

    public static Gen<ColumnMetadata> columnMetadataGen(Gen<ColumnMetadata.Kind> kindGen, Gen<AbstractType<?>> typeGen)
    {
        Gen<String> ksNameGen = CassandraGenerators.KEYSPACE_NAME_GEN;
        Gen<String> tableNameGen = IDENTIFIER_GEN;
        return rs -> {
            String ks = ksNameGen.generate(rs);
            String table = tableNameGen.generate(rs);
            ColumnMetadata.Kind kind = kindGen.generate(rs);
            return createColumnDefinition(ks, table, kind, 0, (r, i1, i2) -> IDENTIFIER_GEN.generate(r), typeGen, rs);
        };
    }

    public interface ColumnNameGen
    {
        String next(RandomnessSource rs, ColumnMetadata.Kind kind, int kindOffset);
    }

    private static ColumnMetadata createColumnDefinition(String ks, String table,
                                                         ColumnMetadata.Kind kind,
                                                         int kindOffset,
                                                         ColumnNameGen nameGen,
                                                         Gen<AbstractType<?>> typeGen,
                                                         RandomnessSource rnd)
    {
        switch (kind)
        {
            // partition and clustering keys require frozen types, so make sure all types generated will be frozen
            // empty type is also not supported, so filter out
            case PARTITION_KEY:
            case CLUSTERING:
                typeGen = Generators.filter(typeGen, t -> t != EmptyType.instance).map(AbstractType::freeze);
                break;
        }
        if (kind == ColumnMetadata.Kind.CLUSTERING)
        {
            // when working on a clustering column, add in reversed types periodically
            typeGen = allowReversed(typeGen);
        }
        String str = nameGen.next(rnd, kind, kindOffset);

        ColumnIdentifier name = new ColumnIdentifier(str, true);
        int position = !kind.isPrimaryKeyKind() ? -1 : kindOffset;
        AbstractType<?> type = typeGen.generate(rnd);
        return new ColumnMetadata(ks, table, name, type, position, kind, null);
    }

    public static Gen<ByteBuffer> partitionKeyDataGen(TableMetadata metadata)
    {
        ImmutableList<ColumnMetadata> columns = metadata.partitionKeyColumns();
        assert !columns.isEmpty() : "Unable to find partition key columns";
        if (columns.size() == 1)
            return getTypeSupport(columns.get(0).type).withoutEmptyData().bytesGen();
        List<Gen<ByteBuffer>> columnGens = new ArrayList<>(columns.size());
        for (ColumnMetadata cm : columns)
            columnGens.add(getTypeSupport(cm.type).bytesGen());
        return rnd -> {
            ByteBuffer[] buffers = new ByteBuffer[columnGens.size()];
            for (int i = 0; i < columnGens.size(); i++)
                buffers[i] = columnGens.get(i).generate(rnd);
            return CompositeType.build(ByteBufferAccessor.instance, buffers);
        };
    }

    public static Gen<ByteBuffer[]> data(TableMetadata metadata, @Nullable Gen<ValueDomain> valueDomainGen)
    {
        return new DataGeneratorBuilder(metadata).withValueDomain(valueDomainGen).build();
    }

    /**
     * Hacky workaround to make sure different generic MessageOut types can be used for {@link #MESSAGE_GEN}.
     */
    private static Gen<Message<?>> cast(Gen<? extends Message<?>> gen)
    {
        return (Gen<Message<?>>) gen;
    }

    /**
     * Java's type inferrence with chaining doesn't work well, so this is used to infer the root type early in cases
     * where javac can't figure it out
     */
    private static <T> Gen<T> gen(Gen<T> fn)
    {
        return fn;
    }

    /**
     * Uses reflection to generate a toString.  This method is aware of common Cassandra classes and can be used for
     * generators or tests to provide more details for debugging.
     */
    public static String toStringRecursive(Object o)
    {
        return ReflectionToStringBuilder.toString(o, new MultilineRecursiveToStringStyle()
        {
            private String spacer = "";

            {
                // common lang uses start/end chars that are not the common ones used, so switch to the common ones
                setArrayStart("[");
                setArrayEnd("]");
                setContentStart("{");
                setContentEnd("}");
                setUseIdentityHashCode(false);
                setUseShortClassName(true);
            }

            protected boolean accept(Class<?> clazz)
            {
                return !clazz.isEnum() // toString enums
                       && Stream.of(clazz.getDeclaredFields()).anyMatch(f -> !Modifier.isStatic(f.getModifiers())); // if no fields, just toString
            }

            public void appendDetail(StringBuffer buffer, String fieldName, Object value)
            {
                if (value instanceof ByteBuffer)
                {
                    value = ByteBufferUtil.bytesToHex((ByteBuffer) value);
                }
                else if (value instanceof AbstractType)
                {
                    value = SchemaCQLHelper.toCqlType((AbstractType) value);
                }
                else if (value instanceof Token || value instanceof InetAddressAndPort || value instanceof FieldIdentifier)
                {
                    value = value.toString();
                }
                else if (value instanceof TableMetadata)
                {
                    // to make sure the correct indents are taken, convert to CQL, then replace newlines with the indents
                    // then prefix with the indents.
                    String cql = SchemaCQLHelper.getTableMetadataAsCQL((TableMetadata) value, null);
                    cql = NEWLINE_PATTERN.matcher(cql).replaceAll(Matcher.quoteReplacement("\n  " + spacer));
                    cql = "\n  " + spacer + cql;
                    value = cql;
                }
                super.appendDetail(buffer, fieldName, value);
            }

            // MultilineRecursiveToStringStyle doesn't look at what was set and instead hard codes the values when it "resets" the level
            protected void setArrayStart(String arrayStart)
            {
                super.setArrayStart(arrayStart.replace("{", "["));
            }

            protected void setArrayEnd(String arrayEnd)
            {
                super.setArrayEnd(arrayEnd.replace("}", "]"));
            }

            protected void setContentStart(String contentStart)
            {
                // use this to infer the spacer since it isn't exposed.
                String[] split = contentStart.split("\n", 2);
                spacer = split.length == 2 ? split[1] : "";
                super.setContentStart(contentStart.replace("[", "{"));
            }

            protected void setContentEnd(String contentEnd)
            {
                super.setContentEnd(contentEnd.replace("]", "}"));
            }
        }, true);
    }

    public static Gen<Token> murmurToken()
    {
        Constraint token = Constraint.between(Long.MIN_VALUE, Long.MAX_VALUE);
        return rs -> new Murmur3Partitioner.LongToken(rs.next(token));
    }

    public static Gen<Token> murmurTokenIn(Range<Token> range)
    {
        // left exclusive, right inclusive
        if (range.isWrapAround())
        {
            List<Range<Token>> unwrap = range.unwrap();
            return rs -> {
                Range<Token> subRange = unwrap.get(Math.toIntExact(rs.next(Constraint.between(0, unwrap.size() - 1))));
                long end = ((Murmur3Partitioner.LongToken) subRange.right).token;
                if (end == Long.MIN_VALUE)
                    end = Long.MAX_VALUE;
                Constraint token = Constraint.between(((Murmur3Partitioner.LongToken) subRange.left).token + 1, end);
                return new Murmur3Partitioner.LongToken(rs.next(token));
            };
        }
        else
        {
            Constraint token = Constraint.between(((Murmur3Partitioner.LongToken) range.left).token + 1, ((Murmur3Partitioner.LongToken) range.right).token);
            return rs -> new Murmur3Partitioner.LongToken(rs.next(token));
        }
    }

    public static Gen<Token> byteOrderToken()
    {
        // empty token only happens if partition key is byte[0], which isn't allowed
        Constraint size = Constraint.between(1, 10);
        Constraint byteRange = Constraint.between(Byte.MIN_VALUE, Byte.MAX_VALUE);
        return rs -> {
            byte[] token = new byte[Math.toIntExact(rs.next(size))];
            for (int i = 0; i < token.length; i++)
                token[i] = (byte) rs.next(byteRange);
            return new ByteOrderedPartitioner.BytesToken(token);
        };
    }

    public static Gen<Token> randomPartitionerToken()
    {
        // valid range is -1 -> 2^127
        Constraint domain = Constraint.between(-1, Long.MAX_VALUE);
        // TODO (coverage): handle the range [2^63-1, 2^127]
        return rs -> new RandomPartitioner.BigIntegerToken(BigInteger.valueOf(rs.next(domain)));
    }

    public static Gen<Token> localPartitionerToken(LocalPartitioner partitioner)
    {
        Gen<ByteBuffer> bytes = AbstractTypeGenerators.getTypeSupport(partitioner.getTokenValidator()).bytesGen();
        return rs -> partitioner.getToken(bytes.generate(rs));
    }

    public static Gen<LocalPartitioner> localPartitioner()
    {
        return AbstractTypeGenerators.safeTypeGen().map(LocalPartitioner::new);
    }

    public static Gen<LocalCompositePrefixPartitioner> localCompositePrefixPartitioner()
    {
        return AbstractTypeGenerators.safeTypeGen().map(type -> {
            if (type instanceof CompositeType)
                return new LocalCompositePrefixPartitioner((CompositeType) type);
            else
                return new LocalCompositePrefixPartitioner(type);
        });
    }

    public static Gen<Token> localPartitionerToken()
    {
        var lpGen = localPartitioner();
        return rs -> {
            var lp = lpGen.generate(rs);
            var bytes = AbstractTypeGenerators.getTypeSupport(lp.getTokenValidator()).bytesGen();
            return lp.getToken(bytes.generate(rs));
        };
    }

    public static Gen<Token> localCompositePrefixPartitionerToken(LocalCompositePrefixPartitioner partitioner)
    {
        return rs -> {
            var bytes = AbstractTypeGenerators.getTypeSupport(partitioner.getTokenValidator()).bytesGen();
            return partitioner.getToken(bytes.generate(rs));
        };
    }

    public static Gen<Token> reversedLongLocalToken()
    {
        Constraint range = Constraint.between(0, Long.MAX_VALUE);
        return rs -> new ReversedLongLocalPartitioner.ReversedLongLocalToken(rs.next(range));
    }

    public static Gen<ByteBuffer> reversedLongLocalKeys()
    {
        Constraint range = Constraint.between(0, Long.MAX_VALUE);
        return rs -> {
            long value = rs.next(range);
            return ByteBufferUtil.bytes(value);
        };
    }

    public static Gen<Token> orderPreservingToken()
    {
        // empty token only happens if partition key is byte[0], which isn't allowed
        Gen<String> string = Generators.utf8(1, 10);
        return rs -> new OrderPreservingPartitioner.StringToken(string.generate(rs));
    }

    public static Gen<Token> tokensInRange(Range<Token> range)
    {
        IPartitioner partitioner = range.left.getPartitioner();
        if (partitioner instanceof Murmur3Partitioner) return murmurTokenIn(range);
        throw new UnsupportedOperationException("Unsupported partitioner: " + partitioner.getClass());
    }

    private enum SupportedPartitioners
    {
        Murmur(Murmur3Partitioner.class,                                ignore -> Murmur3Partitioner.instance),
        ByteOrdered(ByteOrderedPartitioner.class,                       ignore -> ByteOrderedPartitioner.instance),
        Random(RandomPartitioner.class,                                 ignore -> RandomPartitioner.instance),
        Local(LocalPartitioner.class,                                   localPartitioner()),
        OrderPreserving(OrderPreservingPartitioner.class,               ignore -> OrderPreservingPartitioner.instance),
        LocalCompositePrefix(LocalCompositePrefixPartitioner.class,     localCompositePrefixPartitioner());

        private final Class<? extends IPartitioner> clazz;
        private final Gen<? extends IPartitioner> partitioner;

        <T extends IPartitioner> SupportedPartitioners(Class<T> clazz, Gen<T> partitionerGen)
        {
            this.clazz = clazz;
            partitioner = partitionerGen;
        }

        public Gen<? extends IPartitioner> partitioner()
        {
            return partitioner;
        }

        public static Set<Class<? extends IPartitioner>> knownPartitioners()
        {
            ImmutableSet.Builder<Class<? extends IPartitioner>> builder = ImmutableSet.builder();
            for (SupportedPartitioners p : values())
                builder.add(p.clazz);
            return builder.build();
        }
    }

    public static Set<Class<? extends IPartitioner>> knownPartitioners()
    {
        return SupportedPartitioners.knownPartitioners();
    }

    public static Gen<IPartitioner> partitioners()
    {
        return SourceDSL.arbitrary().enumValues(SupportedPartitioners.class)
                        .flatMap(SupportedPartitioners::partitioner);
    }


    public static Gen<IPartitioner> nonLocalPartitioners()
    {
        return SourceDSL.arbitrary().enumValues(SupportedPartitioners.class)
                        .assuming(p -> p != SupportedPartitioners.Local &&
                                       p != SupportedPartitioners.LocalCompositePrefix)
                        .flatMap(SupportedPartitioners::partitioner);
    }

    /**
     * For {@link LocalPartitioner} it can have a very complex type which can lead to generating data larger than
     * allowed in a primary key.  If a test needs to filter out those cases, can just
     * {@code .map(CassandraGenerators::simplify)} to resolve.
     */
    public static IPartitioner simplify(IPartitioner partitioner)
    {
        // serializers require tokens to fit within 1 << 16, but that makes the test flakey when LocalPartitioner with a nested type is found...
        if (!(partitioner instanceof LocalPartitioner)) return partitioner;
        if (!shouldSimplify(partitioner.getTokenValidator())) return partitioner;
        return new LocalPartitioner(Int32Type.instance);
    }

    private static boolean shouldSimplify(AbstractType<?> type)
    {
        return AbstractTypeGenerators.contains(type, t -> t.isCollection());
    }

    public static Gen<Token> token()
    {
        return partitioners().flatMap(CassandraGenerators::token);
    }

    public static Gen<Token> token(IPartitioner partitioner)
    {
        if (partitioner instanceof Murmur3Partitioner) return murmurToken();
        if (partitioner instanceof ByteOrderedPartitioner) return byteOrderToken();
        if (partitioner instanceof RandomPartitioner) return randomPartitionerToken();
        if (partitioner instanceof LocalCompositePrefixPartitioner) return localCompositePrefixPartitionerToken((LocalCompositePrefixPartitioner) partitioner);
        if (partitioner instanceof LocalPartitioner) return localPartitionerToken((LocalPartitioner) partitioner);
        if (partitioner instanceof OrderPreservingPartitioner) return orderPreservingToken();
        throw new UnsupportedOperationException("Unsupported partitioner: " + partitioner.getClass());
    }

    public static Gen<? extends Collection<Token>> tokens(IPartitioner partitioner)
    {
        Gen<Token> tokenGen = token(partitioner);
        return SourceDSL.lists().of(tokenGen).ofSizeBetween(1, 16);
    }

    public static Gen<HeartBeatState> heartBeatStates()
    {
        Constraint generationDomain = Constraint.between(0, Integer.MAX_VALUE);
        Constraint versionDomain = Constraint.between(-1, Integer.MAX_VALUE);
        return rs -> new HeartBeatState(Math.toIntExact(rs.next(generationDomain)), Math.toIntExact(rs.next(versionDomain)));
    }

    private static Gen<Map<ApplicationState, VersionedValue>> gossipApplicationStates()
    {
        //TODO support all application states...
        // atm only used by a single test, which only looks at status
        Gen<Boolean> statusWithPort = SourceDSL.booleans().all();
        Gen<VersionedValue> statusGen = gossipStatusValue();

        return rs -> {
            ApplicationState statusState = statusWithPort.generate(rs) ? ApplicationState.STATUS_WITH_PORT : ApplicationState.STATUS;
            VersionedValue vv = statusGen.generate(rs);
            if (vv == null) return ImmutableMap.of();
            return ImmutableMap.of(statusState, vv);
        };
    }

    private static Gen<String> gossipStatus()
    {
        return SourceDSL.arbitrary()
                        .pick(VersionedValue.STATUS_NORMAL,
                              VersionedValue.STATUS_BOOTSTRAPPING_REPLACE,
                              VersionedValue.STATUS_BOOTSTRAPPING,
                              VersionedValue.STATUS_MOVING,
                              VersionedValue.STATUS_LEAVING,
                              VersionedValue.STATUS_LEFT,

                              //TODO would be good to prefix with STATUS_ like others
                              VersionedValue.REMOVING_TOKEN,
                              VersionedValue.REMOVED_TOKEN,
                              VersionedValue.HIBERNATE + VersionedValue.DELIMITER + true,
                              VersionedValue.HIBERNATE + VersionedValue.DELIMITER + false,
                              VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + true,
                              VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + false,
                              ""
                        );
    }

    private static Gen<VersionedValue> gossipStatusValue()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Gen<String> statusGen = gossipStatus();
        Gen<Token> tokenGen = token(partitioner);
        Gen<? extends Collection<Token>> tokensGen = tokens(partitioner);
        Gen<InetAddress> addressGen = Generators.INET_ADDRESS_GEN;
        Gen<InetAddressAndPort> addressAndGenGen = INET_ADDRESS_AND_PORT_GEN;
        Gen<Boolean> bool = SourceDSL.booleans().all();
        Constraint millis = Constraint.between(0, Long.MAX_VALUE);
        Constraint version = Constraint.between(0, Integer.MAX_VALUE);
        Gen<UUID> hostId = Generators.UUID_RANDOM_GEN;
        VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(partitioner);
        return rs -> {
            String status = statusGen.generate(rs);
            switch (status)
            {
                case "":
                    return null;
                case VersionedValue.STATUS_NORMAL:
                    return factory.normal(tokensGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.STATUS_BOOTSTRAPPING:
                    return factory.bootstrapping(tokensGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.STATUS_BOOTSTRAPPING_REPLACE:
                    if (bool.generate(rs)) return factory.bootReplacingWithPort(addressAndGenGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                    else return factory.bootReplacing(addressGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.STATUS_MOVING:
                    return factory.moving(tokenGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.STATUS_LEAVING:
                    return factory.leaving(tokensGen.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.STATUS_LEFT:
                    return factory.left(tokensGen.generate(rs), rs.next(millis)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.REMOVING_TOKEN:
                    return factory.removingNonlocal(hostId.generate(rs)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.REMOVED_TOKEN:
                    return factory.removedNonlocal(hostId.generate(rs), rs.next(millis)).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.HIBERNATE + VersionedValue.DELIMITER + true:
                    return factory.hibernate(true).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.HIBERNATE + VersionedValue.DELIMITER + false:
                    return factory.hibernate(false).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + true:
                    return factory.shutdown(true).withVersion(Math.toIntExact(rs.next(version)));
                case VersionedValue.SHUTDOWN + VersionedValue.DELIMITER + false:
                    return factory.shutdown(false).withVersion(Math.toIntExact(rs.next(version)));
                default:
                    throw new AssertionError("Unexpected status: " + status);
            }
        };
    }

    public static Gen<EndpointState> endpointStates()
    {
        Gen<HeartBeatState> hbGen = heartBeatStates();
        Gen<Map<ApplicationState, VersionedValue>> appStates = gossipApplicationStates();
        Gen<Boolean> alive = SourceDSL.booleans().all();
        Constraint updateTimestamp = Constraint.between(0, Long.MAX_VALUE);
        return rs -> {
            EndpointState state = new EndpointState(hbGen.generate(rs));
            Map<ApplicationState, VersionedValue> map = appStates.generate(rs);
            if (!map.isEmpty()) state.addApplicationStates(map);
            if (alive.generate(rs)) state.markAlive();
            else state.markDead();
            state.unsafeSetUpdateTimestamp(rs.next(updateTimestamp));
            return state;
        };
    }

    public static Gen<Duration> duration()
    {
        Constraint ints = Constraint.between(0, Integer.MAX_VALUE);
        Constraint longs = Constraint.between(0, Long.MAX_VALUE);
        Gen<Boolean> neg = SourceDSL.booleans().all();
        return rnd -> {
            int months = (int) rnd.next(ints);
            int days = (int) rnd.next(ints);
            long nanoseconds = rnd.next(longs);
            if (neg.generate(rnd))
            {
                months = -1 * months;
                days = -1 * days;
                nanoseconds = -1 * nanoseconds;
            }
            return Duration.newInstance(months, days, nanoseconds);
        };
    }

    public static Gen<DecoratedKey> decoratedKeys()
    {
        return decoratedKeys(partitioners(), Generators.bytes(0, 100));
    }

    public static Gen<DecoratedKey> decoratedKeys(Gen<IPartitioner> partitionerGen)
    {
        return decoratedKeys(partitionerGen, Generators.bytes(0, 100));
    }

    public static Gen<DecoratedKey> decoratedKeys(Gen<IPartitioner> partitionerGen, Gen<ByteBuffer> keyGen)
    {
        return rs -> {
            IPartitioner partitioner = partitionerGen.generate(rs);
            Gen<ByteBuffer> valueGen = keyGen;
            if (partitioner instanceof LocalPartitioner)
            {
                LocalPartitioner lp = (LocalPartitioner) partitioner;
                valueGen = AbstractTypeGenerators.getTypeSupport(lp.getTokenValidator()).bytesGen();
            }
            else if (partitioner instanceof ReversedLongLocalPartitioner)
            {
                valueGen = reversedLongLocalKeys();
            }
            return partitioner.decorateKey(valueGen.generate(rs));
        };
    }

    public static void visitUDTs(TableMetadata metadata, Consumer<UserType> fn)
    {
        Set<UserType> udts = CassandraGenerators.extractUDTs(metadata);
        if (!udts.isEmpty())
        {
            Deque<UserType> pending = new ArrayDeque<>(udts);
            Set<ByteBuffer> visited = new HashSet<>();
            while (!pending.isEmpty())
            {
                UserType next = pending.poll();
                Set<UserType> subTypes = AbstractTypeGenerators.extractUDTs(next);
                subTypes.remove(next); // it includes self
                if (subTypes.isEmpty() || subTypes.stream().allMatch(t -> visited.contains(t.name)))
                {
                    fn.accept(next);
                    visited.add(next.name);
                }
                else
                {
                    pending.add(next);
                }
            }
        }
    }

    public static class DataGeneratorBuilder
    {
        private final TableMetadata metadata;
        @Nullable
        private Gen<ValueDomain> valueDomainGen = null;

        public DataGeneratorBuilder(TableMetadata metadata)
        {
            this.metadata = metadata;
        }

        public DataGeneratorBuilder withValueDomain(@Nullable Gen<ValueDomain> valueDomainGen)
        {
            this.valueDomainGen = valueDomainGen;
            return this;
        }

        public Gen<Gen<ByteBuffer[]>> build(Gen<Integer> numUniqPartitionsGen)
        {
            AbstractTypeGenerators.TypeSupport<?>[] types = typeSupport();
            return rnd -> {
                int numPartitions = numUniqPartitionsGen.generate(rnd);
                Set<List<ByteBuffer>> partitions = Sets.newHashSetWithExpectedSize(numPartitions);
                int partitionColumns = metadata.partitionKeyColumns().size();
                for (int i = 0; i < numPartitions; i++)
                {
                    List<ByteBuffer> pk = new ArrayList<>(partitionColumns);
                    int attempts = 0;
                    do
                    {
                        attempts++;
                        pk.clear();
                        for (int c = 0; c < partitionColumns; c++)
                            pk.add(types[c].bytesGen().generate(rnd));
                    }
                    while (!partitions.add(pk) && attempts < 42);
                }
                List<List<ByteBuffer>> deterministicOrder = new ArrayList<>(partitions);
                deterministicOrder.sort((a, b) -> {
                    int rc = 0;
                    for (int i = 0; i < a.size(); i++)
                    {
                        rc = a.get(i).compareTo(b.get(i));
                        if (rc != 0) return rc;
                    }
                    return rc;
                });

                Gen<List<ByteBuffer>> pkGen = SourceDSL.arbitrary().pick(deterministicOrder);

                return next -> {
                    // select partition
                    List<ByteBuffer> pk = pkGen.generate(next);
                    // generate rest
                    ByteBuffer[] row = new ByteBuffer[types.length];
                    for (int i = 0; i < pk.size(); i++)
                        row[i] = pk.get(i);

                    for (int i = partitionColumns; i < row.length; i++)
                        row[i] = types[i].bytesGen().generate(rnd);
                    return row;
                };
            };
        }

        public Gen<ByteBuffer[]> build()
        {
            AbstractTypeGenerators.TypeSupport<?>[] types = typeSupport();
            return rnd -> {
                ByteBuffer[] row = new ByteBuffer[types.length];
                for (int i = 0; i < row.length; i++)
                    row[i] = types[i].bytesGen().generate(rnd);
                return row;
            };
        }

        private AbstractTypeGenerators.TypeSupport<?>[] typeSupport()
        {
            AbstractTypeGenerators.TypeSupport<?>[] types = new AbstractTypeGenerators.TypeSupport[metadata.columns().size()];
            Iterator<ColumnMetadata> it = metadata.allColumnsInSelectOrder();
            int partitionColumns = metadata.partitionKeyColumns().size();
            int clusteringColumns = metadata.clusteringColumns().size();
            int primaryKeyColumns = partitionColumns + clusteringColumns;
            for (int i = 0; it.hasNext(); i++)
            {
                ColumnMetadata col = it.next();
                types[i] = AbstractTypeGenerators.getTypeSupportWithNulls(col.type, i < partitionColumns ? null : valueDomainGen);
                if (i < partitionColumns)
                    types[i] = types[i].withoutEmptyData();
                if (i >= partitionColumns && i < primaryKeyColumns)
                    // clustering doesn't allow null...
                    types[i] = types[i].mapBytes(b -> b == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : b);
            }
            return types;
        }
    }

    private enum EpochConstants { FIRST, EMPTY, UPGRADE_STARTUP, UPGRADE_GOSSIP}
    public static Gen<Epoch> epochs()
    {
        return rnd -> {
            if (SourceDSL.booleans().all().generate(rnd))
            {
                switch (SourceDSL.arbitrary().enumValues(EpochConstants.class).generate(rnd))
                {
                    case FIRST: return Epoch.FIRST;
                    case EMPTY: return Epoch.EMPTY;
                    case UPGRADE_STARTUP: return Epoch.UPGRADE_STARTUP;
                    case UPGRADE_GOSSIP: return Epoch.UPGRADE_GOSSIP;
                    default: throw new UnsupportedOperationException();
                }
            }

            return Epoch.create(SourceDSL.longs().between(2, Long.MAX_VALUE).generate(rnd));
        };
    }

    public static Gen<Node.Id> accordNodeId()
    {
        return SourceDSL.integers().between(0, Integer.MAX_VALUE).map(Node.Id::new);
    }

    public static Gen<AccordStaleReplicas> accordStaleReplicas()
    {
        Gen<Set<Node.Id>> staleIdsGen = Generators.set(accordNodeId(), SourceDSL.integers().between(0, 10));
        Gen<Epoch> epochGen = epochs();
        return rnd -> new AccordStaleReplicas(staleIdsGen.generate(rnd), epochGen.generate(rnd));
    }

    public static Gen<AccordFastPath> accordFastPath()
    {
        Gen<List<Node.Id>> nodesGen = Generators.uniqueList(accordNodeId(), SourceDSL.integers().between(0, 10));
        Gen<AccordFastPath.Status> statusGen = SourceDSL.arbitrary().enumValues(AccordFastPath.Status.class);
        Gen<Long> updateTimeMillis = TIMESTAMP_NANOS.map(TimeUnit.NANOSECONDS::toMillis);
        Gen<Long> updateDelayMillis = SourceDSL.longs().between(0, TimeUnit.HOURS.toMillis(2));
        return rnd -> {
            AccordFastPath accum = AccordFastPath.EMPTY;
            for (Node.Id node : nodesGen.generate(rnd))
            {
                AccordFastPath.Status status = statusGen.generate(rnd);
                // can't add a NORMAL node that doesn't exist, it must be ab-NORMAL first...
                if (status == AccordFastPath.Status.NORMAL)
                    accum = accum.withNodeStatusSince(node, AccordFastPath.Status.UNAVAILABLE, 0, 0);
                accum = accum.withNodeStatusSince(node, status, updateTimeMillis.generate(rnd), updateDelayMillis.generate(rnd));
            }
            return accum;
        };
    }

    public static class ClusterMetadataBuilder
    {
        private Gen<Epoch> epochGen = epochs();
        private Gen<IPartitioner> partitionerGen = nonLocalPartitioners();
        private Gen<AccordStaleReplicas> accordStaleReplicasGen = accordStaleReplicas();
        private Gen<AccordFastPath> accordFastPathGen = accordFastPath();
        public Gen<ClusterMetadata> build()
        {
            return rnd -> {
                Epoch epoch = epochGen.generate(rnd);
                IPartitioner partitioner = partitionerGen.generate(rnd);
                Directory directory = Directory.EMPTY;
                DistributedSchema schema = DistributedSchema.first(directory.knownDatacenters());
                TokenMap tokenMap = new TokenMap(partitioner);
                DataPlacements placements = DataPlacements.EMPTY;
                AccordFastPath accordFastPath = accordFastPathGen.generate(rnd);
                LockedRanges lockedRanges = LockedRanges.EMPTY;
                InProgressSequences inProgressSequences = InProgressSequences.EMPTY;
                ConsensusMigrationState consensusMigrationState = ConsensusMigrationState.EMPTY;
                Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions = ImmutableMap.of();
                AccordStaleReplicas accordStaleReplicas = accordStaleReplicasGen.generate(rnd);
                return new ClusterMetadata(epoch, partitioner, schema, directory, tokenMap, placements, accordFastPath, lockedRanges, inProgressSequences, consensusMigrationState, extensions, accordStaleReplicas);
            };
        }
    }
}
