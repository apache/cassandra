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

package org.apache.cassandra.repair;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.cassandra.config.UnitConfigOverride;
import org.junit.Before;
import org.junit.BeforeClass;

import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.apache.cassandra.concurrent.ExecutorBuilder;
import org.apache.cassandra.concurrent.ExecutorBuilderFactory;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.SimulatedExecutorFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.ICompactionManager;
import org.apache.cassandra.db.marshal.EmptyType;
import org.apache.cassandra.db.repair.CassandraTableRepairManager;
import org.apache.cassandra.db.repair.PendingAntiCompaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.IGossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.ConnectionType;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.ValidationResponse;
import org.apache.cassandra.repair.state.Completable;
import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.repair.state.JobState;
import org.apache.cassandra.repair.state.SessionState;
import org.apache.cassandra.repair.state.ValidationState;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamReceiveException;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.StreamingDataInputPlus;
import org.apache.cassandra.tools.nodetool.Repair;
import org.apache.cassandra.utils.AbstractTypeGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.FailingBiConsumer;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.config.CassandraRelevantProperties.CLOCK_GLOBAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;

public abstract class FuzzTestBase extends CQLTester.InMemory
{
    private static final int MISMATCH_NUM_PARTITIONS = 1;
    private static final Gen<String> IDENTIFIER_GEN = fromQT(Generators.IDENTIFIER_GEN);
    private static final Gen<String> KEYSPACE_NAME_GEN = fromQT(CassandraGenerators.KEYSPACE_NAME_GEN);
    private static final Gen<TableId> TABLE_ID_GEN = fromQT(CassandraGenerators.TABLE_ID_GEN);
    private static final Gen<InetAddressAndPort> ADDRESS_W_PORT = fromQT(CassandraGenerators.INET_ADDRESS_AND_PORT_GEN);

    private static boolean SETUP_SCHEMA = false;
    static String KEYSPACE;
    static List<String> TABLES;

    @BeforeClass
    public static void setUpClass()
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
        CLOCK_GLOBAL.setString(ClockAccess.class.getName());
        // when running in CI an external actor will replace the test configs based off the test type (such as trie, cdc, etc.), this could then have failing tests
        // that do not repo with the same seed!  To fix that, go to UnitConfigOverride and update the config type to match the one that failed in CI, this should then
        // use the same config, so the seed should not reproduce.
        UnitConfigOverride.maybeOverrideConfig();

        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance); // TOOD (coverage): random select
        DatabaseDescriptor.setLocalDataCenter("test");
        StreamingChannel.Factory.Global.unsafeSet(new StreamingChannel.Factory()
        {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public StreamingChannel create(InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind) throws IOException
            {
                StreamingChannel mock = Mockito.mock(StreamingChannel.class);
                int id = counter.incrementAndGet();
                StreamSession session = Mockito.mock(StreamSession.class);
                StreamReceiveException access = new StreamReceiveException(session, "mock access rejected");
                StreamingDataInputPlus input = Mockito.mock(StreamingDataInputPlus.class, invocationOnMock -> {
                    throw access;
                });
                Mockito.doNothing().when(input).close();
                Mockito.when(mock.in()).thenReturn(input);
                Mockito.when(mock.id()).thenReturn(id);
                Mockito.when(mock.peer()).thenReturn(to);
                Mockito.when(mock.connectedTo()).thenReturn(to);
                Mockito.when(mock.send(Mockito.any())).thenReturn(ImmediateFuture.success(null));
                Mockito.when(mock.close()).thenReturn(ImmediateFuture.success(null));
                return mock;
            }
        });
        ExecutorFactory delegate = ExecutorFactory.Global.executorFactory();
        ExecutorFactory.Global.unsafeSet(new ExecutorFactory()
        {
            @Override
            public LocalAwareSubFactory localAware()
            {
                return delegate.localAware();
            }

            @Override
            public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority, SimulatorSemantics simulatorSemantics)
            {
                return delegate.scheduled(executeOnShutdown, name, priority, simulatorSemantics);
            }

            private boolean shouldMock()
            {
                return StackWalker.getInstance().walk(frame -> {
                    StackWalker.StackFrame caller = frame.skip(3).findFirst().get();
                    return caller.getClassName().startsWith("org.apache.cassandra.streaming.");
                });
            }

            @Override
            public Thread startThread(String name, Runnable runnable, InfiniteLoopExecutor.Daemon daemon)
            {
                if (shouldMock()) return new Thread();
                return delegate.startThread(name, runnable, daemon);
            }

            @Override
            public Interruptible infiniteLoop(String name, Interruptible.Task task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe, InfiniteLoopExecutor.Daemon daemon, InfiniteLoopExecutor.Interrupts interrupts)
            {
                return delegate.infiniteLoop(name, task, simulatorSafe, daemon, interrupts);
            }

            @Override
            public ThreadGroup newThreadGroup(String name)
            {
                return delegate.newThreadGroup(name);
            }

            @Override
            public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmx(String jmxPath)
            {
                return delegate.withJmx(jmxPath);
            }

            @Override
            public ExecutorBuilder<? extends SequentialExecutorPlus> configureSequential(String name)
            {
                return delegate.configureSequential(name);
            }

            @Override
            public ExecutorBuilder<? extends ExecutorPlus> configurePooled(String name, int threads)
            {
                return delegate.configurePooled(name, threads);
            }
        });

        // will both make sure this is loaded and used
        if (!(Clock.Global.clock() instanceof ClockAccess)) throw new IllegalStateException("Unable to override clock");

        // set the repair rcp timeout high so we don't hit it... this class is mostly testing repair reaching success
        // so don't want to deal with unlucky histories...
        DatabaseDescriptor.setRepairRpcTimeout(TimeUnit.DAYS.toMillis(1));


        InMemory.setUpClass();
    }

    @Before
    public void setupSchema()
    {
        if (SETUP_SCHEMA) return;
        SETUP_SCHEMA = true;
        // StorageService can not be mocked out, nor can ColumnFamilyStores, so make sure that the keyspace is a "local" keyspace to avoid replication as the peers don't actually exist for replication
        schemaChange(String.format("CREATE KEYSPACE %s WITH REPLICATION = {'class': '%s'}", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, HackStrat.class.getName()));
        for (TableMetadata table : SystemDistributedKeyspace.metadata().tables)
            schemaChange(table.toCqlString(false, false));

        createSchema();
    }

    protected void cleanupRepairTables()
    {
        for (String table : Arrays.asList(SystemKeyspace.REPAIRS))
            execute(String.format("TRUNCATE %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, table));
    }

    private void createSchema()
    {
        // The main reason to use random here with a fixed seed is just to have a set of tables that are not hard coded.
        // The tables will have diversity to them that most likely doesn't matter to repair (hence why the tables are shared), but
        // is useful just in case some assumptions change.
        RandomSource rs = new DefaultRandom(42);
        String ks = KEYSPACE_NAME_GEN.next(rs);
        List<String> tableNames = Gens.lists(IDENTIFIER_GEN).unique().ofSizeBetween(10, 100).next(rs);
        JavaRandom qt = new JavaRandom(rs.asJdkRandom());
        Tables.Builder tableBuilder = Tables.builder();
        List<TableId> ids = Gens.lists(TABLE_ID_GEN).unique().ofSize(tableNames.size()).next(rs);
        for (int i = 0; i < tableNames.size(); i++)
        {
            String name = tableNames.get(i);
            TableId id = ids.get(i);
            TableMetadata tableMetadata = new CassandraGenerators.TableMetadataBuilder().withKeyspaceName(ks).withTableName(name).withTableId(id).withTableKinds(TableMetadata.Kind.REGULAR)
                                                                                        // shouldn't matter, just wanted to avoid UDT as that needs more setup
                                                                                        .withDefaultTypeGen(AbstractTypeGenerators.builder().withTypeKinds(AbstractTypeGenerators.TypeKind.PRIMITIVE).withoutPrimitive(EmptyType.instance).build()).build().generate(qt);
            tableBuilder.add(tableMetadata);
        }
        KeyspaceParams params = KeyspaceParams.simple(3);
        KeyspaceMetadata metadata = KeyspaceMetadata.create(ks, params, tableBuilder.build());

        // create
        schemaChange(metadata.toCqlString(false, false));
        KEYSPACE = ks;
        for (TableMetadata table : metadata.tables)
            schemaChange(table.toCqlString(false, false));
        TABLES = tableNames;
    }

    static void enableMessageFaults(Cluster cluster)
    {
        cluster.allowedMessageFaults(new BiFunction<>()
        {
            private final LongHashSet noFaults = new LongHashSet();
            private final LongHashSet allowDrop = new LongHashSet();

            @Override
            public Set<Faults> apply(Cluster.Node node, Message<?> message)
            {
                if (RepairMessage.ALLOWS_RETRY.contains(message.verb()))
                {
                    allowDrop.add(message.id());
                    return Faults.DROPPED;
                }
                switch (message.verb())
                {
                    // these messages are not resilent to ephemeral issues
                    case STATUS_REQ:
                    case STATUS_RSP:
                        noFaults.add(message.id());
                        return Faults.NONE;
                    default:
                        if (noFaults.contains(message.id())) return Faults.NONE;
                        if (allowDrop.contains(message.id())) return Faults.DROPPED;
                        // was a new message added and the test not updated?
                        IllegalStateException e = new IllegalStateException("Verb: " + message.verb());
                        cluster.failures.add(e);
                        throw e;
                }
            }
        });
    }

    static void runAndAssertSuccess(Cluster cluster, int example, boolean shouldSync, RepairCoordinator repair)
    {
        cluster.processAll();
        assertSuccess(example, shouldSync, repair);
    }

    static void assertSuccess(int example, boolean shouldSync, RepairCoordinator repair)
    {
        Completable.Result result = repair.state.getResult();
        Assertions.assertThat(result)
                  .describedAs("Expected repair to have completed with success, but is still running... %s; example %d", repair.state, example).isNotNull()
                  .describedAs("Unexpected state: %s -> %s; example %d", repair.state, result, example).isEqualTo(Completable.Result.success(repairSuccessMessage(repair)));
        Assertions.assertThat(repair.state.getStateTimesMillis().keySet()).isEqualTo(EnumSet.allOf(CoordinatorState.State.class));
        Assertions.assertThat(repair.state.getSessions()).isNotEmpty();
        boolean shouldSnapshot = repair.state.options.getParallelism() != RepairParallelism.PARALLEL
                                 && (!repair.state.options.isIncremental() || repair.state.options.isPreview());
        for (SessionState session : repair.state.getSessions())
        {
            Assertions.assertThat(session.getStateTimesMillis().keySet()).isEqualTo(EnumSet.allOf(SessionState.State.class));
            Assertions.assertThat(session.getJobs()).isNotEmpty();
            for (JobState job : session.getJobs())
            {
                EnumSet<JobState.State> expected = EnumSet.allOf(JobState.State.class);
                if (!shouldSnapshot)
                {
                    expected.remove(JobState.State.SNAPSHOT_START);
                    expected.remove(JobState.State.SNAPSHOT_COMPLETE);
                }
                if (!shouldSync)
                {
                    expected.remove(JobState.State.STREAM_START);
                }
                Set<JobState.State> actual = job.getStateTimesMillis().keySet();
                Assertions.assertThat(actual).isEqualTo(expected);
            }
        }
    }

    static String repairSuccessMessage(RepairCoordinator repair)
    {
        RepairOption options = repair.state.options;
        if (options.isPreview())
        {
            String suffix;
            switch (options.getPreviewKind())
            {
                case UNREPAIRED:
                case ALL:
                    suffix = "Previewed data was in sync";
                    break;
                case REPAIRED:
                    suffix = "Repaired data is in sync";
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected preview repair kind: " + options.getPreviewKind());
            }
            return "Repair preview completed successfully; " + suffix;
        }
        return "Repair completed successfully";
    }

    InetAddressAndPort pickParticipant(RandomSource rs, Cluster.Node coordinator, RepairCoordinator repair)
    {
        if (repair.state.isComplete())
            throw new IllegalStateException("Repair is completed! " + repair.state.getResult());
        List<InetAddressAndPort> participaents = new ArrayList<>(repair.state.getNeighborsAndRanges().participants.size() + 1);
        if (rs.nextBoolean()) participaents.add(coordinator.broadcastAddressAndPort());
        participaents.addAll(repair.state.getNeighborsAndRanges().participants);
        participaents.sort(Comparator.naturalOrder());

        InetAddressAndPort selected = rs.pick(participaents);
        return selected;
    }

    static void addMismatch(RandomSource rs, ColumnFamilyStore cfs, Validator validator)
    {
        ValidationState state = validator.state;
        int maxDepth = DatabaseDescriptor.getRepairSessionMaxTreeDepth();
        state.phase.start(MISMATCH_NUM_PARTITIONS, 1024);

        MerkleTrees trees = new MerkleTrees(cfs.getPartitioner());
        for (Range<Token> range : validator.desc.ranges)
        {
            int depth = (int) Math.min(Math.ceil(Math.log(MISMATCH_NUM_PARTITIONS) / Math.log(2)), maxDepth);
            trees.addMerkleTree((int) Math.pow(2, depth), range);
        }
        Set<Token> allTokens = new HashSet<>();
        for (Range<Token> range : validator.desc.ranges)
        {
            Gen<Token> gen = fromQT(CassandraGenerators.tokensInRange(range));
            Set<Token> tokens = new LinkedHashSet<>();
            for (int i = 0, size = rs.nextInt(1, 10); i < size; i++)
            {
                for (int attempt = 0; !tokens.add(gen.next(rs)) && attempt < 5; attempt++)
                {
                }
            }
            // tokens may or may not be of the expected size; this depends on how wide the range is
            for (Token token : tokens)
                trees.split(token);
            allTokens.addAll(tokens);
        }
        for (Token token : allTokens)
        {
            findCorrectRange(trees, token, range -> {
                Digest digest = Digest.forValidator();
                digest.update(ByteBuffer.wrap(token.toString().getBytes(StandardCharsets.UTF_8)));
                range.addHash(new MerkleTree.RowHash(token, digest.digest(), 1));
            });
        }
        state.partitionsProcessed++;
        state.bytesRead = 1024;
        state.phase.sendingTrees();
        Stage.ANTI_ENTROPY.execute(() -> {
            state.phase.success();
            validator.respond(new ValidationResponse(validator.desc, trees));
        });
    }

    private static void findCorrectRange(MerkleTrees trees, Token token, Consumer<MerkleTree.TreeRange> fn)
    {
        MerkleTrees.TreeRangeIterator it = trees.rangeIterator();
        while (it.hasNext())
        {
            MerkleTree.TreeRange next = it.next();
            if (next.contains(token))
            {
                fn.accept(next);
                return;
            }
        }
    }

    private enum RepairType
    {FULL, IR}

    private enum PreviewType
    {NONE, REPAIRED, UNREPAIRED}

    static RepairOption repairOption(RandomSource rs, Cluster.Node coordinator, String ks, List<String> tableNames)
    {
        return repairOption(rs, coordinator, ks, Gens.lists(Gens.pick(tableNames)).ofSizeBetween(1, tableNames.size()), Gens.enums().all(RepairType.class), Gens.enums().all(PreviewType.class), Gens.enums().all(RepairParallelism.class));
    }

    static RepairOption irOption(RandomSource rs, Cluster.Node coordinator, String ks, Gen<List<String>> tablesGen)
    {
        return repairOption(rs, coordinator, ks, tablesGen, Gens.constant(RepairType.IR), Gens.constant(PreviewType.NONE), Gens.enums().all(RepairParallelism.class));
    }

    static RepairOption previewOption(RandomSource rs, Cluster.Node coordinator, String ks, Gen<List<String>> tablesGen)
    {
        return repairOption(rs, coordinator, ks, tablesGen, Gens.constant(RepairType.FULL), Gens.constant(PreviewType.REPAIRED), Gens.enums().all(RepairParallelism.class));
    }

    private static RepairOption repairOption(RandomSource rs, Cluster.Node coordinator, String ks, Gen<List<String>> tablesGen, Gen<RepairType> repairTypeGen, Gen<PreviewType> previewTypeGen, Gen<RepairParallelism> repairParallelismGen)
    {
        List<String> args = new ArrayList<>();
        args.add(ks);
        args.addAll(tablesGen.next(rs));
        args.add("-pr");
        RepairType type = repairTypeGen.next(rs);
        switch (type)
        {
            case IR:
                // default
                break;
            case FULL:
                args.add("--full");
                break;
            default:
                throw new AssertionError("Unsupported repair type: " + type);
        }
        PreviewType previewType = previewTypeGen.next(rs);
        switch (previewType)
        {
            case NONE:
                break;
            case REPAIRED:
                args.add("--validate");
                break;
            case UNREPAIRED:
                args.add("--preview");
                break;
            default:
                throw new AssertionError("Unsupported preview type: " + previewType);
        }
        RepairParallelism parallelism = repairParallelismGen.next(rs);
        switch (parallelism)
        {
            case SEQUENTIAL:
                args.add("--sequential");
                break;
            case PARALLEL:
                // default
                break;
            case DATACENTER_AWARE:
                args.add("--dc-parallel");
                break;
            default:
                throw new AssertionError("Unknown parallelism: " + parallelism);
        }
        if (rs.nextBoolean()) args.add("--optimise-streams");
        RepairOption options = RepairOption.parse(Repair.parseOptionMap(() -> "test", args), DatabaseDescriptor.getPartitioner());
        if (options.getRanges().isEmpty())
        {
            if (options.isPrimaryRange())
            {
                // when repairing only primary range, neither dataCenters nor hosts can be set
                if (options.getDataCenters().isEmpty() && options.getHosts().isEmpty())
                    options.getRanges().addAll(coordinator.getPrimaryRanges(ks));
                    // except dataCenters only contain local DC (i.e. -local)
                else if (options.isInLocalDCOnly())
                    options.getRanges().addAll(coordinator.getPrimaryRangesWithinDC(ks));
                else
                    throw new IllegalArgumentException("You need to run primary range repair on all nodes in the cluster.");
            }
            else
            {
                Iterables.addAll(options.getRanges(), coordinator.getLocalReplicas(ks).onlyFull().ranges());
            }
        }
        return options;
    }

    enum Faults
    {
        DELAY, DROP;

        public static final Set<Faults> NONE = Collections.emptySet();
        public static final Set<Faults> DROPPED = EnumSet.of(DELAY, DROP);
    }

    private static class Connection
    {
        final InetAddressAndPort from, to;

        private Connection(InetAddressAndPort from, InetAddressAndPort to)
        {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Connection that = (Connection) o;
            return from.equals(that.from) && to.equals(that.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }

        @Override
        public String toString()
        {
            return "Connection{" + "from=" + from + ", to=" + to + '}';
        }
    }

    interface MessageListener
    {
        default void preHandle(Cluster.Node node, Message<?> msg) {}
    }

    static class Cluster
    {
        private static final FailingBiConsumer<ColumnFamilyStore, Validator> DEFAULT_VALIDATION = ValidationManager::doValidation;

        final Map<InetAddressAndPort, Node> nodes;
        private final IFailureDetector failureDetector = Mockito.mock(IFailureDetector.class);
        private final IEndpointSnitch snitch = Mockito.mock(IEndpointSnitch.class);
        private final SimulatedExecutorFactory globalExecutor;
        final ScheduledExecutorPlus unorderedScheduled;
        final ExecutorPlus orderedExecutor;
        private final Gossip gossiper = new Gossip();
        private final MBeanWrapper mbean = Mockito.mock(MBeanWrapper.class);
        private final List<Throwable> failures = new ArrayList<>();
        private final List<MessageListener> listeners = new ArrayList<>();
        private final RandomSource rs;
        private BiFunction<Node, Message<?>, Set<Faults>> allowedMessageFaults = (a, b) -> Collections.emptySet();

        private final Map<Connection, LongSupplier> networkLatencies = new HashMap<>();
        private final Map<Connection, Supplier<Boolean>> networkDrops = new HashMap<>();

        Cluster(RandomSource rs)
        {
            ClockAccess.includeThreadAsOwner();
            this.rs = rs;
            globalExecutor = new SimulatedExecutorFactory(rs, fromQT(Generators.TIMESTAMP_GEN.map(Timestamp::getTime)).mapToLong(TimeUnit.MILLISECONDS::toNanos).next(rs));
            orderedExecutor = globalExecutor.configureSequential("ignore").build();
            unorderedScheduled = globalExecutor.scheduled("ignored");



            // We run tests in an isolated JVM per class, so not cleaing up is safe... but if that assumption ever changes, will need to cleanup
            Stage.ANTI_ENTROPY.unsafeSetExecutor(orderedExecutor);
            Stage.INTERNAL_RESPONSE.unsafeSetExecutor(unorderedScheduled);
            Mockito.when(failureDetector.isAlive(Mockito.any())).thenReturn(true);
            Thread expectedThread = Thread.currentThread();
            NoSpamLogger.unsafeSetClock(() -> {
                if (Thread.currentThread() != expectedThread)
                    throw new AssertionError("NoSpamLogger.Clock accessed outside of fuzzing...");
                return globalExecutor.nanoTime();
            });

            int numNodes = rs.nextInt(3, 10);
            List<String> dcs = Gens.lists(IDENTIFIER_GEN).unique().ofSizeBetween(1, Math.min(10, numNodes)).next(rs);
            Map<InetAddressAndPort, Node> nodes = Maps.newHashMapWithExpectedSize(numNodes);
            Gen<Token> tokenGen = fromQT(CassandraGenerators.token(DatabaseDescriptor.getPartitioner()));
            Gen<UUID> hostIdGen = fromQT(Generators.UUID_RANDOM_GEN);
            Set<Token> tokens = new HashSet<>();
            Set<UUID> hostIds = new HashSet<>();
            for (int i = 0; i < numNodes; i++)
            {
                InetAddressAndPort addressAndPort = ADDRESS_W_PORT.next(rs);
                while (nodes.containsKey(addressAndPort)) addressAndPort = ADDRESS_W_PORT.next(rs);
                Token token;
                while (!tokens.add(token = tokenGen.next(rs)))
                {
                }
                UUID hostId;
                while (!hostIds.add(hostId = hostIdGen.next(rs)))
                {
                }

                String dc = rs.pick(dcs);
                String rack = "rack";
                Mockito.when(snitch.getDatacenter(Mockito.eq(addressAndPort))).thenReturn(dc);
                Mockito.when(snitch.getRack(Mockito.eq(addressAndPort))).thenReturn(rack);

                VersionedValue.VersionedValueFactory valueFactory = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
                EndpointState state = new EndpointState(new HeartBeatState(42, 42));
                state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(Collections.singleton(token)));
                state.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(Collections.singleton(token)));
                state.addApplicationState(ApplicationState.HOST_ID, valueFactory.hostId(hostId));
                state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(Collections.singleton(token)));
                state.addApplicationState(ApplicationState.DC, valueFactory.datacenter(dc));
                state.addApplicationState(ApplicationState.RACK, valueFactory.rack(rack));
                state.addApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

                gossiper.endpoints.put(addressAndPort, state);

                Node node = new Node(hostId, addressAndPort, Collections.singletonList(token), new Messaging(addressAndPort));
                nodes.put(addressAndPort, node);
            }
            this.nodes = nodes;

            TokenMetadata tm = StorageService.instance.getTokenMetadata();
            tm.clearUnsafe();
            for (Node inst : nodes.values())
            {
                tm.updateHostId(inst.hostId(), inst.broadcastAddressAndPort());
                for (Token token : inst.tokens())
                    tm.updateNormalToken(token, inst.broadcastAddressAndPort());
            }
        }

        public Closeable addListener(MessageListener listener)
        {
            listeners.add(listener);
            return () -> removeListener(listener);
        }

        public void removeListener(MessageListener listener)
        {
            listeners.remove(listener);
        }

        public void allowedMessageFaults(BiFunction<Node, Message<?>, Set<Faults>> fn)
        {
            this.allowedMessageFaults = fn;
        }

        public void checkFailures()
        {
            if (Thread.interrupted())
                failures.add(new InterruptedException());
            if (failures.isEmpty()) return;
            AssertionError error = new AssertionError("Unexpected exceptions found");
            failures.forEach(error::addSuppressed);
            failures.clear();
            throw error;
        }

        public boolean processOne()
        {
            boolean result = globalExecutor.processOne();
            checkFailures();
            return result;
        }

        public void processAll()
        {
            while (processOne())
            {
            }
        }

        private class CallbackContext
        {
            final RequestCallback callback;

            private CallbackContext(RequestCallback callback)
            {
                this.callback = Objects.requireNonNull(callback);
            }

            public void onResponse(Message msg)
            {
                callback.onResponse(msg);
            }

            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                if (callback.invokeOnFailure()) callback.onFailure(from, failureReason);
            }
        }

        private class Messaging implements MessageDelivery
        {
            final InetAddressAndPort broadcastAddressAndPort;
            final Long2ObjectHashMap<CallbackContext> callbacks = new Long2ObjectHashMap<>();

            private Messaging(InetAddressAndPort broadcastAddressAndPort)
            {
                this.broadcastAddressAndPort = broadcastAddressAndPort;
            }

            @Override
            public <REQ> void send(Message<REQ> message, InetAddressAndPort to)
            {
                message = message.withFrom(broadcastAddressAndPort);
                maybeEnqueue(message, to, null);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb)
            {
                message = message.withFrom(broadcastAddressAndPort);
                maybeEnqueue(message, to, cb);
            }

            @Override
            public <REQ, RSP> void sendWithCallback(Message<REQ> message, InetAddressAndPort to, RequestCallback<RSP> cb, ConnectionType specifyConnection)
            {
                message = message.withFrom(broadcastAddressAndPort);
                maybeEnqueue(message, to, cb);
            }

            private <REQ, RSP> void maybeEnqueue(Message<REQ> message, InetAddressAndPort to, @Nullable RequestCallback<RSP> callback)
            {
                CallbackContext cb;
                if (callback != null)
                {
                    if (callbacks.containsKey(message.id()))
                        throw new AssertionError("Message id " + message.id() + " already has a callback");
                    cb = new CallbackContext(callback);
                    callbacks.put(message.id(), cb);
                }
                else
                {
                    cb = null;
                }
                boolean toSelf = this.broadcastAddressAndPort.equals(to);
                Node node = nodes.get(to);
                Set<Faults> allowedFaults = allowedMessageFaults.apply(node, message);
                if (allowedFaults.isEmpty())
                {
                    // enqueue so stack overflow doesn't happen with the inlining
                    unorderedScheduled.submit(() -> node.handle(message));
                }
                else
                {
                    Runnable enqueue = () -> {
                        if (!allowedFaults.contains(Faults.DELAY))
                        {
                            unorderedScheduled.submit(() -> node.handle(message));
                        }
                        else
                        {
                            if (toSelf) unorderedScheduled.submit(() -> node.handle(message));
                            else
                                unorderedScheduled.schedule(() -> node.handle(message), networkJitterNanos(to), TimeUnit.NANOSECONDS);
                        }
                    };

                    if (!allowedFaults.contains(Faults.DROP)) enqueue.run();
                    else
                    {
                        if (!toSelf && networkDrops(to))
                        {
//                            logger.warn("Dropped message {}", message);
                            // drop
                        }
                        else
                        {
                            enqueue.run();
                        }
                    }

                    if (cb != null)
                    {
                        unorderedScheduled.schedule(() -> {
                            CallbackContext ctx = callbacks.remove(message.id());
                            if (ctx != null)
                            {
                                assert ctx == cb;
                                try
                                {
                                    ctx.onFailure(to, RequestFailureReason.TIMEOUT);
                                }
                                catch (Throwable t)
                                {
                                    failures.add(t);
                                }
                            }
                        }, message.verb().expiresAfterNanos(), TimeUnit.NANOSECONDS);
                    }
                }
            }

            private long networkJitterNanos(InetAddressAndPort to)
            {
                return networkLatencies.computeIfAbsent(new Connection(broadcastAddressAndPort, to), ignore -> {
                    long min = TimeUnit.MICROSECONDS.toNanos(500);
                    long maxSmall = TimeUnit.MILLISECONDS.toNanos(5);
                    long max = TimeUnit.SECONDS.toNanos(5);
                    LongSupplier small = () -> rs.nextLong(min, maxSmall);
                    LongSupplier large = () -> rs.nextLong(maxSmall, max);
                    return Gens.bools().runs(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15)).mapToLong(b -> b ? large.getAsLong() : small.getAsLong()).asLongSupplier(rs);
                }).getAsLong();
            }

            private boolean networkDrops(InetAddressAndPort to)
            {
                return networkDrops.computeIfAbsent(new Connection(broadcastAddressAndPort, to), ignore -> Gens.bools().runs(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15)).asSupplier(rs)).get();
            }

            @Override
            public <REQ, RSP> Future<Message<RSP>> sendWithResult(Message<REQ> message, InetAddressAndPort to)
            {
                AsyncPromise<Message<RSP>> promise = new AsyncPromise<>();
                sendWithCallback(message, to, new RequestCallback<RSP>()
                {
                    @Override
                    public void onResponse(Message<RSP> msg)
                    {
                        promise.trySuccess(msg);
                    }

                    @Override
                    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
                    {
                        promise.tryFailure(new MessagingService.FailureResponseException(from, failureReason));
                    }

                    @Override
                    public boolean invokeOnFailure()
                    {
                        return true;
                    }
                });
                return promise;
            }

            @Override
            public <V> void respond(V response, Message<?> message)
            {
                send(message.responseWith(response), message.respondTo());
            }
        }

        private class Gossip implements IGossiper
        {
            private final Map<InetAddressAndPort, EndpointState> endpoints = new HashMap<>();

            @Override
            public void register(IEndpointStateChangeSubscriber subscriber)
            {

            }

            @Override
            public void unregister(IEndpointStateChangeSubscriber subscriber)
            {

            }

            @Nullable
            @Override
            public EndpointState getEndpointStateForEndpoint(InetAddressAndPort ep)
            {
                return endpoints.get(ep);
            }
        }

        class Node implements SharedContext
        {
            private final ICompactionManager compactionManager = Mockito.mock(ICompactionManager.class);
            final UUID hostId;
            final InetAddressAndPort addressAndPort;
            final Collection<Token> tokens;
            final ActiveRepairService activeRepairService;
            final IVerbHandler verbHandler;
            final Messaging messaging;
            final IValidationManager validationManager;
            private FailingBiConsumer<ColumnFamilyStore, Validator> doValidation = DEFAULT_VALIDATION;
            private final StreamExecutor defaultStreamExecutor = plan -> {
                long delayNanos = rs.nextLong(TimeUnit.SECONDS.toNanos(5), TimeUnit.MINUTES.toNanos(10));
                unorderedScheduled.schedule(() -> {
                    StreamState success = new StreamState(plan.planId(), plan.streamOperation(), Collections.emptySet());
                    for (StreamEventHandler handler : plan.handlers())
                        handler.onSuccess(success);
                }, delayNanos, TimeUnit.NANOSECONDS);
                return null;
            };
            private StreamExecutor streamExecutor = defaultStreamExecutor;

            private Node(UUID hostId, InetAddressAndPort addressAndPort, Collection<Token> tokens, Messaging messaging)
            {
                this.hostId = hostId;
                this.addressAndPort = addressAndPort;
                this.tokens = tokens;
                this.messaging = messaging;
                this.activeRepairService = new ActiveRepairService(this);
                this.validationManager = (cfs, validator) -> unorderedScheduled.submit(() -> {
                    try
                    {
                        doValidation.acceptOrFail(cfs, validator);
                    }
                    catch (Throwable e)
                    {
                        validator.fail(e);
                    }
                });
                this.verbHandler = new RepairMessageVerbHandler(this);

                activeRepairService.start();
            }

            public Closeable doValidation(FailingBiConsumer<ColumnFamilyStore, Validator> fn)
            {
                FailingBiConsumer<ColumnFamilyStore, Validator> previous = this.doValidation;
                if (previous != DEFAULT_VALIDATION)
                    throw new IllegalStateException("Attemptted to override validation, but was already overridden");
                this.doValidation = fn;
                return () -> this.doValidation = previous;
            }

            public Closeable doValidation(Function<FailingBiConsumer<ColumnFamilyStore, Validator>, FailingBiConsumer<ColumnFamilyStore, Validator>> fn)
            {
                FailingBiConsumer<ColumnFamilyStore, Validator> previous = this.doValidation;
                this.doValidation = fn.apply(previous);
                return () -> this.doValidation = previous;
            }

            public Closeable doSync(StreamExecutor streamExecutor)
            {
                StreamExecutor previous = this.streamExecutor;
                if (previous != defaultStreamExecutor)
                    throw new IllegalStateException("Attemptted to override sync, but was already overridden");
                this.streamExecutor = streamExecutor;
                return () -> this.streamExecutor = previous;
            }

            void handle(Message msg)
            {
                msg = serde(msg);
                if (msg == null)
                {
                    logger.warn("Got a message that failed to serialize/deserialize");
                    return;
                }
                for (MessageListener l : listeners)
                    l.preHandle(this, msg);
                if (msg.verb().isResponse())
                {
                    // handle callbacks
                    if (messaging.callbacks.containsKey(msg.id()))
                    {
                        CallbackContext callback = messaging.callbacks.remove(msg.id());
                        if (callback == null) return;
                        try
                        {
                            if (msg.isFailureResponse())
                                callback.onFailure(msg.from(), (RequestFailureReason) msg.payload);
                            else callback.onResponse(msg);
                        }
                        catch (Throwable t)
                        {
                            failures.add(t);
                        }
                    }
                }
                else
                {
                    try
                    {
                        verbHandler.doVerb(msg);
                    }
                    catch (Throwable e)
                    {
                        failures.add(e);
                    }
                }
            }

            public UUID hostId()
            {
                return hostId;
            }

            @Override
            public InetAddressAndPort broadcastAddressAndPort()
            {
                return addressAndPort;
            }

            public Collection<Token> tokens()
            {
                return tokens;
            }

            public IFailureDetector failureDetector()
            {
                return failureDetector;
            }

            @Override
            public IEndpointSnitch snitch()
            {
                return snitch;
            }

            @Override
            public IGossiper gossiper()
            {
                return gossiper;
            }

            @Override
            public ICompactionManager compactionManager()
            {
                return compactionManager;
            }

            public ExecutorFactory executorFactory()
            {
                return globalExecutor;
            }

            public ScheduledExecutorPlus optionalTasks()
            {
                return unorderedScheduled;
            }

            @Override
            public Supplier<Random> random()
            {
                return () -> rs.fork().asJdkRandom();
            }

            public Clock clock()
            {
                return globalExecutor;
            }

            public MessageDelivery messaging()
            {
                return messaging;
            }

            public MBeanWrapper mbean()
            {
                return mbean;
            }

            public RepairCoordinator repair(String ks, RepairOption options)
            {
                return repair(ks, options, true);
            }

            public RepairCoordinator repair(String ks, RepairOption options, boolean addFailureOnErrorNotification)
            {
                RepairCoordinator repair = new RepairCoordinator(this, (name, tables) -> StorageService.instance.getValidColumnFamilies(false, false, name, tables), name -> StorageService.instance.getReplicas(name, broadcastAddressAndPort()), 42, options, ks);
                if (addFailureOnErrorNotification)
                {
                    repair.addProgressListener((tag, event) -> {
                        if (event.getType() == ProgressEventType.ERROR)
                            failures.add(new AssertionError(event.getMessage()));
                    });
                }
                return repair;
            }

            public RangesAtEndpoint getLocalReplicas(String ks)
            {
                return StorageService.instance.getReplicas(ks, broadcastAddressAndPort());
            }

            public Collection<? extends Range<Token>> getPrimaryRanges(String ks)
            {
                return StorageService.instance.getPrimaryRangesForEndpoint(ks, broadcastAddressAndPort());
            }

            public Collection<? extends Range<Token>> getPrimaryRangesWithinDC(String ks)
            {
                return StorageService.instance.getPrimaryRangeForEndpointWithinDC(ks, broadcastAddressAndPort());
            }

            @Override
            public ActiveRepairService repair()
            {
                return activeRepairService;
            }

            @Override
            public IValidationManager validationManager()
            {
                return validationManager;
            }

            @Override
            public TableRepairManager repairManager(ColumnFamilyStore store)
            {
                return new CassandraTableRepairManager(store, this)
                {
                    @Override
                    public void snapshot(String name, Collection<Range<Token>> ranges, boolean force)
                    {
                        // no-op
                    }
                };
            }

            @Override
            public StreamExecutor streamExecutor()
            {
                return streamExecutor;
            }
        }

        private Message serde(Message msg)
        {
            try (DataOutputBuffer b = DataOutputBuffer.scratchBuffer.get())
            {
                int messagingVersion = MessagingService.current_version;
                Message.serializer.serialize(msg, b, messagingVersion);
                DataInputBuffer in = new DataInputBuffer(b.unsafeGetBufferAndFlip(), false);
                return Message.serializer.deserialize(in, msg.from(), messagingVersion);
            }
            catch (Throwable e)
            {
                failures.add(e);
                return null;
            }
        }
    }

    private static <T> Gen<T> fromQT(org.quicktheories.core.Gen<T> qt)
    {
        return rs -> {
            JavaRandom r = new JavaRandom(rs.asJdkRandom());
            return qt.generate(r);
        };
    }

    public static class HackStrat extends LocalStrategy
    {
        public HackStrat(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
        {
            super(keyspaceName, tokenMetadata, snitch, configOptions);
        }
    }

    /**
     * Since the clock is accessable via {@link Global#currentTimeMillis()} and {@link Global#nanoTime()}, and only repair subsystem has the requirement not to touch that, this class
     * acts as a safty check that validates that repair does not touch these methods but allows the other subsystems to do so.
     */
    public static class ClockAccess implements Clock
    {
        private static final Set<Thread> OWNERS = Collections.synchronizedSet(new HashSet<>());
        private final Clock delegate = new Default();

        public static void includeThreadAsOwner()
        {
            OWNERS.add(Thread.currentThread());
        }

        @Override
        public long nanoTime()
        {
            checkAccess();
            return delegate.nanoTime();
        }

        @Override
        public long currentTimeMillis()
        {
            checkAccess();
            return delegate.currentTimeMillis();
        }

        private enum Access
        {MAIN_THREAD_ONLY, REJECT, IGNORE}

        private void checkAccess()
        {
            Access access = StackWalker.getInstance().walk(frames -> {
                Iterator<StackWalker.StackFrame> it = frames.iterator();
                boolean topLevel = false;
                while (it.hasNext())
                {
                    StackWalker.StackFrame next = it.next();
                    if (!topLevel)
                    {
                        // need to find the top level!
                        while (!Clock.Global.class.getName().equals(next.getClassName()))
                        {
                            assert it.hasNext();
                            next = it.next();
                        }
                        topLevel = true;
                        assert it.hasNext();
                        next = it.next();
                    }
                    if (FuzzTestBase.class.getName().equals(next.getClassName())) return Access.MAIN_THREAD_ONLY;
                    if (next.getClassName().startsWith("org.apache.cassandra.db.") || next.getClassName().startsWith("org.apache.cassandra.gms.") || next.getClassName().startsWith("org.apache.cassandra.cql3.") || next.getClassName().startsWith("org.apache.cassandra.metrics.") || next.getClassName().startsWith("org.apache.cassandra.utils.concurrent.")
                        || next.getClassName().startsWith("org.apache.cassandra.utils.TimeUUID") // this would be good to solve
                        || next.getClassName().startsWith(PendingAntiCompaction.class.getName()))
                        return Access.IGNORE;
                    if (next.getClassName().startsWith("org.apache.cassandra.repair") || ActiveRepairService.class.getName().startsWith(next.getClassName()))
                        return Access.REJECT;
                }
                return Access.IGNORE;
            });
            Thread current = Thread.currentThread();
            switch (access)
            {
                case IGNORE:
                    return;
                case REJECT:
                    throw new IllegalStateException("Rejecting access");
                case MAIN_THREAD_ONLY:
                    if (!OWNERS.contains(current)) throw new IllegalStateException("Accessed in wrong thread: " + current);
                    break;
            }
        }
    }

    static class SimulatedFault extends RuntimeException
    {
        SimulatedFault(String message)
        {
            super(message);
        }
    }
}
