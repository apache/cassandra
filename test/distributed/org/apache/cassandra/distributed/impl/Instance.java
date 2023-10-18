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

package org.apache.cassandra.distributed.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.cassandra.Util;
import org.apache.cassandra.auth.AuthCache;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspaceMigrator41;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionLogger;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.action.GossipHelper;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IListen;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbe;
import org.apache.cassandra.distributed.mock.nodetool.InternalNodeProbeFactory;
import org.apache.cassandra.distributed.shared.Metrics;
import org.apache.cassandra.distributed.shared.ThrowingRunnable;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.DTestSerializer;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.Sampler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.MigrationCoordinator;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.service.paxos.PaxosRepair;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.uncommitted.UncommittedTableData;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamReceiveTask;
import org.apache.cassandra.streaming.StreamTransferTask;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.SystemExitException;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.progress.jmx.JMXBroadcastExecutor;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONSISTENT_RANGE_MOVEMENT;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONSISTENT_SIMULTANEOUS_MOVES_ALLOW;
import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_SUITENAME;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG;
import static org.apache.cassandra.distributed.api.Feature.BLANK_GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.fromCassandraInetAddressAndPort;
import static org.apache.cassandra.distributed.impl.DistributedTestSnitch.toCassandraInetAddressAndPort;
import static org.apache.cassandra.net.Verb.BATCH_STORE_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * This class is instantiated on the relevant classloader, so its methods invoke the correct target classes automatically
 */
public class Instance extends IsolatedExecutor implements IInvokableInstance
{
    private Logger inInstancelogger; // Defer creation until running in the instance context
    public final IInstanceConfig config;
    private volatile boolean initialized = false;
    private volatile boolean internodeMessagingStarted = false;
    private final AtomicLong startedAt = new AtomicLong();
    private IsolatedJmx isolatedJmx;

    /** @deprecated See CASSANDRA-17013 */
    @Deprecated(since = "4.1")
    Instance(IInstanceConfig config, ClassLoader classLoader)
    {
        this(config, classLoader, null);
    }

    Instance(IInstanceConfig config, ClassLoader classLoader, FileSystem fileSystem)
    {
        this(config, classLoader, fileSystem, null);
    }

    Instance(IInstanceConfig config, ClassLoader classLoader, FileSystem fileSystem, ShutdownExecutor shutdownExecutor)
    {
        super("node" + config.num(), classLoader, executorFactory().pooled("isolatedExecutor", Integer.MAX_VALUE), shutdownExecutor);
        this.config = config;
        if (fileSystem != null)
            File.unsafeSetFilesystem(fileSystem);
        Object clusterId = Objects.requireNonNull(config.get(Constants.KEY_DTEST_API_CLUSTER_ID), "cluster_id is not defined");
        ClusterIDDefiner.setId("cluster-" + clusterId);
        InstanceIDDefiner.setInstanceId(config.num());
        FBUtilities.setBroadcastInetAddressAndPort(InetAddressAndPort.getByAddressOverrideDefaults(config.broadcastAddress().getAddress(),
                                                                                                   config.broadcastAddress().getPort()));

        // Set the config at instance creation, possibly before startup() has run on all other instances.
        // setMessagingVersions below will call runOnInstance which will instantiate
        // the MessagingService and dependencies preventing later changes to network parameters.
        Config single = loadConfig(config);
        Config.setOverrideLoadConfig(() -> single);

        // Enable streaming inbound handler tracking so they can be closed properly without leaking
        // the blocking IO thread.
        NettyStreamingChannel.trackInboundHandlers();
    }

    @Override
    public boolean getLogsEnabled()
    {
        return true;
    }

    @Override
    public LogAction logs()
    {
        // the path used is defined by test/conf/logback-dtest.xml and looks like the following
        // ./build/test/logs/${cassandra.testtag}/${suitename}/${cluster_id}/${instance_id}/system.log
        String tag = TEST_CASSANDRA_TESTTAG.getString();
        String suite = TEST_CASSANDRA_SUITENAME.getString();
        String clusterId = ClusterIDDefiner.getId();
        String instanceId = InstanceIDDefiner.getInstanceId();
        File f = new File(FileSystems.getDefault(), String.format("build/test/logs/%s/%s/%s/%s/system.log", tag, suite, clusterId, instanceId));
        // when creating a cluster globally in a test class we get the logs without the suite, try finding those logs:
        if (!f.exists())
            f = new File(FileSystems.getDefault(), String.format("build/test/logs/%s/%s/%s/system.log", tag, clusterId, instanceId));
        if (!f.exists())
            throw new AssertionError("Unable to locate system.log under " + f.absolutePath() + "; make sure ICluster.setup() is called or extend TestBaseImpl and do not define a static beforeClass function with @BeforeClass");
        return new FileLogAction(f);
    }

    @Override
    public IInstanceConfig config()
    {
        return config;
    }

    @Override
    public ICoordinator coordinator()
    {
        return new Coordinator(this);
    }

    public IListen listen()
    {
        return new Listen(this);
    }

    @Override
    public InetSocketAddress broadcastAddress() { return config.broadcastAddress(); }

    @Override
    public SimpleQueryResult executeInternalWithResult(String query, Object... args)
    {
        return sync(() -> unsafeExecuteInternalWithResult(query, args)).call();
    }

    public static SimpleQueryResult unsafeExecuteInternalWithResult(String query, Object ... args)
    {
        ClientWarn.instance.captureWarnings();
        CoordinatorWarnings.init();
        try
        {
            QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
            ResultMessage result = prepared.statement.executeLocally(QueryProcessor.internalQueryState(),
                                                                     QueryProcessor.makeInternalOptions(prepared.statement, args));
            CoordinatorWarnings.done();

            if (result != null)
                result.setWarnings(ClientWarn.instance.getWarnings());
            return RowUtil.toQueryResult(result);
        }
        catch (Exception | Error e)
        {
            CoordinatorWarnings.done();
            throw e;
        }
        finally
        {
            CoordinatorWarnings.reset();
            ClientWarn.instance.resetWarnings();
        }
    }

    @Override
    public UUID schemaVersion()
    {
        // we do not use method reference syntax here, because we need to sync on the node-local schema instance
        //noinspection Convert2MethodRef
        return Schema.instance.getVersion();
    }

    public void startup()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isShutdown()
    {
        return isolatedExecutor.isShutdown();
    }

    @Override
    public void schemaChangeInternal(String query)
    {
        sync(() -> {
            try
            {
                ClientState state = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
                QueryState queryState = new QueryState(state);

                CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getClientState());
                statement.validate(state);

                QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());
                statement.executeLocally(queryState, options);
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
            }
        }).run();
    }

    private void registerMockMessaging(ICluster<?> cluster)
    {
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (!internodeMessagingStarted)
            {
                inInstancelogger.debug("Dropping outbound message {} to {} as internode messaging has not been started yet",
                                       message, to);
                return false;
            }
            cluster.deliverMessage(to, serializeMessage(message.from(), to, message));
            return false;
        });
    }

    private void registerInboundFilter(ICluster<?> cluster)
    {
        MessagingService.instance().inboundSink.add(message -> {
            if (!cluster.filters().hasInbound())
                return true;
            if (isShutdown())
                return false;
            IMessage serialized = serializeMessage(message.from(), toCassandraInetAddressAndPort(broadcastAddress()), message);
            IInstance from = cluster.get(serialized.from());
            if (from == null)
                return false;
            int fromNum = from.config().num();
            int toNum = config.num(); // since this instance is reciving the message, to will always be this instance
            return cluster.filters().permitInbound(fromNum, toNum, serialized);
        });
    }

    private void registerOutboundFilter(ICluster cluster)
    {
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (isShutdown())
                return false; // TODO: Simulator needs this to trigger a failure
            IMessage serialzied = serializeMessage(message.from(), to, message);
            int fromNum = config.num(); // since this instance is sending the message, from will always be this instance
            IInstance toInstance = cluster.get(fromCassandraInetAddressAndPort(to));
            if (toInstance == null)
                return false; // TODO: Simulator needs this to trigger a failure
            int toNum = toInstance.config().num();
            return cluster.filters().permitOutbound(fromNum, toNum, serialzied);
        });
    }

    public void uncaughtException(Thread thread, Throwable throwable)
    {
        sync(JVMStabilityInspector::uncaughtException).accept(thread, throwable);
    }

    public static IMessage serializeMessage(InetAddressAndPort from, InetAddressAndPort to, Message<?> messageOut)
    {
        int fromVersion = MessagingService.instance().versions.get(from);
        int toVersion = MessagingService.instance().versions.get(to);

        // If we're re-serializing a pre-4.0 message for filtering purposes, take into account possible empty payload
        // See CASSANDRA-16157 for details.
        if (fromVersion < MessagingService.current_version &&
            ((messageOut.verb().serializer() == ((IVersionedAsymmetricSerializer) NoPayload.serializer) || messageOut.payload == null)))
        {
            return new MessageImpl(messageOut.verb().id,
                                   ByteArrayUtil.EMPTY_BYTE_ARRAY,
                                   messageOut.id(),
                                   toVersion,
                                   messageOut.expiresAtNanos(),
                                   fromCassandraInetAddressAndPort(from));
        }

        try (DataOutputBuffer out = new DataOutputBuffer(1024))
        {
            // On a 4.0+ node, C* makes a distinction between "local" and "remote" batches, where only the former can 
            // be serialized and sent to a remote node, where they are deserialized and written to the batch commitlog
            // without first being converted into mutation objects. Batch serialization is therfore not symmetric, and
            // we use a special procedure here that "re-serializes" a "remote" batch to build the message.
            if (fromVersion >= MessagingService.VERSION_40 && messageOut.verb().id == BATCH_STORE_REQ.id)
            {
                Object maybeBatch = messageOut.payload;

                if (maybeBatch instanceof Batch)
                {
                    Batch batch = (Batch) maybeBatch;

                    // If the batch is local, it can be serialized along the normal path.
                    if (!batch.isLocal())
                    {
                        reserialize(batch, out, toVersion);
                        byte[] bytes = out.toByteArray();
                        return new MessageImpl(messageOut.verb().id, bytes, messageOut.id(), toVersion, messageOut.expiresAtNanos(), fromCassandraInetAddressAndPort(from));
                    }
                }
            }
            
            Message.serializer.serialize(messageOut, out, toVersion);
            byte[] bytes = out.toByteArray();
            if (messageOut.serializedSize(toVersion) != bytes.length)
                throw new AssertionError(String.format("Message serializedSize(%s) does not match what was written with serialize(out, %s) for verb %s and serializer %s; " +
                                                       "expected %s, actual %s", toVersion, toVersion, messageOut.verb(), Message.serializer.getClass(),
                                                       messageOut.serializedSize(toVersion), bytes.length));
            return new MessageImpl(messageOut.verb().id, bytes, messageOut.id(), toVersion, messageOut.expiresAtNanos(), fromCassandraInetAddressAndPort(from));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Only "local" batches can be passed through {@link Batch.Serializer#serialize(Batch, DataOutputPlus, int)} and 
     * sent to a remote node during normal operation, but there are testing scenarios where we may intercept and 
     * forward a "remote" batch. This method allows us to put the already encoded mutations back onto a stream.
     */
    private static void reserialize(Batch batch, DataOutputPlus out, int version) throws IOException
    {
        assert !batch.isLocal() : "attempted to reserialize a 'local' batch";

        batch.id.serialize(out);
        out.writeLong(batch.creationTime);

        out.writeUnsignedVInt32(batch.getEncodedMutations().size());

        for (ByteBuffer mutation : batch.getEncodedMutations())
        {
            out.write(mutation);
        }
    }

    @VisibleForTesting
    public static Message<?> deserializeMessage(IMessage message)
    {
        try (DataInputBuffer in = new DataInputBuffer(message.bytes()))
        {
            return Message.serializer.deserialize(in, toCassandraInetAddressAndPort(message.from()), message.version());
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can not deserialize message " + message, t);
        }
    }

    @Override
    public void receiveMessage(IMessage message)
    {
        sync(receiveMessageRunnable(message)).accept(false);
    }

    @Override
    public void receiveMessageWithInvokingThread(IMessage message)
    {
        if (classLoader != Thread.currentThread().getContextClassLoader())
            throw new IllegalStateException("Must be invoked by a Thread utilising the node's class loader");
        receiveMessageRunnable(message).accept(true);
    }

    private SerializableConsumer<Boolean> receiveMessageRunnable(IMessage message)
    {
        return runOnCaller -> {
            if (!internodeMessagingStarted)
            {
                inInstancelogger.debug("Dropping inbound message {} to {} as internode messaging has not been started yet",
                             message, config().broadcastAddress());
                return;
            }
            if (message.version() > MessagingService.current_version)
            {
                throw new IllegalStateException(String.format("Node%d received message version %d but current version is %d",
                                                              this.config.num(),
                                                              message.version(),
                                                              MessagingService.current_version));
            }

            Message<?> messageIn = deserializeMessage(message);
            Message.Header header = messageIn.header;
            TraceState state = Tracing.instance.initializeFromMessage(header);
            if (state != null)
                state.trace("{} message received from {}", header.verb, header.from);

            if (runOnCaller)
            {
                try (Closeable close = ExecutorLocals.create(state))
                {
                    MessagingService.instance().inboundSink.accept(messageIn);
                }
            }
            else
            {
                ExecutorPlus executor = header.verb.stage.executor();
                if (executor.isShutdown())
                {
                    MessagingService.instance().metrics.recordDroppedMessage(messageIn, messageIn.elapsedSinceCreated(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
                    inInstancelogger.warn("Dropping message {} due to stage {} being shutdown", messageIn, header.verb.stage);
                    return;
                }
                executor.execute(ExecutorLocals.create(state), () -> MessagingService.instance().inboundSink.accept(messageIn));
            }
        };
    }

    public int getMessagingVersion()
    {
        if (DatabaseDescriptor.isDaemonInitialized())
            return MessagingService.current_version;
        else
            return 0;
    }

    @Override
    public void setMessagingVersion(InetSocketAddress endpoint, int version)
    {
        if (DatabaseDescriptor.isDaemonInitialized())
            MessagingService.instance().versions.set(toCassandraInetAddressAndPort(endpoint), version);
        else
            inInstancelogger.warn("Skipped setting messaging version for {} to {} as daemon not initialized yet. Stacktrace attached for debugging.",
                        endpoint, version, new RuntimeException());
    }

    @Override
    public String getReleaseVersionString()
    {
        return FBUtilities.getReleaseVersionString();
    }

    public void flush(String keyspace)
    {
        Util.flushKeyspace(keyspace);
    }

    public void forceCompact(String keyspace, String table)
    {
        try
        {
            Keyspace.open(keyspace).getColumnFamilyStore(table).forceMajorCompaction();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public ExecutorPlus executorFor(int verbId)
    {
        return Verb.fromId(verbId).stage.executor();
    }

    @Override
    public void startup(ICluster cluster)
    {
        // Defer initialisation of Clock.Global until cluster/instance identifiers are set.
        // Otherwise, the instance classloader's logging classes are setup ahead of time and
        // the patterns/file paths are not set correctly. This will be addressed in a subsequent
        // commit to extend the functionality of the @Shared annotation to app classes.
        assert startedAt.compareAndSet(0L, System.nanoTime()) : "startedAt uninitialized";

        sync(() -> {
            inInstancelogger = LoggerFactory.getLogger(Instance.class);
            try
            {
                // org.apache.cassandra.distributed.impl.AbstractCluster.startup sets the exception handler for the thread
                // so extract it to populate ExecutorFactory.Global
                ExecutorFactory.Global.tryUnsafeSet(new ExecutorFactory.Default(Thread.currentThread().getContextClassLoader(), null, Thread.getDefaultUncaughtExceptionHandler()));
                if (config.has(GOSSIP))
                {
                    // TODO: hacky
                    RING_DELAY.setLong(15000);
                    CONSISTENT_RANGE_MOVEMENT.setBoolean(false);
                    CONSISTENT_SIMULTANEOUS_MOVES_ALLOW.setBoolean(true);
                }

                mkdirs();

                assert config.networkTopology().contains(config.broadcastAddress()) : String.format("Network topology %s doesn't contain the address %s",
                                                                                                    config.networkTopology(), config.broadcastAddress());
                DistributedTestSnitch.assign(config.networkTopology());

                if (config.has(JMX))
                    startJmx();

                DatabaseDescriptor.daemonInitialization();
                LoggingSupportFactory.getLoggingSupport().onStartup();

                FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
                DatabaseDescriptor.createAllDirectories();
                CassandraDaemon.getInstanceForTesting().migrateSystemDataIfNeeded();
                CassandraDaemon.logSystemInfo(inInstancelogger);
                CommitLog.instance.start();

                CassandraDaemon.getInstanceForTesting().runStartupChecks();

                // We need to persist this as soon as possible after startup checks.
                // This should be the first write to SystemKeyspace (CASSANDRA-11742)
                SystemKeyspace.persistLocalMetadata(config::hostId);
                SystemKeyspaceMigrator41.migrate();

                // Same order to populate tokenMetadata for the first time,
                // see org.apache.cassandra.service.CassandraDaemon.setup
                StorageService.instance.populateTokenMetadata();

                try
                {
                    // load schema from disk
                    Schema.instance.loadFromDisk();
                }
                catch (Exception e)
                {
                    throw e;
                }

                // Start up virtual table support
                CassandraDaemon.getInstanceForTesting().setupVirtualKeyspaces();

                // clean up debris in data directories
                CassandraDaemon.getInstanceForTesting().scrubDataDirectories();

                Keyspace.setInitialized();

                // Replay any CommitLogSegments found on disk
                try
                {
                    CommitLog.instance.recoverSegmentsOnDisk();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                // Re-populate token metadata after commit log recover (new peers might be loaded onto system keyspace #10293)
                StorageService.instance.populateTokenMetadata();

                try
                {
                    PaxosState.maybeRebuildUncommittedState();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

                Verb.HINT_REQ.unsafeSetSerializer(DTestSerializer::new);

                if (config.has(NETWORK))
                {
                    MessagingService.instance().listen();
                }
                else
                {
                    // Even though we don't use MessagingService, access the static SocketFactory
                    // instance here so that we start the static event loop state
//                    -- not sure what that means?  SocketFactory.instance.getClass();
                    registerMockMessaging(cluster);
                }
                registerInboundFilter(cluster);
                registerOutboundFilter(cluster);
                if (!config.has(NETWORK))
                {
                    propagateMessagingVersions(cluster); // fake messaging needs to know messaging version for filters
                }
                internodeMessagingStarted = true;

                JVMStabilityInspector.replaceKiller(new InstanceKiller(Instance.this::shutdown));

                // TODO: this is more than just gossip
                StorageService.instance.registerDaemon(CassandraDaemon.getInstanceForTesting());
                if (config.has(GOSSIP))
                {
                    MigrationCoordinator.setUptimeFn(() -> TimeUnit.NANOSECONDS.toMillis(nanoTime() - startedAt.get()));
                    try
                    {
                        StorageService.instance.initServer();
                    }
                    catch (Exception e)
                    {
                        // I am tired of looking up my notes for how to fix this... so why not tell the user?
                        Throwable cause = com.google.common.base.Throwables.getRootCause(e);
                        if (cause instanceof BindException && "Can't assign requested address".equals(cause.getMessage()))
                            throw new RuntimeException("Unable to bind, run the following in a termanl and try again:\nfor subnet in $(seq 0 5); do for id in $(seq 0 5); do sudo ifconfig lo0 alias \"127.0.$subnet.$id\"; done; done;", e);
                        throw e;
                    }
                    StorageService.instance.removeShutdownHook();

                    Gossiper.waitToSettle();
                }
                else
                {
                    Schema.instance.startSync();
                    Stream peers = cluster.stream().filter(instance -> ((IInstance) instance).isValid());
                    SystemKeyspace.setLocalHostId(config.hostId());
                    if (config.has(BLANK_GOSSIP))
                        peers.forEach(peer -> GossipHelper.statusToBlank((IInvokableInstance) peer).accept(this));
                    else if (cluster instanceof Cluster)
                        peers.forEach(peer -> GossipHelper.statusToNormal((IInvokableInstance) peer).accept(this));
                    else
                        peers.forEach(peer -> GossipHelper.unsafeStatusToNormal(this, (IInstance) peer));

                    StorageService.instance.setUpDistributedSystemKeyspaces();
                    StorageService.instance.setNormalModeUnsafe();
                    Gossiper.instance.register(StorageService.instance);
                    StorageService.instance.startSnapshotManager();
                    StorageService.instance.completeInitialization();
                }

                // Populate tokenMetadata for the second time,
                // see org.apache.cassandra.service.CassandraDaemon.setup
                StorageService.instance.populateTokenMetadata();

                CassandraDaemon.getInstanceForTesting().completeSetup();

                if (config.has(NATIVE_PROTOCOL))
                {
                    CassandraDaemon.getInstanceForTesting().initializeClientTransports();
                    CassandraDaemon.getInstanceForTesting().start();
                }

                if (!FBUtilities.getBroadcastAddressAndPort().getAddress().equals(broadcastAddress().getAddress()) ||
                    FBUtilities.getBroadcastAddressAndPort().getPort() != broadcastAddress().getPort())
                    throw new IllegalStateException(String.format("%s != %s", FBUtilities.getBroadcastAddressAndPort(), broadcastAddress()));

                ActiveRepairService.instance().start();
                StreamManager.instance.start();

                PaxosState.startAutoRepairs();

                CassandraDaemon.getInstanceForTesting().completeSetup();
            }
            catch (Throwable t)
            {
                if (t instanceof RuntimeException)
                    throw (RuntimeException) t;
                throw new RuntimeException(t);
            }
        }).run();

        initialized = true;
    }

    private synchronized void startJmx()
    {
        this.isolatedJmx = new IsolatedJmx(this, inInstancelogger);
        isolatedJmx.startJmx();
    }

    private synchronized void stopJmx()
    {
        if (config.has(JMX))
        {
            isolatedJmx.stopJmx();
            isolatedJmx = null;
        }
    }

    // Update the messaging versions for all instances
    // that have initialized their configurations.
    private static void propagateMessagingVersions(ICluster cluster)
    {
        cluster.stream().forEach(reportToObj -> {
            IInstance reportTo = (IInstance) reportToObj;
            if (reportTo.isShutdown())
                return;

            int reportToVersion = reportTo.getMessagingVersion();
            if (reportToVersion == 0)
                return;

            cluster.stream().forEach(reportFromObj -> {
                IInstance reportFrom = (IInstance) reportFromObj;
                if (reportFrom == reportTo || reportFrom.isShutdown())
                    return;

                int reportFromVersion = reportFrom.getMessagingVersion();
                if (reportFromVersion == 0) // has not read configuration yet, no accessing messaging version
                    return;
                // TODO: decide if we need to take care of the minversion
                reportTo.setMessagingVersion(reportFrom.broadcastAddress(), reportFromVersion);
            });
        });
    }

    @Override
    public void postStartup()
    {
        sync(() ->
            StorageService.instance.doAuthSetup(false)
        ).run();
    }

    private void mkdirs()
    {
        new File(config.getString("saved_caches_directory")).tryCreateDirectories();
        new File(config.getString("hints_directory")).tryCreateDirectories();
        new File(config.getString("commitlog_directory")).tryCreateDirectories();
        for (String dir : (String[]) config.get("data_file_directories"))
            new File(dir).tryCreateDirectories();
    }

    private Config loadConfig(IInstanceConfig overrides)
    {
        Map<String, Object> params = overrides.getParams();
        boolean check = true;
        if (overrides.get(Constants.KEY_DTEST_API_CONFIG_CHECK) != null)
            check = (boolean) overrides.get(Constants.KEY_DTEST_API_CONFIG_CHECK);
        return YamlConfigurationLoader.fromMap(params, check, Config.class);
    }

    public Future<Void> shutdown()
    {
        return shutdown(true);
    }

    @Override
    public Future<Void> shutdown(boolean graceful)
    {
        inInstancelogger.info("Shutting down instance {} / {}", config.num(), config.broadcastAddress().getHostString());
        Future<?> future = async((ExecutorService executor) -> {
            Throwable error = null;

            error = parallelRun(error, executor,
                    () -> StorageService.instance.setRpcReady(false),
                    CassandraDaemon.getInstanceForTesting()::destroyClientTransports);

            if (config.has(GOSSIP) || config.has(NETWORK))
            {
                StorageService.instance.shutdownServer();
            }

            error = parallelRun(error, executor, StorageService.instance::disableAutoCompaction);
            while (CompactionManager.instance.hasOngoingOrPendingTasks() && !Thread.currentThread().isInterrupted())
            {
                inInstancelogger.info("Waiting for compactions to finish");
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }

            // trigger init early or else it could try to init and touch a thread pool that got shutdown
            HintsService hints = HintsService.instance;
            ThrowingRunnable shutdownHints = () -> {
                // this is to allow shutdown in the case hints were halted already
                try
                {
                    HintsService.instance.shutdownBlocking();
                }
                catch (IllegalStateException e)
                {
                    if (!"HintsService has already been shut down".equals(e.getMessage()))
                        throw e;
                }
            };
            error = parallelRun(error, executor,
                                () -> Gossiper.instance.stopShutdownAndWait(1L, MINUTES),
                                CompactionManager.instance::forceShutdown,
                                () -> BatchlogManager.instance.shutdownAndWait(1L, MINUTES),
                                shutdownHints,
                                () -> CompactionLogger.shutdownNowAndWait(1L, MINUTES),
                                () -> AuthCache.shutdownAllAndWait(1L, MINUTES),
                                () -> Sampler.shutdownNowAndWait(1L, MINUTES),
                                NettyStreamingChannel::shutdown,
                                () -> StreamReceiveTask.shutdownAndWait(1L, MINUTES),
                                () -> StreamTransferTask.shutdownAndWait(1L, MINUTES),
                                () -> StreamManager.instance.stop(),
                                () -> SecondaryIndexManager.shutdownAndWait(1L, MINUTES),
                                () -> IndexSummaryManager.instance.shutdownAndWait(1L, MINUTES),
                                () -> ColumnFamilyStore.shutdownExecutorsAndWait(1L, MINUTES),
                                () -> BufferPools.shutdownLocalCleaner(1L, MINUTES),
                                () -> PaxosRepair.shutdownAndWait(1L, MINUTES),
                                () -> Ref.shutdownReferenceReaper(1L, MINUTES),
                                () -> UncommittedTableData.shutdownAndWait(1L, MINUTES),
                                () -> AbstractAllocatorMemtable.MEMORY_POOL.shutdownAndWait(1L, MINUTES),
                                () -> DiagnosticSnapshotService.instance.shutdownAndWait(1L, MINUTES),
                                () -> SSTableReader.shutdownBlocking(1L, MINUTES),
                                () -> shutdownAndWait(Collections.singletonList(ActiveRepairService.repairCommandExecutor())),
                                () -> ActiveRepairService.instance().shutdownNowAndWait(1L, MINUTES),
                                () -> SnapshotManager.shutdownAndWait(1L, MINUTES)
            );

            internodeMessagingStarted = false;
            error = parallelRun(error, executor,
                                // can only shutdown message once, so if the test shutsdown an instance, then ignore the failure
                                (IgnoreThrowingRunnable) () -> MessagingService.instance().shutdown(1L, MINUTES, false, config.has(NETWORK))
            );
            error = parallelRun(error, executor,
                                () -> { if (config.has(NETWORK)) { try { GlobalEventExecutor.INSTANCE.awaitInactivity(1L, MINUTES); } catch (IllegalStateException ignore) {} } },
                                () -> Stage.shutdownAndWait(1L, MINUTES),
                                () -> SharedExecutorPool.SHARED.shutdownAndWait(1L, MINUTES)
            );

            // CommitLog must shut down after Stage, or threads from the latter may attempt to use the former.
            // (ex. A Mutation stage thread may attempt to add a mutation to the CommitLog.)
            error = parallelRun(error, executor, CommitLog.instance::shutdownBlocking);
            error = parallelRun(error, executor,
                                () -> PendingRangeCalculatorService.instance.shutdownAndWait(1L, MINUTES),
                                () -> shutdownAndWait(Collections.singletonList(JMXBroadcastExecutor.executor))
            );

            // ScheduledExecutors shuts down after MessagingService, as MessagingService may issue tasks to it.
            error = parallelRun(error, executor, () -> ScheduledExecutors.shutdownNowAndWait(1L, MINUTES));
            
            error = parallelRun(error, executor, this::stopJmx);

            error = parallelRun(error, executor, () -> DatabaseDescriptor.getCryptoProvider().uninstall());

            // Make sure any shutdown hooks registered for DeleteOnExit are released to prevent
            // references to the instance class loaders from being held
            if (graceful)
            {
                PathUtils.runOnExitThreadsAndClear();
            }
            else
            {
                PathUtils.clearOnExitThreads();
            }

            Throwables.maybeFail(error);
        }).apply(isolatedExecutor);

        return isolatedExecutor.submit(() -> {
            try
            {
                future.get();
                return null;
            }
            finally
            {
                super.shutdown();
                startedAt.set(0L);
            }
        });
    }

    @Override
    public int liveMemberCount()
    {
        if (!initialized || isShutdown())
            return 0;

        return sync(() -> {
            if (!DatabaseDescriptor.isDaemonInitialized() || !Gossiper.instance.isEnabled())
                return 0;
            return Gossiper.instance.getLiveMembers().size();
        }).call();
    }

    @Override
    public Metrics metrics()
    {
        return new InstanceMetrics(CassandraMetricsRegistry.Metrics);
    }

    @Override
    public NodeToolResult nodetoolResult(boolean withNotifications, String... commandAndArgs)
    {
        return sync(() -> {
            try (CapturingOutput output = new CapturingOutput();
                 DTestNodeTool nodetool = new DTestNodeTool(withNotifications, output.delegate))
            {
                // install security manager to get informed about the exit-code
                System.setSecurityManager(new SecurityManager()
                {
                    public void checkExit(int status)
                    {
                        throw new SystemExitException(status);
                    }

                    public void checkPermission(Permission perm)
                    {
                    }

                    public void checkPermission(Permission perm, Object context)
                    {
                    }
                });
                int rc;
                try
                {
                    rc = nodetool.execute(commandAndArgs);
                }
                catch (SystemExitException e)
                {
                    rc = e.status;
                }
                finally
                {
                    System.setSecurityManager(null);
                }
                return new NodeToolResult(commandAndArgs, rc,
                                          new ArrayList<>(nodetool.notifications.notifications),
                                          nodetool.latestError,
                                          output.getOutString(),
                                          output.getErrString());
            }
        }).call();
    }

    @Override
    public String toString()
    {
        return "node" + config.num();
    }

    private static class CapturingOutput implements Closeable
    {
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream outBase = new ByteArrayOutputStream();
        @SuppressWarnings("resource")
        private final ByteArrayOutputStream errBase = new ByteArrayOutputStream();

        public final PrintStream out;
        public final PrintStream err;
        private final Output delegate;

        public CapturingOutput()
        {
            PrintStream out = new PrintStream(outBase, true);
            PrintStream err = new PrintStream(errBase, true);
            this.delegate = new Output(out, err);
            this.out = out;
            this.err = err;
        }

        public String getOutString()
        {
            out.flush();
            return outBase.toString();
        }

        public String getErrString()
        {
            err.flush();
            return errBase.toString();
        }

        public void close()
        {
            out.close();
            err.close();
        }
    }

    public static class DTestNodeTool extends NodeTool implements AutoCloseable
    {
        private final StorageServiceMBean storageProxy;
        private final CollectingNotificationListener notifications = new CollectingNotificationListener();
        private final InternalNodeProbe internalNodeProbe;
        private Throwable latestError;

        public DTestNodeTool(boolean withNotifications, Output output)
        {
            super(new InternalNodeProbeFactory(withNotifications), output);
            internalNodeProbe = new InternalNodeProbe(withNotifications);
            storageProxy = internalNodeProbe.getStorageService();
            storageProxy.addNotificationListener(notifications, null, null);
        }

        public List<Notification> getNotifications()
        {
            return new ArrayList<>(notifications.notifications);
        }

        public Throwable getLatestError()
        {
            return latestError;
        }

        public int execute(String... args)
        {
            try
            {
                return super.execute(args);
            }
            finally
            {
                try
                {
                    storageProxy.removeNotificationListener(notifications, null, null);
                }
                catch (ListenerNotFoundException e)
                {
                    // ignored
                }
            }
        }

        protected void badUse(Exception e)
        {
            super.badUse(e);
            latestError = e;
        }

        protected void err(Throwable e)
        {
            if (e instanceof SystemExitException)
                return;
            super.err(e);
            latestError = e;
        }

        @Override
        public void close()
        {
            internalNodeProbe.close();
        }
    }

    private static final class CollectingNotificationListener implements NotificationListener
    {
        private final List<Notification> notifications = new CopyOnWriteArrayList<>();

        public void handleNotification(Notification notification, Object handback)
        {
            notifications.add(notification);
        }
    }

    public long killAttempts()
    {
        return callOnInstance(InstanceKiller::getKillAttempts);
    }

    private static void shutdownAndWait(List<ExecutorService> executors) throws TimeoutException, InterruptedException
    {
        ExecutorUtils.shutdownNowAndWait(1L, MINUTES, executors);
    }

    private static Throwable parallelRun(Throwable accumulate, ExecutorService runOn, ThrowingRunnable ... runnables)
    {
        List<Future<Throwable>> results = new ArrayList<>();
        for (ThrowingRunnable runnable : runnables)
        {
            results.add(runOn.submit(() -> {
                try
                {
                    runnable.run();
                    return null;
                }
                catch (Throwable t)
                {
                    return t;
                }
            }));
        }
        for (Future<Throwable> future : results)
        {
            try
            {
                Throwable t = future.get();
                if (t != null)
                    throw t;
            }
            catch (Throwable t)
            {
                accumulate = Throwables.merge(accumulate, t);
            }
        }
        return accumulate;
    }

    @FunctionalInterface
    private interface IgnoreThrowingRunnable extends ThrowingRunnable
    {
        void doRun() throws Throwable;

        @Override
        default void run()
        {
            try
            {
                doRun();
            }
            catch (Throwable e)
            {
                JVMStabilityInspector.inspectThrowable(e);
            }
        }
    }
}
