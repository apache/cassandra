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
package org.apache.cassandra.service;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SharedMetricRegistries;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.JMXServerOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SizeEstimatesRecorder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.SystemKeyspaceMigrator41;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Locator;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.RegistrationStatus;
import org.apache.cassandra.tcm.Startup;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.net.StartupClusterConnectivityChecker;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.security.ThreadAwareSecurityManager;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.snapshot.SnapshotManager;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Mx4jTool;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.logging.LoggingSupportFactory;
import org.apache.cassandra.utils.logging.VirtualTableAppender;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_FOREGROUND;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_PID_FILE;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_CLASS_PATH;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_RMI_SERVER_RANDOM_ID;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VM_NAME;
import static org.apache.cassandra.config.CassandraRelevantProperties.SIZE_RECORDER_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.START_NATIVE_TRANSPORT;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.createMetricsKeyspaceTables;
import static org.apache.cassandra.schema.SchemaConstants.VIRTUAL_METRICS;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 */
public class CassandraDaemon
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";
    public static boolean SKIP_GC_INSPECTOR = CassandraRelevantProperties.SKIP_GC_INSPECTOR.getBoolean();

    private static final Logger logger;

    @VisibleForTesting
    public static CassandraDaemon getInstanceForTesting()
    {
        return instance;
    }

    @VisibleForTesting
    public NativeTransportService nativeTransportService()
    {
        return nativeTransportService;
    }

    static {
        // Need to register metrics before instrumented appender is created(first access to LoggerFactory).
        SharedMetricRegistries.getOrCreate("logback-metrics").addListener(new MetricRegistryListener.Base()
        {
            @Override
            public void onMeterAdded(String metricName, Meter meter)
            {
                // Given metricName consists of appender name in logback.xml + "." + metric name.
                // We first separate appender name
                int separator = metricName.lastIndexOf('.');
                String appenderName = metricName.substring(0, separator);
                String metric = metricName.substring(separator + 1); // remove "."
                CassandraMetricsRegistry.Metrics.register(DefaultNameFactory.createMetricName(appenderName, metric, null), meter);
            }
        });
        logger = LoggerFactory.getLogger(CassandraDaemon.class);
    }

    private void maybeInitJmx()
    {
        // If the standard com.sun.management.jmxremote.port property has been set
        // then the JVM agent will have already started up a default JMX connector
        // server. This behaviour is deprecated, but some clients may be relying
        // on it, so log a warning and skip setting up the server with the settings
        // as configured in cassandra.yaml or cassandra-env.sh.
        // See: CASSANDRA-11540 & CASSANDRA-11725
        if (COM_SUN_MANAGEMENT_JMXREMOTE_PORT.isPresent())
        {
            logger.warn("JMX settings in cassandra.yaml or cassandra-env.sh have been bypassed as the JMX connector server is " +
                        "already initialized. Please refer to cassandra.yaml or cassandra-env.sh for JMX configuration info");
            return;
        }

        JAVA_RMI_SERVER_RANDOM_ID.setBoolean(true);

        JMXServerOptions jmxServerOptions = DatabaseDescriptor.getJmxServerOptions();
        if (!jmxServerOptions.enabled)
            return;

        try
        {
            jmxServer = JMXServerUtils.createJMXServer(jmxServerOptions);
        }
        catch (IOException e)
        {
            exitOrFail(1, e.getMessage(), e.getCause());
        }
    }

    @VisibleForTesting
    public static Runnable SPECULATION_THRESHOLD_UPDATER = 
        () -> 
        {
            try
            {
                Keyspace.allExisting().forEach(k -> k.getColumnFamilyStores().forEach(ColumnFamilyStore::updateSpeculationThreshold));
            }
            catch (Throwable t)
            {
                logger.warn("Failed to update speculative retry thresholds.", t);
                JVMStabilityInspector.inspectThrowable(t);
            }
        };
    
    static final CassandraDaemon instance = new CassandraDaemon();

    private volatile NativeTransportService nativeTransportService;
    private JMXConnectorServer jmxServer;

    private final boolean runManaged;
    protected final StartupChecks startupChecks;
    private boolean setupCompleted;

    public CassandraDaemon()
    {
        this(false);
    }

    public CassandraDaemon(boolean runManaged)
    {
        this.runManaged = runManaged;
        this.startupChecks = new StartupChecks().withDefaultTests().withTest(new FileSystemOwnershipCheck());
        this.setupCompleted = false;
    }

    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     *
     * Subclasses should override this to finish the job (listening on ports, etc.)
     */
    protected void setup()
    {
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());

        // Since CASSANDRA-14793 the local system keyspaces data are not dispatched across the data directories
        // anymore to reduce the risks in case of disk failures. By consequence, the system need to ensure in case of
        // upgrade that the old data files have been migrated to the new directories before we start deleting
        // snapshots and upgrading system tables.
        try
        {
            migrateSystemDataIfNeeded();
        }
        catch (IOException e)
        {
            exitOrFail(StartupException.ERR_WRONG_DISK_STATE, e.getMessage(), e);
        }

        maybeInitJmx();

        Mx4jTool.maybeLoad();

        ThreadAwareSecurityManager.install();

        logSystemInfo(logger);

        NativeLibrary.tryMlockall();

        DatabaseDescriptor.createAllDirectories();
        Keyspace.setInitialized();
        CommitLog.instance.start();

        SnapshotManager.instance.start(false);
        SnapshotManager.instance.clearExpiredSnapshots();
        SnapshotManager.instance.clearEphemeralSnapshots();
        SnapshotManager.instance.resumeSnapshotCleanup();
        SnapshotManager.instance.registerMBean();

        // clearing of snapshots above here will in fact clear all ephemeral snapshots
        // which were cleared as part of startup checks before CASSANDRA-18111
        runStartupChecks();

        try
        {
            disableAutoCompaction(Schema.instance.localKeyspaces().names());
            Startup.initialize(DatabaseDescriptor.getSeeds());
            disableAutoCompaction(Schema.instance.distributedKeyspaces().names());
            CMSOperations.initJmx();
            if (ClusterMetadata.current().myNodeId() != null)
                RegistrationStatus.instance.onRegistration();
        }
        catch (InterruptedException | ExecutionException | IOException e)
        {
            throw new AssertionError("Can't initialize cluster metadata service", e);
        }
        catch (StartupException e)
        {
            exitOrFail(e.returnCode, e.getMessage(), e.getCause());
        }

        QueryProcessor.registerStatementInvalidatingListener();

        try
        {
            SystemKeyspace.snapshotOnVersionChange();
        }
        catch (Throwable e)
        {
            exitOrFail(StartupException.ERR_WRONG_DISK_STATE, e.getMessage(), e.getCause());
        }

        // We need to persist this as soon as possible after startup checks.
        // This should be the first write to SystemKeyspace (CASSANDRA-11742)
        SystemKeyspace.persistLocalMetadata();

        Thread.setDefaultUncaughtExceptionHandler(JVMStabilityInspector::uncaughtException);

        SystemKeyspaceMigrator41.migrate();
        setupVirtualKeyspaces();

        try
        {
            loadRowAndKeyCacheAsync().get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Error loading key or row cache", t);
        }

        if (!SKIP_GC_INSPECTOR)
        {
            try
            {
                GCInspector.register();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
            }
        }

        // Replay any CommitLogSegments found on disk
        PaxosState.initializeTrackers();

        // replay the log if necessary
        // TODO samt - when restarting a previously running instance, this needs to happen after reconstructing schema
        //  from the cluster metadata log or all mutations will throw IncompatibleSchemaException on deserialisation
        try
        {
            CommitLog.instance.recoverSegmentsOnDisk();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            PaxosState.maybeRebuildUncommittedState();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // Clean up system.size_estimates entries left lying around from missed keyspace drops (CASSANDRA-14905)
        SystemKeyspace.clearAllEstimates();

        // schedule periodic dumps of table size estimates into SystemKeyspace.SIZE_ESTIMATES_CF
        // set cassandra.size_recorder_interval to 0 to disable
        int sizeRecorderInterval = SIZE_RECORDER_INTERVAL.getInt();
        if (sizeRecorderInterval > 0)
            ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SizeEstimatesRecorder.instance, 30, sizeRecorderInterval, TimeUnit.SECONDS);

        ActiveRepairService.instance().start();
        StreamManager.instance.start();

        // Prepared statements
        QueryProcessor.instance.preloadPreparedStatements();

        // start server internals
        StorageService.instance.registerDaemon(this);
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            exitOrFail(1, "Fatal configuration error", e);
        }

        // The local rack may have been changed at some point, which will now be reflected in cluster metadata. Update
        // the system.local table just in case the actual value doesn't match what the configured location provided
        // reported when the earlier call to SystemKeyspace::persistLocalMetadata was made prior to initialising cluster
        // metadata.
        SystemKeyspace.updateRack(ClusterMetadata.current().locator.local().rack);
        ScheduledExecutors.optionalTasks.execute(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());

        // Because we are writing to the system_distributed keyspace, this should happen after that is created, which
        // happens in StorageService.instance.initServer()
        Runnable viewRebuild = () -> {
            for (Keyspace keyspace : Keyspace.all())
            {
                keyspace.viewManager.buildAllViews();
            }
            logger.debug("Completed submission of build tasks for any materialized views defined at startup");
        };

        ScheduledExecutors.optionalTasks.schedule(viewRebuild, StorageService.RING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        StorageService.instance.doAuthSetup();

        // re-enable auto-compaction after replay, so correct disk boundaries are used
        enableAutoCompaction(Schema.instance.getKeyspaces());

        AuditLogManager.instance.initialize();

        // schedule periodic background compaction task submission. this is simply a backstop against compactions stalling
        // due to scheduling errors or race conditions
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(ColumnFamilyStore.getBackgroundCompactionTaskSubmitter(), 5, 1, TimeUnit.MINUTES);

        // schedule periodic recomputation of speculative retry thresholds
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SPECULATION_THRESHOLD_UPDATER, 
                                                                DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS),
                                                                DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS),
                                                                NANOSECONDS);

        initializeClientTransports();

        // Ensure you've registered all caches during startup you want pre-warmed before this call -> be wary of adding
        // init below this mark before completeSetup().
        if (DatabaseDescriptor.getAuthCacheWarmingEnabled())
            AuthCacheService.instance.warmCaches();
        else
            logger.info("Prewarming of auth caches is disabled");

        PaxosState.startAutoRepairs();

        completeSetup();
    }

    public void runStartupChecks()
    {
        try
        {
            startupChecks.verify(DatabaseDescriptor.getStartupChecksOptions());
        }
        catch (StartupException e)
        {
            exitOrFail(e.returnCode, e.getMessage(), e.getCause());
        }

    }

    /**
     * Checks if the data of the local system keyspaces need to be migrated to a different location.
     *
     * @throws IOException
     */
    public void migrateSystemDataIfNeeded() throws IOException
    {
        // If there is only one directory and no system keyspace directory has been specified we do not need to do
        // anything. If it is not the case we want to try to migrate the data.
        if (!DatabaseDescriptor.useSpecificLocationForLocalSystemData()
                && DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations().length <= 1)
            return;

        // We can face several cases:
        //  1) The system data are spread accross the data file locations and need to be moved to
        //     the first data location (upgrade to 4.0)
        //  2) The system data are spread accross the data file locations and need to be moved to
        //     the system keyspace location configured by the user (upgrade to 4.0)
        //  3) The system data are stored in the first data location and need to be moved to
        //     the system keyspace location configured by the user (system_data_file_directory has been configured)
        Path target = File.getPath(DatabaseDescriptor.getLocalSystemKeyspacesDataFileLocations()[0]);

        String[] nonLocalSystemKeyspacesFileLocations = DatabaseDescriptor.getNonLocalSystemKeyspacesDataFileLocations();
        String[] sources = DatabaseDescriptor.useSpecificLocationForLocalSystemData() ? nonLocalSystemKeyspacesFileLocations
                                                                                      : Arrays.copyOfRange(nonLocalSystemKeyspacesFileLocations,
                                                                                                           1,
                                                                                                           nonLocalSystemKeyspacesFileLocations.length);

        for (String source : sources)
        {
            Path dataFileLocation = File.getPath(source);

            if (!Files.exists(dataFileLocation))
                continue;

            try (Stream<Path> locationChildren = Files.list(dataFileLocation))
            {
                Path[] keyspaceDirectories = locationChildren.filter(p -> SchemaConstants.isLocalSystemKeyspace(p.getFileName().toString()))
                                                             .toArray(Path[]::new);

                for (Path keyspaceDirectory : keyspaceDirectories)
                {
                    try (Stream<Path> keyspaceChildren = Files.list(keyspaceDirectory))
                    {
                        Path[] tableDirectories = keyspaceChildren.filter(Files::isDirectory)
                                                                  .filter(p -> SystemKeyspace.TABLES_SPLIT_ACROSS_MULTIPLE_DISKS.stream().noneMatch(t -> p.getFileName().toString().startsWith(t + '-')))
                                                                  .toArray(Path[]::new);

                        for (Path tableDirectory : tableDirectories)
                        {
                            FileUtils.moveRecursively(tableDirectory,
                                                      target.resolve(dataFileLocation.relativize(tableDirectory)));
                        }

                        if (!SchemaConstants.SYSTEM_KEYSPACE_NAME.equals(keyspaceDirectory.getFileName().toString()))
                        {
                            FileUtils.deleteDirectoryIfEmpty(keyspaceDirectory);
                        }
                    }
                }
            }
        }
    }

    public static void disableAutoCompaction(Collection<String> keyspaces)
    {
        for (String keyspaceName : keyspaces)
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace {}", keyspaceName);
            // disable auto compaction until gossip settles since disk boundaries may be affected by ring layout
            for (ColumnFamilyStore cfs : Keyspace.open(keyspaceName).getColumnFamilyStores())
            {
                for (ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.disableAutoCompaction();
                }
            }
        }
    }

    public static void enableAutoCompaction(Collection<String> keyspaces)
    {
        for (String ksNme : keyspaces)
        {
            Keyspace keyspace = Keyspace.open(ksNme);
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.reload(store.metadata()); //reload CFs in case there was a change of disk boundaries
                    if (store.getCompactionStrategyManager().shouldBeEnabled())
                    {
                        if (DatabaseDescriptor.getAutocompactionOnStartupEnabled())
                        {
                            store.enableAutoCompaction();
                        }
                        else
                        {
                            logger.info("Not enabling compaction for {}.{}; autocompaction_on_startup_enabled is set to false", store.getKeyspaceName(), store.name);
                        }
                    }
                }
            }
        }
    }

    public void setupVirtualKeyspaces()
    {
        VirtualKeyspaceRegistry.instance.register(VirtualSchemaKeyspace.instance);
        VirtualKeyspaceRegistry.instance.register(SystemViewsKeyspace.instance);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(VIRTUAL_METRICS, createMetricsKeyspaceTables()));

        // flush log messages to system_views.system_logs virtual table as there were messages already logged
        // before that virtual table was instantiated
        LoggingSupportFactory.getLoggingSupport()
                             .getAppender(VirtualTableAppender.class, VirtualTableAppender.APPENDER_NAME)
                             .ifPresent(appender -> ((VirtualTableAppender) appender).flushBuffer());
    }

    public synchronized void initializeClientTransports()
    {
        // Native transport
        if (nativeTransportService == null)
            nativeTransportService = new NativeTransportService();
    }

    /*
     * Asynchronously load the row and key cache in one off threads and return a compound future of the result.
     * Error handling is pushed into the cache load since cache loads are allowed to fail and are handled by logging.
     */
    private Future<?> loadRowAndKeyCacheAsync()
    {
        final Future<Integer> keyCacheLoad = CacheService.instance.keyCache.loadSavedAsync();

        final Future<Integer> rowCacheLoad = CacheService.instance.rowCache.loadSavedAsync();

        @SuppressWarnings("unchecked")
        Future<List<Integer>> retval = FutureCombiner.allOf(ImmutableList.of(keyCacheLoad, rowCacheLoad));

        return retval;
    }

    @VisibleForTesting
    public void completeSetup()
    {
        setupCompleted = true;
    }

    public boolean setupCompleted()
    {
        return setupCompleted;
    }

    public static void logSystemInfo(Logger logger)
    {
    	if (logger.isInfoEnabled())
    	{
	        try
	        {
	            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName() + ":" + DatabaseDescriptor.getStoragePort() + ":" + DatabaseDescriptor.getSSLStoragePort());
	        }
	        catch (UnknownHostException e1)
	        {
	            logger.info("Could not resolve local host");
	        }

	        logger.info("JVM vendor/version: {}/{}", JAVA_VM_NAME.getString(), JAVA_VERSION.getString());
	        logger.info("Heap size: {}/{}",
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().totalMemory()),
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().maxMemory()));

	        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
	            logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());

	        logger.info("Classpath: {}", JAVA_CLASS_PATH.getString());

            logger.info("JVM Arguments: {}", ManagementFactory.getRuntimeMXBean().getInputArguments());
    	}
    }

    /**
     * Initialize the Cassandra Daemon based on the given <a
     * href="http://commons.apache.org/daemon/jsvc.html">Commons
     * Daemon</a>-specific arguments. To clarify, this is a hook for JSVC.
     *
     * @param arguments
     *            the arguments passed in from JSVC
     * @throws IOException
     */
    public void init(String[] arguments) throws IOException
    {
        setup();
    }

    /**
     * Start the Cassandra Daemon, assuming that it has already been
     * initialized via {@link #init(String[])}
     *
     * Hook for JSVC
     */
    public void start()
    {
        StartupClusterConnectivityChecker connectivityChecker = StartupClusterConnectivityChecker.create(DatabaseDescriptor.getBlockForPeersTimeoutInSeconds(),
                                                                                                         DatabaseDescriptor.getBlockForPeersInRemoteDatacenters());
        Locator locator = DatabaseDescriptor.getLocator();
        Set<InetAddressAndPort> peers = new HashSet<>(ClusterMetadata.current().directory.allJoinedEndpoints());
        connectivityChecker.execute(peers, ep -> locator.location(ep).datacenter);

        // check to see if transports may start else return without starting.  This is needed when in survey mode or
        // when bootstrap has not completed.
        try
        {
            validateTransportsCanStart();
        }
        catch (IllegalStateException isx)
        {
            // If there are any errors, we just log and return in this case
            logger.warn(isx.getMessage());
            return;
        }

        startClientTransports();
    }

    private void startClientTransports()
    {
        String nativeFlag = START_NATIVE_TRANSPORT.getString();
        if (START_NATIVE_TRANSPORT.getBoolean() || (nativeFlag == null && DatabaseDescriptor.startNativeTransport()))
        {
            startNativeTransport();
            StorageService.instance.setRpcReady(true);
        }
        else
            logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");
    }

    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC / Procrun
     */
    public void stop()
    {
        // On linux, this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        destroyClientTransports();
        StorageService.instance.setRpcReady(false);

        if (jmxServer != null)
        {
            try
            {
                jmxServer.stop();
            }
            catch (IOException e)
            {
                logger.error("Error shutting down local JMX server: ", e);
            }
        }
    }

    @VisibleForTesting
    public void destroyClientTransports()
    {
        stopNativeTransport();
        if (nativeTransportService != null)
            nativeTransportService.destroy();
    }

    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate(boolean closeStdOutErr)
    {
        // Do not put any references to DatabaseDescriptor above the forceStaticInitialization call.
        try
        {
            applyConfig();

            registerNativeAccess();

            setup();

            String pidFile = CASSANDRA_PID_FILE.getString();

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (closeStdOutErr)
            {
                System.out.close();
                System.err.close();
            }

            start();

            logger.info("Startup complete");
        }
        catch (Throwable e)
        {
            boolean logStackTrace =
                    e instanceof ConfigurationException ? ((ConfigurationException)e).logStackTrace : true;

            System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

            if (logStackTrace)
            {
                if (runManaged)
                    logger.error("Exception encountered during startup", e);
                // try to warn user on stdout too, if we haven't already detached
                e.printStackTrace();
                exitOrFail(3, "Exception encountered during startup", e);
            }
            else
            {
                if (runManaged)
                    logger.error("Exception encountered during startup: {}", e.getMessage());
                // try to warn user on stdout too, if we haven't already detached
                System.err.println(e.getMessage());
                exitOrFail(3, "Exception encountered during startup: " + e.getMessage());
            }
        }
    }

    @VisibleForTesting
    public static void registerNativeAccess() throws javax.management.NotCompliantMBeanException
    {
        MBeanWrapper.instance.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), MBEAN_NAME, MBeanWrapper.OnException.LOG);
    }

    public void applyConfig()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public void validateTransportsCanStart()
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        MultiStepOperation<?> startupSequence = metadata.inProgressSequences.get(metadata.myNodeId());

        // We only start transports if bootstrap has completed, and we're not in survey mode, OR if we are in
        // survey mode and streaming has completed, but we're not using auth.
        // OR if we have not joined the ring yet.
        if (startupSequence != null)
        {
            if (StorageService.instance.isSurveyMode())
            {
                if (!StorageService.instance.readyToFinishJoiningRing() || DatabaseDescriptor.getAuthenticator().requireAuthentication())
                {
                    throw new IllegalStateException("Not starting client transports in write_survey mode as it's bootstrapping or " +
                                                    "auth is enabled");
                }
            }
            else
            {
                throw new IllegalStateException("Node is not yet bootstrapped completely");
            }
        }
        else
        {
            // Bootstrap with same address is an edge-case here, since we rely on HIBERNATE to prevent writes
            // toward the bootstrapping replacement, so there's no startup sequence involved.
            if (StorageService.instance.isReplacingSameAddress() && StorageService.instance.isSurveyMode())
                return;

            // This node has not joined the ring (i.e. it was started with -Dcassandra.join_ring=false)
            if (StorageService.instance.isStarting())
                return;

            if (!SystemKeyspace.bootstrapComplete())
            {
                throw new IllegalStateException("Node is not yet bootstrapped completely. Use nodetool to check bootstrap" +
                                                " state and resume. For more, see `nodetool help bootstrap`");
            }
        }
    }

    public void startNativeTransport()
    {
        validateTransportsCanStart();

        if (nativeTransportService == null)
            throw new IllegalStateException("setup() must be called first for CassandraDaemon");

        // this iterates over a collection of servers and returns true if one of them is started
        boolean alreadyRunning = nativeTransportService.isRunning();

        // this might in practice start all servers which are not started yet
        nativeTransportService.start();

        // interact with gossip only in case if no server was started before to signal they are started now
        if (!alreadyRunning)
            StorageService.instance.setRpcReady(true);
    }

    @Deprecated(since = "5.0.0")
    public void stopNativeTransport()
    {
        stopNativeTransport(false);
    }

    public void stopNativeTransport(boolean force)
    {
        if (nativeTransportService != null)
            nativeTransportService.stop(force);
    }

    public boolean isNativeTransportRunning()
    {
        return nativeTransportService != null && nativeTransportService.isRunning();
    }

    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
        // completely shut down cassandra
        if(!runManaged)
        {
            System.exit(0);
        }
    }

    public static void stop(String[] args)
    {
        instance.deactivate();
    }

    public static void main(String[] args)
    {
        instance.activate(CASSANDRA_FOREGROUND.getString() == null);
    }

    public void clearConnectionHistory()
    {
        nativeTransportService.clearConnectionHistory();
    }

    private void exitOrFail(int code, String message)
    {
        exitOrFail(code, message, null);
    }

    private void exitOrFail(int code, String message, Throwable cause)
    {
        if (runManaged)
        {
            RuntimeException t = cause!=null ? new RuntimeException(message, cause) : new RuntimeException(message);
            throw t;
        }
        else
        {
            logger.error(message, cause);
            System.exit(code);
        }
    }

    static class NativeAccess implements NativeAccessMBean
    {
        public boolean isAvailable()
        {
            return NativeLibrary.isAvailable();
        }

        public boolean isMemoryLockable()
        {
            return NativeLibrary.jnaMemoryLockable();
        }
    }

    public interface Server
    {
        /**
         * Start the server.
         * This method shoud be able to restart a server stopped through stop().
         * Should throw a RuntimeException if the server cannot be started
         */
        public void start();

        /**
         * Stop the server.
         * This method should be able to stop server started through start().
         * Should throw a RuntimeException if the server cannot be stopped
         */
        public void stop();

        /**
         * Returns whether the server is currently running.
         */
        public boolean isRunning();

        public void clearConnectionHistory();
    }
}
