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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnectorServer;

import com.addthis.metrics3.reporter.config.ReporterConfig;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.batchlog.LegacyBatchlogMigrator;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.LegacyHintsMigrator;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.LegacySchemaMigrator;
import org.apache.cassandra.thrift.ThriftServer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.security.ThreadAwareSecurityManager;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 */
public class CassandraDaemon
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=NativeAccess";

    private static final Logger logger;
    static
    {
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
                ObjectName name = DefaultNameFactory.createMetricName(appenderName, metric, null).getMBeanName();
                CassandraMetricsRegistry.Metrics.registerMBean(meter, name);
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
        // as configured in cassandra-env.(sh|ps1)
        // See: CASSANDRA-11540 & CASSANDRA-11725
        if (System.getProperty("com.sun.management.jmxremote.port") != null)
        {
            logger.warn("JMX settings in cassandra-env.sh have been bypassed as the JMX connector server is " +
                        "already initialized. Please refer to cassandra-env.(sh|ps1) for JMX configuration info");
            return;
        }

        System.setProperty("java.rmi.server.randomIDs", "true");

        // If a remote port has been specified then use that to set up a JMX
        // connector server which can be accessed remotely. Otherwise, look
        // for the local port property and create a server which is bound
        // only to the loopback address. Auth options are applied to both
        // remote and local-only servers, but currently SSL is only
        // available for remote.
        // If neither is remote nor local port is set in cassandra-env.(sh|ps)
        // then JMX is effectively  disabled.
        boolean localOnly = false;
        String jmxPort = System.getProperty("cassandra.jmx.remote.port");

        if (jmxPort == null)
        {
            localOnly = true;
            jmxPort = System.getProperty("cassandra.jmx.local.port");
        }

        if (jmxPort == null)
            return;

        try
        {
            jmxServer = JMXServerUtils.createJMXServer(Integer.parseInt(jmxPort), localOnly);
            if (jmxServer == null)
                return;
        }
        catch (IOException e)
        {
            exitOrFail(1, e.getMessage(), e.getCause());
        }
    }

    static final CassandraDaemon instance = new CassandraDaemon();

    public Server thriftServer;
    private NativeTransportService nativeTransportService;
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
        this.startupChecks = new StartupChecks().withDefaultTests();
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

        // Delete any failed snapshot deletions on Windows - see CASSANDRA-9658
        if (FBUtilities.isWindows)
            WindowsFailedSnapshotTracker.deleteOldSnapshots();

        maybeInitJmx();

        Mx4jTool.maybeLoad();

        ThreadAwareSecurityManager.install();

        logSystemInfo();

        NativeLibrary.tryMlockall();

        try
        {
            startupChecks.verify();
        }
        catch (StartupException e)
        {
            exitOrFail(e.returnCode, e.getMessage(), e.getCause());
        }

        try
        {
            if (SystemKeyspace.snapshotOnVersionChange())
            {
                SystemKeyspace.migrateDataDirs();
            }
        }
        catch (IOException e)
        {
            exitOrFail(3, e.getMessage(), e.getCause());
        }

        // We need to persist this as soon as possible after startup checks.
        // This should be the first write to SystemKeyspace (CASSANDRA-11742)
        SystemKeyspace.persistLocalMetadata();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                StorageMetrics.exceptions.inc();
                logger.error("Exception in thread " + t, e);
                Tracing.trace("Exception in thread {}", t, e);
                for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
                {
                    JVMStabilityInspector.inspectThrowable(e2);

                    if (e2 instanceof FSError)
                    {
                        if (e2 != e) // make sure FSError gets logged exactly once.
                            logger.error("Exception in thread " + t, e2);
                        FileUtils.handleFSError((FSError) e2);
                    }

                    if (e2 instanceof CorruptSSTableException)
                    {
                        if (e2 != e)
                            logger.error("Exception in thread " + t, e2);
                        FileUtils.handleCorruptSSTable((CorruptSSTableException) e2);
                    }
                }
            }
        });

        /*
         * Migrate pre-3.0 keyspaces, tables, types, functions, and aggregates, to their new 3.0 storage.
         * We don't (and can't) wait for commit log replay here, but we don't need to - all schema changes force
         * explicit memtable flushes.
         */
        LegacySchemaMigrator.migrate();

        // Populate token metadata before flushing, for token-aware sstable partitioning (#6696)
        StorageService.instance.populateTokenMetadata();

        // load schema from disk
        Schema.instance.loadFromDisk();

        // clean up debris in the rest of the keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            // Skip system as we've already cleaned it
            if (keyspaceName.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                continue;

            for (CFMetaData cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                try
                {
                    ColumnFamilyStore.scrubDataDirectories(cfm);
                }
                catch (StartupException e)
                {
                    exitOrFail(e.returnCode, e.getMessage(), e.getCause());
                }
            }
        }

        Keyspace.setInitialized();

        // initialize keyspaces
        for (String keyspaceName : Schema.instance.getKeyspaces())
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

        try
        {
            loadRowAndKeyCacheAsync().get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Error loading key or row cache", t);
        }

        try
        {
            GCInspector.register();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

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

        // migrate any legacy (pre-3.0) hints from system.hints table into the new store
        new LegacyHintsMigrator(DatabaseDescriptor.getHintsDirectory(), DatabaseDescriptor.getMaxHintsFileSize()).migrate();

        // migrate any legacy (pre-3.0) batch entries from system.batchlog to system.batches (new table format)
        LegacyBatchlogMigrator.migrate();

        SystemKeyspace.finishStartup();

        // Clean up system.size_estimates entries left lying around from missed keyspace drops (CASSANDRA-14905)
        StorageService.instance.cleanupSizeEstimates();

        // schedule periodic dumps of table size estimates into SystemKeyspace.SIZE_ESTIMATES_CF
        // set cassandra.size_recorder_interval to 0 to disable
        int sizeRecorderInterval = Integer.getInteger("cassandra.size_recorder_interval", 5 * 60);
        if (sizeRecorderInterval > 0)
            ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(SizeEstimatesRecorder.instance, 30, sizeRecorderInterval, TimeUnit.SECONDS);

        // Prepared statements
        QueryProcessor.preloadPreparedStatement();

        // Metrics
        String metricsReporterConfigFile = System.getProperty("cassandra.metricsReporterConfigFile");
        if (metricsReporterConfigFile != null)
        {
            logger.info("Trying to load metrics-reporter-config from file: {}", metricsReporterConfigFile);
            try
            {
                // enable metrics provided by metrics-jvm.jar
                CassandraMetricsRegistry.Metrics.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
                CassandraMetricsRegistry.Metrics.register("jvm.gc", new GarbageCollectorMetricSet());
                CassandraMetricsRegistry.Metrics.register("jvm.memory", new MemoryUsageGaugeSet());
                CassandraMetricsRegistry.Metrics.register("jvm.fd.usage", new FileDescriptorRatioGauge());
                // initialize metrics-reporter-config from yaml file
                URL resource = CassandraDaemon.class.getClassLoader().getResource(metricsReporterConfigFile);
                if (resource == null)
                {
                    logger.warn("Failed to load metrics-reporter-config, file does not exist: {}", metricsReporterConfigFile);
                }
                else
                {
                    String reportFileLocation = resource.getFile();
                    ReporterConfig.loadFromFile(reportFileLocation).enableAll(CassandraMetricsRegistry.Metrics);
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to load metrics-reporter-config, metric sinks will not be activated", e);
            }
        }

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

        // Because we are writing to the system_distributed keyspace, this should happen after that is created, which
        // happens in StorageService.instance.initServer()
        Runnable viewRebuild = () -> {
            for (Keyspace keyspace : Keyspace.all())
            {
                keyspace.viewManager.buildAllViews();
            }
            logger.debug("Completed submission of build tasks for any materialized views defined at startup");
        };

        ScheduledExecutors.optionalTasks.schedule(viewRebuild, StorageService.RING_DELAY, TimeUnit.MILLISECONDS);

        if (!FBUtilities.getBroadcastAddress().equals(InetAddress.getLoopbackAddress()))
            Gossiper.waitToSettle();

        // re-enable auto-compaction after gossip is settled, so correct disk boundaries are used
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                for (final ColumnFamilyStore store : cfs.concatWithIndexes())
                {
                    store.reload(); //reload CFs in case there was a change of disk boundaries
                    if (store.getCompactionStrategyManager().shouldBeEnabled())
                    {
                        store.enableAutoCompaction();
                    }
                }
            }
        }

        // schedule periodic background compaction task submission. this is simply a backstop against compactions stalling
        // due to scheduling errors or race conditions
        ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(ColumnFamilyStore.getBackgroundCompactionTaskSubmitter(), 5, 1, TimeUnit.MINUTES);

        // Thrift
        InetAddress rpcAddr = DatabaseDescriptor.getRpcAddress();
        int rpcPort = DatabaseDescriptor.getRpcPort();
        int listenBacklog = DatabaseDescriptor.getRpcListenBacklog();
        thriftServer = new ThriftServer(rpcAddr, rpcPort, listenBacklog);

        // Native transport
        nativeTransportService = new NativeTransportService();

        completeSetup();
    }

    /*
     * Asynchronously load the row and key cache in one off threads and return a compound future of the result.
     * Error handling is pushed into the cache load since cache loads are allowed to fail and are handled by logging.
     */
    private ListenableFuture<?> loadRowAndKeyCacheAsync()
    {
        final ListenableFuture<Integer> keyCacheLoad = CacheService.instance.keyCache.loadSavedAsync();

        final ListenableFuture<Integer> rowCacheLoad = CacheService.instance.rowCache.loadSavedAsync();

        @SuppressWarnings("unchecked")
        ListenableFuture<List<Integer>> retval = Futures.successfulAsList(keyCacheLoad, rowCacheLoad);

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

    private void logSystemInfo()
    {
    	if (logger.isInfoEnabled())
    	{
	        try
	        {
	            logger.info("Hostname: {}", InetAddress.getLocalHost().getHostName());
	        }
	        catch (UnknownHostException e1)
	        {
	            logger.info("Could not resolve local host");
	        }

	        logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version"));
	        logger.info("Heap size: {}/{}",
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().totalMemory()),
                        FBUtilities.prettyPrintMemory(Runtime.getRuntime().maxMemory()));

	        for(MemoryPoolMXBean pool: ManagementFactory.getMemoryPoolMXBeans())
	            logger.info("{} {}: {}", pool.getName(), pool.getType(), pool.getPeakUsage());

	        logger.info("Classpath: {}", System.getProperty("java.class.path"));

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
        // We only start transports if bootstrap has completed and we're not in survey mode, OR if we are in
        // survey mode and streaming has completed but we're not using auth.
        // OR if we have not joined the ring yet.
        if (StorageService.instance.hasJoined())
        {
            if (StorageService.instance.isSurveyMode())
            {
                if (StorageService.instance.isBootstrapMode() || DatabaseDescriptor.getAuthenticator().requireAuthentication())
                {
                    logger.info("Not starting client transports in write_survey mode as it's bootstrapping or " +
                            "auth is enabled");
                    return;
                }
            }
            else
            {
                if (!SystemKeyspace.bootstrapComplete())
                {
                    logger.info("Not starting client transports as bootstrap has not completed");
                    return;
                }
            }
        }

        String nativeFlag = System.getProperty("cassandra.start_native_transport");
        if ((nativeFlag != null && Boolean.parseBoolean(nativeFlag)) || (nativeFlag == null && DatabaseDescriptor.startNativeTransport()))
        {
            startNativeTransport();
            StorageService.instance.setRpcReady(true);
        }
        else
            logger.info("Not starting native transport as requested. Use JMX (StorageService->startNativeTransport()) or nodetool (enablebinary) to start it");

        String rpcFlag = System.getProperty("cassandra.start_rpc");
        if ((rpcFlag != null && Boolean.parseBoolean(rpcFlag)) || (rpcFlag == null && DatabaseDescriptor.startRpc()))
            thriftServer.start();
        else
            logger.info("Not starting RPC server as requested. Use JMX (StorageService->startRPCServer()) or nodetool (enablethrift) to start it");
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
        if (thriftServer != null)
            thriftServer.stop();
        if (nativeTransportService != null)
            nativeTransportService.destroy();
        StorageService.instance.setRpcReady(false);

        // On windows, we need to stop the entire system as prunsrv doesn't have the jsvc hooks
        // We rely on the shutdown hook to drain the node
        if (FBUtilities.isWindows)
            System.exit(0);

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


    /**
     * Clean up all resources obtained during the lifetime of the daemon. This
     * is a hook for JSVC.
     */
    public void destroy()
    {}

    /**
     * A convenience method to initialize and start the daemon in one shot.
     */
    public void activate()
    {
        // Do not put any references to DatabaseDescriptor above the forceStaticInitialization call.
        try
        {
            applyConfig();

            MBeanWrapper.instance.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), MBEAN_NAME, MBeanWrapper.OnException.LOG);

            if (FBUtilities.isWindows)
            {
                // We need to adjust the system timer on windows from the default 15ms down to the minimum of 1ms as this
                // impacts timer intervals, thread scheduling, driver interrupts, etc.
                WindowsTimer.startTimerPeriod(DatabaseDescriptor.getWindowsTimerInterval());
            }

            setup();

            String pidFile = System.getProperty("cassandra-pidfile");

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            start();
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

    public void applyConfig()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public void startNativeTransport()
    {
        // We only start transports if bootstrap has completed and we're not in survey mode, OR if we are in
        // survey mode and streaming has completed but we're not using auth.
        // OR if we have not joined the ring yet.
        if (StorageService.instance.hasJoined())
        {
            if (StorageService.instance.isSurveyMode())
            {
                if (StorageService.instance.isBootstrapMode() || DatabaseDescriptor.getAuthenticator().requireAuthentication())
                {
                    throw new IllegalStateException("Not starting client transports in write_survey mode as it's bootstrapping or " +
                            "auth is enabled");
                }
            }
            else
            {
                if (!SystemKeyspace.bootstrapComplete())
                {
                    throw new IllegalStateException("Node is not yet bootstrapped completely. Use nodetool to check bootstrap" +
                            " state and resume. For more, see `nodetool help bootstrap`");
                }
            }
        }

        if (nativeTransportService == null)
            throw new IllegalStateException("setup() must be called first for CassandraDaemon");
        else
            nativeTransportService.start();
    }

    public void stopNativeTransport()
    {
        if (nativeTransportService != null)
            nativeTransportService.stop();
    }

    public boolean isNativeTransportRunning()
    {
        return nativeTransportService != null ? nativeTransportService.isRunning() : false;
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
        instance.activate();
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
    }
}
