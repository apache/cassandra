/**
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
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.gms.Gossiper;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Mx4jTool;

/**
 * The <code>CassandraDaemon</code> is an abstraction for a Cassandra daemon
 * service, which defines not only a way to activate and deactivate it, but also
 * hooks into its lifecycle methods (see {@link #setup()}, {@link #start()},
 * {@link #stop()} and {@link #setup()}).
 * 
 */
public abstract class AbstractCassandraDaemon implements CassandraDaemon
{
    /**
     * Initialize logging in such a way that it checks for config changes every 10 seconds.
     */
    public static void initLog4j()
    {
        if (System.getProperty("log4j.defaultInitOverride","false").equalsIgnoreCase("true"))
        {
            String config = System.getProperty("log4j.configuration", "log4j-server.properties");
            URL configLocation = null;
            try
            {
                // try loading from a physical location first.
                configLocation = new URL(config);
            }
            catch (MalformedURLException ex)
            {
                // then try loading from the classpath.
                configLocation = AbstractCassandraDaemon.class.getClassLoader().getResource(config);
            }
        
            if (configLocation == null)
                throw new RuntimeException("Couldn't figure out log4j configuration: "+config);

            // Now convert URL to a filename
            String configFileName = null;
            try
            {
                // first try URL.getFile() which works for opaque URLs (file:foo) and paths without spaces
                configFileName = configLocation.getFile();
                File configFile = new File(configFileName);
                // then try alternative approach which works for all hierarchical URLs with or without spaces
                if (!configFile.exists())
                    configFileName = new File(configLocation.toURI()).getCanonicalPath();
            }
            catch (Exception e)
            {
                throw new RuntimeException("Couldn't convert log4j configuration location to a valid file", e);
            }

            PropertyConfigurator.configureAndWatch(configFileName, 10000);
            org.apache.log4j.Logger.getLogger(AbstractCassandraDaemon.class).info("Logging initialized");
        }
    }

    private static Logger logger = LoggerFactory.getLogger(AbstractCassandraDaemon.class);

    static final AtomicInteger exceptions = new AtomicInteger();
    
    protected InetAddress listenAddr;
    protected int listenPort;
    protected volatile boolean isRunning = false;
    
    /**
     * This is a hook for concrete daemons to initialize themselves suitably.
     *
     * Subclasses should override this to finish the job (listening on ports, etc.)
     *
     * @throws IOException
     */
    protected void setup() throws IOException
    {
    	logger.info("JVM vendor/version: {}/{}", System.getProperty("java.vm.name"), System.getProperty("java.version") );
        logger.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
		logger.info("Classpath: {}", System.getProperty("java.class.path"));
    	CLibrary.tryMlockall();

        listenPort = DatabaseDescriptor.getRpcPort();
        listenAddr = DatabaseDescriptor.getRpcAddress();

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                exceptions.incrementAndGet();
                logger.error("Fatal exception in thread " + t, e);
                for (Throwable e2 = e; e2 != null; e2 = e2.getCause())
                {
                    // some code, like FileChannel.map, will wrap an OutOfMemoryError in another exception
                    if (e2 instanceof OutOfMemoryError)
                        System.exit(100);
                }
            }
        });
        
        // check the system table to keep user from shooting self in foot by changing partitioner, cluster name, etc.
        // we do a one-off scrub of the system table first; we can't load the list of the rest of the tables,
        // until system table is opened.
        for (CFMetaData cfm : Schema.instance.getTableMetaData(Table.SYSTEM_TABLE).values())
            ColumnFamilyStore.scrubDataDirectories(Table.SYSTEM_TABLE, cfm.cfName);
        try
        {
            SystemTable.checkHealth();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }
        
        // load keyspace descriptions.
        try
        {
            DatabaseDescriptor.loadSchemas();
        }
        catch (IOException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }
        
        // clean up debris in the rest of the tables
        for (String table : Schema.instance.getTables())
        {
            for (CFMetaData cfm : Schema.instance.getTableMetaData(table).values())
            {
                ColumnFamilyStore.scrubDataDirectories(table, cfm.cfName);
            }
        }

        // initialize keyspaces
        for (String table : Schema.instance.getTables())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace " + table);
            Table.open(table);
        }

        try
        {
            GCInspector.instance.start();
        }
        catch (Throwable t)
        {
            logger.warn("Unable to start GCInspector (currently only supported on the Sun JVM)");
        }

        // replay the log if necessary
        CommitLog.recover();

        // check to see if CL.recovery modified the lastMigrationId. if it did, we need to re apply migrations. this isn't
        // the same as merely reloading the schema (which wouldn't perform file deletion after a DROP). The solution
        // is to read those migrations from disk and apply them.
        UUID currentMigration = Schema.instance.getVersion();
        UUID lastMigration = Migration.getLastMigrationId();
        if ((lastMigration != null) && (lastMigration.timestamp() > currentMigration.timestamp()))
        {
            Gossiper.instance.maybeInitializeLocalState(SystemTable.incrementAndGetGeneration());
            MigrationManager.applyMigrations(currentMigration, lastMigration);
        }
        
        SystemTable.finishStartup();

        // start server internals
        StorageService.instance.registerDaemon(this);
        try
        {
            StorageService.instance.initServer();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal configuration error", e);
            System.err.println(e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
            System.exit(1);
        }

        Mx4jTool.maybeLoad();
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
     *
     * @throws IOException
     */
    public void start()
    {
        if (Boolean.parseBoolean(System.getProperty("cassandra.start_rpc", "true")))
        {
            startRPCServer();
        }
        else
        {
            logger.info("Not starting RPC server as requested. Use JMX (StorageService->startRPCServer()) to start it");
        }
    }
    
    /**
     * Stop the daemon, ideally in an idempotent manner.
     *
     * Hook for JSVC
     */
    public void stop()
    {
        // this doesn't entirely shut down Cassandra, just the RPC server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        stopRPCServer();
    }

    /**
     * Start the underlying RPC server in idempotent manner.
     */
    public void startRPCServer()
    {
        if (!isRunning)
        {
            startServer();
            isRunning = true;
        }
    }

    /**
     * Stop the underlying RPC server in idempotent manner.
     */
    public void stopRPCServer()
    {
        if (isRunning)
        {
            stopServer();
            isRunning = false;
        }
    }

    /**
     * Returns whether the underlying RPC server is running or not.
     */
    public boolean isRPCServerRunning()
    {
        return isRunning;
    }

    /**
     * Start the underlying RPC server.
     * This method shoud be able to restart a server stopped through stopServer().
     * Should throw a RuntimeException if the server cannot be started
     */
    protected abstract void startServer();

    /**
     * Stop the underlying RPC server.
     * This method should be able to stop server started through startServer().
     * Should throw a RuntimeException if the server cannot be stopped
     */
    protected abstract void stopServer();

    
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
        String pidFile = System.getProperty("cassandra-pidfile");
        
        try
        {
            setup();
            
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
            logger.error("Exception encountered during startup", e);
            
            // try to warn user on stdout too, if we haven't already detached
            e.printStackTrace();
            System.out.println("Exception encountered during startup: " + e.getMessage());

            System.exit(3);
        }
    }
    
    /**
     * A convenience method to stop and destroy the daemon in one shot.
     */
    public void deactivate()
    {
        stop();
        destroy();
    }
    
    /**
     * A subclass of Java's ThreadPoolExecutor which implements Jetty's ThreadPool
     * interface (for integration with Avro), and performs ClientState cleanup.
     */
    public static class CleaningThreadPool extends ThreadPoolExecutor 
    {
        private ThreadLocal<ClientState> state;
        public CleaningThreadPool(ThreadLocal<ClientState> state, int minWorkerThread, int maxWorkerThreads)
        {
            super(minWorkerThread, maxWorkerThreads, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
            this.state = state;
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t)
        {
            super.afterExecute(r, t);
            DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
            state.get().logout();
        }


    }
}
