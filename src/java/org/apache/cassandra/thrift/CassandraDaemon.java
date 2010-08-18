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

package org.apache.cassandra.thrift;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Mx4jTool;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class supports two methods for creating a Cassandra node daemon, 
 * invoking the class's main method, and using the jsvc wrapper from 
 * commons-daemon, (for more information on using this class with the 
 * jsvc wrapper, see the 
 * <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation).
 */

public class CassandraDaemon extends org.apache.cassandra.service.AbstractCassandraDaemon
{
    private static Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);
    private TServer serverEngine;

    protected void setup() throws IOException
    {
        FBUtilities.tryMlockall();

        int listenPort = DatabaseDescriptor.getRpcPort();
        InetAddress listenAddr = DatabaseDescriptor.getRpcAddress();
        
        /* 
         * If ThriftAddress was left completely unconfigured, then assume
         * the same default as ListenAddress
         */
        if (listenAddr == null)
            listenAddr = FBUtilities.getLocalAddress();
        
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
        {
            public void uncaughtException(Thread t, Throwable e)
            {
                logger.error("Uncaught exception in thread " + t, e);
                if (e instanceof OutOfMemoryError)
                {
                    System.exit(100);
                }
            }
        });
        
        // check the system table for mismatched partitioner.
        try
        {
            SystemTable.checkHealth();
        }
        catch (ConfigurationException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }

        try
        {
            DatabaseDescriptor.loadSchemas();
        }
        catch (IOException e)
        {
            logger.error("Fatal exception during initialization", e);
            System.exit(100);
        }
        
        // initialize keyspaces
        for (String table : DatabaseDescriptor.getTables())
        {
            if (logger.isDebugEnabled())
                logger.debug("opening keyspace " + table);
            Table.open(table);
        }

        // replay the log if necessary and check for compaction candidates
        CommitLog.recover();
        CompactionManager.instance.checkAllColumnFamilies();
        
        // check to see if CL.recovery modified the lastMigrationId. if it did, we need to re apply migrations. this isn't
        // the same as merely reloading the schema (which wouldn't perform file deletion after a DROP). The solution
        // is to read those migrations from disk and apply them.
        UUID currentMigration = DatabaseDescriptor.getDefsVersion();
        UUID lastMigration = Migration.getLastMigrationId();
        if ((lastMigration != null) && (lastMigration.timestamp() > currentMigration.timestamp()))
        {
            MigrationManager.applyMigrations(currentMigration, lastMigration);
        }
        
        // start server internals
        StorageService.instance.initServer();
        
        // now we start listening for clients
        final CassandraServer cassandraServer = new CassandraServer();
        Cassandra.Processor processor = new Cassandra.Processor(cassandraServer);

        // Transport
        TServerSocket tServerSocket = null;
        
        try
        {
            tServerSocket = new TServerSocket(new InetSocketAddress(listenAddr, listenPort));
        }
        catch (TTransportException e)
        {
            throw new IOException(String.format("Unable to create thrift socket to %s:%s",
                                                listenAddr, listenPort), e);
        }
        
        logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

        // Protocol factory
        TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, 
                                                                        true, 
                                                                        DatabaseDescriptor.getThriftMaxMessageLength());
        
        // Transport factory
        TTransportFactory inTransportFactory, outTransportFactory;
        if (DatabaseDescriptor.isThriftFramed())
        {
            int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
            inTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            logger.info("Using TFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);
        }
        else
        {
            inTransportFactory = new TTransportFactory();
            outTransportFactory = new TTransportFactory();
        }


        // ThreadPool Server
        CustomTThreadPoolServer.Options options = new CustomTThreadPoolServer.Options();
        options.minWorkerThreads = 64;

        SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();

        ExecutorService executorService = new ThreadPoolExecutor(options.minWorkerThreads,
                                                                 options.maxWorkerThreads,
                                                                 60,
                                                                 TimeUnit.SECONDS,
                                                                 executorQueue)
        {
            @Override
            protected void afterExecute(Runnable r, Throwable t)
            {
                super.afterExecute(r, t);
                cassandraServer.logout();
            }
        };
        serverEngine = new CustomTThreadPoolServer(new TProcessorFactory(processor),
                                             tServerSocket,
                                             inTransportFactory,
                                             outTransportFactory,
                                             tProtocolFactory,
                                             tProtocolFactory,
                                             options,
                                             executorService);
    }

    /** hook for JSVC */
    public void start()
    {
        logger.info("Listening for thrift clients...");
        Mx4jTool.maybeLoad();
        serverEngine.serve();
    }

    /** hook for JSVC */
    public void stop()
    {
        // this doesn't entirely shut down Cassandra, just the Thrift server.
        // jsvc takes care of taking the rest down
        logger.info("Cassandra shutting down...");
        serverEngine.stop();
    }
    
    public static void main(String[] args)
    {
        new CassandraDaemon().activate();
    }
}
