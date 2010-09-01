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

package org.apache.cassandra.avro;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.ResponderServlet;
import org.apache.avro.specific.SpecificResponder;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.migration.Migration;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Mx4jTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// see CASSANDRA-1440
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * The Avro analogue to org.apache.cassandra.service.CassandraDaemon.
 *
 */
public class CassandraDaemon extends org.apache.cassandra.service.AbstractCassandraDaemon {
    private static Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);
    private org.mortbay.jetty.Server server;
    private InetAddress listenAddr;
    private int listenPort;
    
    protected void setup() throws IOException
    {
        FBUtilities.tryMlockall();

        listenPort = DatabaseDescriptor.getRpcPort();
        listenAddr = DatabaseDescriptor.getRpcAddress();
        
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
                logger.error("Fatal exception in thread " + t, e);
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
        
        SystemTable.purgeIncompatibleHints();

        // start server internals
        StorageService.instance.initServer();

    }
    
    /** hook for JSVC */
    public void start() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format("Binding avro service to %s:%s", listenAddr, listenPort));
        CassandraServer cassandraServer = new CassandraServer();
        SpecificResponder responder = new SpecificResponder(Cassandra.class, cassandraServer);
        
        logger.info("Listening for avro clients...");
        Mx4jTool.maybeLoad();

        // FIXME: This isn't actually binding to listenAddr (it should).
        server = new org.mortbay.jetty.Server(listenPort);
        server.setThreadPool(new CleaningThreadPool(cassandraServer.clientState,
                                                    MIN_WORKER_THREADS,
                                                    Integer.MAX_VALUE));
        try
        {
            // see CASSANDRA-1440
            ResponderServlet servlet = new ResponderServlet(responder);
            new Context(server, "/").addServlet(new ServletHolder(servlet), "/*");

            server.start();
        }
        catch (Exception e)
        {
            throw new IOException("Could not start Avro server.", e);
        }
    }
    
    /** hook for JSVC */
    public void stop()
    {
        logger.info("Cassandra shutting down...");
        try
        {
            server.stop();
        }
        catch (Exception e)
        {
            logger.error("Avro server did not exit cleanly.", e);
        }
    }
    
    public static void main(String[] args) {
        new CassandraDaemon().activate();
    }
}
