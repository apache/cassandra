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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.specific.SpecificResponder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Mx4jTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.log4j.PropertyConfigurator;

/**
 * The Avro analogue to org.apache.cassandra.service.CassandraDaemon.
 *
 */
public class CassandraDaemon {
    private static Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);
    private HttpServer server;
    private InetAddress listenAddr;
    private int listenPort;
    
    private void setup() throws IOException
    {
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

        // start server internals
        StorageService.instance.initServer();

    }
    
    /** hook for JSVC */
    public void load(String[] arguments) throws IOException
    {
        setup();
    }
    
    /** hook for JSVC */
    public void start() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format("Binding avro service to %s:%s", listenAddr, listenPort));
        SpecificResponder responder = new SpecificResponder(Cassandra.class, new CassandraServer());
        
        logger.info("Cassandra starting up...");
        Mx4jTool.maybeLoad();
        // FIXME: This isn't actually binding to listenAddr (it should).
        server = new HttpServer(responder, listenPort);
    }
    
    /** hook for JSVC */
    public void stop()
    {
        logger.info("Cassandra shutting down...");
        server.close();
    }
    
    /** hook for JSVC */
    public void destroy()
    {
    }
    
    public static void main(String[] args) {
        CassandraDaemon daemon = new CassandraDaemon();
        String pidFile = System.getProperty("cassandra-pidfile");
        
        try
        {   
            daemon.setup();

            if (pidFile != null)
            {
                new File(pidFile).deleteOnExit();
            }

            if (System.getProperty("cassandra-foreground") == null)
            {
                System.out.close();
                System.err.close();
            }

            daemon.start();
        }
        catch (Throwable e)
        {
            String msg = "Exception encountered during startup.";
            logger.error(msg, e);

            // try to warn user on stdout too, if we haven't already detached
            System.out.println(msg);
            e.printStackTrace();

            System.exit(3);
        }
    }

}
