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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.ipc.ResponderServlet;
import org.apache.avro.specific.SpecificResponder;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * The Avro analogue to org.apache.cassandra.service.CassandraDaemon.
 *
 */
public class CassandraDaemon extends org.apache.cassandra.service.AbstractCassandraDaemon {
    private static Logger logger = LoggerFactory.getLogger(CassandraDaemon.class);
    private org.mortbay.jetty.Server server;

    /** hook for JSVC */
    public void start() throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug(String.format("Binding avro service to %s:%s", listenAddr, listenPort));
        CassandraServer cassandraServer = new CassandraServer();
        SpecificResponder responder = new SpecificResponder(Cassandra.class, cassandraServer);
        
        logger.info("Listening for avro clients...");

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
