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
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

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
        super.setup();

        // now we start listening for clients
        final CassandraServer cassandraServer = new CassandraServer();
        Cassandra.Processor processor = new Cassandra.Processor(cassandraServer);

        // Transport
        TServerSocket tServerSocket = null;

        try
        {
            tServerSocket = new TCustomServerSocket(new InetSocketAddress(listenAddr, listenPort),
                                                    DatabaseDescriptor.getRpcKeepAlive(),
                                                    DatabaseDescriptor.getRpcSendBufferSize(),
                                                    DatabaseDescriptor.getRpcRecvBufferSize());
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
            inTransportFactory  = new TFastFramedTransport.Factory(64 * 1024, tFramedTransportSize);
            outTransportFactory = new TFastFramedTransport.Factory(64 * 1024, tFramedTransportSize);
            logger.info("Using TFastFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);
        }
        else
        {
            inTransportFactory = new TTransportFactory();
            outTransportFactory = new TTransportFactory();
        }

        // ThreadPool Server
        CustomTThreadPoolServer.Options options = new CustomTThreadPoolServer.Options();
        options.minWorkerThreads = MIN_WORKER_THREADS;

        ExecutorService executorService = new CleaningThreadPool(cassandraServer.clientState,
                                                                 options.minWorkerThreads,
                                                                 options.maxWorkerThreads);
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
