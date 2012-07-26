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
package org.apache.cassandra.thrift;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

public class ThriftServer implements CassandraDaemon.Server
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftServer.class);
    private final static String SYNC = "sync";
    private final static String ASYNC = "async";
    private final static String HSHA = "hsha";
    public final static List<String> rpc_server_types = Arrays.asList(SYNC, ASYNC, HSHA);

    private final InetAddress address;
    private final int port;
    private volatile ThriftServerThread server;

    public ThriftServer(InetAddress address, int port)
    {
        this.address = address;
        this.port = port;
    }

    public void start()
    {
        if (server == null)
        {
            server = new ThriftServerThread(address, port);
            server.start();
        }
    }

    public void stop()
    {
        if (server != null)
        {
            server.stopServer();
            try
            {
                server.join();
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting thrift server to stop", e);
            }
            server = null;
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServerThread extends Thread
    {
        private TServer serverEngine;

        public ThriftServerThread(InetAddress listenAddr, int listenPort)
        {
            // now we start listening for clients
            final CassandraServer cassandraServer = new CassandraServer();
            Cassandra.Processor processor = new Cassandra.Processor(cassandraServer);

            // Transport
            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            // Protocol factory
            TProtocolFactory tProtocolFactory = new TBinaryProtocol.Factory(true, true, DatabaseDescriptor.getThriftMaxMessageLength());

            // Transport factory
            int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
            TTransportFactory inTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            TTransportFactory outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            logger.info("Using TFastFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);

            if (DatabaseDescriptor.getRpcServerType().equalsIgnoreCase(SYNC))
            {
                TServerTransport serverTransport;
                try
                {
                    serverTransport = new TCustomServerSocket(new InetSocketAddress(listenAddr, listenPort),
                                                              DatabaseDescriptor.getRpcKeepAlive(),
                                                              DatabaseDescriptor.getRpcSendBufferSize(),
                                                              DatabaseDescriptor.getRpcRecvBufferSize());
                }
                catch (TTransportException e)
                {
                    throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", listenAddr, listenPort), e);
                }
                // ThreadPool Server and will be invocation per connection basis...
                TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                                                                         .minWorkerThreads(DatabaseDescriptor.getRpcMinThreads())
                                                                         .maxWorkerThreads(DatabaseDescriptor.getRpcMaxThreads())
                                                                         .inputTransportFactory(inTransportFactory)
                                                                         .outputTransportFactory(outTransportFactory)
                                                                         .inputProtocolFactory(tProtocolFactory)
                                                                         .outputProtocolFactory(tProtocolFactory)
                                                                         .processor(processor);
                ExecutorService executorService = new CleaningThreadPool(cassandraServer.clientState, serverArgs.minWorkerThreads, serverArgs.maxWorkerThreads);
                serverEngine = new CustomTThreadPoolServer(serverArgs, executorService);
                logger.info(String.format("Using synchronous/threadpool thrift server on %s : %s", listenAddr, listenPort));
            }
            else
            {
                TNonblockingServerTransport serverTransport;
                try
                {
                    serverTransport = new TCustomNonblockingServerSocket(new InetSocketAddress(listenAddr, listenPort),
                                                                             DatabaseDescriptor.getRpcKeepAlive(),
                                                                             DatabaseDescriptor.getRpcSendBufferSize(),
                                                                             DatabaseDescriptor.getRpcRecvBufferSize());
                }
                catch (TTransportException e)
                {
                    throw new RuntimeException(String.format("Unable to create thrift socket to %s:%s", listenAddr, listenPort), e);
                }

                if (DatabaseDescriptor.getRpcServerType().equalsIgnoreCase(ASYNC))
                {
                    // This is single threaded hence the invocation will be all
                    // in one thread.
                    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(inTransportFactory)
                                                                                                     .outputTransportFactory(outTransportFactory)
                                                                                                     .inputProtocolFactory(tProtocolFactory)
                                                                                                     .outputProtocolFactory(tProtocolFactory)
                                                                                                     .processor(processor);
                    logger.info(String.format("Using non-blocking/asynchronous thrift server on %s : %s", listenAddr, listenPort));
                    serverEngine = new CustomTNonBlockingServer(serverArgs);
                }
                else if (DatabaseDescriptor.getRpcServerType().equalsIgnoreCase(HSHA))
                {
                    // This is NIO selector service but the invocation will be Multi-Threaded with the Executor service.
                    ExecutorService executorService = new JMXEnabledThreadPoolExecutor(DatabaseDescriptor.getRpcMinThreads(),
                                                                                       DatabaseDescriptor.getRpcMaxThreads(),
                                                                                       60L,
                                                                                       TimeUnit.SECONDS,
                                                                                       new SynchronousQueue<Runnable>(),
                                                                                       new NamedThreadFactory("RPC-Thread"), "RPC-THREAD-POOL");
                    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport).inputTransportFactory(inTransportFactory)
                                                                                       .outputTransportFactory(outTransportFactory)
                                                                                       .inputProtocolFactory(tProtocolFactory)
                                                                                       .outputProtocolFactory(tProtocolFactory)
                                                                                       .processor(processor);
                    logger.info(String.format("Using custom half-sync/half-async thrift server on %s : %s", listenAddr, listenPort));
                    // Check for available processors in the system which will be equal to the IO Threads.
                    serverEngine = new CustomTHsHaServer(serverArgs, executorService, Runtime.getRuntime().availableProcessors());
                }
            }
        }

        public void run()
        {
            logger.info("Listening for thrift clients...");
            serverEngine.serve();
        }

        public void stopServer()
        {
            logger.info("Stop listening to thrift clients");
            serverEngine.stop();
        }
    }

    /**
     * A subclass of Java's ThreadPoolExecutor which implements Jetty's ThreadPool
     * interface (for integration with Avro), and performs ClientState cleanup.
     *
     * (Note that the tasks being executed perform their own while-command-process
     * loop until the client disconnects.)
     */
    private static class CleaningThreadPool extends ThreadPoolExecutor
    {
        private final ThreadLocal<ClientState> state;
        public CleaningThreadPool(ThreadLocal<ClientState> state, int minWorkerThread, int maxWorkerThreads)
        {
            super(minWorkerThread, maxWorkerThreads, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new NamedThreadFactory("Thrift"));
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
