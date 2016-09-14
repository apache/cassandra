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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ThriftServer implements CassandraDaemon.Server
{
    private static final Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    protected final InetAddress address;
    protected final int port;
    protected final int backlog;
    private volatile ThriftServerThread server;

    public ThriftServer(InetAddress address, int port, int backlog)
    {
        this.address = address;
        this.port = port;
        this.backlog = backlog;
    }

    public void start()
    {
        if (server == null)
        {
            CassandraServer iface = getCassandraServer();
            server = new ThriftServerThread(address, port, backlog, getProcessor(iface), getTransportFactory());
            server.start();
        }
    }

    public synchronized void stop()
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

    /*
     * These methods are intended to be overridden to provide custom implementations.
     */
    protected CassandraServer getCassandraServer()
    {
        return new CassandraServer();
    }

    protected TProcessor getProcessor(CassandraServer server)
    {
        return new Cassandra.Processor<Cassandra.Iface>(server);
    }

    protected TTransportFactory getTransportFactory()
    {
        int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();
        return new TFramedTransport.Factory(tFramedTransportSize);
    }

    /**
     * Simple class to run the thrift connection accepting code in separate
     * thread of control.
     */
    private static class ThriftServerThread extends Thread
    {
        private final TServer serverEngine;

        public ThriftServerThread(InetAddress listenAddr,
                                  int listenPort,
                                  int listenBacklog,
                                  TProcessor processor,
                                  TTransportFactory transportFactory)
        {
            // now we start listening for clients
            logger.info("Binding thrift service to {}:{}", listenAddr, listenPort);

            TServerFactory.Args args = new TServerFactory.Args();
            args.tProtocolFactory = new TBinaryProtocol.Factory(true, true);
            args.addr = new InetSocketAddress(listenAddr, listenPort);
            args.listenBacklog = listenBacklog;
            args.processor = processor;
            args.keepAlive = DatabaseDescriptor.getRpcKeepAlive();
            args.sendBufferSize = DatabaseDescriptor.getRpcSendBufferSize();
            args.recvBufferSize = DatabaseDescriptor.getRpcRecvBufferSize();
            args.inTransportFactory = transportFactory;
            args.outTransportFactory = transportFactory;
            serverEngine = new TServerCustomFactory(DatabaseDescriptor.getRpcServerType()).buildTServer(args);
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

    public static final class ThriftServerType
    {
        public final static String SYNC = "sync";
        public final static String ASYNC = "async";
        public final static String HSHA = "hsha";
    }
}
