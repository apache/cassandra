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
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;

public class ThriftServer implements CassandraDaemon.Server
{
    private static Logger logger = LoggerFactory.getLogger(ThriftServer.class);
    final static String SYNC = "sync";
    final static String ASYNC = "async";
    final static String HSHA = "hsha";

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
            logger.info(String.format("Binding thrift service to %s:%s", listenAddr, listenPort));

            TServerFactory.Args args = new TServerFactory.Args();
            args.tProtocolFactory = new TBinaryProtocol.Factory(true, true, DatabaseDescriptor.getThriftMaxMessageLength());
            args.addr = new InetSocketAddress(listenAddr, listenPort);
            args.cassandraServer = new CassandraServer();
            args.processor = new Cassandra.Processor(args.cassandraServer);
            args.keepAlive = DatabaseDescriptor.getRpcKeepAlive();
            args.sendBufferSize = DatabaseDescriptor.getRpcSendBufferSize();
            args.recvBufferSize = DatabaseDescriptor.getRpcRecvBufferSize();
            int tFramedTransportSize = DatabaseDescriptor.getThriftFramedTransportSize();

            logger.info("Using TFramedTransport with a max frame size of {} bytes.", tFramedTransportSize);
            args.inTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
            args.outTransportFactory = new TFramedTransport.Factory(tFramedTransportSize);
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
}
