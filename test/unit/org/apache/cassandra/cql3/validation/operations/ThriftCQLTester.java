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

package org.apache.cassandra.cql3.validation.operations;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;

public class ThriftCQLTester extends CQLTester
{
    private Cassandra.Client client;

    private static ThriftServer thriftServer;
    private static int thriftPort;

    static {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            thriftPort = serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            // ignore
        }
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        StorageService.instance.initServer(0);

        if (thriftServer == null || ! thriftServer.isRunning())
        {
            thriftServer = new ThriftServer(InetAddress.getLocalHost(), thriftPort, 50);
            thriftServer.start();
        }
    }

    @AfterClass
    public static void teardown()
    {
        if (thriftServer != null && thriftServer.isRunning())
        {
            thriftServer.stop();
        }
    }

    public Cassandra.Client getClient() throws Throwable
    {
        return getClient(InetAddress.getLocalHost().getHostName(), thriftPort);
    }

    public Cassandra.Client getClient(String hostname, int thriftPort) throws Throwable
	{
        if (client == null)
            client = new Cassandra.Client(new TBinaryProtocol(new TFramedTransportFactory().openTransport(hostname, thriftPort)));

        return client;
    }
}
