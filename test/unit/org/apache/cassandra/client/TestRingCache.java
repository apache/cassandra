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
package org.apache.cassandra.client;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 *  Sample code that uses RingCache in the client.
 */
public class TestRingCache
{
    private RingCache ringCache;
    private Cassandra.Client thriftClient;

    public TestRingCache(String keyspace) throws IOException
    {
        String seed = DatabaseDescriptor.getSeeds().iterator().next().getHostAddress();
    	ringCache = new RingCache(keyspace, DatabaseDescriptor.getPartitioner(), seed, DatabaseDescriptor.getRpcPort());
    }
    
    private void setup(String server, int port) throws Exception
    {
        /* Establish a thrift connection to the cassandra instance */
        TSocket socket = new TSocket(server, port);
        TTransport transport;
        System.out.println(" connected to " + server + ":" + port + ".");
        transport = socket;
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(transport);
        Cassandra.Client cassandraClient = new Cassandra.Client(binaryProtocol);
        transport.open();
        thriftClient = cassandraClient;
    }

    /**
     * usage: java -cp <configpath> org.apache.cassandra.client.TestRingCache [keyspace row-id-prefix row-id-int]
     * to test a single keyspace/row, use the parameters. row-id-prefix and row-id-int are appended together to form a
     * single row id.  If you supply now parameters, 'Keyspace1' is assumed and will check 9 rows ('row1' through 'row9').
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Throwable
    {
        int minRow;
        int maxRow;
        String rowPrefix, keyspace = "Keyspace1";
        
        if (args.length > 0)
        {
            keyspace = args[0];
            rowPrefix = args[1];
            minRow = Integer.parseInt(args[2]);
            maxRow = minRow + 1;
        }
        else
        {
            minRow = 1;
            maxRow = 10;
            rowPrefix = "row";
        }
        
        TestRingCache tester = new TestRingCache(keyspace);

        for (int nRows = minRow; nRows < maxRow; nRows++)
        {
            byte[] row = (rowPrefix + nRows).getBytes();
            ColumnPath col = new ColumnPath("Standard1").setSuper_column(null).setColumn("col1".getBytes());
            ColumnParent parent = new ColumnParent("Standard1").setSuper_column(null);

            Collection<InetAddress> endpoints = tester.ringCache.getEndpoint(row);
            InetAddress firstEndpoint = endpoints.iterator().next();
            System.out.printf("hosts with key %s : %s; choose %s%n",
                              new String(row), StringUtils.join(endpoints, ","), firstEndpoint);

            // now, read the row back directly from the host owning the row locally
            tester.setup(firstEndpoint.getHostAddress(), DatabaseDescriptor.getRpcPort());
            tester.thriftClient.set_keyspace(keyspace);
            Clock clock = new Clock();
            clock.setTimestamp(1);
            tester.thriftClient.insert(row, parent, new Column("col1".getBytes(), "val1".getBytes(), clock), ConsistencyLevel.ONE);
            Column column = tester.thriftClient.get(row, col, ConsistencyLevel.ONE).column;
            System.out.println("read row " + new String(row) + " " + new String(column.name) + ":" + new String(column.value) + ":" + column.clock.timestamp);
        }

        System.exit(1);
    }
}
