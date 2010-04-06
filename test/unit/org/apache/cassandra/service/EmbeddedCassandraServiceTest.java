/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example how to use an embedded cassandra service.
 *
 * Tests connect to localhost:9160 when the embedded server is running.
 *
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class EmbeddedCassandraServiceTest
{

    private static EmbeddedCassandraService cassandra;

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException, InterruptedException, ConfigurationException
    {

        // Tell cassandra where the configuration files are.
        // Use the test configuration file.
        System.setProperty("storage-config", "test/conf");
        DatabaseDescriptor.readTablesFromXml();

        cassandra = new EmbeddedCassandraService();
        cassandra.init();

        // spawn cassandra in a new thread
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }

    @Test
    public void testEmbeddedCassandraService() throws UnsupportedEncodingException, InvalidRequestException,
            UnavailableException, TimedOutException, TException, NotFoundException
    {
        Cassandra.Client client = getClient();

        String key_user_id = "1";

        long timestamp = System.currentTimeMillis();
        ColumnPath cp = new ColumnPath("Standard1");
        cp.setColumn("name".getBytes("utf-8"));

        // insert
        client.insert("Keyspace1", key_user_id, cp, "Ran".getBytes("UTF-8"),
                timestamp, ConsistencyLevel.ONE);

        // read
        ColumnOrSuperColumn got = client.get("Keyspace1", key_user_id, cp,
                ConsistencyLevel.ONE);

        // assert
        assertNotNull("Got a null ColumnOrSuperColumn", got);
        assertEquals("Ran", new String(got.getColumn().getValue(), "utf-8"));
    }

    /**
     * Gets a connection to the localhost client
     *
     * @return
     * @throws TTransportException
     */
    private Cassandra.Client getClient() throws TTransportException
    {
        TTransport tr = new TSocket("localhost", 9170);
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }
}
