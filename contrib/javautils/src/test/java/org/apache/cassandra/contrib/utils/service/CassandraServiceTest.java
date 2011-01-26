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

package org.apache.cassandra.contrib.utils.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.AuthenticationException;
import org.apache.cassandra.thrift.AuthorizationException;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Example how to use an embedded and a data cleaner.
 *
 * @author Ran Tavory (rantav@gmail.com)
 *
 */
public class CassandraServiceTest {

    private static EmbeddedCassandraService cassandra;
    private static CassandraServiceDataCleaner cleaner;

    /**
     * Set embedded cassandra up and spawn it in a new thread.
     *
     * @throws TTransportException
     * @throws IOException
     * @throws InterruptedException
     */
    @BeforeClass
    public static void setup() throws TTransportException, IOException,
            InterruptedException, ConfigurationException {
        // Tell cassandra where the configuration files are.
        // Use the test configuration file.
        System.setProperty("cassandra.config", "file:../../test/conf/cassandra.yaml");
        System.setProperty("log4j.configuration", "file:../../test/conf/log4j-junit.properties");
        //String configUrl = System.getProperty("cassandra.config");
        loadYamlTables();
        initCleaner();
    }
    
    private static void initCleaner() throws IOException, TTransportException, ConfigurationException {
        cleaner = new CassandraServiceDataCleaner();
        cleaner.prepare();
        
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }
    

    /** Manually load tables from the test configuration file.
     * @throws ConfigurationException */
    private static void loadYamlTables() throws ConfigurationException {
      for (KSMetaData table : DatabaseDescriptor.readTablesFromYaml()) {
        for (CFMetaData cfm : table.cfMetaData().values()) {
          CFMetaData.map(cfm);
        }
        DatabaseDescriptor.setTableDefinition(table, DatabaseDescriptor.getDefsVersion());
      }
    }


    @Test
    public void testInProcessCassandraServer()
            throws UnsupportedEncodingException, InvalidRequestException,
            UnavailableException, TimedOutException, TException,
            NotFoundException, AuthenticationException, AuthorizationException {
        Cassandra.Client client = getClient();

        client.set_keyspace("Keyspace1");        

        String key_user_id = "1";
        
        long timestamp = System.currentTimeMillis();   

        // insert
        ColumnParent colParent = new ColumnParent("Standard1");
        Column column = new Column(ByteBufferUtil.bytes("name"), 
                ByteBufferUtil.bytes("Ran"), timestamp);
        
        client.insert(ByteBufferUtil.bytes(key_user_id), colParent, column, ConsistencyLevel.ONE);

        // read
        ColumnPath cp = new ColumnPath("Standard1");
        cp.setColumn(ByteBufferUtil.bytes("name"));

        ColumnOrSuperColumn got = client.get(ByteBufferUtil.bytes(key_user_id), cp,
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
    private Cassandra.Client getClient() throws TTransportException {
        TTransport tr = new TFramedTransport(new TSocket("localhost", 9170));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }
}
