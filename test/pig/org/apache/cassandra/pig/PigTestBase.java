/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.pig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.MiniCluster;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;

public class PigTestBase extends SchemaLoader
{
    protected static EmbeddedCassandraService cassandra;
    protected static Configuration conf;
    protected static MiniCluster cluster; 
    protected static PigServer pig;
    protected static String defaultParameters= "init_address=localhost&rpc_port=9170&partitioner=org.apache.cassandra.dht.ByteOrderedPartitioner";
    protected static String nativeParameters = "&core_conns=2&max_conns=10&min_simult_reqs=3&max_simult_reqs=10&native_timeout=10000000"  +
                                               "&native_read_timeout=10000000&send_buff_size=4096&receive_buff_size=4096&solinger=3" +
                                               "&tcp_nodelay=true&reuse_address=true&keep_alive=true&native_port=9042";

    static
    {
        System.setProperty("logback.configurationFile", "logback-test.xml");
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Before
    public void beforeTest() throws Exception {
        pig = new PigServer(new PigContext(ExecType.LOCAL, ConfigurationUtil.toProperties(conf)));
        PigContext.initializeImportList("org.apache.cassandra.hadoop.pig");   
    }

    @After
    public void tearDown() throws Exception {
        pig.shutdown();
    }

    protected static Cassandra.Client getClient() throws TTransportException
    {
        TTransport tr = new TFramedTransport(new TSocket("localhost", 9170));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();
        return client;
    }

    protected static void startCassandra() throws IOException
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        cassandra = new EmbeddedCassandraService();
        cassandra.start();
    }

    protected static void startHadoopCluster()
    {
        cluster = MiniCluster.buildCluster();
        conf = cluster.getConfiguration();
    }

    protected AbstractType parseType(String type) throws IOException
    {
        try
        {
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }

    protected static void setupDataByCli(String[] statements) throws CharacterCodingException, ClassNotFoundException, TException, TimedOutException, NotFoundException, InvalidRequestException, NoSuchFieldException, UnavailableException, IllegalAccessException, InstantiationException
    {
        // new error/output streams for CliSessionState
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        // checking if we can connect to the running cassandra node on localhost
        CliMain.connect("127.0.0.1", 9170);

        // setting new output stream
        CliMain.sessionState.setOut(new PrintStream(outStream));
        CliMain.sessionState.setErr(new PrintStream(errStream));

        // re-creating keyspace for tests
        try
        {
            // dropping in case it exists e.g. could be left from previous run
            CliMain.processStatement("drop keyspace thriftKs;");
        }
        catch (Exception e)
        {
        }

        for (String statement : statements)
        {
            errStream.reset();
            System.out.println("Executing statement: " + statement);
            CliMain.processStatement(statement);
            String result = outStream.toString();
            System.out.println("result: " + result);
            outStream.reset(); // reset stream so we have only output from next statement all the time
            errStream.reset(); // no errors to the end user.
        }
    }
    
    protected static void setupDataByCql(String[] statements) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        Cassandra.Client client = getClient();
        // re-creating keyspace for tests
        try
        {
            // dropping in case it exists e.g. could be left from previous run
            client.execute_cql3_query(ByteBufferUtil.bytes("DROP KEYSPACE cql3ks"), Compression.NONE, ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
        }

        for (String statement : statements)
        {
            try
            {
                System.out.println("Executing statement: " + statement);
                client.execute_cql3_query(ByteBufferUtil.bytes(statement), Compression.NONE, ConsistencyLevel.ONE);
            }
            catch (SchemaDisagreementException e)
            {
                Assert.fail();
            }
        }
    }
}
