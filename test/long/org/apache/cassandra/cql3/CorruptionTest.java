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
package org.apache.cassandra.cql3;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.utils.Bytes;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

public class CorruptionTest extends SchemaLoader
{

    private static EmbeddedCassandraService cassandra;
    private static Cluster cluster;
    private static Session session;

    private static PreparedStatement getStatement;
    private static PreparedStatement putStatement;
    private static String KEYSPACE = "cass_test";
    private static final String TABLE="put_test";
    private static final String KEY = "SingleFailingKey";
    private static String VALUE;
    private final int THREADPOOL_SIZE=40;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        Schema.instance.clear();

        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                         .withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
                         .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +" WITH replication " +
                        "= {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("USE " + KEYSPACE);
        session.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (" +
                         "key blob," +
                         "value blob," +
                         "PRIMARY KEY (key));");


        // Prepared statements
        getStatement = session.prepare("SELECT value FROM " + TABLE + " WHERE key = ?;");
        getStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);

        putStatement = session.prepare("INSERT INTO " + TABLE + " (key, value) VALUES (?, ?);");
        putStatement.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);



        StringBuilder s = new StringBuilder();
        char a='a';
        char z='z';
        for (int i = 0; i < 500*1024; i++)
        {
            char x = (char)((i%((z-a)+1))+a);
            if (x == 'a')
            {
                x = '\n';
            }
            s.append(x);
        }
        VALUE = s.toString();
    }

    @Test
    public void runCorruptionTest()
    {

        final CountDownLatch failure = new CountDownLatch(1);


        ExecutorService executor = Executors.newFixedThreadPool(THREADPOOL_SIZE);
        for (int i = 0; i < THREADPOOL_SIZE; i++)
        {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    for (int i = 0; i < 100000; i++)
                    {
                        put(KEY.getBytes(), VALUE.getBytes());
                        byte[] res = get(KEY.getBytes());
                        //since we're flooding the server we might get some timeouts, that's not
                        //relevant for this test
                        if (res == null)
                            continue;

                        if (!Arrays.equals(VALUE.getBytes(), res))
                        {
                            /*try
                            {
                                dumpKeys(VALUE.getBytes(), res);
                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }*/
                            failure.countDown();
                        }
                    }
                }

                private void dumpKeys(byte[] putdata, byte[] getdata) throws IOException {
                    String basename = "bad-data-tid" + Thread.currentThread().getId();
                    File put = new File(basename+"-put");
                    File get = new File(basename+"-get");
                    try(FileWriter pw = new FileWriter(put)) {
                        pw.write(new String(putdata));
                    }
                    try(FileWriter pw = new FileWriter(get)) {
                        pw.write(new String(getdata));
                    }
                }
            });
        }

        try
        {
            assert!failure.await(2, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {

        }
        executor.shutdownNow();

    }

    public static byte[] get(byte[] key)
    {
        BoundStatement boundStatement = new BoundStatement(getStatement);
        boundStatement.setBytes(0, ByteBuffer.wrap(key));

        final com.datastax.driver.core.ResultSet resultSet =  session.execute(boundStatement);
        final Row row = resultSet.one();
        if (row != null)
        {
            final ByteBuffer byteBuf = row.getBytes("value");
            return Bytes.getArray(byteBuf);
        }

        return null;
    }

    public static void put(byte[] key, byte[] value)
    {
        BoundStatement boundStatement = new BoundStatement(putStatement);
        boundStatement.setBytes(0, ByteBuffer.wrap(key));
        boundStatement.setBytes(1, ByteBuffer.wrap(value));

        session.execute(boundStatement);
    }
}
