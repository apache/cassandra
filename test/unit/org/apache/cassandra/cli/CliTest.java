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

package org.apache.cassandra.cli;

import junit.framework.TestCase;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class CliTest extends TestCase
{
    // please add new statements here so they could be auto-runned by this test.
    private String[] statements = {
        "use TestKeySpace",
        "create column family CF1 with comparator=UTF8Type and column_metadata=[{ column_name:world, validation_class:IntegerType, index_type:0, index_name:IdxName }, { column_name:world2, validation_class:LongType, index_type:0, index_name:LongIdxName}]",
        "set CF1[hello][world] = 123848374878933948398384",
        "get CF1[hello][world]",
        "set CF1[hello][world2] = 15",
        "get CF1 where world2 = long(15)",
        "set CF1['hello'][time_spent_uuid] = timeuuid(a8098c1a-f86e-11da-bd1a-00112444be1e)",
        "create column family CF2 with comparator=IntegerType",
        "set CF2['key'][98349387493847748398334] = 'some text'",
        "get CF2['key'][98349387493847748398334]",
        "set CF2['key'][98349387493] = 'some text other'",
        "get CF2['key'][98349387493]",
        "create column family CF3 with comparator=UTF8Type and column_metadata=[{column_name:'big world', validation_class:LongType}]",
        "set CF3['hello']['big world'] = 3748",
        "get CF3['hello']['big world']",
        "list CF3",
        "list CF3[:]",
        "list CF3[h:]",
        "list CF3 limit 10",
        "list CF3[h:g] limit 10",
        "truncate CF1",
        "update keyspace TestKeySpace with placement_strategy='org.apache.cassandra.locator.LocalStrategy'",
        "update keyspace TestKeySpace with replication_factor=1 and strategy_options=[{DC1:3, DC2:4, DC5:1}]"
    };
    
    @Test
    public void testCli() throws IOException, TTransportException, ConfigurationException
    {
        setup();

        // new error/output streams for CliSessionState
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();

        // checking if we can connect to the running cassandra node on localhost
        CliMain.connect("127.0.0.1", 9170);

        // setting new output stream
        CliMain.sessionState.setOut(new PrintStream(outStream));
        CliMain.sessionState.setErr(new PrintStream(errStream));

        // re-creating keyspace for tests
        // dropping in case it exists e.g. could be left from previous run
        CliMain.processStatement("drop keyspace TestKeySpace");
        CliMain.processStatement("create keyspace TestKeySpace");

        for (String statement : statements)
        {
            CliMain.processStatement(statement);
            String result = outStream.toString();

            if (statement.startsWith("drop ") || statement.startsWith("create ") || statement.startsWith("update "))
            {
                assertTrue(result.matches("(.{8})-(.{4})-(.{4})-(.{4})-(.{12})\n"));
            }
            else if (statement.startsWith("set "))
            {
                assertEquals(result, "Value inserted.\n");
            }
            else if (statement.startsWith("get "))
            {
                if (statement.contains("where"))
                {
                    assertTrue(result.startsWith("-------------------\nRowKey:"));
                }
                else
                {
                    assertTrue(result.startsWith("=> (column="));
                }
            }
            else if (statement.startsWith("truncate "))
            {
                assertTrue(result.contains(" truncated."));
            }

            outStream.reset(); // reset stream so we have only output from next statement all the time
            errStream.reset(); // no errors to the end user.
        }
    }

    /**
     * Setup embedded cassandra instance using test config.
     * @throws TTransportException - when trying to bind address
     * @throws IOException - when reading config file
     * @throws ConfigurationException - when can set up configuration
     */
    private void setup() throws TTransportException, IOException, ConfigurationException
    {
        EmbeddedCassandraService cassandra;

        cassandra = new EmbeddedCassandraService();
        cassandra.init();

        // spawn cassandra in a new thread
        Thread t = new Thread(cassandra);
        t.setDaemon(true);
        t.start();
    }

}
