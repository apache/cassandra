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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.*;

public class ClientOnlyExample
{
    private static final Logger logger = LoggerFactory.getLogger(ClientOnlyExample.class);

    private static final String KEYSPACE = "keyspace1";
    private static final String COLUMN_FAMILY = "standard1";

    private static void startClient() throws Exception
    {
        StorageService.instance.initClient();
    }

    private static void testWriting() throws Exception
    {
        // do some writing.
        for (int i = 0; i < 100; i++)
        {
            QueryProcessor.process(String.format("INSERT INTO %s.%s (id, name, value) VALUES ( 'key%d', 'colb', 'value%d')",
                                                 KEYSPACE,
                                                 COLUMN_FAMILY,
                                                 i,
                                                 i),
                                   ConsistencyLevel.QUORUM);

            System.out.println("wrote key" + i);
        }
        System.out.println("Done writing.");
    }

    private static void testReading() throws Exception
    {
        // do some queries.
        for (int i = 0; i < 100; i++)
        {
            String query = String.format("SELECT id, name, value FROM %s.%s WHERE id = 'key%d'",
                                         KEYSPACE,
                                         COLUMN_FAMILY,
                                         i);
            UntypedResultSet.Row row = QueryProcessor.process(query, ConsistencyLevel.QUORUM).one();
            System.out.println(String.format("ID: %s, Name: %s, Value: %s", row.getString("id"), row.getString("name"), row.getString("value")));
        }
    }

    /**
     * First, bring one or more nodes up. Then run ClientOnlyExample with these VM arguments:
     * <p/>
     * -Xmx1G
     * -Dcassandra.config=/Users/gary/cassandra/conf/cassandra.yaml (optional, will first look for cassandra.yaml on classpath)
     * <p/>
     * Pass "write" or "read" into the program to exercise the various methods.
     * <p/>
     * Caveats:
     * <p/>
     * 1.  Much of cassandra is not reentrant.  That is, you can't spin a client up, down, then back up in the same jvm.
     * 2.  Because of the above, you still need to force-quit the process. StorageService.stopClient() doesn't (can't)
     * spin everything down.
     */
    public static void main(String args[]) throws Exception
    {
        startClient();
        setupKeyspace();
        testWriting();
        logger.info("Writing is done. Sleeping, then will try to read.");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        testReading();

        // no need to do this:
        // StorageService.instance().decommission();
        // do this instead:
        StorageService.instance.stopClient();
        System.exit(0); // the only way to really stop the process.
    }

    private static void setupKeyspace() throws RequestExecutionException, RequestValidationException, InterruptedException
    {
        QueryProcessor.process("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                               ConsistencyLevel.ANY);
        QueryProcessor.process("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + COLUMN_FAMILY + " (id ascii PRIMARY KEY, name ascii, value ascii )",
                               ConsistencyLevel.ANY);
        TimeUnit.MILLISECONDS.sleep(1000);
    }
}
