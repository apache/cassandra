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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.messages.ResultMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        ClientState state = new ClientState(false);
        state.setKeyspace(KEYSPACE);
        // do some writing.
        for (int i = 0; i < 100; i++)
        {
            QueryProcessor.process(
                    new StringBuilder()
                            .append("INSERT INTO ")
                            .append(COLUMN_FAMILY)
                            .append(" (id, name, value) VALUES ( 'key")
                            .append(i)
                            .append("', 'colb', 'value")
                            .append(i)
                            .append("' )")
                            .toString(),
                    ConsistencyLevel.QUORUM,
                    new QueryState(state)
            );

            System.out.println("wrote key" + i);
        }
        System.out.println("Done writing.");
    }

    private static void testReading() throws Exception
    {
        // do some queries.
        ClientState state = new ClientState(false);
        state.setKeyspace(KEYSPACE);
        for (int i = 0; i < 100; i++)
        {
            List<List<ByteBuffer>> rows = ((ResultMessage.Rows)QueryProcessor.process(
                    new StringBuilder()
                    .append("SELECT id, name, value FROM ")
                    .append(COLUMN_FAMILY)
                    .append(" WHERE id = 'key")
                    .append(i)
                    .append("'")
                    .toString(),
                    ConsistencyLevel.QUORUM,
                    new QueryState(state)
            )).result.rows;

            assert rows.size() == 1;
            List<ByteBuffer> r = rows.get(0);
            assert r.size() == 3;
            System.out.println(new StringBuilder()
                    .append("ID: ")
                    .append(AsciiType.instance.compose(r.get(0)))
                    .append(", Name: ")
                    .append(AsciiType.instance.compose(r.get(1)))
                    .append(", Value: ")
                    .append(AsciiType.instance.compose(r.get(2)))
                    .toString());
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
        try
        {
            Thread.currentThread().sleep(1000);
        }
        catch (InterruptedException ex)
        {
            throw new RuntimeException(ex);
        }

        testReading();

        // no need to do this:
        // StorageService.instance().decommission();
        // do this instead:
        StorageService.instance.stopClient();
        System.exit(0); // the only way to really stop the process.
    }

    private static void setupKeyspace() throws RequestExecutionException, RequestValidationException, InterruptedException
    {
        if (QueryProcessor.process(
                new StringBuilder()
                        .append("SELECT * FROM system.schema_keyspaces WHERE keyspace_name='")
                        .append(KEYSPACE)
                        .append("'")
                        .toString(), ConsistencyLevel.QUORUM)
                .isEmpty())
        {
            QueryProcessor.process(new StringBuilder()
                    .append("CREATE KEYSPACE ")
                    .append(KEYSPACE)
                    .append(" WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }")
                    .toString(), ConsistencyLevel.QUORUM);
            Thread.sleep(1000);
        }

        if (QueryProcessor.process(
                new StringBuilder()
                        .append("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='")
                        .append(KEYSPACE)
                        .append("' AND columnfamily_name='")
                        .append(COLUMN_FAMILY)
                        .append("'")
                        .toString(), ConsistencyLevel.QUORUM)
                .isEmpty())
        {
            ClientState state = new ClientState();
            state.setKeyspace(KEYSPACE);

            QueryProcessor.process(new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(COLUMN_FAMILY)
                    .append(" ( id ascii PRIMARY KEY, name ascii, value ascii )")
                    .toString(), ConsistencyLevel.QUORUM, new QueryState(state));
            Thread.sleep(1000);
        }
    }
}
