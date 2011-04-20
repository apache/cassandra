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

package org.apache.cassandra;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.WrappedRunnable;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class MovementTest extends TestBase
{
    private static final String STANDARD_CF = "Standard1";
    private static final ColumnParent STANDARD = new ColumnParent(STANDARD_CF);

    /** Inserts 1000 keys with names such that at least 1 key ends up on each host. */
    private static Map<ByteBuffer,List<ColumnOrSuperColumn>> insertBatch(Cassandra.Client client) throws Exception
    {
        final int N = 1000;
        Column col1 = new Column(ByteBufferUtil.bytes("c1"))
            .setValue(ByteBufferUtil.bytes("v1"))
            .setTimestamp(0);
        Column col2 = new Column(ByteBufferUtil.bytes("c2"))
            .setValue(ByteBufferUtil.bytes("v2"))
            .setTimestamp(0);

        // build N rows
        Map<ByteBuffer,List<ColumnOrSuperColumn>> rows = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
        Map<ByteBuffer,Map<String,List<Mutation>>> batch = new HashMap<ByteBuffer,Map<String,List<Mutation>>>();
        for (int i = 0; i < N; i++)
        {
            String rawKey = String.format("test.key.%d", i);
            ByteBuffer key = ByteBufferUtil.bytes(rawKey);
            Mutation m1 = (new Mutation()).setColumn_or_supercolumn((new ColumnOrSuperColumn()).setColumn(col1));
            Mutation m2 = (new Mutation()).setColumn_or_supercolumn((new ColumnOrSuperColumn()).setColumn(col2));
            rows.put(key, Arrays.asList(m1.getColumn_or_supercolumn(),
                                        m2.getColumn_or_supercolumn()));

            // add row to batch
            Map<String,List<Mutation>> rowmap = new HashMap<String,List<Mutation>>();
            rowmap.put(STANDARD_CF, Arrays.asList(m1, m2));
            batch.put(key, rowmap);
        }
        // insert the batch
        client.batch_mutate(batch, ConsistencyLevel.ONE);
        return rows;
    }

    private static void verifyBatch(Cassandra.Client client, Map<ByteBuffer,List<ColumnOrSuperColumn>> batch) throws Exception
    {
        for (Map.Entry<ByteBuffer,List<ColumnOrSuperColumn>> entry : batch.entrySet())
        {
            // verify slice
            SlicePredicate sp = new SlicePredicate();
            sp.setSlice_range(
                new SliceRange(
                    ByteBuffer.wrap(new byte[0]),
                    ByteBuffer.wrap(new byte[0]),
                    false,
                    1000
                    )
                );
            assertEquals(client.get_slice(entry.getKey(), STANDARD, sp, ConsistencyLevel.ONE),
                         entry.getValue());
        }
    }

    @Test
    public void testLoadbalance() throws Exception
    {
        final String keyspace = "TestLoadbalance";
        addKeyspace(keyspace, 1);
        List<InetAddress> hosts = controller.getHosts();
        Cassandra.Client client = controller.createClient(hosts.get(0));
        client.set_keyspace(keyspace);

        // add keys to each node
        Map<ByteBuffer,List<ColumnOrSuperColumn>> rows = insertBatch(client);

        Thread.sleep(100);

        // ask a node to move to a new location
        controller.nodetool("loadbalance", hosts.get(0));

        // trigger cleanup on all nodes
        for (InetAddress host : hosts)
            controller.nodetool("cleanup", host);

        // check that all keys still exist
        verifyBatch(client, rows);
    }
}
