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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.thrift.TException;

import org.apache.cassandra.thrift.*;

import org.apache.cassandra.CassandraServiceController.Failure;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

public class CountersTest extends TestBase
{
    @Test
    public void testWriteOneReadAll() throws Exception
    {
        List<InetAddress> hosts = controller.getHosts();
        // create a keyspace that performs counter validation
        final String keyspace = "TestOneNodeWrite";
        keyspace(keyspace).rf(3).validator("CounterColumnType").create();

        for (InetAddress host : hosts)
        {
            Cassandra.Client client = controller.createClient(host);
            client.set_keyspace(keyspace);

            ByteBuffer key = newKey();

            add(client, key, "Standard1", "c1", 1, ConsistencyLevel.ONE);
            add(client, key, "Standard1", "c2", 2, ConsistencyLevel.ONE);

            new CounterGet(client, "Standard1", key).name("c1").value(1L).perform(ConsistencyLevel.ALL);
            new CounterGet(client, "Standard1", key).name("c2").value(2L).perform(ConsistencyLevel.ALL);
        }
    }

    protected class CounterGet extends RetryingAction<Long>
    {
        public CounterGet(Cassandra.Client client, String cf, ByteBuffer key)
        {
            super(client, cf, key);
        }

        public void tryPerformAction(ConsistencyLevel cl) throws Exception
        {
            ByteBuffer bname = ByteBuffer.wrap(name.getBytes());
            ColumnPath cpath = new ColumnPath(cf).setColumn(bname);
            CounterColumn col = client.get(key, cpath, cl).counter_column;
            assertEquals(bname, col.name);
            assertEquals(value.longValue(), col.value);
        }
    }

    /** NB: Counter increments are unfortunately not idempotent, so we don't provide a RetyingAction to perform them. */
    protected void add(Cassandra.Client client, ByteBuffer key, String cf, String name, long value, ConsistencyLevel cl)
        throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        CounterColumn col = new CounterColumn(ByteBuffer.wrap(name.getBytes()), value);
        client.add(key, new ColumnParent(cf), col, cl);
    }
}
