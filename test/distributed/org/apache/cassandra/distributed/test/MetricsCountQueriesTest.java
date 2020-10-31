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

package org.apache.cassandra.distributed.test;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class MetricsCountQueriesTest extends TestBaseImpl
{
    @Test
    public void testMetricsCountQueries() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"), ALL, i, i, i);

            long readCount1 = readCount(cluster.get(1));
            long readCount2 = readCount(cluster.get(2));
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk = ? and ck = ?"), ALL, i, i);

            readCount1 = readCount(cluster.get(1)) - readCount1;
            readCount2 = readCount(cluster.get(2)) - readCount2;
            Assert.assertEquals(readCount1, readCount2);
            Assert.assertEquals(100, readCount1);
        }
    }

    private static long readCount(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.readLatency.latency.getCount());
    }
}
