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

import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


public class CreateTableNonDeterministicTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            TableId node1id = tableId(cluster.get(1), "tbl");
            TableId node2id = tableId(cluster.get(2), "tbl");
            assertEquals(node1id, node2id);
            cluster.schemaChange(withKeyspace("drop table %s.tbl"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            TableId node1id2 = tableId(cluster.get(1), "tbl");
            TableId node2id2 = tableId(cluster.get(2), "tbl");
            assertNotEquals(node1id, node1id2);
            assertEquals(node1id2, node2id2);
        }
    }

    @Test
    public void testIdClash() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            long epoch = epoch(cluster.get(1));
            for (long i = epoch + 10; i < epoch + 15; i++)
            {
                cluster.schemaChange(withKeyspace("create table %s.tbl" + i + " (id int primary key) with id = " + new UUID(TableId.MAGIC, i)));
                TableId justCreated = tableId(cluster.get(1), "tbl"+i);
                assertEquals(justCreated.asUUID().getLeastSignificantBits(), i);
            }

            for (int i = 0; i < 10; i++)
            {
                long epochBeforeCreate = epoch(cluster.get(1));
                cluster.schemaChange(withKeyspace("create table %s.tblx" + i + " (id int primary key)"));
                TableId justCreated = tableId(cluster.get(1), "tblx"+i);
                long lsb = justCreated.asUUID().getLeastSignificantBits();
                assertEquals(epochBeforeCreate, lsb - (i < 5 ? 0 : 5));
            }
        }
    }

    @Test
    public void testCreateLikeTable() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.sourcetb (k int primary key, v text)"));
            TableId node1id = tableId(cluster.get(1), "sourcetb");
            TableId node2id = tableId(cluster.get(2), "sourcetb");
            assertEquals(node1id, node2id);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".targettb LIKE " + KEYSPACE + ".sourcetb");
            TableId node1id2 = tableId(cluster.get(1), "targettb");
            TableId node2id2 = tableId(cluster.get(2), "targettb");
            assertNotEquals(node1id, node1id2);
            assertEquals(node1id2, node2id2);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.sourcetb(k, v) VALUES (1, 'v1')"), ConsistencyLevel.QUORUM);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.targettb(k, v) VALUES (1, 'v1')"), ConsistencyLevel.QUORUM);
            Object[] row = row(1, "v1");
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.sourcetb"), ConsistencyLevel.QUORUM), row);
            assertRows(cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.targettb "), ConsistencyLevel.QUORUM), row);
        }
    }

    long epoch(IInvokableInstance inst)
    {
        return inst.callOnInstance(() -> ClusterMetadata.current().epoch.getEpoch());
    }

    TableId tableId(IInvokableInstance inst, String tbl)
    {
        return TableId.fromString(inst.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(tbl).metadata.id.toString()));
    }
}
