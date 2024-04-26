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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class PaxosRepairTCMTest extends TestBaseImpl
{
    @Test
    public void paxosRepairWithReversePartitionerTest() throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2").asserts().success();
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            for (int i = 0; i < 1000; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
            cluster.forEach(i -> i.flush(KEYSPACE));

            // flush system.paxos to create some UncommittedTableData files
            for (int i = 0; i < 3; i++)
            {
                for (int j = 0; j < 3; j++)
                    cluster.schemaChange(withKeyspace(String.format("alter table %%s.tbl with comment = 'comment " + i + j + "'", i, j)));
                cluster.forEach(inst -> inst.flush("system"));
            }

            // unflushed rows in the system.paxos memtable
            for (int i = 0; i < 3; i++)
                cluster.schemaChange(withKeyspace(String.format("alter table %%s.tbl with comment = 'comment x " + i + "'", i)));

            long start = Long.MIN_VALUE;
            long interval = Long.MAX_VALUE/5;
            long end = start + interval;
            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).nodetoolResult("repair", "-full", "-st", String.valueOf(start), "-et", String.valueOf(end));
                start += interval;
                end += interval;
            }

            cluster.schemaChange(withKeyspace("create table %s.tbl2 (id int primary key)"));
        }
    }
}
