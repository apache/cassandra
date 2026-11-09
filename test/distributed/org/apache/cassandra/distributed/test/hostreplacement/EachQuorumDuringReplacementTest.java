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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;

import static org.apache.cassandra.distributed.shared.ClusterUtils.replaceHostAndStart;

public class EachQuorumDuringReplacementTest extends TestBaseImpl
{
    @Test
    public void testReplacement() throws Exception
    {
        TokenSupplier tokens = TokenSupplier.evenlyDistributedTokens(6);
        try (Cluster cluster = init(Cluster.build()
                                      .withDC("dc1", 3)
                                      .withDC("dc2", 3)
                                      .withTokenSupplier(node -> tokens.token(node == 7 ? 2 : node))
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                      .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication={'class':'NetworkTopologyStrategy', 'dc1':3, 'dc2':3}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.get(3).shutdown().get();
            Callable<Epoch> pending = ClusterUtils.pauseBeforeCommit(cluster.get(1), (transformation -> transformation.kind() == Transformation.Kind.MID_REPLACE));
            AtomicBoolean stop = new AtomicBoolean(false);
            Thread t = new Thread(() -> {
                replaceHostAndStart(cluster, cluster.get(3));
                stop.set(true);
            });
            t.start();
            pending.call();
            int i = 0;
            while (!stop.get())
            {
                if (i == 50)
                    ClusterUtils.unpauseCommits(cluster.get(1));
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.EACH_QUORUM, i++);
            }
            t.join();
        }
    }
}
