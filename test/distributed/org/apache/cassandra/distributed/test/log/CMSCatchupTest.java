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

package org.apache.cassandra.distributed.test.log;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.sequences.AddToCMS;

import static org.junit.Assert.assertTrue;

public class CMSCatchupTest extends TestBaseImpl
{
    @Test
    public void testCMSCatchup() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(4)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)) // needed for addtocms below
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key)"));
            cluster.get(2).runOnInstance(() -> AddToCMS.initiate());
            cluster.get(3).runOnInstance(() -> AddToCMS.initiate());
            // isolate node2 from the other CMS members to ensure it's behind
            cluster.filters().inbound().from(1).to(2).drop();
            cluster.filters().inbound().from(3).to(2).drop();
            AtomicInteger fetchedFromPeer = new AtomicInteger();
            cluster.filters().inbound().from(2).to(4).messagesMatching((from, to, msg) -> {
                if (msg.verb() == Verb.TCM_FETCH_PEER_LOG_REQ.id)
                    fetchedFromPeer.getAndIncrement();
                return false;
            }).drop().on();

            long mark = cluster.get(4).logs().mark();
            cluster.coordinator(1).execute(withKeyspace("alter table %s.tbl with comment='test 123'"), ConsistencyLevel.ONE);
            cluster.get(4).logs().watchFor(mark, "AlterOptions");

            mark = cluster.get(2).logs().mark();
            cluster.get(1).shutdown().get();
            cluster.get(2).logs().watchFor(mark, "/127.0.0.1:7012 state jump to shutdown");
            // node2, a CMS member, is now behind and node1 is shut down.
            // Try reading at QUORUM from node4, node2 should detect it's behind and catch up from node4
            int before = fetchedFromPeer.get();
            cluster.coordinator(4).execute(withKeyspace("select * from %s.tbl where id = 55"), ConsistencyLevel.QUORUM);
            assertTrue(fetchedFromPeer.get() > before);
        }
    }

}
