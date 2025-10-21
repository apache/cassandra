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

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.distributed.test.tcm.CMSPlacementAfterReplacementTest.assertInCMS;

public class CMSPlacementAfterMoveTest extends TestBaseImpl
{
    @Test
    public void testMoveToCMS() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(4)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .withoutVNodes()
                                           .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            long node1Token = cluster.get(1).callOnInstance(() -> {
                ClusterMetadata metadata = ClusterMetadata.current();
                ImmutableList<Token> tokens = ClusterMetadata.current().tokenMap.tokens(metadata.myNodeId());
                return ((Murmur3Partitioner.LongToken) tokens.get(0)).token;
            });
            long newNode4Token = node1Token + 100; // token after node1s token should be in cms
            cluster.get(4).nodetoolResult("move", String.valueOf(newNode4Token));
            int moveNodeId = cluster.get(4).callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            assertInCMS(cluster, moveNodeId);
        }
    }
}
