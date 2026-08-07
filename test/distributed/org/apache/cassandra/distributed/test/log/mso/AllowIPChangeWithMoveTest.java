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

package org.apache.cassandra.distributed.test.log.mso;

import java.util.Collection;

import org.junit.Test;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.Move;

import static org.junit.Assert.assertEquals;

public class AllowIPChangeWithMoveTest extends IPChangeWithMSOBase
{
    /**
     * test a move fail, change all ip addresses with the move MSO active, resume move
     */
    static int TO_MOVE = 5;
    @Test
    public void testMove() throws Exception
    {
        runTest((cl, i) -> BBHelper.install(TO_MOVE, Move.class, cl, i),
                (cluster) -> {
                    long token = cluster.get(TO_MOVE).callOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        Collection<Token> tokens = metadata.tokenMap.tokens(metadata.myNodeId());
                        return tokens.iterator().next().getLongValue();
                    });
                    cluster.get(TO_MOVE).nodetoolResult("move", String.valueOf(token - 1000)).asserts().failure();
                },
                (cluster) -> {
                    cluster.get(TO_MOVE + 6).runOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        assertEquals(NodeState.MOVING, metadata.directory.peerState(metadata.myNodeId()));
                    });
                    // resume decommission
                    cluster.get(TO_MOVE + 6).nodetoolResult("move", "--resume").asserts().success();
                });
    }
}
