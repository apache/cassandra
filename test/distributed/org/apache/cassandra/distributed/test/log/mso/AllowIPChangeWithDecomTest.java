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

import org.junit.Test;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;

import static org.junit.Assert.assertEquals;

public class AllowIPChangeWithDecomTest extends IPChangeWithMSOBase
{
    static int TO_DECOM = 5;
    @Test
    public void testDecommission() throws Exception
    {

        runTest((cl, i) -> BBHelper.install(TO_DECOM, UnbootstrapAndLeave.class, cl, i),
                (cluster) -> cluster.get(TO_DECOM).nodetoolResult("decommission").asserts().failure(),
                (cluster) -> {
                    int nodeId = cluster.get(TO_DECOM + 6).callOnInstance(() -> {
                        ClusterMetadata metadata = ClusterMetadata.current();
                        assertEquals(NodeState.LEAVING, metadata.directory.peerState(metadata.myNodeId()));
                        return metadata.myNodeId().id();
                    });
                    // resume decommission
                    cluster.get(TO_DECOM + 6).nodetoolResult("decommission").asserts().success();
                    cluster.get(7).runOnInstance(() -> assertEquals(NodeState.LEFT, ClusterMetadata.current().directory.peerState(new NodeId(nodeId))));
                });
    }
}
