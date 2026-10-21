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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;

import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.startHostReplacement;

public class ConcurrentCMSReconfigurationAndReplacementOfJoiningMemberTest extends ConcurrentCMSReconfigurationAndNodeOperationTestBase
{
    @Test
    public void test() throws Exception
    {
        try (Cluster cluster = newCluster())
        {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            IInvokableInstance firstCMS = cluster.get(1);
            initialCMSSetup(cluster, firstCMS, 2);

            // nodes 1 & 2 are now CMS members, prepare another reconfiguration which will add node 3
            // but force it to halt before the first step is committed by pausing commits on the node
            // driving it (node1)
            Future<NodeToolResult> inFlightReconfig = beginAndPauseReconfiguration(executor, firstCMS, 3, "ADDITIONS: [/127.0.0.3");

            // Now the reconfiguration which is active in cluster metadata includes node3, try replacing it.
            // node1 is still paused so cannot receive commit requests itself, but it can still participate
            // in consensus decisions with node2
            IInvokableInstance nodeToRemove = cluster.get(3);
            nodeToRemove.shutdown().get();
            IInvokableInstance replacingNode = addInstance(cluster, nodeToRemove.config(), c -> c.set("auto_bootstrap", true));
            startHostReplacement(nodeToRemove, replacingNode, (ignore1_, ignore2_) -> {});
            awaitRingJoin(cluster.get(1), replacingNode);
            awaitRingJoin(replacingNode, cluster.get(1));

            // now that node4 has replaced node3, unpause node1 to allow it to continue with the inflight CMS
            // reconfiguration, which will fail
            ClusterUtils.unpauseCommits(firstCMS);
            inFlightReconfig.get().asserts().failure();
            // Cancel the inflight reconfiguration and resubmit another, which will now succeed
            firstCMS.nodetoolResult("cms", "reconfigure", "--cancel").asserts().success();
            firstCMS.nodetoolResult("cms", "reconfigure", "3").asserts().success();
        }
    }
}
