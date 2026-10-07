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

import com.google.common.collect.Iterables;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.service.StorageService;

public class ConcurrentCMSReconfigurationAndMoveTest extends ConcurrentCMSReconfigurationAndNodeOperationTestBase
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
            Future<NodeToolResult> reconfigureResult = beginAndPauseReconfiguration(executor, firstCMS, 3, "ADDITIONS: [/127.0.0.3");

            // Now the reconfiguration which is active in cluster metadata includes node3, try moving it to a new token.
            // node1 is still paused so cannot receive commit requests itself, but it can still participate in consensus
            // decisions with node2
            IInvokableInstance nodeToMove = cluster.get(3);
            String token = nodeToMove.callsOnInstance(() -> Iterables.getOnlyElement(StorageService.instance.getLocalTokens()).toString()).call();
            long moveTo = Long.parseLong(token) + 1;
            nodeToMove.nodetoolResult("move", Long.toString(moveTo)).asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, nodeToMove);

            // now that node3 has completed its token move, unpause node1 to allow it to continue with the inflight
            // CMS reconfiguration, which will succeed.
            ClusterUtils.unpauseCommits(firstCMS);
            reconfigureResult.get().asserts().success();
        }
    }
}
