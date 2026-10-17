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

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.tcm.Transformation.Kind.ADVANCE_CMS_RECONFIGURATION;
import static org.junit.Assert.assertTrue;

/**
 * Common scaffolding for tests which pause an in-flight CMS reconfiguration mid-commit and then
 * perform some other node operation (replacement, move, ...) concurrently, before unpausing and
 * observing the outcome of the reconfiguration.
 */
public abstract class ConcurrentCMSReconfigurationAndNodeOperationTestBase extends TestBaseImpl
{
    protected Cluster newCluster() throws IOException
    {
        // Configure a short cms_await_timeout as one of the CMS nodes will be paused, meaning
        // any commit requests sent to it will eventually timeout and be resubmitted to another
        // member. Also minimize the number of acks required by ProgressBarrier, as the paused
        // CMS member may not respond to watermark requests if its INTERNAL_METADATA stage is
        // filled by backed up commit requests.
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        return init(builder().withNodes(3)
                             .withTokenSupplier(node -> even.token(node == 4 ? 3 : node))
                             .withoutVNodes()
                             .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                               .set("cms_await_timeout", "1s")
                                               .set("progress_barrier_default_consistency_level", ConsistencyLevel.ONE))
                             .start());
    }

    /**
     * Set up the CMS ready to begin the test
     */
    protected void initialCMSSetup(Cluster cluster, IInvokableInstance driver, int newSize)
    {
        ClusterUtils.waitForCMSToQuiesce(cluster, driver);
        driver.nodetoolResult("cms", "reconfigure", Integer.toString(newSize)).asserts().success();
        ClusterUtils.waitForCMSToQuiesce(cluster, driver);
    }

    /**
     * Prepares another reconfiguration and forces it to halt before its first step is committed,
     * by pausing commits on the node driving it, so the caller can perform a node operation while
     * the reconfiguration is stalled mid-flight.
     */
    protected Future<NodeToolResult> beginAndPauseReconfiguration(ExecutorService executor,
                                                                  IInvokableInstance driver,
                                                                  int newSize,
                                                                  String expectedStatus) throws Exception
    {
        Callable<Epoch> pending = ClusterUtils.pauseBeforeCommit(driver, e -> e.kind() == ADVANCE_CMS_RECONFIGURATION);
        Future<NodeToolResult> inFlightReconfig = executor.submit(() -> driver.nodetoolResult("cms", "reconfigure", Integer.toString(newSize)));
        pending.call();

        String status = driver.nodetoolResult("cms", "reconfigure", "--status").getStdout();
        assertTrue(status.contains(expectedStatus));
        return inFlightReconfig;
    }
}
