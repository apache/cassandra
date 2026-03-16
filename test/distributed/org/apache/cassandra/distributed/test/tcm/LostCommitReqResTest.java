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
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class LostCommitReqResTest extends TestBaseImpl
{
    @Test
    public void lostMoveCommitResponseTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("cms_await_timeout", "1s").set("cms_default_max_retries", "5"))
                                             .start()))
        {
            // no commit responses
            cluster.filters().verbs(Verb.TCM_COMMIT_RSP.id).from(1).to(2).drop();
            // lost response when committing PrepareMove, fails the nodetool command and halts progress
            cluster.get(2).nodetoolResult("move", "1234").asserts().failure();
            assertMoveFailed(cluster.get(2)); // we should be in MOVE_FAILED state to allow abortmove
            // still no responses, committing CancelInProgressSequence response is lost, but is actually committed
            cluster.get(2).nodetoolResult("abortmove").asserts().failure();
            assertNormal(cluster.get(2)); // and we should be back to normal
            cluster.get(2).nodetoolResult("move", "1234").asserts().failure();
            assertMoveFailed(cluster.get(2));
            // finishing the MSO does not depend on any commit responses, just that ClusterMetadata.current() is up to date, so this is successful;
            cluster.get(2).nodetoolResult("move", "--resume").asserts().success();
            assertNormal(cluster.get(2));
        }
    }

    @Test
    public void lostMoveCommitRequestTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("cms_await_timeout", "1s").set("cms_default_max_retries", "5"))
                                             .start()))
        {
            // no commit requests
            cluster.filters().verbs(Verb.TCM_COMMIT_REQ.id).from(2).to(1).drop();
            cluster.get(2).nodetoolResult("move", "1234").asserts().failure();
            // state should be "move failed" since we don't know if the request or response went missing:
            assertMoveFailed(cluster.get(2));
            cluster.filters().reset();
            // abort move should be successful, it only clears the transient state in this case though
            cluster.get(2).nodetoolResult("abortmove").asserts().success();
            assertNormal(cluster.get(2));
            cluster.get(2).nodetoolResult("move", "1234").asserts().success();
            assertNormal(cluster.get(2));
        }
    }

    @Test
    public void lostDecomCommitResponseTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("cms_await_timeout", "1s").set("cms_default_max_retries", "5"))
                                             .start()))
        {
            cluster.filters().verbs(Verb.TCM_COMMIT_RSP.id).from(1).to(2).drop();
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            assertDecomFailed(cluster.get(2));
            cluster.get(2).nodetoolResult("abortdecommission").asserts().failure();
            assertNormal(cluster.get(2));
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            assertDecomFailed(cluster.get(2));
            cluster.get(2).nodetoolResult("decommission").asserts().success();
        }
    }

    @Test
    public void lostDecomCommitRequestTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP).set("cms_await_timeout", "1s").set("cms_default_max_retries", "5"))
                                             .start()))
        {
            cluster.filters().verbs(Verb.TCM_COMMIT_REQ.id).from(2).to(1).drop();
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().failure();
            assertDecomFailed(cluster.get(2));
            cluster.filters().reset();
            cluster.get(2).nodetoolResult("abortdecommission").asserts().success();
            assertNormal(cluster.get(2));
            cluster.get(2).nodetoolResult("decommission", "--force").asserts().success();
        }
    }

    private static void assertNormal(IInvokableInstance i)
    {
        assertOperationMode(i, StorageService.Mode.NORMAL);
    }

    private static void assertMoveFailed(IInvokableInstance i)
    {
        assertOperationMode(i, StorageService.Mode.MOVE_FAILED);
    }

    private static void assertDecomFailed(IInvokableInstance i)
    {
        assertOperationMode(i, StorageService.Mode.DECOMMISSION_FAILED);
    }

    private static void assertOperationMode(IInvokableInstance i, StorageService.Mode expectedMode)
    {
        String mode = i.callOnInstance(() -> StorageService.instance.operationMode().toString());
        assertEquals(expectedMode.toString(), mode);
    }
}
