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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.TestEndpointCache;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;

public class CMSMembershipMetricsTest extends TestBaseImpl
{
    @Test
    public void testCMSMembershipMetrics() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)) // needed for addtocms below
                                             .start()))
        {
            cluster.forEach(i -> assertMembershipSize(1, i));
            cluster.forEach(i -> assertUnreachableCMSMembers(0, i));
            assertCMSMembership(1, cluster.get(1));
            assertCMSMembership(0, cluster.get(2), cluster.get(3));

            cluster.get(2).runOnInstance(() -> AddToCMS.initiate());
            cluster.forEach(i -> assertMembershipSize(2, i));
            cluster.forEach(i -> assertUnreachableCMSMembers(0, i));
            assertCMSMembership(1, cluster.get(1), cluster.get(2));
            assertCMSMembership(0, cluster.get(3));

            cluster.get(3).runOnInstance(() -> AddToCMS.initiate());
            cluster.forEach(i -> assertMembershipSize(3, i));
            cluster.forEach(i -> assertUnreachableCMSMembers(0, i));
            assertCMSMembership(1, cluster.get(1), cluster.get(2), cluster.get(3));

            // mark node3 down and ensure it stays that way
            cluster.filters().allVerbs().from(3).to(1,2).drop();
            cluster.filters().allVerbs().from(1,2).to(3).drop();
            markDown(cluster.get(3), cluster.get(1));
            markDown(cluster.get(3), cluster.get(2));

            cluster.forEach(i -> assertMembershipSize(3, i));
            assertUnreachableCMSMembers(1, cluster.get(1));
            assertUnreachableCMSMembers(1, cluster.get(2));
            assertUnreachableCMSMembers(0, cluster.get(3));

            cluster.filters().reset();
            markUp(cluster.get(3), cluster.get(1));
            markUp(cluster.get(3), cluster.get(2));
            cluster.forEach(i -> assertMembershipSize(3, i));
            cluster.forEach(i -> assertUnreachableCMSMembers(0, i));
        }
    }

    private void assertMembershipSize(long expected, IInvokableInstance instance)
    {
        long actual = instance.callOnInstance(() -> TCMMetrics.instance.currentCMSSize.getValue());
        assertEquals(String.format("Expected %s CMS members, node %s disagrees", expected, instance.config().num()),
                                   expected, actual);
    }

    private void assertUnreachableCMSMembers(long expected, IInvokableInstance instance)
    {
        long actual = instance.callOnInstance(() -> TCMMetrics.instance.unreachableCMSMembers.getValue());
        assertEquals(String.format("Expected %s down CMS members, node %s disagrees", expected, instance.config().num()),
                     expected, actual);
    }

    void assertCMSMembership(int expected, IInvokableInstance... instances)
    {
        for(IInvokableInstance instance: instances)
        {
            int actual = instance.callOnInstance(() -> TCMMetrics.instance.isCMSMember.getValue());
            assertEquals(String.format("Expected isCMSMember value to be %s, node %s disagrees", expected, instance.config().num()),
                         expected, actual);
        }
    }

    private void markDown(IInvokableInstance down, IInvokableInstance inst)
    {
        InetSocketAddress node3Address = down.config().broadcastAddress();
        inst.runOnInstance(() -> FailureDetector.instance.forceConviction(TestEndpointCache.toCassandraInetAddressAndPort(node3Address)));
    }

    private void markUp(IInvokableInstance down, IInvokableInstance inst)
    {
        InetSocketAddress downAddress = down.config().broadcastAddress();
        inst.runOnInstance(() -> FailureDetector.instance.report(TestEndpointCache.toCassandraInetAddressAndPort(downAddress)));
        Awaitility.waitAtMost(10, TimeUnit.SECONDS)
                  .until(() -> inst.callOnInstance(() -> FailureDetector.instance.isAlive(TestEndpointCache.toCassandraInetAddressAndPort(downAddress))));
    }
}
