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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.transformations.cms.PrepareCMSReconfiguration;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Tests for {@link ReconfigureCMS#maybeReconfigureCMS} waiting for an already in-progress CMS reconfiguration
 * before removing a node from the CMS.
 */
public class ReconfigureCMSRemovalWaitTest extends FuzzTestBase
{
    @Test
    public void maybeReconfigureCMSWaitsForInProgressReconfigurationThenSucceeds() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.NETWORK))
                                        .withoutVNodes()
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                long originalInitialDelay = DatabaseDescriptor.getCmsCommitRetryInitialDelay().to(TimeUnit.MILLISECONDS);
                long originalMaxDelay = DatabaseDescriptor.getCmsCommitRetryMaxDelay().to(TimeUnit.MILLISECONDS);
                try
                {
                    DatabaseDescriptor.setCmsCommitRetryInitialDelay(10);
                    DatabaseDescriptor.setCmsCommitRetryMaxDelay(50);

                    InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
                    ClusterMetadata current = ClusterMetadata.current();
                    Assert.assertTrue(current.fullCMSMembers().contains(self));

                    // Stub the in-progress sequence to be "occupied" for 3 checks, and then "free" for any check after that
                    MultiStepOperation<?> placeholder = ReconfigureCMS.newSequence(LockedRanges.keyFor(current.nextEpoch()), PrepareCMSReconfiguration.Diff.NOCHANGE);
                    AtomicInteger lookupCalls = new AtomicInteger();
                    long retriesBefore = TCMMetrics.instance.cmsReconfigurationRetries.getCount();

                    ClusterMetadata after = ReconfigureCMS.maybeReconfigureCMS(current, self, m -> lookupCalls.incrementAndGet() <= 3 ? placeholder : null);

                    // 3 "occupied" responses followed by the 4th "free" one
                    Assert.assertEquals(4, lookupCalls.get());
                    Assert.assertEquals(retriesBefore + 3, TCMMetrics.instance.cmsReconfigurationRetries.getCount());
                    Assert.assertFalse(after.fullCMSMembers().contains(self));
                    Assert.assertNull(after.inProgressSequences.get(ReconfigureCMS.SequenceKey.instance));
                }
                finally
                {
                    DatabaseDescriptor.setCmsCommitRetryMaxDelay(originalMaxDelay);
                    DatabaseDescriptor.setCmsCommitRetryInitialDelay(originalInitialDelay);
                }
            });
        }
    }

    @Test
    public void maybeReconfigureCMSTimesOutWhenExistingReconfigurationNeverClears() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.NETWORK))
                                        .withoutVNodes()
                                        .start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.get(1).runOnInstance(() -> {
                long originalWaitTimeout = DatabaseDescriptor.getCmsReconfigurationWaitTimeout().to(TimeUnit.MILLISECONDS);
                long originalInitialDelay = DatabaseDescriptor.getCmsCommitRetryInitialDelay().to(TimeUnit.MILLISECONDS);
                long originalMaxDelay = DatabaseDescriptor.getCmsCommitRetryMaxDelay().to(TimeUnit.MILLISECONDS);
                try
                {
                    InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();

                    DatabaseDescriptor.setCmsCommitRetryInitialDelay(20);
                    DatabaseDescriptor.setCmsCommitRetryMaxDelay(50);
                    DatabaseDescriptor.setCmsReconfigurationWaitTimeout(TimeUnit.SECONDS.toMillis(2));

                    // Stub the in-progress-sequence lookup to report "occupied" forever
                    MultiStepOperation<?> placeholder = ReconfigureCMS.newSequence(LockedRanges.keyFor(ClusterMetadata.current().nextEpoch()), PrepareCMSReconfiguration.Diff.NOCHANGE);
                    long retriesBefore = TCMMetrics.instance.cmsReconfigurationRetries.getCount();
                    try
                    {
                        ReconfigureCMS.maybeReconfigureCMS(ClusterMetadata.current(), self, m -> placeholder);
                        Assert.fail("Expected IllegalStateException after timing out waiting for the CMS reconfiguration slot to clear");
                    }
                    catch (IllegalStateException e)
                    {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("Timed out"));
                        long retries = TCMMetrics.instance.cmsReconfigurationRetries.getCount() - retriesBefore;
                        Assert.assertTrue("Expected the wait loop to back off repeatedly before giving up, but it retried " + retries + " time(s)",
                                          retries >= 5);
                    }
                }
                finally
                {
                    DatabaseDescriptor.setCmsReconfigurationWaitTimeout(originalWaitTimeout);
                    DatabaseDescriptor.setCmsCommitRetryMaxDelay(originalMaxDelay);
                    DatabaseDescriptor.setCmsCommitRetryInitialDelay(originalInitialDelay);
                }
            });
        }
    }
}
