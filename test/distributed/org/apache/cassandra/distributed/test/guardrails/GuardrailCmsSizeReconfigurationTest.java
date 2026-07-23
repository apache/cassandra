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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.Cluster.build;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Exercises the minimum CMS size guardrail via the real {@code nodetool cms reconfigure} path against a live in-JVM
 * cluster. The guardrail applies unless the requested CMS size already uses every joined node (the cluster cannot
 * host more replicas), so multiple nodes are required to trigger it.
 */
public class GuardrailCmsSizeReconfigurationTest extends TestBaseImpl
{
    private static final int THRESHOLD = 3;

    @Test
    public void reconfigureBelowThresholdIsRejected() throws IOException
    {
        try (Cluster cluster = build(3).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2")
                   .asserts()
                   .failure()
                   .stderrContains("failure threshold of " + THRESHOLD);
        }
    }

    @Test
    public void reconfigureAtThresholdIsAllowed() throws IOException
    {
        try (Cluster cluster = build(3).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3")
                   .asserts()
                   .success();
        }
    }

    @Test
    public void reconfigureBelowThresholdIsAllowedWhenGuardrailDisabled() throws IOException
    {
        // Default threshold is -1 (disabled), so a below-safe reconfiguration is permitted.
        try (Cluster cluster = build(3).withConfig(config(null)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2")
                   .asserts()
                   .success();
        }
    }

    @Test
    public void reconfigureBelowThresholdIsAllowedWhenClusterTooSmall() throws IOException
    {
        // The requested CMS size (2) already uses every node in the cluster, so there is nothing more to enforce.
        try (Cluster cluster = build(2).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2")
                   .asserts()
                   .success();
        }
    }

    @Test
    public void reconfigureBelowClusterCapacityIsRejectedWhenThresholdAboveClusterSize() throws IOException
    {
        // Threshold (4) is above the cluster size (3), but the cluster can still host a CMS of 3. Requesting a smaller
        // size than the cluster can support must be rejected rather than silently skipped, so a high threshold can't
        // be used to sidestep the floor.
        try (Cluster cluster = build(3).withConfig(config(4)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2")
                   .asserts()
                   .failure()
                   .stderrContains("failure threshold of 4");
        }
    }

    @Test
    public void reconfigureAtClusterCapacityIsAllowedWhenThresholdAboveClusterSize() throws IOException
    {
        // Threshold (4) is above the cluster size (3). A CMS of 3 already uses every node, so it is allowed even though
        // it is below the threshold: the cluster simply cannot do better, and blocking it would brick reconfiguration.
        try (Cluster cluster = build(3).withConfig(config(4)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3")
                   .asserts()
                   .success();
        }
    }

    @Test
    public void reconfigureMultiDcBelowThresholdIsRejected() throws IOException
    {
        // 2 DCs x 2 nodes = 4 nodes; requested aggregate size is 1 + 1 = 2, below the threshold of 3.
        try (Cluster cluster = builder().withRacks(2, 1, 2).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "datacenter1:1", "datacenter2:1")
                   .asserts()
                   .failure()
                   .stderrContains("failure threshold of " + THRESHOLD);
        }
    }

    @Test
    public void reconfigureMultiDcAtOrAboveThresholdIsAllowed() throws IOException
    {
        // 2 DCs x 2 nodes = 4 nodes; requested aggregate size is 2 + 2 = 4, at or above the threshold of 3.
        try (Cluster cluster = builder().withRacks(2, 1, 2).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "datacenter1:2", "datacenter2:2")
                   .asserts()
                   .success();
        }
    }

    private static Consumer<IInstanceConfig> config(Integer threshold)
    {
        return config -> {
            config.with(GOSSIP).with(NETWORK);
            if (threshold != null)
                config.set("minimum_cms_size_fail_threshold", threshold);
        };
    }
}
