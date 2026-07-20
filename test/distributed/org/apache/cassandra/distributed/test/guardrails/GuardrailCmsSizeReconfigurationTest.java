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
 * cluster. The guardrail only applies once the cluster has at least as many nodes as the threshold, so multiple nodes
 * are required to trigger it.
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
        // The cluster has fewer nodes than the threshold, so it cannot host that many replicas and the guardrail is skipped.
        try (Cluster cluster = build(2).withConfig(config(THRESHOLD)).start())
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "2")
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
