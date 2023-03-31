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

package org.apache.cassandra.tcm.ownership;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.distributed.test.log.MetadataChangeSimulationTest;
import org.apache.cassandra.distributed.test.log.PlacementSimulator;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

public class UniformRangePlacementIntegrationTest
{
    @BeforeClass
    public static void beforeClass()
    {
        SchemaLoader.prepareServerNoRegister();
    }

    @Before
    public void before() throws ExecutionException, InterruptedException
    {
        ClusterMetadataService.unsetInstance();
        new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, 3);
    }

    @Before
    public void after() throws ExecutionException, InterruptedException
    {
        ClusterMetadataService.unsetInstance();
    }

    @Test
    public void testMultiDC() throws Throwable
    {
        UniformRangePlacement rangePlacement = new UniformRangePlacement();
        Random rng = new Random(1);
        int idx = 1;
        List<PlacementSimulator.Node> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            for (int j = 1; j <= 3; j++)
            {
                long token = rng.nextLong();
                int dc = j;
                int rack = (rng.nextInt(3) + 1);
                PlacementSimulator.Node node = new PlacementSimulator.Node(token, idx, dc, rack);
                ClusterMetadataTestHelper.register(idx, node.dc(), node.rack());
                ClusterMetadataTestHelper.join(idx, token);
                nodes.add(node);
                idx++;
            }
        }

        ClusterMetadataService.instance().replayAndWait();
        DataPlacements placements = rangePlacement.calculatePlacements(ClusterMetadata.current(),
                                                                       Keyspaces.of(ClusterMetadata.current().schema.getKeyspaces().get("test_nts").get()));
        Map<String, Integer> rf = new HashMap<>();
        for (int i = 1; i <= 3; i++)
        {
            String dc = "datacenter" + i;
            rf.put(dc, 3);
        }

        Map<PlacementSimulator.Range, List<PlacementSimulator.Node>> predicted = PlacementSimulator.replicate(nodes, rf);

        ReplicationParams replicationParams = ClusterMetadata.current().schema.getKeyspaces().get("test_nts").get().params.replication;
        MetadataChangeSimulationTest.match(placements.get(replicationParams).reads,
                                           predicted);
    }
}

