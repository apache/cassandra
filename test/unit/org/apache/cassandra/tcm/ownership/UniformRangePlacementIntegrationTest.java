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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.distributed.test.log.MetadataChangeSimulationTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.AtomicLongBackedProcessor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;

public class UniformRangePlacementIntegrationTest
{
    private static final TokenPlacementModel.ReplicationFactor RF = new TokenPlacementModel.NtsReplicationFactor(3, 3);
    private CMSTestBase.CMSSut sut;
    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void before() throws ExecutionException, InterruptedException
    {
        ClusterMetadataService.unsetInstance();
        sut = new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, RF);
    }

    @After
    public void after() throws Exception
    {
        sut.close();
    }

    @Test
    public void testMultiDC() throws Throwable
    {
        UniformRangePlacement rangePlacement = new UniformRangePlacement();
        Random rng = new Random(1);
        int idx = 1;
        TokenPlacementModel.NodeFactory factory = TokenPlacementModel.nodeFactory();
        List<TokenPlacementModel.Node> nodes = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            for (int j = 1; j <= 3; j++)
            {
                int dc = j;
                int rack = (rng.nextInt(3) + 1);
                TokenPlacementModel.Node node = factory.make(idx, dc, rack);
                ClusterMetadataTestHelper.register(idx, node.dc(), node.rack());
                ClusterMetadataTestHelper.join(idx, node.token());
                nodes.add(node);
                idx++;
            }
        }
        nodes.sort(TokenPlacementModel.Node::compareTo);

        ClusterMetadataService.instance().processor().fetchLogAndWait();
        DataPlacements placements = rangePlacement.calculatePlacements(ClusterMetadata.current().epoch,
                                                                       ClusterMetadata.current(),
                                                                       Keyspaces.of(ClusterMetadata.current().schema.getKeyspaces().get("test").get()));

        TokenPlacementModel.ReplicatedRanges predicted = RF.replicate(nodes);

        ReplicationParams replicationParams = ClusterMetadata.current().schema.getKeyspaces().get("test").get().params.replication;
        MetadataChangeSimulationTest.match(placements.get(replicationParams).reads,
                                           predicted.asMap());
    }
}

