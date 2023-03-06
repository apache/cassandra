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

package org.apache.cassandra.tcm;

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.tcm.ownership.DataPlacement;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.getLeavePlan;
import static org.junit.Assert.assertTrue;


public class ClusterMetadataTest
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

    @Test
    public void testWritePlacementAllSettledLeaving() throws ExecutionException, InterruptedException
    {
        for (int i = 1; i <= 4; i++)
        {
            ClusterMetadataTestHelper.register(i);
            ClusterMetadataTestHelper.join(i, i);
        }
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareLeave(1));
        UnbootstrapAndLeave plan = getLeavePlan(1);

        ClusterMetadataService.instance().commit(plan.startLeave);
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));

        DataPlacement writeAllSettled = ClusterMetadata.current().writePlacementAllSettled(ksm);
        ClusterMetadataService.instance().commit(plan.midLeave);
        ClusterMetadataService.instance().commit(plan.finishLeave);

        DataPlacement actualFinishedWritePlacements = ClusterMetadata.current().placements.get(ksm.params.replication);

        assertTrue(actualFinishedWritePlacements.difference(writeAllSettled).writes.removals.isEmpty());
        assertTrue(actualFinishedWritePlacements.difference(writeAllSettled).writes.additions.isEmpty());
    }

    @Test
    public void testWritePlacementAllSettledJoining()
    {
        for (int i = 1; i <= 4; i++)
        {
            ClusterMetadataTestHelper.register(i);
            ClusterMetadataTestHelper.join(i, i);
        }

        ClusterMetadataTestHelper.register(10);
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareJoin(10));

        BootstrapAndJoin plan = ClusterMetadataTestHelper.getBootstrapPlan(10);
        ClusterMetadataService.instance().commit(plan.startJoin);
        KeyspaceMetadata ksm = KeyspaceMetadata.create("ks", KeyspaceParams.simple(3));
        DataPlacement writeAllSettled = ClusterMetadata.current().writePlacementAllSettled(ksm);

        ClusterMetadataService.instance().commit(plan.midJoin);
        ClusterMetadataService.instance().commit(plan.finishJoin);

        DataPlacement actualFinishedWritePlacements = ClusterMetadata.current().placements.get(ksm.params.replication);
        assertTrue(actualFinishedWritePlacements.difference(writeAllSettled).writes.removals.isEmpty());
        assertTrue(actualFinishedWritePlacements.difference(writeAllSettled).writes.additions.isEmpty());
    }

    @Test
    public void testWritePlacementAllSettledMoving()
    {
        // todo
    }
}
