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

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.distributed.test.log.CMSTestBase;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.UniformRangePlacement;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.AlterSchema;
import org.apache.cassandra.tcm.transformations.Assassinate;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.addr;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.getLeavePlan;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterMetadataTest
{
    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Before
    public void before() throws ExecutionException, InterruptedException
    {
        ClusterMetadataService.unsetInstance();
        new CMSTestBase.CMSSut(AtomicLongBackedProcessor::new, false, new TokenPlacementModel.SimpleReplicationFactor(3));
    }

    @Test
    public void testWritePlacementAllSettledLeaving()
    {
        for (int i = 1; i <= 4; i++)
        {
            ClusterMetadataTestHelper.register(i);
            ClusterMetadataTestHelper.join(i, i);
        }
        ClusterMetadataService.instance().commit(ClusterMetadataTestHelper.prepareLeave(3));
        UnbootstrapAndLeave plan = getLeavePlan(3);

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

    @Test
    public void testNewTransformationCommit()
    {
        newTransformationHelper(new CustomTransformation("TEST", new NewTransformation()));
    }

    @Test
    public void testNewSchemaTransformation()
    {
        newTransformationHelper(new AlterSchema(new V5SchemaTransformation()));
    }

    private static void newTransformationHelper(Transformation transformation)
    {
        NodeId v4Node = null;
        for (int i = 1; i <= 4; i++)
        {
            NodeId nodeId = ClusterMetadataTestHelper.register(addr(i), "dc0", "rack0", new NodeVersion(CassandraVersion.CASSANDRA_5_0, i == 4 ? Version.V4 : Version.V5));
            if (i == 4)
                v4Node = nodeId;
            ClusterMetadataTestHelper.join(i, i);
        }

        try
        {
            ClusterMetadataService.instance().commit(transformation);
            fail("Should not be able to commit V5 transformation in V4 cluster");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("Transformation rejected"));
        }
        ClusterMetadataService.instance().commit(new Assassinate(v4Node, new UniformRangePlacement()));
        ClusterMetadataService.instance().commit(transformation);
    }

    public static class V5SchemaTransformation implements SchemaTransformation
    {
        @Override
        public Keyspaces apply(ClusterMetadata metadata)
        {
            return metadata.schema.getKeyspaces();
        }

        @Override
        public boolean compatibleWith(ClusterMetadata metadata)
        {
            return metadata.directory.commonSerializationVersion.isAtLeast(Version.V5);
        }
    }

    public static class NewTransformation implements Transformation
    {
        @Override
        public Kind kind()
        {
            return Kind.CUSTOM;
        }

        @Override
        public Result execute(ClusterMetadata metadata)
        {
            return new Success(metadata, LockedRanges.AffectedRanges.EMPTY, ImmutableSet.of());
        }

        @Override
        public boolean eligibleToCommit(ClusterMetadata metadata)
        {
            return metadata.directory.commonSerializationVersion.isAtLeast(Version.V5);
        }
    }
}
