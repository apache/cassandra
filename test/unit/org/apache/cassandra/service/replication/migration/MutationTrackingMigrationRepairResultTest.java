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

package org.apache.cassandra.service.replication.migration;

import org.junit.Test;

import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MutationTrackingMigrationRepairResultTest
{
    private static final Epoch TEST_EPOCH = Epoch.create(5);

    @Test
    public void testGlobalIncrementalDataRepairEligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, false, false, true, true, true);
        assertTrue(result.eligible);
        assertEquals(TEST_EPOCH, result.minEpoch);
        assertNull(result.ineligibleReason);
    }

    @Test
    public void testExcludedDeadNodesIneligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, true, false, true, true, true);
        assertFalse(result.eligible);
        assertEquals(Epoch.EMPTY, result.minEpoch);
        assertEquals("dead nodes were excluded from the repair", result.ineligibleReason);
    }

    @Test
    public void testPreviewRepairIneligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, false, true, true, true, true);
        assertFalse(result.eligible);
        assertEquals(Epoch.EMPTY, result.minEpoch);
        assertEquals("the repair was a preview", result.ineligibleReason);
    }

    @Test
    public void testFullRepairIneligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, false, false, false, true, true);
        assertFalse(result.eligible);
        assertEquals(Epoch.EMPTY, result.minEpoch);
        assertTrue(result.ineligibleReason.contains("the repair was not incremental"));
    }

    @Test
    public void testNotAllReplicasIneligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, false, false, true, false, true);
        assertFalse(result.eligible);
        assertEquals(Epoch.EMPTY, result.minEpoch);
        assertTrue(result.ineligibleReason.contains("not all replicas were included"));
    }

    @Test
    public void testDataRepairNotRunIneligible()
    {
        MutationTrackingMigrationRepairResult result =
            MutationTrackingMigrationRepairResult.fromRepair(TEST_EPOCH, false, false, true, true, false);
        assertFalse(result.eligible);
        assertEquals(Epoch.EMPTY, result.minEpoch);
        assertTrue(result.ineligibleReason.contains("data repair was not performed"));
    }
}
