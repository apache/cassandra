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

package org.apache.cassandra.repair.autorepair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils.SizeEstimate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class KeyspaceRepairPlanTest
{
    private static final Range<Token> RANGE = new Range<>(new LongToken(0), new LongToken(10));
    private static final Range<Token> OTHER_RANGE = new Range<>(new LongToken(10), new LongToken(20));

    @Test
    public void testSnapshotIsIndependentOfMutableInputs()
    {
        List<String> tables = new ArrayList<>(List.of("table"));
        List<Range<Token>> ranges = new ArrayList<>(List.of(RANGE));
        SizeEstimate estimate = new SizeEstimate(RepairType.FULL, "ks", "table", RANGE, 1, 53, 53);
        Map<Range<Token>, SizeEstimate> tableEstimates = new HashMap<>(Map.of(RANGE, estimate));
        Map<String, Map<Range<Token>, SizeEstimate>> estimates = new HashMap<>(Map.of("ks.table", tableEstimates));
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan("ks", tables, ranges, estimates);

        tables.clear();
        ranges.clear();
        tableEstimates.clear();
        estimates.clear();
        assertEquals(List.of("table"), plan.getTableNames());
        assertEquals(List.of(RANGE), plan.getTokenRanges());
        assertEquals(estimate, plan.getSizeEstimate("ks.table", RANGE));
        assertEquals(53, plan.getEstimatedBytes());
        assertEquals(53, plan.getTableEstimatedBytes("ks.table"));
        assertThatThrownBy(() -> plan.getTableNames().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> plan.getTokenRanges().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testEqualityIncludesRangeSnapshot()
    {
        SizeEstimate first = new SizeEstimate(RepairType.FULL, "ks", "table", RANGE, 1, 53, 53);
        SizeEstimate second = new SizeEstimate(RepairType.FULL, "ks", "table", OTHER_RANGE, 1, 53, 53);
        Map<String, Map<Range<Token>, SizeEstimate>> estimates = Map.of("ks.table", Map.of(RANGE, first, OTHER_RANGE, second));
        KeyspaceRepairPlan plan = new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE, OTHER_RANGE), estimates);
        KeyspaceRepairPlan equal = new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE, OTHER_RANGE), estimates);
        KeyspaceRepairPlan reordered = new KeyspaceRepairPlan("ks", List.of("table"), List.of(OTHER_RANGE, RANGE), estimates);
        assertEquals(plan, equal);
        assertEquals(plan.hashCode(), equal.hashCode());
        assertNotEquals(plan, reordered);
    }

    @Test
    public void testMissingTableEstimateIsRejected()
    {
        assertThatThrownBy(() -> new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE), Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMissingRangeEstimateIsRejected()
    {
        assertThatThrownBy(() -> new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE), Map.of("ks.table", Map.of())))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUnexpectedRangeEstimateIsRejected()
    {
        SizeEstimate estimate = new SizeEstimate(RepairType.FULL, "ks", "table", OTHER_RANGE, 1, 53, 53);
        assertThatThrownBy(() -> new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE), Map.of("ks.table", Map.of(OTHER_RANGE, estimate))))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testDuplicateRangesAreRejected()
    {
        SizeEstimate estimate = new SizeEstimate(RepairType.FULL, "ks", "table", RANGE, 1, 53, 53);
        assertThatThrownBy(() -> new KeyspaceRepairPlan("ks", List.of("table"), List.of(RANGE, RANGE), Map.of("ks.table", Map.of(RANGE, estimate))))
        .isInstanceOf(IllegalArgumentException.class);
    }
}
