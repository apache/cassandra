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

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType}
 */
public class AutoRepairConfigRepairTypeTest
{
    @Test
    public void testRepairTypeParsing()
    {
        Assert.assertEquals(AutoRepairConfig.RepairType.FULL, AutoRepairConfig.RepairType.parse("FULL"));
        Assert.assertEquals(AutoRepairConfig.RepairType.FULL, AutoRepairConfig.RepairType.parse("FuLl"));
        Assert.assertEquals(AutoRepairConfig.RepairType.FULL, AutoRepairConfig.RepairType.parse("full"));
        Assert.assertEquals(AutoRepairConfig.RepairType.INCREMENTAL, AutoRepairConfig.RepairType.parse("INCREMENTAL"));
        Assert.assertEquals(AutoRepairConfig.RepairType.INCREMENTAL, AutoRepairConfig.RepairType.parse("incremental"));
        Assert.assertEquals(AutoRepairConfig.RepairType.INCREMENTAL, AutoRepairConfig.RepairType.parse("inCRemenTal"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PREVIEW_REPAIRED, AutoRepairConfig.RepairType.parse("PREVIEW_REPAIRED"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PREVIEW_REPAIRED, AutoRepairConfig.RepairType.parse("preview_repaired"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PREVIEW_REPAIRED, AutoRepairConfig.RepairType.parse("Preview_Repaired"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PAXOS_CLEANUP, AutoRepairConfig.RepairType.parse("PAXOS_CLEANUP"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PAXOS_CLEANUP, AutoRepairConfig.RepairType.parse("paxos_cleanup"));
        Assert.assertEquals(AutoRepairConfig.RepairType.PAXOS_CLEANUP, AutoRepairConfig.RepairType.parse("Paxos_Cleanup"));
        Assert.assertEquals(AutoRepairConfig.RepairType.BOOTSTRAP, AutoRepairConfig.RepairType.parse("BOOTSTRAP"));
        Assert.assertEquals(AutoRepairConfig.RepairType.BOOTSTRAP, AutoRepairConfig.RepairType.parse("bootstrap"));
        Assert.assertEquals(AutoRepairConfig.RepairType.BOOTSTRAP, AutoRepairConfig.RepairType.parse("Bootstrap"));
    }

    @Test(expected = NullPointerException.class)
    public void testNullRepairTypeParsing()
    {
        AutoRepairConfig.RepairType.parse(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyRepairTypeParsing()
    {
        AutoRepairConfig.RepairType.parse("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRepairTypeParsing()
    {
        AutoRepairConfig.RepairType.parse("very_FULL");
    }
}
