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

package org.apache.cassandra.service;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import static org.apache.cassandra.Util.setUnifiedRepairEnabled;
import static org.apache.cassandra.config.CassandraRelevantProperties.SYSTEM_DISTRIBUTED_DEFAULT_RF;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class UnifiedRepairServiceRepairTypeTest extends CQLTester {
    @Parameterized.Parameter()
    public UnifiedRepairConfig.RepairType repairType;

    private final UUID host1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private final UUID host2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

    private UnifiedRepairService instance;

    @Parameterized.Parameters(name = "repairType={0}")
    public static Collection<UnifiedRepairConfig.RepairType> repairTypes() {
        return Arrays.asList(UnifiedRepairConfig.RepairType.values());
    }


    @BeforeClass
    public static void setupClass() throws Exception {
        SYSTEM_DISTRIBUTED_DEFAULT_RF.setInt(1);
        setUnifiedRepairEnabled(true);
        requireNetwork();
    }

    @Before
    public void setUpTest() {
        UnifiedRepairUtils.setup();
        instance = new UnifiedRepairService();
    }

    @Test
    public void testGetOnGoingRepairHostIdsTest() {
        long now = System.currentTimeMillis();
        UnifiedRepairUtils.insertNewRepairHistory(repairType, host1, now, now - 1000000);
        UnifiedRepairUtils.insertNewRepairHistory(repairType, host2, now, now - 1000000);

        Set<String> hosts = instance.getOnGoingRepairHostIds(repairType);

        assertEquals(ImmutableSet.of(host1.toString(), host2.toString()), hosts);
    }
}
