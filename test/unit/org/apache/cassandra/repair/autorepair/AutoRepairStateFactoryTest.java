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

import org.junit.Test;

import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AutoRepairStateFactoryTest
{
    @Test
    public void testGetRepairState() {
        AutoRepairState state = RepairType.getAutoRepairState(RepairType.full);

        assert state instanceof FullRepairState;

        state = RepairType.getAutoRepairState(RepairType.incremental);

        assert state instanceof IncrementalRepairState;
    }

    @Test
    public void testGetRepairStateSupportsAllRepairTypes() {
        for (RepairType repairType : RepairType.values()) {
            try {
                AutoRepairState state = RepairType.getAutoRepairState(repairType);
                assertNotNull(state);
            } catch (IllegalArgumentException e) {
                assertNull(e);
            }
        }
    }
}
