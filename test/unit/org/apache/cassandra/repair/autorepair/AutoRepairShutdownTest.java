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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import static org.apache.cassandra.Util.setAutoRepairEnabled;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests to validate the executor shutdown inside {@link AutoRepair}
 */
public class AutoRepairShutdownTest extends CQLTester
{
    @BeforeClass
    public static void setupClass() throws Exception
    {
        setAutoRepairEnabled(true);
        requireNetwork();
    }

    @Test
    public void testAutoRepairShutdown() throws Exception
    {
        AutoRepair.instance.setup();

        for (RepairType type : RepairType.values())
        {
            assertFalse("RepairRunnableExecutor should not have been shut down", AutoRepair.instance.getRepairRunnableExecutors().get(type).isShutdown());
            assertFalse("RepairExecutor should not have been shut down", AutoRepair.instance.getRepairExecutors().get(type).isShutdown());
        }
        assertFalse("AutoRepair should not be marked as shut down", AutoRepair.instance.isShutDown);

        AutoRepair.instance.shutdownBlocking();

        for (RepairType type : RepairType.values())
        {
            assertTrue("RepairRunnableExecutor should be shut down", AutoRepair.instance.getRepairRunnableExecutors().get(type).isShutdown());
            assertTrue("RepairExecutor should be shut down", AutoRepair.instance.getRepairExecutors().get(type).isShutdown());
        }
        assertTrue("AutoRepair should be marked as shut down", AutoRepair.instance.isShutDown);

        try
        {
            AutoRepair.instance.shutdownBlocking();
            fail("A second call to shutdown should have thrown an exception");
        }
        catch (IllegalStateException e)
        {
            // expected
        }
    }
}
