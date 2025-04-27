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

package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.text.ParseException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.test.TestBaseImpl;

/**
 * Test {@code totalBytesToRepair}, {@code bytesAlreadyRepaired}, {@code totalKeyspaceRepairPlansToRepair},
 * and {@code keyspaceRepairPlansAlreadyRepaired}
 * for {@link org.apache.cassandra.repair.autorepair.AutoRepairState} scheduler with v-nodes
 */
public class AutoRepairSchedulerStatsVNodesTest extends TestBaseImpl
{
    @BeforeClass
    public static void init() throws IOException
    {
        AutoRepairSchedulerStatsHelper.init(16);
    }

    @AfterClass
    public static void tearDown()
    {
        AutoRepairSchedulerStatsHelper.tearDown();
    }

    @Test
    public void testSchedulerStats() throws ParseException
    {
        AutoRepairSchedulerStatsHelper.testSchedulerStats();
    }
}
