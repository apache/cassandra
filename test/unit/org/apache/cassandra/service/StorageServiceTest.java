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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.fail;

public class StorageServiceTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testRepairSessionMaximumTreeDepth()
    {
        StorageService storageService = StorageService.instance;
        int previousDepth = storageService.getRepairSessionMaximumTreeDepth();
        try
        {
            Assert.assertEquals(18, storageService.getRepairSessionMaximumTreeDepth());
            storageService.setRepairSessionMaximumTreeDepth(10);
            Assert.assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            try
            {
                storageService.setRepairSessionMaximumTreeDepth(9);
                fail("Should have received a IllegalArgumentException for depth of 9");
            }
            catch (IllegalArgumentException ignored) { }
            Assert.assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            try
            {
                storageService.setRepairSessionMaximumTreeDepth(-20);
                fail("Should have received a IllegalArgumentException for depth of -20");
            }
            catch (IllegalArgumentException ignored) { }
            Assert.assertEquals(10, storageService.getRepairSessionMaximumTreeDepth());

            storageService.setRepairSessionMaximumTreeDepth(22);
            Assert.assertEquals(22, storageService.getRepairSessionMaximumTreeDepth());
        }
        finally
        {
            storageService.setRepairSessionMaximumTreeDepth(previousDepth);
        }
    }
}