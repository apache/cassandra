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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.schema.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

public class AutoRepairKeyspaceTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testEnsureAutoRepairTablesArePresent()
    {
        KeyspaceMetadata keyspaceMetadata = SystemDistributedKeyspace.metadata();
        Iterator<TableMetadata> iter = keyspaceMetadata.tables.iterator();
        Set<String> actualDistributedTablesIter = new HashSet<>();
        while (iter.hasNext())
        {
            actualDistributedTablesIter.add(iter.next().name);
        }

        Assert.assertTrue(actualDistributedTablesIter.contains(SystemDistributedKeyspace.AUTO_REPAIR_HISTORY));
        Assert.assertTrue(actualDistributedTablesIter.contains(SystemDistributedKeyspace.AUTO_REPAIR_PRIORITY));
    }
}
