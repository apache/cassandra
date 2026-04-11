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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;

import static org.junit.Assert.assertTrue;

public class ColumnFamilyStoreMBeansTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = init(Cluster.build(1)
                              .start());

        CLUSTER.schemaChange(withKeyspace("DROP KEYSPACE %s"));
        CLUSTER.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': 'UnifiedCompactionStrategy', 'scaling_parameters': 'L10'}"));

        for (int i = 0; i < 10000; i++)
            CLUSTER.get(1).executeInternal(withKeyspace("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');"), Integer.toString(i));

        CLUSTER.get(1).nodetool("flush");
    }

    @AfterClass
    public static void teardownCluster() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testPerLevelAverageTokenSpace() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            double[] perLevelAvgTokenSpace = cfs.getPerLevelAvgTokenSpace();
            assertTrue(perLevelAvgTokenSpace.length > 0);
            for (int i = 0; i < perLevelAvgTokenSpace.length; i++)
                assertTrue(perLevelAvgTokenSpace[i] > 0);
        });
    }

    @Test
    public void testGetPerLevelMaxDensityThreshold() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            assertTrue(cfs.getPerLevelMaxDensityThreshold().length > 0);
        });
    }

    @Test
    public void testGetPerLevelAvgSize() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            double[] perLevelAvgSize = cfs.getPerLevelAvgSize();
            assertTrue(perLevelAvgSize.length > 0);
            for (int i = 0; i < perLevelAvgSize.length; i++)
                assertTrue(perLevelAvgSize[i] > 0);
        });
    }

    @Test
    public void testGetPerLevelAvgDensity() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            double[] perLevelAvgDensity = cfs.getPerLevelAvgDensity();
            assertTrue(perLevelAvgDensity.length > 0);
            for (int i = 0; i < perLevelAvgDensity.length; i++)
                assertTrue(perLevelAvgDensity[i] > 0);
        });
    }

    @Test
    public void testGetPerLevelAvgDensityMaxDensityThresholdRatio() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            double[] perLevelAvgDensityMaxDensityThresholdRatio = cfs.getPerLevelAvgDensityMaxDensityThresholdRatio();
            assertTrue(perLevelAvgDensityMaxDensityThresholdRatio.length > 0);
            for (int i = 0; i < perLevelAvgDensityMaxDensityThresholdRatio.length; i++)
            {
                assertTrue(0 <= perLevelAvgDensityMaxDensityThresholdRatio[i]);
                assertTrue(perLevelAvgDensityMaxDensityThresholdRatio[i] < 1);
            }
        });
    }

    @Test
    public void testGetPerLevelMaxDensityMaxDensityThresholdRatio() throws Throwable
    {
        CLUSTER.get(1).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("cf");
            double[] perLevelMaxDensityMaxDensityThresholdRatio = cfs.getPerLevelMaxDensityMaxDensityThresholdRatio();
            assertTrue(perLevelMaxDensityMaxDensityThresholdRatio.length > 0);
            for (int i = 0; i < perLevelMaxDensityMaxDensityThresholdRatio.length; i++) {
                assertTrue(0 <= perLevelMaxDensityMaxDensityThresholdRatio[i]);
                assertTrue(perLevelMaxDensityMaxDensityThresholdRatio[i] < 1);
            }
        });
    }
}
