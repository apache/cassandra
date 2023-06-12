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

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionContainer;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.unified.AdaptiveController;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.StaticController;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.shared.FutureUtils.waitOn;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class CompactionControllerConfigTest extends TestBaseImpl
{
    @Test
    public void storedAdaptiveCompactionOptionsTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'true', " +
                                              "'adaptive_starting_scaling_parameter': '0'};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl2 (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'true', " +
                                              "'adaptive_starting_scaling_parameter': '0'};"));
            cluster.get(1).runOnInstance(() ->
                                             {
                                                 ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                                 UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                                 UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                                 Controller controller = ucs.getController();
                                                 assertTrue(controller instanceof AdaptiveController);
                                                 //scaling parameter on L0 should be 0 to start
                                                 assertEquals(0, controller.getScalingParameter(0));

                                                 //manually write new scaling parameters and flushSizeBytes to see if they are picked up on restart
                                                 int[] scalingParameters = new int[32];
                                                 Arrays.fill(scalingParameters, 5);
                                                 AdaptiveController.storeOptions("ks", "tbl", scalingParameters, 10 << 20);


                                                 //write different scaling parameters to second table to make sure each table keeps its own configuration
                                                 Arrays.fill(scalingParameters, 8);
                                                 AdaptiveController.storeOptions("ks", "tbl2", scalingParameters, 10 << 20);
                                             });
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof AdaptiveController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(5, controller.getScalingParameter(0));
                                             assertEquals(10 << 20, controller.getFlushSizeBytes());

                                             ColumnFamilyStore cfs2 = Keyspace.open("ks").getColumnFamilyStore("tbl2");
                                             UnifiedCompactionContainer container2 = (UnifiedCompactionContainer) cfs2.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs2 = (UnifiedCompactionStrategy) container2.getStrategies().get(0);
                                             Controller controller2 = ucs2.getController();
                                             assertTrue(controller2 instanceof AdaptiveController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(8, controller2.getScalingParameter(0));
                                             assertEquals(10 << 20, controller2.getFlushSizeBytes());
                                         });
        }
    }

    @Test
    public void storedStaticCompactionOptionsTest() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof StaticController);
                                             //scaling parameter on L0 should be 0 to start
                                             assertEquals(0, controller.getScalingParameter(0));

                                             //manually write new flushSizeBytes to see if it is picked up on restart
                                             int[] scalingParameters = new int[32];
                                             Arrays.fill(scalingParameters, 0);
                                             AdaptiveController.storeOptions("ks", "tbl", scalingParameters, 10 << 20);
                                         });
            waitOn(cluster.get(1).shutdown());
            cluster.get(1).startup();

            cluster.get(1).runOnInstance(() ->
                                         {
                                             ColumnFamilyStore cfs = Keyspace.open("ks").getColumnFamilyStore("tbl");
                                             UnifiedCompactionContainer container = (UnifiedCompactionContainer) cfs.getCompactionStrategy();
                                             UnifiedCompactionStrategy ucs = (UnifiedCompactionStrategy) container.getStrategies().get(0);
                                             Controller controller = ucs.getController();
                                             assertTrue(controller instanceof StaticController);
                                             //when the node is restarted, it should see the new configuration that was manually written
                                             assertEquals(10 << 20, controller.getFlushSizeBytes());
                                         });
        }
    }

    @Test
    public void testStoreAndCleanupControllerConfig() throws Throwable
    {
        try(Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks2.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH compaction = " +
                                              "{'class': 'UnifiedCompactionStrategy', " +
                                              "'adaptive': 'false', " +
                                              "'scaling_parameters': '0'};"));

            cluster.get(1).runOnInstance(() ->
                                         {
                                             //logs should show that scaling parameters and flush size are written to a file for each table
                                             CompactionManager.storeControllerConfig();

                                             //store controller config for a table that does not exist to see if it is removed by the cleanup method
                                             int[] scalingParameters = new int[32];
                                             Arrays.fill(scalingParameters, 5);
                                             AdaptiveController.storeOptions("does_not", "exist", scalingParameters, 10 << 20);

                                             //verify that the file was created
                                             assert Controller.getControllerConfigPath("does_not", "exist").exists();

                                             //cleanup method should remove the file corresponding to the table "does_not.exist"
                                             CompactionManager.cleanupControllerConfig();

                                             //verify that the file was deleted
                                             assert !Controller.getControllerConfigPath("does_not", "exist").exists();

                                         });

        }
    }
}
