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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class JoinTokenRingTest
{
    @BeforeClass
    public static void setup() throws ConfigurationException
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.startGossiper();
        SchemaLoader.schemaDefinition("JoinTokenRingTest");
    }

    @Test
    public void testIndexPreJoinInvocation() throws IOException, ExecutionException, InterruptedException
    {
        ClusterMetadataTestHelper.addEndpoint(FBUtilities.getBroadcastAddressAndPort(),
                                              ClusterMetadata.current().partitioner.getRandomToken());
        ScheduledExecutors.optionalTasks.submit(() -> null).get(); // make sure the LegacyStateListener has finished executing
        SecondaryIndexManager indexManager = ColumnFamilyStore.getIfExists("JoinTokenRingTestKeyspace7", "Indexed1").indexManager;
        StubIndex stub = (StubIndex) indexManager.getIndexByName("Indexed1_value_index");
        Assert.assertTrue(stub.preJoinInvocation);
    }


    @Test @Ignore("Implement equivalent test when joining from write survey mode is supported")
    public void testIndexPreJoinInvocationFromWriteSurveyMode() throws IOException
    {
       // TODO implement new test
    }
}
