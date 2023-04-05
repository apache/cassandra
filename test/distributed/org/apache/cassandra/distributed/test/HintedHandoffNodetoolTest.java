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
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.service.StorageProxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * These tests replace the nodetool-related Python dtests in hintedhandoff_test.py.
 */
@RunWith(Parameterized.class)
public class HintedHandoffNodetoolTest extends TestBaseImpl
{
    private static Cluster cluster;

    @Parameterized.Parameter
    public int node;

    @Parameterized.Parameters(name = "node={0}")
    public static List<Object[]> data()
    {   
        List<Object[]> result = new ArrayList<>();
        result.add(new Object[]{ 1 });
        result.add(new Object[]{ 2 });
        return result;
    }

    @BeforeClass
    public static void before() throws IOException
    {
        cluster = init(Cluster.build().withNodes(2).withDCs(2).start());
    }

    @AfterClass
    public static void after()
    {
        if (cluster != null)
            cluster.close();
    }
    
    @Before
    public void enableHandoff()
    {
        cluster.get(1).nodetoolResult("enablehandoff");
        cluster.get(2).nodetoolResult("enablehandoff");
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testEnableHandoff()
    {
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Hinted handoff is running");
        assertTrue(cluster.get(node).callOnInstance(() -> StorageProxy.instance.getHintedHandoffEnabled()));
    }

    @Test
    public void testDisableHandoff()
    {
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Hinted handoff is running");
        cluster.get(node).nodetoolResult("disablehandoff");
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Hinted handoff is not running");
        cluster.get(node).nodetoolResult("enablehandoff");
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Hinted handoff is running");
    }

    @Test
    public void testDisableForDC()
    {
        cluster.get(node).nodetoolResult("disablehintsfordc", "datacenter1");
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Data center datacenter1 is disabled");
        cluster.get(node).nodetoolResult("enablehintsfordc", "datacenter1");
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutNotContains("Data center datacenter1 is disabled");
    }

    @Test
    public void testPauseHandoff()
    {
        cluster.get(node).nodetoolResult("statushandoff").asserts().success().stdoutContains("Hinted handoff is running");
        
        cluster.get(node).nodetoolResult("pausehandoff").asserts().success();
        Boolean isPaused = cluster.get(node).callOnInstance(() -> HintsService.instance.isDispatchPaused());
        assertTrue(isPaused);

        cluster.get(node).nodetoolResult("resumehandoff").asserts().success();
        isPaused = cluster.get(node).callOnInstance(() -> HintsService.instance.isDispatchPaused());
        assertFalse(isPaused);
    }

    @Test
    public void testThrottle()
    {
        Integer throttleInKiB = cluster.get(node).callOnInstance(DatabaseDescriptor::getHintedHandoffThrottleInKiB);
        cluster.get(node).nodetoolResult("sethintedhandoffthrottlekb", String.valueOf(throttleInKiB * 2)).asserts().success();
        Integer newThrottleInKB = cluster.get(node).callOnInstance(DatabaseDescriptor::getHintedHandoffThrottleInKiB);
        assertEquals(throttleInKiB * 2, newThrottleInKB.intValue());
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testMaxHintWindow()
    {
        Integer hintWindowMillis = cluster.get(node).callOnInstance(() -> StorageProxy.instance.getMaxHintWindow());
        
        cluster.get(node).nodetoolResult("getmaxhintwindow")
                         .asserts()
                         .success()
                         .stdoutContains("Current max hint window: " + hintWindowMillis + " ms");

        cluster.get(node).nodetoolResult("setmaxhintwindow", String.valueOf(hintWindowMillis * 2)).asserts().success();

        cluster.get(node).nodetoolResult("getmaxhintwindow")
                         .asserts()
                         .success()
                         .stdoutContains("Current max hint window: " + hintWindowMillis * 2 + " ms");
    }
}
