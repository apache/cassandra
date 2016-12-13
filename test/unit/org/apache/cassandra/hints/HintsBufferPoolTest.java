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
package org.apache.cassandra.hints;

import org.apache.cassandra.Util;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableList;

import static junit.framework.Assert.*;

import java.lang.Thread.State;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

@RunWith(BMUnitRunner.class)
public class HintsBufferPoolTest
{

    @BeforeClass
    public static void defineSchema()
    {
        HintsBufferTest.defineSchema();
    }

    /*
     * Check that the hints buffer pool will only drain a limited number of buffers
     */
    static volatile boolean blockedOnBackpressure = false;
    @Test
    @BMRule(name = "Greatest name in the world",
            targetClass="HintsBufferPool",
            targetMethod="switchCurrentBuffer",
            targetLocation="AT INVOKE java.util.concurrent.BlockingQueue.take",
            action="org.apache.cassandra.hints.HintsBufferPoolTest.blockedOnBackpressure = true;")
    public void testBackpressure() throws Exception
    {
        Queue<HintsBuffer> returnedBuffers = new ConcurrentLinkedQueue<>();
        HintsBufferPool pool = new HintsBufferPool(256, (buffer, p) -> returnedBuffers.offer(buffer));

        Thread blocked = new Thread(() -> {
            for (int ii = 0; ii < 512; ii++)
                pool.write(ImmutableList.of(UUID.randomUUID()), HintsBufferTest.createHint(ii, ii));
        });
        blocked.start();

        Util.spinAssertEquals(true, () -> blockedOnBackpressure, 60);

        while (blocked.isAlive())
            if (!returnedBuffers.isEmpty())
                pool.offer(returnedBuffers.poll().recycle());

        assertTrue(blockedOnBackpressure);
    }
}
