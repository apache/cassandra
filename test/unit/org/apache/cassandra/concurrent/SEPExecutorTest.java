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

package org.apache.cassandra.concurrent;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class SEPExecutorTest
{
    @Test
    public void shutdownTest() throws Throwable
    {
        for (int i = 0; i < 1000; i++)
        {
            shutdownOnce(i);
        }
    }

    private static void shutdownOnce(int run) throws Throwable
    {
        SharedExecutorPool sharedPool = new SharedExecutorPool("SharedPool");
        String MAGIC = "UNREPEATABLE_MAGIC_STRING";
        OutputStream nullOutputStream = new OutputStream() {
            public void write(int b) { }
        };
        PrintStream nullPrintSteam = new PrintStream(nullOutputStream);

        for (int idx = 0; idx < 20; idx++)
        {
            ExecutorService es = sharedPool.newExecutor(FBUtilities.getAvailableProcessors(), Integer.MAX_VALUE, "STAGE", run + MAGIC + idx);
            // Write to black hole
            es.execute(() -> nullPrintSteam.println("TEST" + es));
        }

        // shutdown does not guarantee that threads are actually dead once it exits, only that they will stop promptly afterwards
        sharedPool.shutdown();
        for (Thread thread : Thread.getAllStackTraces().keySet())
        {
            if (thread.getName().contains(MAGIC))
            {
                thread.join(100);
                if (thread.isAlive())
                    Assert.fail(thread + " is still running " + Arrays.toString(thread.getStackTrace()));
            }
        }
    }
}
