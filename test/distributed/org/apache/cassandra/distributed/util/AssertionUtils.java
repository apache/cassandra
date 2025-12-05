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

package org.apache.cassandra.distributed.util;

import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.TimeUnit;

public class AssertionUtils
{
    /**
     * Run the assertions in a loop until the first success within the timeout.
     * Otherwise, it fails with the last assertion failure.
     *
     * @param timeoutSeconds timeout
     * @param assertions     assertions
     */
    public static void loopAssert(int timeoutSeconds, int delayMillis, Runnable assertions)
    {
        long start = System.nanoTime();
        long timeout = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        AssertionError failure = null;
        while (System.nanoTime() - start < timeout)
        {
            try
            {
                assertions.run();
                return;
            }
            catch (AssertionError error)
            {
                failure = error;
            }
            Uninterruptibles.sleepUninterruptibly(delayMillis, TimeUnit.MILLISECONDS);
        }
        // times out
        if (failure != null)
        {
            throw failure;
        }
        else
        {
            throw new RuntimeException("Loop assert times out with no failure"); // it should never happen
        }
    }
}
