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

package org.apache.cassandra.net;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AsyncOneResponseTest
{
    @Test(expected = TimeoutException.class)
    public void getThrowsExceptionAfterTimeout() throws InterruptedException, TimeoutException
    {
        AsyncOneResponse<Object> response = new AsyncOneResponse<>();
        Thread.sleep(2000);
        response.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void getThrowsExceptionAfterCorrectTimeout()
    {
        AsyncOneResponse<Object> response = new AsyncOneResponse<>();

        final long expectedTimeoutMillis = 1000; // Should time out after roughly this time
        final long schedulingError = 10; // Scheduling is imperfect
        boolean hitException = false; // Ensure we actually hit the TimeoutException

        long startTime = System.currentTimeMillis();

        try
        {
            response.get(expectedTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        catch(TimeoutException e)
        {
            hitException = true;
        }

        long endTime = System.currentTimeMillis();

        assertTrue(hitException);
        assertTrue(endTime - startTime > (expectedTimeoutMillis - schedulingError));
    }
}
