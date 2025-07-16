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

package org.apache.cassandra.metrics;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractMetricsManagerTest
{
    private static final SyncMetricsManager syncMetricsManager = new SyncMetricsManager();
    private static final AsyncMetricsManager asyncMetricsManager = new AsyncMetricsManager();

    @Test
    public void testGetMetricsSync()
    {
        try
        {
            asyncMetricsManager.getMetricsSync("test");
            fail("Expected IllegalStateException when calling getMetricsSync with async registration enabled");
        }
        catch (IllegalStateException e)
        {
            assertTrue(e.getMessage().contains("getMetricsSync is not supported for AsyncMetricsManager"));
        }

        // getMetricsSync should work if async registration is disabled
        assertEquals("test", syncMetricsManager.getMetricsSync("test"));
    }

    @Test
    public void testAsyncRegistration() throws ExecutionException, InterruptedException
    {
        // first time calling should be async
        assertTrue(asyncMetricsManager.maybeRegisterMetricsAsync(null, "test").get());
        // following calls should return the same metric synchronously
        assertFalse(asyncMetricsManager.maybeRegisterMetricsAsync(null, "test").get());
    }

    static class AsyncMetricsManager extends AbstractMetricsManager<String, String>
    {
        protected AsyncMetricsManager()
        {
            super(true);
        }

        @Override
        protected String createMetric(String key) throws IllegalArgumentException
        {
            return key;
        }

        @Override
        protected String buildKey(Object... parts) throws IllegalArgumentException
        {
            if (parts.length != 1)
                throw new IllegalArgumentException("Expected 1 argument: key");
            return (String) parts[0];
        }
    }

    static class SyncMetricsManager extends AbstractMetricsManager<String, String>
    {
        protected SyncMetricsManager()
        {
            super(false);
        }

        @Override
        protected String createMetric(String key) throws IllegalArgumentException
        {
            return key;
        }

        @Override
        protected String buildKey(Object... parts) throws IllegalArgumentException
        {
            if (parts.length != 1)
                throw new IllegalArgumentException("Expected 1 argument: key");
            return (String) parts[0];
        }
    }
}
