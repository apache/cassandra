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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.Ignore;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public abstract class AbstractMetricsTest extends SAITester
{
    @Before
    public void initializeTest() throws Throwable
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();
    }

    protected void waitForIndexCompaction(String keyspace, String table, String index)
    {
        waitForAssert(() -> {
            try
            {
                assertEquals(1L, getMetricValue(objectName("CompactionCount", keyspace, table, index, "IndexMetrics")));
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 60, TimeUnit.SECONDS);
    }

    protected void waitForVerifyHistogram(ObjectName name, long count)
    {
        waitForAssert(() -> {
            try
            {
                assertEquals(count, jmxConnection.getAttribute(name, "Count"));
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 10, TimeUnit.SECONDS);
    }

    protected void waitForGreaterThanZero(ObjectName name)
    {
        waitForAssert(() -> {
            try
            {
                assertTrue(((Number) getMetricValue(name)).doubleValue() > 0);
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 160, TimeUnit.SECONDS);
    }
}
