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

package org.apache.cassandra.tools.nodetool.mock;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;

public class SetConcurrencyMockTest extends AbstractNodetoolMock
{
    @Test
    public void testSetConcurrencyTwoArgs()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        invokeNodetool("setconcurrency", "stage", "15").assertOnCleanExit();
        Mockito.verify(mock).setConcurrency("stage", -1, 15);
    }

    @Test
    public void testSetConcurrencyThreeArgs()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        invokeNodetool("setconcurrency", "stage", "11", "22").assertOnCleanExit();
        Mockito.verify(mock).setConcurrency("stage", 11, 22);
    }

    @Test
    public void testSetConcurrencyIllegalException()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        doThrow(new IllegalArgumentException("Test exception of the illegal set concurrency call"))
                .when(mock).setConcurrency(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt());
        ToolRunner.ToolResult result = invokeNodetool("setconcurrency", "stage", "11", "22");
        result.asserts().failure();
        assertTrue(result.getStdout().contains("Test exception of the illegal set concurrency call"));
    }
}
