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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class BootstrapResumeMockTest extends AbstractNodetoolMock
{
    @Test
    public void testResumeWithException()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        ToolRunner.ToolResult result = invokeNodetool("bootstrap", "resume");
        result.asserts().failure();
        assertTrue(result.getCleanedStderr().contains("'nodetool bootstrap resume' is disabled."));
    }

    @Test
    public void testResume()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("bootstrap", "resume", "--force").assertOnCleanExit();
        Mockito.verify(mock).resumeBootstrap();
    }
}
