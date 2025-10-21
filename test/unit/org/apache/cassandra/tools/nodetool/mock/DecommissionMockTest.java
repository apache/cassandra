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

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

public class DecommissionMockTest extends AbstractNodetoolMock
{
    @Test
    public void testDecomission() throws Throwable
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.isDecommissioning()).thenReturn(false);
        when(mock.getBootstrapState()).thenReturn("COMPLETED");
        invokeNodetool("decommission", "--force").assertOnCleanExit();
        Mockito.verify(mock).decommission(true);
    }

    @Test
    public void testDecomissionInProgress() throws Throwable
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.isDecommissioning()).thenReturn(true);
        ToolRunner.ToolResult result = invokeNodetool("decommission");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("This node is still decommissioning.");
    }

    @Test
    public void testDecomissionedAlready() throws Throwable
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.isDecommissioning()).thenReturn(false);
        when(mock.getBootstrapState()).thenReturn("DECOMMISSIONED");
        ToolRunner.ToolResult result = invokeNodetool("decommission", "--force");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("Node was already decommissioned.");
        Mockito.verify(mock, Mockito.never()).decommission(anyBoolean());
    }
}
