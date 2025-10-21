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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class ViewBuildStatusMockTest extends AbstractNodetoolMock
{
    @Test
    public void testViewBuildStatus()
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getViewBuildStatuses("keyspace", "table")).thenReturn(Map.of("127.0.0.1", "SUCCESS"));
        ToolRunner.ToolResult result = invokeNodetool("viewbuildstatus", "keyspace.table");
        result.assertOnCleanExit();
        assertThat(result.getStdout()).contains("keyspace.table has finished building");
    }

    @Test
    public void testViewBuildStatusUnknownArgs()
    {
        ToolRunner.ToolResult result = invokeNodetool("viewbuildstatus", "keyspace", "table", "unknown");
        assertThat(result.getStdout()).contains("nodetool: Unmatched argument at index 7: 'unknown'");
    }

    @Test
    public void testViewBuildStatusIncorrectArgs()
    {
        ToolRunner.ToolResult result = invokeNodetool("viewbuildstatus", "keyspace_table");
        assertThat(result.getStdout()).contains("nodetool: viewbuildstatus requires keyspace and view name arguments");
    }
}
