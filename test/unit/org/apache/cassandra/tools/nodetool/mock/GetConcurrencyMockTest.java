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
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.assertj.core.api.Assertions;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

public class GetConcurrencyMockTest extends AbstractNodetoolMock
{
    @Test
    public void testGetConcurrency()
    {
        Map<String, List<Integer>> concurrency = Map.of("keyspace1", List.of(1, 2, 3),
                                                        "keyspace2", List.of(4, 5, 6));
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getConcurrency(anyList())).thenReturn(concurrency);
        ToolRunner.ToolResult tool = invokeNodetool("getconcurrency");
        tool.assertOnCleanExit();
        Assertions.assertThat(tool.getStdout()).contains("Stage                        CorePoolSize MaximumPoolSize");
        Assertions.assertThat(tool.getStdout()).contains("keyspace1                               1               2");
        Assertions.assertThat(tool.getStdout()).contains("keyspace2                               4               5");
    }
}
