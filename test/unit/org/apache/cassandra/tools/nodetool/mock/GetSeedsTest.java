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

import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.tools.ToolRunner;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

public class GetSeedsTest extends AbstractNodetoolMock
{
    @Test
    public void testGetSeeds()
    {
        GossiperMBean mock = getMock(GOSSIPER_MBEAN);
        when(mock.getSeeds()).thenReturn(List.of("seed1", "seed2"));
        ToolRunner.ToolResult result = invokeNodetool("getseeds");
        result.assertOnCleanExit();
        Mockito.verify(mock).getSeeds();
        assertThat(result.getStdout()).contains("Current list of seed node IPs, excluding the current node's IP: seed1 seed2");
    }
}
