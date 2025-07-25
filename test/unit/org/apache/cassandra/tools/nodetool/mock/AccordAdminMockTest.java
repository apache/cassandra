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

import org.apache.cassandra.service.accord.AccordOperationsMBean;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

/** Test for the nodetool 'accord admin' command. */
public class AccordAdminMockTest extends AbstractNodetoolMock
{
    @Test
    public void testMarkStale()
    {
        List<String> nodeIds = List.of("node1", "node2");
        AccordOperationsMBean mock = getMock(ACCORD_OPERATIONS_MBEAN);
        invokeNodetool("accord", "mark_stale", "node1", "node2").assertOnCleanExit();
        Mockito.verify(mock).accordMarkStale(nodeIds);
    }
}
