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
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class CleanupMockTest extends AbstractNodetoolMock
{
    @Test
    public void testCleanupKeyspaceWithTables() throws Throwable
    {
        String table = "tableToCleanup";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("cleanup", "--jobs", "4", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCleanup(4, keyspace(), table);
    }

    @Test
    public void testCleanupAll() throws Throwable
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of("ks1", "ks2"));
        when(mock.getNonLocalStrategyKeyspaces()).thenReturn(List.of("ks1", "ks2"));
        invokeNodetool("cleanup").assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCleanup(2, "ks1", EMPTY_STRING_ARRAY);
        Mockito.verify(mock).forceKeyspaceCleanup(2, "ks2", EMPTY_STRING_ARRAY);
    }
}
