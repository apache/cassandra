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

public class GarbageCollectMockTest extends AbstractNodetoolMock
{
    @Test
    public void testGarbageCollect() throws Exception
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("garbagecollect").assertOnCleanExit();
        Mockito.verify(mock).garbageCollect("ROW", 1, keyspace(), EMPTY_STRING_ARRAY);
    }

    @Test
    public void testGarbageCollectCells() throws Exception
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("garbagecollect", "-g", "CELL").assertOnCleanExit();
        Mockito.verify(mock).garbageCollect("CELL", 1, keyspace(), EMPTY_STRING_ARRAY);
    }

    @Test
    public void testGarbageCollectKeyspaceWithTables() throws Exception
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("garbagecollect", "--jobs", "2", "-g", "CELL", keyspace(), "tbl1", "tbl2").assertOnCleanExit();
        Mockito.verify(mock).garbageCollect("CELL", 2, keyspace(), "tbl1", "tbl2");
    }
}
