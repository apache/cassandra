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

import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.when;

/** Test for the nodetool 'disableautocompaction' command. */
public class DisableAutoCompactionMockTest extends AbstractNodetoolMock
{
    @Test
    public void testDisableAutoCompactionForAllKeyspaces() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of("keyspace1", "keyspace2", "keyspace3"));
        invokeNodetool("disableautocompaction").assertOnCleanExit();
        Mockito.verify(mock).disableAutoCompaction("keyspace1", EMPTY_STRING_ARRAY);
        Mockito.verify(mock).disableAutoCompaction("keyspace2", EMPTY_STRING_ARRAY);
        Mockito.verify(mock).disableAutoCompaction("keyspace3", EMPTY_STRING_ARRAY);
    }

    @Test
    public void testDisableAutoCompactionForSpecificKeyspaceAndTables() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of("test_keyspace"));
        invokeNodetool("disableautocompaction", "test_keyspace", "table1", "table2", "table3").assertOnCleanExit();
        Mockito.verify(mock).disableAutoCompaction("test_keyspace", "table1", "table2", "table3");
    }

    @Test
    public void testDisableAutoCompactionWithEmptyKeyspaceList() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of());
        invokeNodetool("disableautocompaction").assertOnCleanExit();
        Mockito.verify(mock, Mockito.never()).disableAutoCompaction(Mockito.anyString(), Mockito.any(String[].class));
    }

    @Test
    public void testDisableAutoCompactionForSingleTable() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of("test_keyspace"));
        invokeNodetool("disableautocompaction", "test_keyspace", "single_table").assertOnCleanExit();
        Mockito.verify(mock).disableAutoCompaction("test_keyspace", "single_table");
    }
}
