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
import org.mockito.Mockito;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.service.StorageServiceMBean;

import static org.mockito.Mockito.when;

public class CompactMockTest extends AbstractNodetoolMock
{
    @Test
    public void testCompactForceKeyspaceCompactionForPartitionKey() throws Throwable
    {
        long key = 43;
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--partition", Long.toString(key), keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForPartitionKey(keyspace(), Long.toString(key), table);
    }

    @Test
    public void testCompactForceKeyspaceCompactionForTokenRange() throws Throwable
    {
        long key = 34;
        String startToken = Long.toString(key - 1);
        String endToken = Long.toString(key + 1);
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--start-token", startToken, "--end-token", endToken, keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompactionForTokenRange(keyspace(), startToken, endToken, table);
    }

    @Test
    public void testCompactForceKeyspaceCompaction() throws Throwable
    {
        String table = "table";
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        when(mock.getNonSystemKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("compact", "--split-output", keyspace(), table).assertOnCleanExit();
        Mockito.verify(mock).forceKeyspaceCompaction(true, keyspace(), table);
    }

    @Test
    public void testCompactForceUserDefinedCompaction() throws Throwable
    {
        String[] ssTables = new String[] { "ssTable1", "ssTable2" };
        CompactionManagerMBean mock = getMock(COMPACTION_MANAGER_MBEAN);
        invokeNodetool("compact", "--user-defined", ssTables[0], ssTables[1]).assertOnCleanExit();
        Mockito.verify(mock).forceUserDefinedCompaction(String.join(",", ssTables));
    }
}
