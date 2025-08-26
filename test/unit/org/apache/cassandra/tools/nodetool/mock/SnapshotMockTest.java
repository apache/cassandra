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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.service.snapshot.SnapshotManagerMBean;
import org.apache.cassandra.service.snapshot.SnapshotOptions;

public class SnapshotMockTest extends AbstractNodetoolMock
{
    @Test
    public void testSnapshotWithKtList() throws Exception
    {
        String snapshotName = "snapshotName1";
        Map<String, String> options = new HashMap<>();
        options.put(SnapshotOptions.SKIP_FLUSH, Boolean.toString(false));
        SnapshotManagerMBean mock = getMock(SNAPSHOT_MANAGER_MBEAN);
        // Note: the space in the kt-list is intentional to match the expected format
        invokeNodetool("snapshot", "--kt-list", "keyspace1.table1, keyspace2.table2", "--tag", snapshotName).assertOnCleanExit();
        Mockito.verify(mock).takeSnapshot(snapshotName, options, "keyspace1.table1", "keyspace2.table2");
    }
}
