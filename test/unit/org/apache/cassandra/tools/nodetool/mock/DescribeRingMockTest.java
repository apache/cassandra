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

import java.io.IOException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.service.StorageServiceMBean;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class DescribeRingMockTest extends AbstractNodetoolMock
{
    @Test
    public void testDescribeRing() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("describering", keyspace()).assertOnCleanExit();
        Mockito.verify(mock).describeRingJMX(keyspace());
    }

    @Test
    public void testDescribeRingWithPort() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getKeyspaces()).thenReturn(List.of(keyspace()));
        invokeNodetool("-pp", "describering", keyspace()).assertOnCleanExit();
        Mockito.verify(mock).describeRingWithPortJMX(keyspace());
    }
}
