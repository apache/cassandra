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

import org.junit.Test;

import org.apache.cassandra.service.StorageServiceMBean;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class VersionMockTest extends AbstractNodetoolMock
{
    @Test
    public void testVersion() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getReleaseVersion()).thenReturn("4.0.0");
        invokeNodetool("version").assertOnCleanExit();
        Mockito.verify(mock).getReleaseVersion();
    }

    @Test
    public void testVersionVerbose() throws IOException
    {
        StorageServiceMBean mock = getMock(STORAGE_SERVICE_MBEAN);
        when(mock.getReleaseVersion()).thenReturn("4.0.0");
        when(mock.getGitSHA()).thenReturn("abcdef");
        invokeNodetool("version", "-v").assertOnCleanExit();
        Mockito.verify(mock).getReleaseVersion();
        Mockito.verify(mock).getGitSHA();
    }
}
