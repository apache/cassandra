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

import org.junit.Test;

import org.apache.cassandra.hints.HintsServiceMBean;
import org.mockito.Mockito;

public class TruncateHintsMockTest extends AbstractNodetoolMock
{
    @Test
    public void testTruncateHints()
    {
        HintsServiceMBean mock = getMock(HINTS_SERVICE_MBEAN);
        invokeNodetool("truncatehints", "127.0.0.1:7199").assertOnCleanExit();
        Mockito.verify(mock).deleteAllHintsForEndpoint("127.0.0.1:7199");
    }

    @Test
    public void testTruncateHintsAll()
    {
        HintsServiceMBean mock = getMock(HINTS_SERVICE_MBEAN);
        invokeNodetool("truncatehints").assertOnCleanExit();
        Mockito.verify(mock).deleteAllHints();
    }
}
