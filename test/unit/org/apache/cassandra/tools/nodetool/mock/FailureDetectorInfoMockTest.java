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

import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.junit.Test;

import org.apache.cassandra.gms.FailureDetectorMBean;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class FailureDetectorInfoMockTest extends AbstractNodetoolMock
{
    @Test
    public void testFailureDetectorInfo() throws Exception
    {
        FailureDetectorMBean mock = getMock(FAILURE_DETECTOR_MBEAN);
        when(mock.getPhiValues()).thenReturn(createPhiValues());
        invokeNodetool("failuredetector").assertOnCleanExit();
        Mockito.verify(mock).getPhiValues();
    }

    @Test
    public void testFailureDetectorInfoWithPort() throws Exception
    {
        FailureDetectorMBean mock = getMock(FAILURE_DETECTOR_MBEAN);
        when(mock.getPhiValuesWithPort()).thenReturn(createPhiValues());
        invokeNodetool("-pp", "failuredetector").assertOnCleanExit();
        Mockito.verify(mock).getPhiValuesWithPort();
    }

    private static TabularData createPhiValues() throws Exception
    {
        return new TabularDataSupport(new TabularType("PhiList",
                                                      "PhiList",
                                                      new CompositeType("Node",
                                                                        "Node",
                                                                        new String[]{ "Endpoint", "PHI" },
                                                                        new String[]{ "IP of the endpoint", "PHI value" },
                                                                        new OpenType[]{ SimpleType.STRING, SimpleType.DOUBLE }),
                                                      new String[]{ "Endpoint" }));
    }
}
