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
package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.Map;

import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface FailureDetectorMBean
{
    public void dumpInterArrivalTimes();

    public void setPhiConvictThreshold(double phi);

    public double getPhiConvictThreshold();

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public String getAllEndpointStates();
    /** @deprecated See CASSANDRA-17934 */
    @Deprecated(since = "5.0") public String getAllEndpointStatesWithResolveIp();
    public String getAllEndpointStatesWithPort();
    public String getAllEndpointStatesWithPortAndResolveIp();

    public String getEndpointState(String address) throws UnknownHostException;

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public Map<String, String> getSimpleStates();
    public Map<String, String> getSimpleStatesWithPort();

    public int getDownEndpointCount();

    public int getUpEndpointCount();

    /** @deprecated See CASSANDRA-7544 */
    @Deprecated(since = "4.0") public TabularData getPhiValues() throws OpenDataException;
    public TabularData getPhiValuesWithPort() throws OpenDataException;
}
