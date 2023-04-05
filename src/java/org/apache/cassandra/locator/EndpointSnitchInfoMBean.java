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
package org.apache.cassandra.locator;

import java.net.UnknownHostException;

/**
 * MBean exposing standard Snitch info
 */
public interface EndpointSnitchInfoMBean
{
    /**
     * Provides the Rack name depending on the respective snitch used, given the host name/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getRack(String host) throws UnknownHostException;

    /**
     * Provides the Datacenter name depending on the respective snitch used, given the hostname/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getDatacenter(String host) throws UnknownHostException;

    /**
     * Provides the Rack name depending on the respective snitch used for this node
     */
    public String getRack();

    /**
     * Provides the Datacenter name depending on the respective snitch used for this node
     */
    public String getDatacenter();

    /**
     * Provides the snitch name of the cluster
     * @return Snitch name
     */
    public String getSnitchName();
}
